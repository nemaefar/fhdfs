package org.apache.hadoop.fs.shell;

import engineering.clientside.throttle.Throttle;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Command(name = "fhdfs [cluster] put",
        description = "fhdfs put can be used as a replacement of a standard HDFS CLI as it is working in parallel",
        version = {
        "Distributed Put 1.1",
        "",
        "Copyright (C) 2018 Mail.Ru LTD",
        "This is free software; see the source for copying conditions. " +
        "There is NO warranty; not even for MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE."
})
public class DistributedPut extends CommandWithDestination implements Tool {

    private static final String DISTRIBUTED_VERSION_PATTERN = "%05d";

    private ExecutorService threadPool;

    private Throttle limitRate = null;

    private List<CompletableFuture> submitted = Collections.synchronizedList(new ArrayList<>());

    private AtomicLong trafficInSecond = new AtomicLong(0);
    private AtomicLong trafficTotal = new AtomicLong(0);
    private AtomicInteger filesToProcess = new AtomicInteger(0);
    private AtomicInteger filesDone = new AtomicInteger(0);
    private AtomicInteger blocksDone = new AtomicInteger(0);

    private AtomicInteger threadsActive = new AtomicInteger(0);

    private Instant startTime;

    @Option(names = {"-v", "--verbose"}, description = "Verbose mode. Helpful for troubleshooting. " +
            "Multiple -v options increase the verbosity.")
    protected boolean[] verbose = new boolean[0];

    @Option(names = {"-t", "--threads"}, description = "Threads to work with")
    protected int threads = 10;

    @Option(names = {"-w", "--wait"}, description = "Time to wait termination")
    protected int wait = Integer.MAX_VALUE;

    @Option(names = {"-p", "--notPreserveAttrs"}, description = "Not preserve file attributes")
    protected boolean preserveAttrs = true;

    @Option(names = {"-f", "--overwrite"}, description = "Overwrite the destination if it already exists")
    protected boolean overwrite = false;

    @Option(names = {"-l", "--limit-rate"},
            description = "Limit upload speed, measured in bytes per second. It is possible to add suffix 'k' for kilobytes or 'm' for megabytes per second",
            converter = SuffixArgConverter.class)
    protected Long limitRateArg = 0L;

    @Option(names = {"-s", "--stat"}, description = "Print statistics")
    protected boolean statistics = false;

    @Option(names = {"--stat_period"}, description = "Statistics period", hidden = true)
    protected int statisticsPeriod = 1000;

    @Option(names = {"--distributed_suffix"}, description = "Suffix for file to be detected by distributed get and put", hidden = true)
    protected String distributedSuffix = ".distributed";

    @Option(names = {"--distributed_subfile"}, description = "Name for sub-file to be detected by distributed get and put", hidden = true)
    protected String distributedSubfile = "part-";

    @Option(names = {"--distributed_attributes"}, description = "File to keep attributes of the original one, if preserving them", hidden = true)
    protected String distributedAttributesFile = "_ATTRIBUTES";

    @Option(names = {"--versioned_suffix"}, description = "Suffix for file to be detected by versioned get and put", hidden = true)
    protected String versionedSuffix = ".ver.";

    @Option(names = {"--versioned_success"}, description = "File to mark version as ready to get", hidden = true)
    protected String versionedSuccessFile = "_SUCCESS_";

    @Option(names = {"-n", "--num"}, description = "Number of versions of the same file/dir to keep (if 1 or less will act as not set)")
    protected int numVersions = 1;

    @Option(names = { "-h", "--help" }, usageHelp = true,
            description = "Displays this help message and quits.")
    protected boolean helpRequested = false;

    @Option(names = { "-V", "--version" }, versionHelp = true,
            description = "print version information and exit")
    protected boolean versionRequested = false;

    @Parameters(arity = "1..*", paramLabel = "FILE", description = "File(s) to process.")
    protected LinkedList<String> inputFiles = new LinkedList<>();

    protected Runnable finalizer = null;

    public static final String NAME = "distPut";
    public static final String USAGE = "";
    public static final String DESCRIPTION = "";

    @Override
    protected void processOptions(LinkedList<String> args)
            throws IOException {
        if (numVersions > 1) {
            if (args.size() > 2) {
                throw new IOException("Can't make more than one item versioned at a time");
            } else if (args.size() < 2) {
                throw new IOException("Can't make item versioned without certain destination");
            }
            String src = args.peekFirst();
            String name = new Path(src).getName();

            String dst = args.pollLast();
            if (!dst.endsWith(name))
                if (dst.endsWith("/")) dst = dst + name;
                else dst = dst + "/" + name;

            Path dstPath = new Path(dst);
            FileSystem fs = dstPath.getFileSystem(getConf());
            Path resultPath = eraseOldVersioned(fs, dstPath);

            finalizer = () -> {
                Path success = new Path(dstPath.getParent(), versionedSuccessFile + dstPath.getName());
                long timestamp = System.currentTimeMillis();
                if (verbose.length > 2)
                    System.err.println("Set timestamp for file " + success + " : " + timestamp );
                try {
                    if (fs.exists(success)) {
                        fs.setTimes(success, timestamp, -1);
                    } else {
                        fs.create(success).close();
                    }
                } catch (IOException e) {
                    if (verbose.length > 0)
                        System.err.println("Failed to create " + success);
                    if (verbose.length > 2)
                        e.printStackTrace();
                    throw new CompletionException(e);
                }
            };
            args.addLast(resultPath.toString());
        }
        getRemoteDestination(args);
        setRecursive(true);
    }

    protected Path eraseOldVersioned(FileSystem fs, Path to) throws IOException {
        FileStatus[] oldVersions = fs.listStatus(to.getParent(), new PrefixFileFilter(to.getName()));
        if (verbose.length > 1)
            System.err.println("Old versions: " + oldVersions.length);

        Path target = fs.makeQualified(new Path(to.toString() + versionedSuffix + Long.toString(System.currentTimeMillis())));

        for (int i = 0; i < (oldVersions.length - numVersions + 1); i++) {
            if (verbose.length > 2)
                System.err.println("Deleting old: " + oldVersions[i].getPath().toString());
            fs.delete(oldVersions[i].getPath(), true);
        }

        return target;
    }

    // commands operating on local paths have no need for glob expansion
    @Override
    protected List<PathData> expandArgument(String arg) throws IOException {
        List<PathData> items = new LinkedList<>();
        items.add(new PathData("file://" + new File(arg).getAbsolutePath(), getConf()));
        return items;
    }

    @Override
    protected void copyFileToTarget(PathData src, PathData target) throws IOException {
        src.fs.setVerifyChecksum(true);
        submitted.add(putFile(src.toFile(), target.path).thenAccept((p) -> {
            if (!preserveAttrs) return;
            if (verbose.length > 2)
                System.err.println("Set timestamp for file " + p + " from " + src.path );
            try {
                target.fs.setTimes(
                        p,
                        src.stat.getModificationTime(), -1); // we do not have access time in HDFS
                target.fs.setPermission(
                        p,
                        src.stat.getPermission());
            } catch (IOException e) {
                if (verbose.length > 0)
                    System.err.println("Failed to set attributes for " + p);
                if (verbose.length > 2)
                    e.printStackTrace();
                throw new CompletionException(e);
            }
        }));
    }

    private CompletableFuture<Path> putFile(File from, Path to) throws IOException {
        if (verbose.length > 0)
            System.err.println("File: " + from);
        filesToProcess.incrementAndGet();
        URI nnURI = FileSystem.getDefaultUri(getConf());

        DistributedFileSystem fs = new DistributedFileSystem();
        fs.initialize(nnURI, getConf());

        Path target = fs.makeQualified(to);

        long blockSize = getConf().getLong("dfs.blocksize", 128*1024*1024L);
        int bufferSize = getConf().getInt("io.file.buffer.size", 131072);

        if (from.length() <= blockSize) {
            if (verbose.length > 1)
                System.err.println("Short-cut for one-block file: " + from.getName());
            return CompletableFuture.supplyAsync(() -> {
                Thread.currentThread().setName("Copying thread: copying file '" + from.getName() + "'");

                try {
                    threadsActive.incrementAndGet();
                    InputStream inputStream = new FileInputStream(from);
                    OutputStream outputStream = fs.getClient().create(target.toUri().getPath(), overwrite);

                    byte buf[] = new byte[bufferSize];
                    int bytesRead = inputStream.read(buf);
                    while (bytesRead >= 0) {
                        trafficInSecond.addAndGet(bytesRead);
                        if (limitRate != null)
                            limitRate.acquire(bytesRead);
                        trafficTotal.addAndGet(bytesRead);
                        outputStream.write(buf, 0, bytesRead);
                        bytesRead = inputStream.read(buf);
                    }

                    inputStream.close();
                    outputStream.close();
                    filesDone.incrementAndGet();
                    blocksDone.incrementAndGet();
                } catch (IOException e) {
                    if (verbose.length > 0)
                        System.err.println("Failed to copy file '" + from.getName() + "'");
                    if (verbose.length > 2)
                        e.printStackTrace();
                    throw new CompletionException(e);
                } catch (InterruptedException e) {
                    System.err.println("Failed to copy '" + from.getName() + "' not retrying");
                    throw new CompletionException(e);
                } finally {
                    threadsActive.decrementAndGet();
                }
                return target;
            }, threadPool);
        } else {
            long blocks = from.length() / blockSize;
            if (from.length() % blockSize != 0) blocks++; // protection from files, splittable for exact number of blocks
            if (verbose.length > 1)
                System.err.println("Will be copying " + blocks + " blocks for '" + from.getName() + "'");
            AsynchronousFileChannel localIn = AsynchronousFileChannel.open(from.toPath(), StandardOpenOption.READ);
            Path parent = new Path(target.toString() + distributedSuffix);

            if (fs.exists(parent)) {
                if (verbose.length > 1)
                    System.err.println("Target distributed dir exists: " + parent);
                if (overwrite)
                    fs.delete(parent, true);
                else
                    throw new FileAlreadyExistsException(parent.toString());
            }

            fs.mkdirs(parent);
            List<CompletableFuture<Void>> futures = new ArrayList<>();

            for (int i = 0; i < blocks; i++) {
                Path output = new Path(parent, String.format(distributedSubfile + DISTRIBUTED_VERSION_PATTERN, i));

                long startOffset = i * blockSize;

                futures.add(CompletableFuture.runAsync(() -> {
                    try {
                        threadsActive.incrementAndGet();
                        String blockAndFile = "'" + startOffset + "' block of '" + output + "' file";
                        Thread.currentThread().setName("Copying thread: copying " + blockAndFile);
                        int retries = 3;
                        while (retries > 0) {
                            try {
                                if (verbose.length > 1)
                                    System.err.println("Copying " + blockAndFile + " retry " + retries);
                                putBlock(localIn, fs, output, startOffset, Math.min(blockSize, from.length() - startOffset), bufferSize, blockAndFile);
                                Thread.currentThread().setName("Copying thread: waiting");
                                return;
                            } catch (IOException e) {
                                System.err.println("Failed to copy " + blockAndFile + " retries left: " + retries);
                                e.printStackTrace();
                                if (--retries <= 0) {
                                    if (verbose.length > 0)
                                        System.err.println("Retries for writing done '" + blockAndFile + "'");
                                    if (verbose.length > 2)
                                        e.printStackTrace();
                                    throw new CompletionException(e);
                                }
                            } catch (InterruptedException e) {
                                System.err.println("Failed to copy " + blockAndFile + " not retrying");
                                throw new CompletionException(e);
                            }
                        }
                    } finally {
                        threadsActive.decrementAndGet();
                    }
                }, threadPool));
            }

            return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).thenApply((v) -> {
                if (verbose.length > 1)
                    System.err.println("Closing file: " + to);
                Path attributesFile = new Path(parent, distributedAttributesFile);
                try {
                    localIn.close();
                    filesDone.incrementAndGet();
                } catch (IOException e) {
                    if (verbose.length > 0)
                        System.err.println("Failed to close input file " + from);
                    if (verbose.length > 2)
                        e.printStackTrace();
                    throw new CompletionException(e);
                }
                if (preserveAttrs) {
                    try {
                        fs.create(attributesFile).close();
                    } catch (IOException e) {
                        if (verbose.length > 0)
                            System.err.println("Failed to create attributes file " + attributesFile);
                        if (verbose.length > 2)
                            e.printStackTrace();
                        throw new CompletionException(e);
                    }
                }
                return attributesFile;
            });
        }
    }

    private void putBlock(AsynchronousFileChannel localIn, DistributedFileSystem fs, Path output, long startOffset, long blockSize, int bufferSize, String blockAndFile) throws IOException, InterruptedException {
        if (verbose.length > 2)
            System.err.println("START Copying block: " + blockAndFile + " with length " + blockSize);
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize * 2);
        String pathName = fs.makeQualified(output).toUri().getPath();
        OutputStream outputStream = fs.getClient().create(pathName, overwrite);
        int cur = 0;
        WritableByteChannel writableByteChannel = Channels.newChannel(outputStream);
        do {

            buffer.limit((int) Math.min(bufferSize, blockSize - cur));
            Integer readBytes;
            try {
                readBytes = localIn.read(buffer, startOffset + cur).get();
            } catch (InterruptedException | ExecutionException e) {
                throw new IOException("Failed to read block data", e);
            }

            if (readBytes == 0) break;

            trafficInSecond.addAndGet(readBytes);

            if (limitRate != null)
                limitRate.acquire(readBytes);
            trafficTotal.addAndGet(readBytes);

            cur += readBytes;
            buffer.flip();
            if (verbose.length > 3)
                System.err.println("Read " + cur + " bytes out of " + blockSize + " " + blockAndFile);

            if (cur > blockSize) break;

            writableByteChannel.write(buffer);

            buffer.clear();

        } while (cur < blockSize);

        writableByteChannel.close();
        outputStream.close();
        blocksDone.incrementAndGet();
        if (verbose.length > 1)
            System.err.println("END Copying block: " + blockAndFile);
    }

    private Runnable statisticsRunnable = () -> {
        try {
            int prevLen = 0;
            while (!Thread.currentThread().isInterrupted()) {
                String message = String.format("\rSpeed: %s/s Files: %d/%d Blocks: %d Threads: %d ",
                        FileUtils.byteCountToDisplaySize(trafficInSecond.getAndSet(0L)),
                        filesDone.get(), filesToProcess.get(), blocksDone.get(), threadsActive.get()
                );
                String paddedMessage = StringUtils.rightPad(message, prevLen);
                prevLen = message.length();
                System.err.print(paddedMessage);

                long sleep = statisticsPeriod - (System.currentTimeMillis() % statisticsPeriod);

                Thread.sleep(sleep);
            }
        } catch (InterruptedException ignored) {
        }
    };


    @Override
    public int run(String[] argv) {
        try {
            if (isDeprecated()) {
                displayWarning(
                        "DEPRECATED: Please use '"+ getReplacementCommand() + "' instead.");
            }
            CommandLine commandLine = new CommandLine(this);
            commandLine.setOverwrittenOptionsAllowed(true);
            try {
                commandLine.parse(argv);
            } catch (CommandLine.MissingParameterException e) {
                System.err.println(e.getMessage());
                commandLine.usage(System.err);
                return 1;
            }

            if (commandLine.isUsageHelpRequested()) {
                commandLine.usage(System.err);
                return 0;
            } else if (commandLine.isVersionHelpRequested()) {
                commandLine.printVersionHelp(System.err);
                return 0;
            }

            Thread statisticsThread = null;
            if (statistics) {
                statisticsThread = new Thread(statisticsRunnable);
                statisticsThread.start();
            }

            startTime = Instant.now();

            processPut();

            if (statistics)
                if (statisticsThread != null)
                    statisticsThread.interrupt();

        } catch (IOException e) {
            displayError(e);
        }

        if (verbose.length > 2)
            for (Exception exception : exceptions) {
                exception.printStackTrace();
            }


        if (statistics) {
            Duration time = Duration.between(startTime, Instant.now());
            System.err.println(String.format("\nTime spent: %s ; %s uploaded; Avg speed: %s/s",
                    DurationFormatUtils.formatDurationWords(time.toMillis(), true, true),
                    FileUtils.byteCountToDisplaySize(trafficTotal.get()),
                    FileUtils.byteCountToDisplaySize(trafficTotal.get() / Math.max(time.getSeconds(), 1))
            ));
        }

        return (numErrors == 0) ? exitCode : exitCodeForError();
    }

    private void processPut() throws IOException {
        threadPool = Executors.newFixedThreadPool(threads);
        if (limitRateArg > 0) {
            limitRate = Throttle.create(limitRateArg);
            if (verbose.length > 1)
                System.err.println("Limiting stream with " + limitRateArg);
        }

        if (verbose.length > 2)
            System.err.println("Input files: " + inputFiles.stream().collect(Collectors.joining(" ; ")));

        try {
            processOptions(inputFiles);
            processRawArguments(inputFiles);
        } finally {
            try {
                if (verbose.length > 1)
                    System.err.println("Finished all submitting " + submitted.size());
                CountDownLatch latch = new CountDownLatch(1);
                CompletableFuture.allOf(submitted.toArray(new CompletableFuture[submitted.size()])).thenRunAsync(() -> {
                    try {
                        if (finalizer != null) {
                            if (verbose.length > 1)
                                System.err.println("Running finalizer");
                            finalizer.run();
                        }
                        if (verbose.length > 1)
                            System.err.println("Terminating pool");
                        threadPool.shutdown();
                    } finally {
                        latch.countDown();
                    }
                });
                latch.await(wait, TimeUnit.SECONDS);

                if (verbose.length > 1)
                    System.err.println("Latch done");

                if (!threadPool.awaitTermination(wait, TimeUnit.SECONDS)) {
                    System.err.println("Thread pool failed to terminate in " + wait + " seconds");
                }
            } catch (InterruptedException | CompletionException e) {
                threadPool.shutdownNow();

                if (verbose.length > 1)
                    System.err.println("Interrupted exception: " + e.getMessage());

                Thread.currentThread().interrupt();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new DistributedPut(), args);
        System.exit(res);
    }
}
