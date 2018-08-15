package org.apache.hadoop.fs.shell;

import engineering.clientside.throttle.Throttle;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Command(name = "fhdfs [cluster] get",
        description = "fhdfs get can be used as a replacement of a standard HDFS CLI as it is working in parallel",
        version = {
        "Distributed Get 1.1",
        "",
        "Copyright (C) 2018 Mail.Ru LTD",
        "This is free software; see the source for copying conditions. " +
        "There is NO warranty; not even for MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE."
        })
public class DistributedGet extends CommandWithDestination implements Tool {

    private ExecutorService threadPool;

    private Throttle limitRate = null;

    private List<CompletableFuture> submitted = Collections.synchronizedList(new ArrayList<>());

    private AtomicInteger flushCycle = new AtomicInteger(0);
    private AtomicLong trafficInSecond = new AtomicLong(0);
    private AtomicLong trafficTotal = new AtomicLong(0);

    private AtomicLong bytesToProcess = new AtomicLong(0);
    private AtomicLong bytesProcessed = new AtomicLong(0);

    private AtomicInteger filesToProcess = new AtomicInteger(0);
    private AtomicInteger filesDone = new AtomicInteger(0);
    private AtomicInteger blocksDone = new AtomicInteger(0);

    private AtomicInteger threadsActive = new AtomicInteger(0);

    private Instant startTime;
    private static final int PROGRESS_BAR_WINDOW = 60;

    @Option(names = {"-v", "--verbose"}, description = "Verbose mode. Helpful for troubleshooting. " +
                                                       "Multiple -v options increase the verbosity.")
    private boolean[] verbose = new boolean[0];

    @Option(names = {"-t", "--threads"}, description = "Threads to work with")
    protected int threads = 10;

    @Option(names = {"-w", "--wait"}, description = "Time to wait termination")
    protected int wait = Integer.MAX_VALUE;

    @Option(names = {"-p", "--notPreserveAttrs"}, description = "Not preserve file attributes")
    protected boolean preserveAttrs = true;

    @Option(names = {"-l", "--limit-rate"},
            description = "Limit download speed, measured in bytes per second. It is possible to add suffix 'k' for kilobytes or 'm' for megabytes per second",
            converter = SuffixArgConverter.class)
    protected Long limitRateArg = 0L;

    @Option(names = {"-x", "--sync"}, description = "Sync after every write")
    protected boolean sync = false;

    @Option(names = {"--sync_iteration"}, description = "Sync when buffer will be filled this number of times", hidden = true)
    protected int syncIteration = 16;

    @Option(names = {"-s", "--stat"}, description = "Print statistics")
    protected boolean statistics = false;

    @Option(names = {"--stat_period"}, description = "Statistics period", hidden = true)
    protected int statisticsPeriod = 1000;

    @Option(names = {"--progress_bar_width"}, description = "Statistics period", hidden = true)
    protected int progressBarWidth = 100;

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

    @Option(names = {"-n", "--num"}, description = "Check if file is versioned - get only the latest one")
    protected boolean versioned = false;

    @Option(names = { "-h", "--help" }, usageHelp = true,
            description = "Displays this help message and quits.")
    private boolean helpRequested = false;

    @Option(names = { "-V", "--version" }, versionHelp = true,
            description = "print version information and exit")
    boolean versionRequested = false;

    @Parameters(arity = "1..*", paramLabel = "FILE", description = "File(s) to process.")
    private LinkedList<String> inputFiles = new LinkedList<>();

    public static final String NAME = "distGet";
    public static final String USAGE = "";

    public static final String DESCRIPTION =
            "Copy files that match the file pattern <src> " +
                    "to the local name.  <src> is kept.  When copying multiple " +
                    "files, the destination must be a directory. Passing " +
                    "-p preserves access and modification times, " +
                    "ownership and the mode.\n";

    @Override
    protected void processOptions(LinkedList<String> args)
            throws IOException {
        setRecursive(true);
        getLocalDestination(args);
    }

    @Override
    protected void processNonexistentPath(PathData item) throws IOException {
        Path distributedFile = new Path(item.path.toString() + distributedSuffix);
        FileSystem fs = distributedFile.getFileSystem(getConf());

        if (versioned) {
            Path success = new Path(item.path.getParent(), versionedSuccessFile + item.path.getName());

            log(2, "Success: ", success);
            if (fs.exists(success)) {
                FileStatus[] versions = fs.listStatus(item.path.getParent(), new PrefixFileFilter(item.path.getName()));
                log(2, "Versions: ", versions.length);
                if (versions.length > 0) {
                    FileStatus successStatus = fs.getFileStatus(success);
                    long ts = successStatus.getModificationTime();
                    for (int i = versions.length - 1; i >= 0; i--) {
                        log(2, "Version: ", i, " ", versions[i].getModificationTime(), " V ", ts);
                        if (versions[i].getModificationTime() < ts) {
                            log(2, "Processing ", i, " ", versions[i].getPath());
                            super.processPathArgument(new PathData(versions[i].getPath().toString(), getConf()));
                            return;
                        }
                    }
                }
            }
        } else if (fs.exists(distributedFile)) {
            log(2, "Converting path to distributed ", item.path);
            super.processPathArgument(new PathData(distributedFile.toString(), getConf()));
            return;
        }

        super.processNonexistentPath(item);
    }

    @Override
    protected void recursePath(PathData src) throws IOException {
        if (src.stat.isDirectory() && src.path.toString().endsWith(distributedSuffix)) {
            log(2, "Processing path as distributed ", src.path);
            src.fs.setVerifyChecksum(true);
            submitted.add(getDistributedFile(src.path, dst.toFile()).thenAccept((p) -> {
                if (!preserveAttrs) return;
                Path distPath = new Path(src.path, distributedAttributesFile);
                log(2, "Set timestamp for file ", p, " from distributed ", distPath );
                try {
                    FileStatus attributesStat = src.fs.getFileStatus(distPath);
                    dst.fs.setTimes(
                            p,
                            attributesStat.getModificationTime(), -1); // we do not have access time in HDFS
                    dst.fs.setPermission(
                            p,
                            attributesStat.getPermission());
                } catch (IOException e) {
                    exceptions.add(e);
                    log(0, "Failed to set attributes: ", p);
                    throw new CompletionException(e);
                }
            }));
        } else {
            super.recursePath(src);
        }
    }

    @Override
    protected void copyFileToTarget(PathData src, PathData target) throws IOException {
        src.fs.setVerifyChecksum(true);
        submitted.add(getFile(src.path, target.toFile()).thenAccept((p) -> {
            if (!preserveAttrs) return;
            log(2, "Set timestamp for file ", p, " from ", src.path );
            try {
                dst.fs.setTimes(
                        p,
                        src.stat.getModificationTime(), -1); // we do not have access time in HDFS
                dst.fs.setPermission(
                        p,
                        src.stat.getPermission());
            } catch (IOException e) {
                exceptions.add(e);
                log(0, "Failed to set attributes: ", p);
                throw new CompletionException(e);
            }
        }));
    }

    protected CompletableFuture<Path> getFile(Path remote, File local) throws IOException {
        filesToProcess.incrementAndGet();
        File localFile;
        if (local.toString().endsWith("/") || local.isDirectory()) {
            if (!local.exists()) throw new IOException("Target dir doesn't exist: " + local.toString());
            localFile = unversionFileName(remote, local);
        } else localFile = local;
        log(1, "Copying '", remote.toString(), "' to '", localFile.toString(), "'");

        URI nnURI = FileSystem.getDefaultUri(getConf());

        DistributedFileSystem fs = new DistributedFileSystem();
        fs.initialize(nnURI, getConf());

        String pathName = fs.makeQualified(remote).toUri().getPath();
        int bufferSize = getConf().getInt("io.file.buffer.size", 131072);
        DFSInputStream remoteIS = fs.getClient().open(pathName, bufferSize, true);

        LazyAsyncFileChannel localOut = new LazyAsyncFileChannel(localFile.toPath(),
                StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE);

        if (remoteIS.getFileLength() == 0) { // shortcut for zero-length files
            localOut.get();
            localOut.close();
            return CompletableFuture.completedFuture(new Path(localFile.toURI()));
        }

        bytesToProcess.addAndGet(remoteIS.getFileLength());

        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (LocatedBlock locatedBlock : remoteIS.getAllBlocks()) {
            DistributedBlock block = new DistributedBlock(locatedBlock);
            futures.add(processBlock(fs, pathName, bufferSize, localOut, block));
        }

        remoteIS.close();

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).thenApply((v) -> {
            log(1, "Closing file: ", localFile.toString());
            try {
                localOut.close();
                filesDone.incrementAndGet();
            } catch (IOException e) {
                exceptions.add(e);
                log(0, "Failed to close file ", localFile.toString());
                throw new CompletionException(e);
            }
            return new Path(localFile.toURI());
        });
    }

    private File unversionFileName(Path remote, File local) {
        String remoteVersionedName = remote.getName();
        int versionedPos = remoteVersionedName.indexOf(versionedSuffix);
        if (versionedPos != -1) {
            return new File(local.getParentFile(), remoteVersionedName.substring(0, versionedPos));
        }
        return local;
    }

    private File undistributeFileName(Path remote, File local) throws IOException {
        String remoteDistributedName = remote.getName();
        if (!remoteDistributedName.endsWith(distributedSuffix)) {
            throw new IOException("Remote name does not end with suffix: " + remote.toString());
        }
        String name = remoteDistributedName.substring(0, remoteDistributedName.length() - distributedSuffix.length());
        return new File(local, name);
    }

    protected CompletableFuture<Path> getDistributedFile(Path remote, File local) throws IOException {
        filesToProcess.incrementAndGet();
        File localFile;
        if (local.toString().endsWith("/") || local.isDirectory()) {
            if (!local.exists()) throw new IOException("Target dir doesn't exist: " + local.toString());
            localFile = unversionFileName(remote, undistributeFileName(remote, local));
        } else localFile = local;
        log(1, "Distributed copying '", remote.toString(), "' to '", localFile.toString(), "'");
        URI nnURI = FileSystem.getDefaultUri(getConf());

        DistributedFileSystem fs = new DistributedFileSystem();
        fs.initialize(nnURI, getConf());

        int bufferSize = getConf().getInt("io.file.buffer.size", 131072);

        LazyAsyncFileChannel localOut = new LazyAsyncFileChannel(localFile.toPath(),
                StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE);

        long offset = 0;
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (FileStatus fileStatus : fs.listStatus(remote, new PrefixFileFilter(distributedSubfile))) {
            String pathName = fs.makeQualified(fileStatus.getPath()).toUri().getPath();
            DFSInputStream remoteIS = fs.getClient().open(pathName, bufferSize, true);

            if (remoteIS.getFileLength() == 0) {
                System.err.println("Zero block in distributed file! " + pathName);
                continue;
            }

            bytesToProcess.addAndGet(remoteIS.getFileLength());

            List<LocatedBlock> allBlocks = remoteIS.getAllBlocks();

            if (allBlocks.size() != 1) {
                System.err.println("More than 1 block in distributed file part! " + pathName);
                throw new IOException("More than 1 block in part");
            }

            LocatedBlock blockToCopy = allBlocks.get(0);

            DistributedBlock block = new DistributedBlock(blockToCopy.getBlock().getBlockName(), blockToCopy.getBlockSize(), offset, 0L);
            futures.add(processBlock(fs, pathName, bufferSize, localOut, block));
            offset += blockToCopy.getBlockSize();

            remoteIS.close();
        }

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).thenApply((v) -> {
            log(1, "Closing file: ", localFile.toString());
            try {
                localOut.close();
                filesDone.incrementAndGet();
            } catch (IOException e) {
                exceptions.add(e);
                log(0, "Failed to close file ", localFile.toString());
                throw new CompletionException(e);
            }
            return new Path(localFile.toURI());
        });
    }

    protected CompletableFuture<Void> processBlock(DistributedFileSystem fs, String pathName, int bufferSize,
                                                   LazyAsyncFileChannel localOut,
                                                   DistributedBlock block) {
        return CompletableFuture.runAsync(() -> {
            try {
                threadsActive.incrementAndGet();
                String blockAndFile = "'" + block.getBlockName() + "' block of '" + pathName + "' file";
                Thread.currentThread().setName("Copying thread: copying " + blockAndFile);
                int retries = 3;
                while (retries > 0) {
                    try {
                        log(2, "Copying ", blockAndFile, " retry ", retries);
                        copyBlock(block, fs, pathName, bufferSize, localOut, blockAndFile);
                        Thread.currentThread().setName("Copying thread: waiting");
                        return;
                    } catch (IOException e) {
                        System.err.println("Failed to copy " + blockAndFile + " retries left: " + retries);
                        exceptions.add(e);
                        if (--retries <= 0) {
                            log(0, "Retries for reading done ", blockAndFile);
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
        }, threadPool);
    }

    protected void copyBlock(DistributedBlock block, DistributedFileSystem fs, String fileName, int bufferSize, LazyAsyncFileChannel outFile, String blockAndFile) throws IOException, InterruptedException {
        long len = block.getBlockSize();
        log(2, "Copying block: start ", len);
        if (len == 0) {
            System.err.println(blockAndFile + " is zero size");
            return;
        }
        DFSInputStream file;
        file = fs.getClient().open(fileName, bufferSize, true);
        file.seek(block.getSeekOffset());
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize * 2);
        int cur = 0;
        do {
            buffer.limit((int) Math.min(bufferSize, block.getBlockSize() - cur));
            int readBytes = file.read(buffer);

            if (readBytes == 0) break;

            trafficInSecond.addAndGet(readBytes);
            
            if (limitRate != null)
                limitRate.acquire(readBytes);

            trafficTotal.addAndGet(readBytes);

            cur += readBytes;
            buffer.flip();
            log(3, "Read ", cur, " bytes out of ", block.getBlockSize(), " ", blockAndFile);

            if (cur > block.getBlockSize()) break;

            try {
                log(3, "Writing: ", buffer.remaining(), " pos: ", (block.getStartOffset() + cur - readBytes), " out: ", outFile.get().isOpen());
                Integer written = outFile.get().write(buffer, block.getStartOffset() + cur - readBytes).get();
                bytesProcessed.addAndGet(written);
                log(3, "Written: ", written);
                if (sync && needToFlush()) {
                    outFile.get().force(false);
                    log(3, "Synced");
                }
            } catch (InterruptedException | ExecutionException e) {
                throw new IOException("Failed to write block data", e);
            }
            buffer.clear();

        } while (cur < block.getBlockSize());

        log(2, "Copying block: finish ", len, " ", blockAndFile);
        file.close();
        blocksDone.incrementAndGet();
    }

    private boolean needToFlush() {
        return flushCycle.updateAndGet((x) -> {if (x == syncIteration) return 0; return x+1;}) == 0;
    }

    private static String progressBar(int percent, int width) {
        int filled = (int)(width * percent / 100.0);

        char[] bar = new char[width + 2];
        for (int i = filled + 1; i <= width; i++)
            bar[i] = ' ';

        for (int i = 1; i <= filled; i++)
            bar[i] = '=';

        bar[0] = '[';
        bar[width + 1] = ']';

        return String.valueOf(bar) + ' ' + percent + '%';
    }

    private static String formatDuration(long estimate, Long avgTrafficInWindow, ChronoUnit chronoUnit) {
        if (avgTrafficInWindow == null || avgTrafficInWindow == 0L) return "--:-";
        Duration duration = Duration.of(estimate / avgTrafficInWindow, chronoUnit);
        return DurationFormatUtils.formatDurationWords(duration.toMillis(), true, true);
    }

    private Runnable statisticsRunnable = () -> {
        try {
            int prevLen = 0;
            EvictingQueue<Long> trafficWindow = EvictingQueue.create(PROGRESS_BAR_WINDOW);
            while (!Thread.currentThread().isInterrupted()) {
                long traffic = trafficInSecond.getAndSet(0L);
                trafficWindow.add(traffic);
                Long avgTrafficInWindow = trafficWindow.stream().reduce(0L, (a, b) -> a + b) / trafficWindow.size();

                long bTP = bytesToProcess.get();
                long bP = bytesProcessed.get();

                String message = String.format("\r\t%s\tETA: %s Speed: %s/s Files: %d/%d Blocks: %d Threads: %d ",
                        progressBar((int) (bP * 100.0 / bTP), progressBarWidth),
                        formatDuration(bTP - bP, avgTrafficInWindow, ChronoUnit.SECONDS),
                        FileUtils.byteCountToDisplaySize(traffic),
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

            processGet();

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
            System.err.println(String.format("\nTime spent: %s ; %s downloaded; Avg speed: %s/s",
                    DurationFormatUtils.formatDurationWords(time.toMillis(), true, true),
                    FileUtils.byteCountToDisplaySize(trafficTotal.get()),
                    FileUtils.byteCountToDisplaySize(trafficTotal.get() / Math.max(time.getSeconds(), 1))
                    ));
        }

        return (numErrors == 0) ? exitCode : exitCodeForError();
    }

    private void processGet() throws IOException {
        threadPool = Executors.newFixedThreadPool(threads);

        if (limitRateArg > 0) {
            limitRate = Throttle.create(limitRateArg);
            log(1, "Limiting stream with ", limitRateArg);
        }

        try {
            processOptions(inputFiles);
            processRawArguments(inputFiles);
        } finally {
            try {
                log(1, "Finished all submitting ", submitted.size());
                CountDownLatch latch = new CountDownLatch(1);
                CompletableFuture.allOf(submitted.toArray(new CompletableFuture[0])).exceptionally((t) -> null).thenRunAsync(() -> {
                    try {
                        log(1, "Terminating pool");
                        threadPool.shutdown();
                    } finally {
                        latch.countDown();
                    }
                });
                latch.await(wait, TimeUnit.SECONDS);

                log(1, "Latch done");

                if (!threadPool.awaitTermination(wait, TimeUnit.SECONDS)) {
                    System.err.println("Thread pool failed to terminate in " + wait + " seconds");
                }
            } catch (InterruptedException | CompletionException e) {
                threadPool.shutdownNow();

                log(1, "Interrupted exception: ", e.getMessage());

                Thread.currentThread().interrupt();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new DistributedGet(), args);
        System.exit(res);
    }

    private void log(int verbosity, Object... args) {
        if (verbose.length > verbosity) {
            StringBuilder builder = new StringBuilder();
            for (Object log : args) {
                builder.append(log);
            }
            System.err.println(builder.toString());
        }
    }

    private static class DistributedBlock {
        private final String blockName;
        private final long blockSize;
        private final long startOffset;
        private final long seekOffset;

        DistributedBlock(String blockName, long blockSize, long startOffset, long seekOffset) {
            this.blockName = blockName;
            this.blockSize = blockSize;
            this.startOffset = startOffset;
            this.seekOffset = seekOffset;
        }

        DistributedBlock(LocatedBlock block) {
            this.blockName = block.getBlock().getBlockName();
            this.blockSize = block.getBlockSize();
            this.startOffset = block.getStartOffset();
            this.seekOffset = block.getStartOffset();
        }

        public String getBlockName() {
            return blockName;
        }

        public long getBlockSize() {
            return blockSize;
        }

        public long getStartOffset() {
            return startOffset;
        }

        public long getSeekOffset() {
            return seekOffset;
        }
    }
}
