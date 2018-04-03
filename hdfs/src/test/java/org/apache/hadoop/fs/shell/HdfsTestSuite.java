package org.apache.hadoop.fs.shell;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import ru.mail.go.corefan.spark.testing.DockerClusterAssistant;
import ru.mail.go.corefan.spark.testing.DockerContainerAssistant;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Properties;

public abstract class HdfsTestSuite {
    public static final String UNIVERSE = "_UNIVERSE_";
    public static final Path TEST_FILE_TXT  = Paths.get(UNIVERSE, "test_file.txt");
    public static final Path TEST_FILE_TXT1 = Paths.get(UNIVERSE, "test_file1.txt");
    public static final Path TEST_FILE_TXT2 = Paths.get(UNIVERSE, "test_file2.txt");
    public static final Path TEST_FILE_TXT3 = Paths.get(UNIVERSE, "test_file3.txt");

    private static DockerContainerAssistant docker;
    private static DockerClusterAssistant cluster;

    DockerContainerAssistant getDocker() {
        return docker;
    }

    DockerClusterAssistant getCluster() {
        return cluster;
    }

    static final int blockLen = 1048576;

    @BeforeClass
    public static void setupSuite() throws Exception {
        docker = new DockerContainerAssistant("reco");

        cluster = new DockerClusterAssistant(new Properties());
    }

    protected static void generateFile(int blocks, int blockLen, Path filename) throws IOException {
        generateFile(blocks, blockLen, filename, 0);
    }

    protected static void generateFile(int blocks, int blockLen, Path filename, int seed) throws IOException {
        try (FileOutputStream outputStream = new FileOutputStream(filename.toFile())) {
            for (int i = 0; i < blocks; i++) {
                byte[] bytes = new byte[blockLen];
                for (int j = 0; j < blockLen; j++) {
                    bytes[j] = (byte) (i + seed);
                }
                outputStream.write(bytes);
            }
        }
    }

    @Before
    public void initCluster() throws Exception {
        cluster.truncateHDFS();

        deleteDir(UNIVERSE);
        Files.createDirectory(Paths.get(UNIVERSE));

        generateFile(1, blockLen, TEST_FILE_TXT1);
        generateFile(2, blockLen, TEST_FILE_TXT2);
        generateFile(3, blockLen, TEST_FILE_TXT3);
    }

    @AfterClass
    public static void cleanupSuite() throws Exception {
        try {
            if (docker != null) {
                docker.close();
            }
        } finally {
            if (cluster != null) {
                cluster.close();
            }
        }
//        deleteDir(UNIVERSE);
    }

    private static void deleteDir(String path) throws IOException {
        Path directory = Paths.get(path);
        if (!Files.exists(directory)) return;
        Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }
}
