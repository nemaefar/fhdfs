package org.apache.hadoop.fs.shell;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class DistributedPutTest extends HdfsTestSuite {

    @Test
    public void simpleTest() throws Exception {
        DistributedPut distributedPut = new DistributedPut();
        distributedPut.setConf(getCluster().config.configuration);

        assertEquals(0, distributedPut.run(new String[]{"-v", "./" + TEST_FILE_TXT2.toString(), "/" + TEST_FILE_TXT3.getFileName()}));

        FileStatus[] fileStatuses = getCluster().getFS().listStatus(new Path("/"));
        assertEquals(2, fileStatuses.length);

        assertEquals(2 * blockLen, fileStatuses[0].getLen());

        getCluster().copyFromHDFS("/" + TEST_FILE_TXT3.getFileName(), TEST_FILE_TXT.toString());
        assertArrayEquals(Files.readAllBytes(TEST_FILE_TXT2), Files.readAllBytes(TEST_FILE_TXT));
    }

    @Test
    public void versionedDelete() throws Exception {
        FileStatus[] fileStatuses = getCluster().getFS().listStatus(new Path("/"));
        assertEquals(1, fileStatuses.length);

        DistributedPut distributedPut = new DistributedPut();
        distributedPut.setConf(getCluster().config.configuration);
        assertEquals(0, distributedPut.run(new String[]{"-v", "-n", "2", "./" + TEST_FILE_TXT1.toString(), "/" + TEST_FILE_TXT1.getFileName()}));
        fileStatuses = getCluster().getFS().listStatus(new Path("/"));
        assertEquals(3, fileStatuses.length);

        distributedPut = new DistributedPut();
        distributedPut.setConf(getCluster().config.configuration);
        assertEquals(0, distributedPut.run(new String[]{"-v", "-n", "2", "./" + TEST_FILE_TXT1.toString(), "/" + TEST_FILE_TXT1.getFileName()}));
        fileStatuses = getCluster().getFS().listStatus(new Path("/"));
        assertEquals(4, fileStatuses.length);

        distributedPut = new DistributedPut();
        distributedPut.setConf(getCluster().config.configuration);
        assertEquals(0, distributedPut.run(new String[]{"-v", "-n", "2", "./" + TEST_FILE_TXT1.toString(), "/" + TEST_FILE_TXT1.getFileName()}));
        fileStatuses = getCluster().getFS().listStatus(new Path("/"));
        assertEquals(4, fileStatuses.length);
    }

    @Test
    public void checkDir() throws Exception {
        DistributedPut distributedPut = new DistributedPut();
        distributedPut.setConf(getCluster().config.configuration);

        Files.deleteIfExists(Paths.get("dir"));
        Files.createDirectory(Paths.get("dir"));

        assertEquals(0, distributedPut.run(new String[]{"-v", "dir", "/"}));

        FileStatus[] fileStatuses = getCluster().getFS().listStatus(new Path("/"));

        assertEquals(2, fileStatuses.length);
    }

    @Test
    public void checkDistributed() throws Exception {
        Configuration configuration = getCluster().config.configuration;
        configuration.setInt("dfs.blocksize", blockLen);

        DistributedPut distributedPut = new DistributedPut();
        distributedPut.setConf(configuration);
        assertEquals(0, distributedPut.run(new String[]{"-v", "./" + TEST_FILE_TXT2.toString(), "/" + TEST_FILE_TXT2.getFileName()}));

        FileStatus[] fileStatuses = getCluster().getFS().listStatus(new Path("/"));

        assertEquals(2, fileStatuses.length);

        fileStatuses = getCluster().getFS().listStatus(fileStatuses[0].getPath());

        assertEquals(3, fileStatuses.length);

        assertEquals("_ATTRIBUTES", fileStatuses[0].getPath().getName());
        assertEquals("rw-rw-r--", fileStatuses[0].getPermission().toString());
        assertEquals(0, fileStatuses[0].getLen());

        assertEquals("part-00000", fileStatuses[1].getPath().getName());
        assertEquals("rw-r--r--", fileStatuses[1].getPermission().toString());
        assertEquals(blockLen, fileStatuses[1].getLen());

        assertEquals("part-00001", fileStatuses[2].getPath().getName());
        assertEquals("rw-r--r--", fileStatuses[2].getPermission().toString());
        assertEquals(blockLen, fileStatuses[2].getLen());
    }

    @Test
    public void checkDistributedVersioned() throws Exception {
        Configuration configuration = getCluster().config.configuration;
        configuration.setInt("dfs.blocksize", blockLen);

        DistributedPut distributedPut = new DistributedPut();
        distributedPut.setConf(configuration);
        assertEquals(0, distributedPut.run(new String[]{"-v", "-n", "3", "./" + TEST_FILE_TXT2.toString(), "/" + TEST_FILE_TXT2.getFileName()}));

        FileStatus[] fileStatuses = getCluster().getFS().listStatus(new Path("/"));

        assertEquals(3, fileStatuses.length);

        assertEquals("_SUCCESS_test_file2.txt", fileStatuses[0].getPath().getName());
        assertEquals("rw-r--r--", fileStatuses[0].getPermission().toString());
        assertEquals(0, fileStatuses[0].getLen());

        fileStatuses = getCluster().getFS().listStatus(fileStatuses[1].getPath());

        assertEquals(3, fileStatuses.length);

        assertEquals("_ATTRIBUTES", fileStatuses[0].getPath().getName());
        assertEquals("rw-rw-r--", fileStatuses[0].getPermission().toString());
        assertEquals(0, fileStatuses[0].getLen());

        assertEquals("part-00000", fileStatuses[1].getPath().getName());
        assertEquals("rw-r--r--", fileStatuses[1].getPermission().toString());
        assertEquals(blockLen, fileStatuses[1].getLen());

        assertEquals("part-00001", fileStatuses[2].getPath().getName());
        assertEquals("rw-r--r--", fileStatuses[2].getPermission().toString());
        assertEquals(blockLen, fileStatuses[2].getLen());
    }

    @Test
    public void checkDoubleDistributed() throws Exception {
        Configuration configuration = getCluster().config.configuration;
        configuration.setInt("dfs.blocksize", blockLen);

        DistributedPut distributedPut = new DistributedPut();
        distributedPut.setConf(configuration);
        assertEquals(0, distributedPut.run(new String[]{"-v", "./" + TEST_FILE_TXT3.toString(), "/" + TEST_FILE_TXT2.getFileName()}));

        FileStatus[] rootStatuses = getCluster().getFS().listStatus(new Path("/"));
        assertEquals(2, rootStatuses.length);

        FileStatus[] fileStatuses = getCluster().getFS().listStatus(rootStatuses[0].getPath());
        assertEquals(4, fileStatuses.length);

        distributedPut = new DistributedPut();
        distributedPut.setConf(configuration);
        assertEquals(0, distributedPut.run(new String[]{"-v", "-f", "./" + TEST_FILE_TXT2.toString(), "/" + TEST_FILE_TXT2.getFileName()}));

        fileStatuses = getCluster().getFS().listStatus(rootStatuses[0].getPath());

        assertEquals(3, fileStatuses.length);

        assertEquals("_ATTRIBUTES", fileStatuses[0].getPath().getName());
        assertEquals("rw-rw-r--", fileStatuses[0].getPermission().toString());
        assertEquals(0, fileStatuses[0].getLen());

        assertEquals("part-00000", fileStatuses[1].getPath().getName());
        assertEquals("rw-r--r--", fileStatuses[1].getPermission().toString());
        assertEquals(blockLen, fileStatuses[1].getLen());

        assertEquals("part-00001", fileStatuses[2].getPath().getName());
        assertEquals("rw-r--r--", fileStatuses[2].getPermission().toString());
        assertEquals(blockLen, fileStatuses[2].getLen());
    }

    @Test
    public void checkNotForcedDistributed() throws Exception {
        Configuration configuration = getCluster().config.configuration;
        configuration.setInt("dfs.blocksize", blockLen);

        DistributedPut distributedPut = new DistributedPut();
        distributedPut.setConf(configuration);
        assertEquals(0, distributedPut.run(new String[]{"-v", "./" + TEST_FILE_TXT3.toString(), "/" + TEST_FILE_TXT2.getFileName()}));

        distributedPut = new DistributedPut();
        distributedPut.setConf(configuration);
        assertEquals(1, distributedPut.run(new String[]{"-v", "./" + TEST_FILE_TXT2.toString(), "/" + TEST_FILE_TXT2.getFileName()}));

        FileStatus[] fileStatuses = getCluster().getFS().listStatus(new Path("/"));
        assertEquals(2, fileStatuses.length);

        fileStatuses = getCluster().getFS().listStatus(fileStatuses[0].getPath());
        assertEquals(4, fileStatuses.length);
    }


    @Test
    public void complexUpload() throws Exception {
        Configuration configuration = getCluster().config.configuration;
        configuration.setInt("dfs.blocksize", blockLen);

        DistributedPut distributedPut = new DistributedPut();
        distributedPut.setConf(configuration);
        assertEquals(0, distributedPut.run(new String[]{"-v", "-n", "3", "./" + UNIVERSE, "/"}));

        FileStatus[] rootStatuses = getCluster().getFS().listStatus(new Path("/"));

        assertEquals(3, rootStatuses.length);

        assertEquals("_SUCCESS__UNIVERSE_", rootStatuses[0].getPath().getName());
        assertEquals("rw-r--r--", rootStatuses[0].getPermission().toString());
        assertEquals(0, rootStatuses[0].getLen());

        // listing _UNIVERSE_.ver.* dir
        FileStatus[] dirStatuses = getCluster().getFS().listStatus(rootStatuses[1].getPath());

        assertEquals(3, dirStatuses.length);

        // listing test_file1.txt
        assertEquals(TEST_FILE_TXT1.getFileName().toString(), dirStatuses[0].getPath().getName());
        assertEquals("rw-rw-r--", dirStatuses[0].getPermission().toString());
        assertEquals(blockLen, dirStatuses[0].getLen());

        // listing test_file2.txt
        FileStatus[] fileStatuses = getCluster().getFS().listStatus(dirStatuses[1].getPath());
        assertEquals(3, fileStatuses.length);

        assertEquals("_ATTRIBUTES", fileStatuses[0].getPath().getName());
        assertEquals("rw-rw-r--", fileStatuses[0].getPermission().toString());
        assertEquals(0, fileStatuses[0].getLen());

        assertEquals("part-00000", fileStatuses[1].getPath().getName());
        assertEquals("rw-r--r--", fileStatuses[1].getPermission().toString());
        assertEquals(blockLen, fileStatuses[1].getLen());

        assertEquals("part-00001", fileStatuses[2].getPath().getName());
        assertEquals("rw-r--r--", fileStatuses[2].getPermission().toString());
        assertEquals(blockLen, fileStatuses[2].getLen());

        // listing test_file3.txt
        fileStatuses = getCluster().getFS().listStatus(dirStatuses[2].getPath());
        assertEquals(4, fileStatuses.length);

        assertEquals("_ATTRIBUTES", fileStatuses[0].getPath().getName());
        assertEquals("rw-rw-r--", fileStatuses[0].getPermission().toString());
        assertEquals(0, fileStatuses[0].getLen());

        assertEquals("part-00000", fileStatuses[1].getPath().getName());
        assertEquals("rw-r--r--", fileStatuses[1].getPermission().toString());
        assertEquals(blockLen, fileStatuses[1].getLen());

        assertEquals("part-00001", fileStatuses[2].getPath().getName());
        assertEquals("rw-r--r--", fileStatuses[2].getPermission().toString());
        assertEquals(blockLen, fileStatuses[2].getLen());

        assertEquals("part-00002", fileStatuses[3].getPath().getName());
        assertEquals("rw-r--r--", fileStatuses[3].getPermission().toString());
        assertEquals(blockLen, fileStatuses[3].getLen());
    }
}
