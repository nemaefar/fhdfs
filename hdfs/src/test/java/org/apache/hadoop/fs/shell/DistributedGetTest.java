package org.apache.hadoop.fs.shell;

import org.bouncycastle.util.Arrays;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class DistributedGetTest extends HdfsTestSuite {

    @Test
    public void simpleTest() throws Exception {
        getCluster().copyToHDFS(TEST_FILE_TXT1.toString(), "/" + TEST_FILE_TXT1.getFileName());

        assertFalse(Files.exists(TEST_FILE_TXT));

        DistributedGet distributedGet = new DistributedGet();
        distributedGet.setConf(getCluster().config.configuration);

        assertEquals(0, distributedGet.run(new String[]{"-v", "/" + TEST_FILE_TXT1.getFileName(), "./" + TEST_FILE_TXT.toString()}));

        List<Path> local = Files.list(Paths.get(UNIVERSE)).collect(Collectors.toList());

        assertEquals(4, local.size());

        assertEquals(blockLen, Files.size(TEST_FILE_TXT));

        assertArrayEquals(Files.readAllBytes(TEST_FILE_TXT1), Files.readAllBytes(TEST_FILE_TXT));
    }

    @Test
    public void distributedSimple() throws Exception {
        String testFileDir = "/" + TEST_FILE_TXT1.getFileName() + ".distributed";
        getCluster().getFS().mkdirs(new org.apache.hadoop.fs.Path(testFileDir));
        getCluster().getFS().create(new org.apache.hadoop.fs.Path(testFileDir + "/_ATTRIBUTES")).close();
        getCluster().copyToHDFS(TEST_FILE_TXT1.toString(), testFileDir + "/part-00001");
        getCluster().copyToHDFS(TEST_FILE_TXT1.toString(), testFileDir + "/part-00002");

        assertFalse(Files.exists(TEST_FILE_TXT));

        DistributedGet distributedGet = new DistributedGet();
        distributedGet.setConf(getCluster().config.configuration);

        assertEquals(0, distributedGet.run(new String[]{"-v", "/" + TEST_FILE_TXT1.getFileName(), "./" + TEST_FILE_TXT.toString()}));

        List<Path> local = Files.list(Paths.get(UNIVERSE)).collect(Collectors.toList());

        assertEquals(4, local.size());

        assertEquals(blockLen * 2, Files.size(TEST_FILE_TXT));

        assertArrayEquals(Arrays.concatenate(Files.readAllBytes(TEST_FILE_TXT1),Files.readAllBytes(TEST_FILE_TXT1)), Files.readAllBytes(TEST_FILE_TXT));
    }

    @Test
    public void versionedSimple() throws Exception {
        long currentTime = System.currentTimeMillis();

        generateFile(1, blockLen, TEST_FILE_TXT1, 0);
        getCluster().copyToHDFS(TEST_FILE_TXT1.toString(), "/" + TEST_FILE_TXT1.getFileName() + ".ver." + (currentTime - 111111));

        generateFile(1, blockLen, TEST_FILE_TXT1, 1);
        getCluster().copyToHDFS(TEST_FILE_TXT1.toString(), "/" + TEST_FILE_TXT1.getFileName() + ".ver." + (currentTime));

        getCluster().getFS().create(new org.apache.hadoop.fs.Path("/_SUCCESS_test_file1.txt")).close();

        // not ready yet file
        generateFile(1, blockLen, TEST_FILE_TXT1, 2);
        getCluster().copyToHDFS(TEST_FILE_TXT1.toString(), "/" + TEST_FILE_TXT1.getFileName() + ".ver." + (currentTime + 111111));

        // restore expected file
        generateFile(1, blockLen, TEST_FILE_TXT1, 1);

        assertFalse(Files.exists(TEST_FILE_TXT));

        DistributedGet distributedGet = new DistributedGet();
        distributedGet.setConf(getCluster().config.configuration);

        assertEquals(0, distributedGet.run(new String[]{"-v", "-n", "/" + TEST_FILE_TXT1.getFileName(), "./" + TEST_FILE_TXT.toString()}));

        List<Path> local = Files.list(Paths.get(UNIVERSE)).collect(Collectors.toList());

        assertEquals(4, local.size());

        assertEquals(blockLen, Files.size(TEST_FILE_TXT));

        assertArrayEquals(Files.readAllBytes(TEST_FILE_TXT1), Files.readAllBytes(TEST_FILE_TXT));
    }

    @Test
    public void versionedDistributed() throws Exception {
        long currentTime = System.currentTimeMillis();

        getCluster().copyToHDFS(TEST_FILE_TXT1.toString(), "/" + TEST_FILE_TXT1.getFileName() + ".ver." + (currentTime - 111111));

        String dir = "/" + TEST_FILE_TXT1.getFileName() + ".ver." + currentTime + ".distributed";

        getCluster().getFS().mkdirs(new org.apache.hadoop.fs.Path(dir));

        getCluster().getFS().create(new org.apache.hadoop.fs.Path(dir + "/_ATTRIBUTES")).close();
        getCluster().copyToHDFS(TEST_FILE_TXT1.toString(), dir + "/part-00000");
        getCluster().copyToHDFS(TEST_FILE_TXT1.toString(), dir + "/part-00001");

        getCluster().getFS().create(new org.apache.hadoop.fs.Path("/_SUCCESS_test_file1.txt")).close();

        getCluster().copyToHDFS(TEST_FILE_TXT1.toString(), "/" + TEST_FILE_TXT1.getFileName() + ".ver." + (currentTime + 111111));

        assertFalse(Files.exists(TEST_FILE_TXT));

        DistributedGet distributedGet = new DistributedGet();
        distributedGet.setConf(getCluster().config.configuration);

        assertEquals(0, distributedGet.run(new String[]{"-vvv", "-n", "/" + TEST_FILE_TXT1.getFileName(), "./" + TEST_FILE_TXT.toString()}));

        List<Path> local = Files.list(Paths.get(UNIVERSE)).collect(Collectors.toList());

        assertEquals(4, local.size());

        assertEquals(blockLen * 2, Files.size(TEST_FILE_TXT));

        assertArrayEquals(Arrays.concatenate(Files.readAllBytes(TEST_FILE_TXT1),Files.readAllBytes(TEST_FILE_TXT1)), Files.readAllBytes(TEST_FILE_TXT));
    }

    @Test
    public void versionedDistributed2() throws Exception {
        long currentTime = System.currentTimeMillis();

        getCluster().copyToHDFS(TEST_FILE_TXT1.toString(), "/" + TEST_FILE_TXT1.getFileName() + ".ver." + (currentTime));

        getCluster().getFS().create(new org.apache.hadoop.fs.Path("/_SUCCESS_test_file1.txt")).close();

        String dir = "/" + TEST_FILE_TXT1.getFileName() + ".ver." + (currentTime + 111111) + ".distributed";

        getCluster().getFS().mkdirs(new org.apache.hadoop.fs.Path(dir));

        getCluster().getFS().create(new org.apache.hadoop.fs.Path(dir + "/_ATTRIBUTES")).close();
        getCluster().copyToHDFS(TEST_FILE_TXT1.toString(), dir + "/part-00000");
        getCluster().copyToHDFS(TEST_FILE_TXT1.toString(), dir + "/part-00001");

        assertFalse(Files.exists(TEST_FILE_TXT));

        DistributedGet distributedGet = new DistributedGet();
        distributedGet.setConf(getCluster().config.configuration);

        assertEquals(0, distributedGet.run(new String[]{"-v", "-n", "/" + TEST_FILE_TXT1.getFileName(), "./" + TEST_FILE_TXT.toString()}));

        List<Path> local = Files.list(Paths.get(UNIVERSE)).collect(Collectors.toList());

        assertEquals(4, local.size());

        assertEquals(blockLen, Files.size(TEST_FILE_TXT));

        assertArrayEquals(Files.readAllBytes(TEST_FILE_TXT1), Files.readAllBytes(TEST_FILE_TXT));
    }

    @Test
    public void directoryVersioned() throws Exception {
        long currentTime = System.currentTimeMillis();

        String dir1 = "/dir.ver." + currentTime;
        String dir2 = "/dir.ver." + (currentTime + 111111);

        getCluster().getFS().mkdirs(new org.apache.hadoop.fs.Path(dir1));
        getCluster().getFS().create(new org.apache.hadoop.fs.Path(dir1, "dir1")).close();

        getCluster().getFS().create(new org.apache.hadoop.fs.Path("/_SUCCESS_dir")).close();

        getCluster().getFS().mkdirs(new org.apache.hadoop.fs.Path(dir2));
        getCluster().getFS().create(new org.apache.hadoop.fs.Path(dir2, "dir2")).close();


        DistributedGet distributedGet = new DistributedGet();
        distributedGet.setConf(getCluster().config.configuration);

        assertEquals(0, distributedGet.run(new String[]{"-v", "-n", "/dir", "./" + UNIVERSE + "/dir_new"}));

        List<Path> local = Files.list(Paths.get(UNIVERSE)).collect(Collectors.toList());

        assertEquals(4, local.size());

        assertTrue(Files.isDirectory(Paths.get(UNIVERSE + "/dir_new")));
        assertTrue(Files.isRegularFile(Paths.get(UNIVERSE + "/dir_new/dir1")));
    }

    @Test
    public void complexCheck() throws Exception {
        long currentTime = System.currentTimeMillis();

        String dir1 = "/dir.ver." + currentTime;

        getCluster().getFS().mkdirs(new org.apache.hadoop.fs.Path(dir1));
        getCluster().getFS().create(new org.apache.hadoop.fs.Path(dir1, "dir1")).close();

        getCluster().copyToHDFS(TEST_FILE_TXT1.toString(), dir1 + "/" + TEST_FILE_TXT1.getFileName());

        getCluster().getFS().create(new org.apache.hadoop.fs.Path(dir1 + "/" + TEST_FILE_TXT2.getFileName() + ".distributed/_ATTRIBUTES")).close();
        getCluster().copyToHDFS(TEST_FILE_TXT1.toString(), dir1 + "/" + TEST_FILE_TXT2.getFileName() + ".distributed/part-00000");
        getCluster().copyToHDFS(TEST_FILE_TXT1.toString(), dir1 + "/" + TEST_FILE_TXT2.getFileName() + ".distributed/part-00001");

        getCluster().getFS().create(new org.apache.hadoop.fs.Path("/_SUCCESS_dir")).close();

        DistributedGet distributedGet = new DistributedGet();
        distributedGet.setConf(getCluster().config.configuration);
        assertEquals(0, distributedGet.run(new String[]{"-v", "-n", "/dir", "./" + UNIVERSE + "/dir_new"}));

        List<Path> local = Files.list(Paths.get(UNIVERSE)).collect(Collectors.toList());

        assertEquals(4, local.size());

        local = Files.list(Paths.get(UNIVERSE + "/dir_new/")).collect(Collectors.toList());

        assertEquals(3, local.size());
        assertEquals(0, Files.size(Paths.get(UNIVERSE + "/dir_new/dir1")));
        assertEquals(blockLen, Files.size(Paths.get(UNIVERSE + "/dir_new/" + TEST_FILE_TXT1.getFileName())));
        assertEquals(blockLen * 2, Files.size(Paths.get(UNIVERSE + "/dir_new/" + TEST_FILE_TXT2.getFileName())));
    }
}
