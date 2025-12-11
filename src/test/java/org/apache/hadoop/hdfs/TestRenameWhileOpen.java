package org.apache.hadoop.hdfs;

import com.aliyun.jindodata.gateway.JindoMultiClusterTestBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class TestRenameWhileOpen extends JindoMultiClusterTestBase {
    private static final String DIR              = makeTestPathStr(TestRenameWhileOpen.class.getSimpleName() + "/");
    private static final int    BLOCK_SIZE       = 8192;
    // soft limit is short and hard limit is long, to test that
    // another thread can lease file after soft limit expired
    private static final long   SOFT_LEASE_LIMIT = 500;
    private static final long   HARD_LEASE_LIMIT = 1000*600;

    @Test
    public void testWhileOpenRenameParent() throws IOException {
        Configuration conf = new HdfsConfiguration(BASE_CONF);
        final int MAX_IDLE_TIME = 2000; // 2s
        conf.setInt("ipc.client.connection.maxidletime", MAX_IDLE_TIME);
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
                1000);
        conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY, 1);
        conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, TestFileCreation2.blockSize);
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 0);  // OK?

        // create cluster
        System.out.println("Test 1*****************************");
        FileSystem fs = null;
        try {
            fs = getDefaultFS();

            // Normally, the in-progress edit log would be finalized by
            // FSEditLog#endCurrentLogSegment. For testing purposes, we
            // disable that here.
            // FSEditLog spyLog =
            // spy(cluster.getNameNode().getFSImage().getEditLog());
            // doNothing().when(spyLog).endCurrentLogSegment(Mockito.anyBoolean());
            // DFSTestUtil.setEditLogForTesting(cluster.getNamesystem(), spyLog);

            // create file1.
            Path dir1 = makeTestPath("user/a+b/dir1");
            Path file1 = new Path(dir1, "file1");
            FSDataOutputStream stm1 = TestFileCreation2.createFile(fs, file1, 1);
            System.out
                    .println("TestFileCreation2DeleteParent: " + "Created file " + file1);
            TestFileCreation2.writeFile(stm1);
            stm1.hflush();

            // create file2.
            Path dir2 = makeTestPath("user/dir2");
            Path file2 = new Path(dir2, "file2");
            FSDataOutputStream stm2 = TestFileCreation2.createFile(fs, file2, 1);
            System.out
                    .println("TestFileCreation2DeleteParent: " + "Created file " + file2);
            TestFileCreation2.writeFile(stm2);
            stm2.hflush();

            // move dir1 while file1 is open
            Path dir3 = makeTestPath("user/dir3");
            fs.mkdirs(dir3);
            fs.rename(dir1, dir3);

            // create file3
            Path file3 = new Path(dir3, "file3");
            FSDataOutputStream stm3 = fs.create(file3);
            fs.rename(file3, new Path(dir3, "bozo"));
            // Get a new block for the file.
            TestFileCreation2.writeFile(stm3, TestFileCreation2.blockSize + 1);
            stm3.hflush();

        } finally {
        }
    }

    @Test
    public void testWhileOpenRenameParentToNonexistentDir() throws IOException {
        Configuration conf = new HdfsConfiguration(BASE_CONF);
        final int MAX_IDLE_TIME = 2000; // 2s
        conf.setInt("ipc.client.connection.maxidletime", MAX_IDLE_TIME);
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
                1000);
        conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY, 1);
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 0);  // OK?

        System.out.println("Test 2************************************");

        FileSystem fs = null;
        try {
            fs = getDefaultFS();

            // create file1.
            Path dir1 = makeTestPath("user/dir1");
            Path file1 = new Path(dir1, "file1");
            FSDataOutputStream stm1 = TestFileCreation2.createFile(fs, file1, 1);
            System.out
                    .println("TestFileCreation2DeleteParent: " + "Created file " + file1);
            TestFileCreation2.writeFile(stm1);
            stm1.hflush();

            // create file2.
            Path dir2 = makeTestPath("user/dir2");
            Path file2 = new Path(dir2, "file2");
            FSDataOutputStream stm2 = TestFileCreation2.createFile(fs, file2, 1);
            System.out
                    .println("TestFileCreation2DeleteParent: " + "Created file " + file2);
            TestFileCreation2.writeFile(stm2);
            stm2.hflush();

            // move dir1 while file1 is open
            Path dir3 = makeTestPath("user/dir3");
            fs.rename(dir1, dir3);
        } finally {
        }
    }

    @Test
    public void testWhileOpenRenameToExistentDirectory() throws IOException {
        Configuration conf = new HdfsConfiguration(BASE_CONF);
        final int MAX_IDLE_TIME = 2000; // 2s
        conf.setInt("ipc.client.connection.maxidletime", MAX_IDLE_TIME);
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
                1000);
        conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY, 1);
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 0);  // OK?

        System.out.println("Test 3************************************");

        // create cluster
        FileSystem fs = null;
        try {
            fs = getDefaultFS();

            // create file1.
            Path dir1 = makeTestPath("user/dir1");
            Path file1 = new Path(dir1, "file1");
            FSDataOutputStream stm1 = TestFileCreation2.createFile(fs, file1, 1);
            System.out
                    .println("TestFileCreation2DeleteParent: " + "Created file " + file1);
            TestFileCreation2.writeFile(stm1);
            stm1.hflush();

            Path dir2 = makeTestPath("user/dir2");
            fs.mkdirs(dir2);

            fs.rename(file1, dir2);
        } finally {
        }
    }

    @Test
    public void testWhileOpenRenameToNonExistentDirectory() throws IOException {
        Configuration conf = new HdfsConfiguration(BASE_CONF);
        final int MAX_IDLE_TIME = 2000; // 2s
        conf.setInt("ipc.client.connection.maxidletime", MAX_IDLE_TIME);
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
                1000);
        conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY, 1);
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 0);  // OK?

        System.out.println("Test 4************************************");

        FileSystem fs = null;
        try {
            fs = getDefaultFS();

            // create file1.
            Path dir1 = makeTestPath("user/dir1");
            Path file1 = new Path(dir1, "file1");
            FSDataOutputStream stm1 = TestFileCreation2.createFile(fs, file1, 1);
            System.out
                    .println("TestFileCreation2DeleteParent: " + "Created file " + file1);
            TestFileCreation2.writeFile(stm1);
            stm1.hflush();

            Path dir2 = makeTestPath("user/dir2");

            fs.rename(file1, dir2);
        } finally {
        }
    }
}
