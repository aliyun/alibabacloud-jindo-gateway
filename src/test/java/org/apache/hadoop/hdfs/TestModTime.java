package org.apache.hadoop.hdfs;

import com.aliyun.jindodata.gateway.JindoMultiClusterTestBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

public class TestModTime extends JindoMultiClusterTestBase {
    static final long seed = 0xDEADBEEFL;
    static final int blockSize = 1024 * 1024;
    static final int fileSize = 16384;
    static final int numDatanodes = 6;
    private void writeFile(FileSystem fileSys, Path name, int repl)
            throws IOException {
        // create and write a file that contains three blocks of data
        FSDataOutputStream stm = fileSys.create(name, true, fileSys.getConf()
                        .getInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, 4096),
                (short) repl, blockSize);
        byte[] buffer = new byte[fileSize];
        Random rand = new Random(seed);
        rand.nextBytes(buffer);
        stm.write(buffer);
        stm.close();
    }

    private void cleanupFile(FileSystem fileSys, Path name) throws IOException {
        assertTrue(fileSys.exists(name));
        fileSys.delete(name, true);
        assertFalse(fileSys.exists(name));
    }

    @Test
    public void testModTime() throws IOException {
        Configuration conf = new HdfsConfiguration();
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 0);  // OK?

        FileSystem fileSys = getDefaultFS();
        int replicas = numDatanodes - 1;
        assertTrue(fileSys instanceof DistributedFileSystem);

        try {

            //
            // create file and record ctime and mtime of test file
            //
            System.out.println("Creating testdir1 and testdir1/test1.dat.");
            Path dir1 = makeTestPath("testdir1");
            Path file1 = new Path(dir1, "test1.dat");
            writeFile(fileSys, file1, replicas);
            FileStatus stat = fileSys.getFileStatus(file1);
            long mtime1 = stat.getModificationTime();
            assertTrue(mtime1 != 0);
            //
            // record dir times
            //
            stat = fileSys.getFileStatus(dir1);
            long mdir1 = stat.getModificationTime();

            //
            // create second test file
            //
            System.out.println("Creating testdir1/test2.dat.");
            Path file2 = new Path(dir1, "test2.dat");
            writeFile(fileSys, file2, replicas);
            stat = fileSys.getFileStatus(file2);

            //
            // verify that mod time of dir remains the same
            // as before. modification time of directory has increased.
            //
            stat = fileSys.getFileStatus(dir1);
            assertTrue(stat.getModificationTime() >= mdir1);
            mdir1 = stat.getModificationTime();
            //
            // create another directory
            //
            Path dir2 = fileSys.makeQualified(makeTestPath("testdir2"));
            System.out.println("Creating testdir2 " + dir2);
            assertTrue(fileSys.mkdirs(dir2));
            stat = fileSys.getFileStatus(dir2);
            long mdir2 = stat.getModificationTime();
            //
            // rename file1 from testdir into testdir2
            //
            Path newfile = new Path(dir2, "testnew.dat");
            System.out.println("Moving " + file1 + " to " + newfile);
            fileSys.rename(file1, newfile);
            //
            // verify that modification time of file1 did not change.
            //
            stat = fileSys.getFileStatus(newfile);
            assertEquals(mtime1, stat.getModificationTime());
            //
            // verify that modification time of  testdir1 and testdir2
            // were changed.
            //
            stat = fileSys.getFileStatus(dir1);
            assertNotEquals(mdir1, stat.getModificationTime());
            mdir1 = stat.getModificationTime();

            stat = fileSys.getFileStatus(dir2);
            assertNotEquals(mdir2, stat.getModificationTime());
            mdir2 = stat.getModificationTime();
            //
            // delete newfile
            //
            System.out.println("Deleting testdir2/testnew.dat.");
            assertTrue(fileSys.delete(newfile, true));
            //
            // verify that modification time of testdir1 has not changed.
            //
            stat = fileSys.getFileStatus(dir1);
            assertEquals(mdir1, stat.getModificationTime());
            //
            // verify that modification time of testdir2 has changed.
            //
            stat = fileSys.getFileStatus(dir2);
            assertNotEquals(mdir2, stat.getModificationTime());
            mdir2 = stat.getModificationTime();

            cleanupFile(fileSys, file2);
            cleanupFile(fileSys, dir1);
            cleanupFile(fileSys, dir2);
        } catch (IOException e) {
            throw e;
        } finally {
        }
    }
}