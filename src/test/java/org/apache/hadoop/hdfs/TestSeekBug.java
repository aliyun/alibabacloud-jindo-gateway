package org.apache.hadoop.hdfs;

import com.aliyun.jindodata.gateway.JindoMultiClusterTestBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumFileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestSeekBug extends JindoMultiClusterTestBase {
    static final long seed = 0xDEADBEEFL;
    static final int ONEMB = 1 << 20;

    private void writeFile(FileSystem fileSys, Path name) throws IOException {
        // create and write a file that contains 1MB
        DataOutputStream stm = fileSys.create(name);
        byte[] buffer = new byte[ONEMB];
        Random rand = new Random(seed);
        rand.nextBytes(buffer);
        stm.write(buffer);
        stm.close();
    }

    private void checkAndEraseData(byte[] actual, int from, byte[] expected, String message) {
        for (int idx = 0; idx < actual.length; idx++) {
            assertEquals(actual[idx], expected[from+idx],
                    message+" byte "+(from+idx)+" differs. expected "+
                            expected[from+idx]+" actual "+actual[idx]);
            actual[idx] = 0;
        }
    }

    private void seekReadFile(FileSystem fileSys, Path name) throws IOException {
        FSDataInputStream stm = fileSys.open(name, 4096);
        byte[] expected = new byte[ONEMB];
        Random rand = new Random(seed);
        rand.nextBytes(expected);

        // First read 128 bytes to set count in BufferedInputStream
        byte[] actual = new byte[128];
        stm.read(actual, 0, actual.length);
        // Now read a byte array that is bigger than the internal buffer
        actual = new byte[100000];
        IOUtils.readFully(stm, actual, 0, actual.length);
        checkAndEraseData(actual, 128, expected, "First Read Test");
        // now do a small seek, within the range that is already read
        stm.seek(96036); // 4 byte seek
        actual = new byte[128];
        IOUtils.readFully(stm, actual, 0, actual.length);
        checkAndEraseData(actual, 96036, expected, "Seek Bug");
        // all done
        stm.close();
    }

    /*
     * Read some data, skip a few bytes and read more. HADOOP-922.
     */
    private void smallReadSeek(FileSystem fileSys, Path name) throws IOException {
        if (fileSys instanceof ChecksumFileSystem) {
            fileSys = ((ChecksumFileSystem)fileSys).getRawFileSystem();
        }
        // Make the buffer size small to trigger code for HADOOP-922
        FSDataInputStream stmRaw = fileSys.open(name, 1);
        byte[] expected = new byte[ONEMB];
        Random rand = new Random(seed);
        rand.nextBytes(expected);

        // Issue a simple read first.
        byte[] actual = new byte[128];
        stmRaw.seek(100000);
        stmRaw.read(actual, 0, actual.length);
        checkAndEraseData(actual, 100000, expected, "First Small Read Test");

        // now do a small seek of 4 bytes, within the same block.
        int newpos1 = 100000 + 128 + 4;
        stmRaw.seek(newpos1);
        stmRaw.read(actual, 0, actual.length);
        checkAndEraseData(actual, newpos1, expected, "Small Seek Bug 1");

        // seek another 256 bytes this time
        int newpos2 = newpos1 + 256;
        stmRaw.seek(newpos2);
        stmRaw.read(actual, 0, actual.length);
        checkAndEraseData(actual, newpos2, expected, "Small Seek Bug 2");

        // all done
        stmRaw.close();
    }

    private void cleanupFile(FileSystem fileSys, Path name) throws IOException {
        assertTrue(fileSys.exists(name));
        fileSys.delete(name, true);
        assertTrue(!fileSys.exists(name));
    }

    @Test
    public void testSeekBugDFS() throws IOException {
        Configuration conf = new HdfsConfiguration(BASE_CONF);
        FileSystem fileSys = getDefaultFS();
        try {
            Path file1 = makeTestPath("seektest.dat");
            writeFile(fileSys, file1);
            seekReadFile(fileSys, file1);
            smallReadSeek(fileSys, file1);
            cleanupFile(fileSys, file1);
        } finally {
        }
    }

    @Test
    public void testNegativeSeek() throws IOException {
        Configuration conf = new HdfsConfiguration(BASE_CONF);
        FileSystem fs = getDefaultFS();
        try {
            Path seekFile = makeTestPath("seekboundaries.dat");
            createFile(
                    fs,
                    seekFile,
                    ONEMB,
                    fs.getDefaultReplication(seekFile),
                    seed);
            FSDataInputStream stream = fs.open(seekFile);
            // Perform "safe seek" (expected to pass)
            stream.seek(65536);
            assertEquals(65536, stream.getPos());
            // expect IOE for this call
            stream.seek(-73);
        } catch (EOFException ioe) {
            System.out.println("Got EOF exception");
        } finally {
        }
    }

    @Test
    public void testSeekPastFileSize() throws IOException {
        FileSystem fs = getDefaultFS();
        try {
            Path seekFile = new Path("seekboundaries.dat");
            createFile(
                    fs,
                    seekFile,
                    ONEMB,
                    fs.getDefaultReplication(seekFile),
                    seed);
            FSDataInputStream stream = fs.open(seekFile);
            // Perform "safe seek" (expected to pass)
            stream.seek(65536);
            assertEquals(65536, stream.getPos());
            // expect IOE for this call
            stream.seek(ONEMB + ONEMB + ONEMB);
        } catch (EOFException ioe) {
            System.out.println("Got EOF exception");
        } finally {
        }
    }
}
