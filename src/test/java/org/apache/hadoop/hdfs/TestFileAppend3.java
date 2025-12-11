package org.apache.hadoop.hdfs;

import com.aliyun.jindodata.gateway.JindoMultiClusterTestBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class TestFileAppend3 extends JindoMultiClusterTestBase {
    static final long BLOCK_SIZE = 1024 * 1024;
    static final short REPLICATION = 3;

    private static int                   buffersize = 4096;
    private static DistributedFileSystem fs;

    @BeforeEach
    public void before() throws IOException {
        fs = getDFS();
    }

    @Test
    public void testTC1() throws Exception {
        final Path p = makeTestPath("TC1/foo");
        System.out.println("p=" + p);

        //a. Create file and write one block of data. Close file.
        final int len1 = (int)BLOCK_SIZE;
        {
            FSDataOutputStream out = fs.create(p, false, buffersize, REPLICATION, BLOCK_SIZE);
            DfsAppendTestUtil.write(out, 0, len1);
            out.close();
        }

        //   Reopen file to append. Append half block of data. Close file.
        final int len2 = (int)BLOCK_SIZE/2;
        {
            FSDataOutputStream out = fs.append(p);
            DfsAppendTestUtil.write(out, len1, len2);
            out.close();
        }

        //b. Reopen file and read 1.5 blocks worth of data. Close file.
        DfsAppendTestUtil.check(fs, p, len1 + len2);
    }

    @Test
    public void testTC1ForAppend2() throws Exception {
        final Path p = makeTestPath("TC1/foo2");

        //a. Create file and write one block of data. Close file.
        final int len1 = (int) BLOCK_SIZE;
        {
            FSDataOutputStream out = fs.create(p, false, buffersize, REPLICATION,
                    BLOCK_SIZE);
            DfsAppendTestUtil.write(out, 0, len1);
            out.close();
        }

        // Reopen file to append. Append half block of data. Close file.
        final int len2 = (int) BLOCK_SIZE / 2;
        {
            FSDataOutputStream out = fs.append(p,
                    EnumSet.of(CreateFlag.APPEND, CreateFlag.NEW_BLOCK), 4096, null);
            DfsAppendTestUtil.write(out, len1, len2);
            out.close();
        }

        // b. Reopen file and read 1.5 blocks of data. Close file.
        DfsAppendTestUtil.check(fs, p, len1 + len2);
    }

    @Test
    public void testTC2() throws Exception {
        final Path p = makeTestPath("TC2/foo");
        System.out.println("p=" + p);

        //a. Create file with one and a half block of data. Close file.
        final int len1 = (int)(BLOCK_SIZE + BLOCK_SIZE/2);
        {
            FSDataOutputStream out = fs.create(p, false, buffersize, REPLICATION, BLOCK_SIZE);
            DfsAppendTestUtil.write(out, 0, len1);
            out.close();
        }

        DfsAppendTestUtil.check(fs, p, len1);

        //   Reopen file to append quarter block of data. Close file.
        final int len2 = (int)BLOCK_SIZE/4;
        {
            FSDataOutputStream out = fs.append(p);
            DfsAppendTestUtil.write(out, len1, len2);
            out.close();
        }

        //b. Reopen file and read 1.75 blocks of data. Close file.
        DfsAppendTestUtil.check(fs, p, len1 + len2);
    }

    @Test
    public void testTC2ForAppend2() throws Exception {
        final Path p = makeTestPath("TC2/foo2");

        //a. Create file with one and a half block of data. Close file.
        final int len1 = (int) (BLOCK_SIZE + BLOCK_SIZE / 2);
        {
            FSDataOutputStream out = fs.create(p, false, buffersize, REPLICATION,
                    BLOCK_SIZE);
            DfsAppendTestUtil.write(out, 0, len1);
            out.close();
        }

        DfsAppendTestUtil.check(fs, p, len1);

        //   Reopen file to append quarter block of data. Close file.
        final int len2 = (int) BLOCK_SIZE / 4;
        {
            FSDataOutputStream out = fs.append(p, EnumSet.of(CreateFlag.APPEND, CreateFlag.NEW_BLOCK),
                    4096, null);
            DfsAppendTestUtil.write(out, len1, len2);
            out.close();
        }

        // b. Reopen file and read 1.75 blocks of data. Close file.
        DfsAppendTestUtil.check(fs, p, len1 + len2);
        List<LocatedBlock> blocks = fs.getClient().getLocatedBlocks(
                p.toString(), 0L).getLocatedBlocks();
        assertEquals(3, blocks.size());
        assertEquals(BLOCK_SIZE, blocks.get(0).getBlockSize());
        assertEquals(BLOCK_SIZE / 2, blocks.get(1).getBlockSize());
        assertEquals(BLOCK_SIZE / 4, blocks.get(2).getBlockSize());
    }

    @Test
    public void testTC5() throws Exception {
        final Path p = makeTestPath("TC5/foo");
        System.out.println("p=" + p);

        //a. Create file on Machine M1. Write half block to it. Close file.
        {
            FSDataOutputStream out = fs.create(p, false, buffersize, REPLICATION, BLOCK_SIZE);
            DfsAppendTestUtil.write(out, 0, (int)(BLOCK_SIZE/2));
            out.close();
        }

        //b. Reopen file in "append" mode on Machine M1.
        FSDataOutputStream out = fs.append(p);

        //c. On Machine M2, reopen file in "append" mode. This should fail.
        try {
            DfsAppendTestUtil.createHdfsWithDifferentUsername(new Configuration()).append(p);
            fail("This should fail.");
        } catch(IOException ioe) {
            DfsAppendTestUtil.LOG.info("GOOD: got an exception", ioe);
        }

        try {
            ((DistributedFileSystem) DfsAppendTestUtil
                    .createHdfsWithDifferentUsername(new Configuration())).append(p,
                    EnumSet.of(CreateFlag.APPEND, CreateFlag.NEW_BLOCK), 4096, null);
            fail("This should fail.");
        } catch(IOException ioe) {
            DfsAppendTestUtil.LOG.info("GOOD: got an exception", ioe);
        }

        //d. On Machine M1, close file.
        out.close();
    }

    @Test
    public void testTC5ForAppend2() throws Exception {
        final Path p = makeTestPath("TC5/foo2");

        // a. Create file on Machine M1. Write half block to it. Close file.
        {
            FSDataOutputStream out = fs.create(p, false, buffersize, REPLICATION,
                    BLOCK_SIZE);
            DfsAppendTestUtil.write(out, 0, (int)(BLOCK_SIZE/2));
            out.close();
        }

        // b. Reopen file in "append" mode on Machine M1.
        FSDataOutputStream out = fs.append(p, EnumSet.of(CreateFlag.APPEND, CreateFlag.NEW_BLOCK),
                4096, null);

        // c. On Machine M2, reopen file in "append" mode. This should fail.
        try {
            ((DistributedFileSystem) DfsAppendTestUtil
                    .createHdfsWithDifferentUsername(new Configuration())).append(p,
                    EnumSet.of(CreateFlag.APPEND, CreateFlag.NEW_BLOCK), 4096, null);
            fail("This should fail.");
        } catch(IOException ioe) {
            DfsAppendTestUtil.LOG.info("GOOD: got an exception", ioe);
        }

        try {
            DfsAppendTestUtil.createHdfsWithDifferentUsername(new Configuration()).append(p);
            fail("This should fail.");
        } catch(IOException ioe) {
            DfsAppendTestUtil.LOG.info("GOOD: got an exception", ioe);
        }

        // d. On Machine M1, close file.
        out.close();
    }

    private void testTC12(boolean appendToNewBlock) throws Exception {
        final Path p = makeTestPath("TC12/foo" + (appendToNewBlock ? "0" : "1"));
        System.out.println("p=" + p);

        //a. Create file with a block size of 64KB
        //   and a default io.bytes.per.checksum of 512 bytes.
        //   Write 25687 bytes of data. Close file.
        final int len1 = 25687;
        {
            FSDataOutputStream out = fs.create(p, false, buffersize, REPLICATION, BLOCK_SIZE);
            DfsAppendTestUtil.write(out, 0, len1);
            out.close();
        }

        //b. Reopen file in "append" mode. Append another 5877 bytes of data. Close file.
        final int len2 = 5877;
        {
            FSDataOutputStream out = appendToNewBlock ?
                    fs.append(p, EnumSet.of(CreateFlag.APPEND, CreateFlag.NEW_BLOCK), 4096, null) :
                    fs.append(p);
            DfsAppendTestUtil.write(out, len1, len2);
            out.close();
        }

        //c. Reopen file and read 25687+5877 bytes of data from file. Close file.
        DfsAppendTestUtil.check(fs, p, len1 + len2);
        if (appendToNewBlock) {
            LocatedBlocks locations = fs.getClient().getNamenode().getBlockLocations(
                    p.toString(), 0, Long.MAX_VALUE);
            LocatedBlocks blks = fs.getClient().getLocatedBlocks(p.toString(), 0);
            assertEquals(2, blks.getLocatedBlocks().size());
            assertEquals(len1, blks.getLocatedBlocks().get(0).getBlockSize());
            assertEquals(len2, blks.getLocatedBlocks().get(1).getBlockSize());
            DfsAppendTestUtil.check(fs, p, 0, len1);
            DfsAppendTestUtil.check(fs, p, len1, len2);
        }
    }

    @Test
    public void testTC12() throws Exception {
        testTC12(false);
    }

    @Test
    public void testTC12ForAppend2() throws Exception {
        testTC12(true);
    }

    private void testAppendToPartialChunk(boolean appendToNewBlock)
            throws IOException {
        final Path p = makeTestPath("partialChunk/foo"
                + (appendToNewBlock ? "0" : "1"));
        final int fileLen = 513;
        System.out.println("p=" + p);

        byte[] fileContents = DfsAppendTestUtil.initBuffer(fileLen);

        // create a new file.
        FSDataOutputStream stm = DfsAppendTestUtil.createFile(fs, p, 1);

        // create 1 byte file
        stm.write(fileContents, 0, 1);
        stm.close();
        System.out.println("Wrote 1 byte and closed the file " + p);

        // append to file
        stm = appendToNewBlock ?
                fs.append(p, EnumSet.of(CreateFlag.APPEND, CreateFlag.NEW_BLOCK), 4096, null) :
                fs.append(p);
        // Append to a partial CRC trunk
        stm.write(fileContents, 1, 1);
        stm.hflush();
        // The partial CRC trunk is not full yet and close the file
        stm.close();
        System.out.println("Append 1 byte and closed the file " + p);

        // write the remainder of the file
        stm = appendToNewBlock ?
                fs.append(p, EnumSet.of(CreateFlag.APPEND, CreateFlag.NEW_BLOCK), 4096, null) :
                fs.append(p);

        // ensure getPos is set to reflect existing size of the file
        assertEquals(2, stm.getPos());

        // append to a partial CRC trunk
        stm.write(fileContents, 2, 1);
        // The partial chunk is not full yet, force to send a packet to DN
        stm.hflush();
        System.out.println("Append and flush 1 byte");
        // The partial chunk is not full yet, force to send another packet to DN
        stm.write(fileContents, 3, 2);
        stm.hflush();
        System.out.println("Append and flush 2 byte");

        // fill up the partial chunk and close the file
        stm.write(fileContents, 5, fileLen-5);
        stm.close();
        System.out.println("Flush 508 byte and closed the file " + p);

        // verify that entire file is good
        DfsAppendTestUtil.checkFullFile(fs, p, fileLen,
                fileContents, "Failed to append to a partial chunk");
    }

    void doSmallAppends(Path file, DistributedFileSystem fs, int iterations)
            throws IOException {
        for (int i = 0; i < iterations; i++) {
            FSDataOutputStream stm;
            try {
                stm = fs.append(file);
            } catch (IOException e) {
                // If another thread is already appending, skip this time.
                continue;
            }
            // Failure in write or close will be terminal.
            DfsAppendTestUtil.write(stm, 0, 123);
            stm.close();
        }
    }

    @Test
    public void testSmallAppendRace()  throws Exception {
        final Path file = makeTestPath("testSmallAppendRace");
        final String fName = file.toUri().getPath();

        // Create the file and write a small amount of data.
        FSDataOutputStream stm = fs.create(file);
        DfsAppendTestUtil.write(stm, 0, 123);
        stm.close();

        // Introduce a delay between getFileInfo and calling append() against NN.
        final DFSClient client = fs.dfs;
        DFSClient spyClient = spy(client);
        when(spyClient.getFileInfo(fName)).thenAnswer(new Answer<HdfsFileStatus>() {
            @Override
            public HdfsFileStatus answer(InvocationOnMock invocation){
                try {
                    HdfsFileStatus stat = client.getFileInfo(fName);
                    Thread.sleep(100);
                    return stat;
                } catch (Exception e) {
                    return null;
                }
            }
        });

        fs.dfs = spyClient;

        // Create two threads for doing appends to the same file.
        Thread worker1 = new Thread() {
            @Override
            public void run() {
                try {
                    doSmallAppends(file, fs, 20);
                } catch (IOException e) {
                }
            }
        };

        Thread worker2 = new Thread() {
            @Override
            public void run() {
                try {
                    doSmallAppends(file, fs, 20);
                } catch (IOException e) {
                }
            }
        };

        worker1.start();
        worker2.start();

        // append will fail when the file size crosses the checksum chunk boundary,
        // if append was called with a stale file stat.
        doSmallAppends(file, fs, 20);
    }

    @Test
    public void testAppendToPartialChunk() throws IOException {
        testAppendToPartialChunk(false);
    }

    @Test
    public void testAppendToPartialChunkforAppend2() throws IOException {
        testAppendToPartialChunk(true);
    }
}