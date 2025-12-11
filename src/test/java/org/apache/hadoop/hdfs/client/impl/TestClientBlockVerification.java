package org.apache.hadoop.hdfs.client.impl;

import com.aliyun.jindodata.gateway.JindoSingleClusterTestBase;
import com.aliyun.jindodata.gateway.JindoTestBase;
import com.aliyun.jindodata.gateway.common.JindoMiniCluster;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.util.List;

import static org.mockito.Mockito.*;

public class TestClientBlockVerification extends JindoSingleClusterTestBase {

    static BlockReaderTestUtil util = null;
    static final Path TEST_FILE = makeTestPath("test.file");
    static final int FILE_SIZE_K = 256;
    static LocatedBlock testBlock = null;
    static JindoMiniCluster cluster;
    static FileSystem fs;

    @BeforeEach
    public void setup() throws Exception {
        final int REPLICATION_FACTOR = 1;
        util = new BlockReaderTestUtil(REPLICATION_FACTOR, BASE_CONF);
        util.writeFile(TEST_FILE, FILE_SIZE_K);
        List<LocatedBlock> blkList = util.getFileBlocks(TEST_FILE, FILE_SIZE_K);
        testBlock = blkList.get(0);     // Use the first block to test
    }

    /**
     * Verify that if we read an entire block, we send CHECKSUM_OK
     */
    @Test
    public void testBlockVerification() throws Exception {
        BlockReaderRemote2 reader = (BlockReaderRemote2) spy(
                util.getBlockReader(testBlock, 0, FILE_SIZE_K * 1024));
        util.readAndCheckEOS(reader, FILE_SIZE_K * 1024, true);
        verify(reader).sendReadResult(Status.CHECKSUM_OK);
        reader.close();
    }

    /**
     * Test that if we do an incomplete read, we don't call CHECKSUM_OK
     */
    public void testIncompleteRead() throws Exception {
        BlockReaderRemote2 reader = (BlockReaderRemote2) spy(
                util.getBlockReader(testBlock, 0, FILE_SIZE_K * 1024));
        util.readAndCheckEOS(reader, FILE_SIZE_K / 2 * 1024, false);

        // We asked the blockreader for the whole file, and only read
        // half of it, so no CHECKSUM_OK
        verify(reader, never()).sendReadResult(Status.CHECKSUM_OK);
        reader.close();
    }

    /**
     * Test that if we ask for a half block, and read it all, we *do*
     * send CHECKSUM_OK. The DN takes care of knowing whether it was
     * the whole block or not.
     */
    public void testCompletePartialRead() throws Exception {
        // Ask for half the file
        BlockReaderRemote2 reader = (BlockReaderRemote2) spy(
                util.getBlockReader(testBlock, 0, FILE_SIZE_K * 1024 / 2));
        // And read half the file
        util.readAndCheckEOS(reader, FILE_SIZE_K * 1024 / 2, true);
        verify(reader).sendReadResult(Status.CHECKSUM_OK);
        reader.close();
    }

    /**
     * Test various unaligned reads to make sure that we properly
     * account even when we don't start or end on a checksum boundary
     */
    public void testUnalignedReads() throws Exception {
        int startOffsets[] = new int[]{0, 3, 129};
        int lengths[] = new int[]{30, 300, 512, 513, 1025};
        for (int startOffset : startOffsets) {
            for (int length : lengths) {
                DFSClient.LOG.info("Testing startOffset = " + startOffset + " and " +
                        " len=" + length);
                BlockReaderRemote2 reader = (BlockReaderRemote2) spy(
                        util.getBlockReader(testBlock, startOffset, length));
                util.readAndCheckEOS(reader, length, true);
                verify(reader).sendReadResult(Status.CHECKSUM_OK);
                reader.close();
            }
        }
    }


    @AfterEach
    public void teardown() throws Exception {
        util.shutdown();
    }

}
