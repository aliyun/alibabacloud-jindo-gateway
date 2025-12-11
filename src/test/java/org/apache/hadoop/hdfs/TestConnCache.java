package org.apache.hadoop.hdfs;

import com.aliyun.jindodata.gateway.JindoSingleClusterTestBase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.impl.BlockReaderTestUtil;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_SOCKET_CACHE_EXPIRY_MSEC_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestConnCache extends JindoSingleClusterTestBase {
    static final Log LOG = LogFactory.getLog(TestConnCache.class);

    static final int BLOCK_SIZE = 4096;
    static final int FILE_SIZE = 3 * BLOCK_SIZE;

    private void pread(DFSInputStream in,
                       long pos,
                       byte[] buffer,
                       int offset,
                       int length,
                       byte[] authenticData)
            throws IOException {
        assertTrue(buffer.length >= offset + length, "Test buffer too small");

        if (pos >= 0)
            in.seek(pos);

        LOG.info("Reading from file of size " + in.getFileLength() +
                " at offset " + in.getPos());

        while (length > 0) {
            int cnt = in.read(buffer, offset, length);
            assertTrue(cnt > 0, "Error in read");
            offset += cnt;
            length -= cnt;
        }

        // Verify
        for (int i = 0; i < length; ++i) {
            byte actual = buffer[i];
            byte expect = authenticData[(int)pos + i];
            assertEquals(expect, actual,
                    "Read data mismatch at file offset " + (pos + i) +
                            ". Expects " + expect + "; got " + actual);
        }
    }

    @Test
    public void testReadFromOneDN() throws Exception {
        HdfsConfiguration configuration = new HdfsConfiguration();
        // One of the goals of this test is to verify that we don't open more
        // than one socket.  So use a different client context, so that we
        // get our own socket cache, rather than sharing with the other test
        // instances.  Also use a really long socket timeout so that nothing
        // gets closed before we get around to checking the cache size at the end.
        final String contextName = "testReadFromOneDNContext";
        configuration.set(HdfsClientConfigKeys.DFS_CLIENT_CONTEXT, contextName);
        configuration.setLong(HdfsClientConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY,
                100000000L);
        configuration.setLong(DFS_CLIENT_SOCKET_CACHE_EXPIRY_MSEC_KEY, 100000000L);
        BlockReaderTestUtil util = new BlockReaderTestUtil(1, configuration);
        final Path testFile = makeTestPath("testConnCache.dat");
        byte[] authenticData = util.writeFile(testFile, FILE_SIZE / 1024);
        DFSClient client = new DFSClient(
                new InetSocketAddress("localhost",
                        getNameNodePort()), util.getConf());
        ClientContext cacheContext =
                ClientContext.get(contextName, client.getConf());
        DFSInputStream in = client.open(testFile.toString());
        LOG.info("opened " + testFile.toString());
        byte[] dataBuf = new byte[BLOCK_SIZE];

        // Initial read
        pread(in, 0, dataBuf, 0, dataBuf.length, authenticData);
        // Read again and verify that the socket is the same
        pread(in, FILE_SIZE - dataBuf.length, dataBuf, 0, dataBuf.length,
                authenticData);
        pread(in, 1024, dataBuf, 0, dataBuf.length, authenticData);
        // No seek; just read
        pread(in, -1, dataBuf, 0, dataBuf.length, authenticData);
        pread(in, 64, dataBuf, 0, dataBuf.length / 2, authenticData);

        in.close();
        client.close();
        // konna : no cache
//        assertEquals(1,
//                ClientContext.getFromConf(configuration).getPeerCache().size());
    }
}