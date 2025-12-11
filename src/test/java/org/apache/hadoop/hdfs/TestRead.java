package org.apache.hadoop.hdfs;

import com.aliyun.jindodata.gateway.JindoMultiClusterTestBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.TestUtil.ShortCircuitTestContext;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class TestRead extends JindoMultiClusterTestBase {
    final private int BLOCK_SIZE = 512;

    private void testEOF(Configuration conf, int fileLength) throws IOException {
        FileSystem fs = getFs(conf);
        Path path = makeTestPath("testEOF." + fileLength);
        createFile(fs, path, fileLength, (short)1, 0xBEEFBEEF);
        FSDataInputStream fis = fs.open(path);
        ByteBuffer empty = ByteBuffer.allocate(0);
        // A read into an empty bytebuffer at the beginning of the file gives 0.
        assertEquals(0, fis.read(empty));
        fis.seek(fileLength);
        // A read into an empty bytebuffer at the end of the file gives -1.
        assertEquals(-1, fis.read(empty));
        if (fileLength > BLOCK_SIZE) {
            fis.seek(fileLength - BLOCK_SIZE + 1);
            ByteBuffer dbb = ByteBuffer.allocateDirect(BLOCK_SIZE);
            assertEquals(BLOCK_SIZE - 1, fis.read(dbb));
        }
        fis.close();
    }

    @Test
    public void testEOFWithBlockReaderLocal() throws Exception {
        ShortCircuitTestContext testContext =
                new ShortCircuitTestContext("testEOFWithBlockReaderLocal");
        Configuration conf = testContext.newConfiguration(BASE_CONF);
        conf.setLong(HdfsClientConfigKeys.DFS_CLIENT_CACHE_READAHEAD, BLOCK_SIZE);
        try {
            testEOF(conf,1);
            testEOF(conf,14);
            testEOF(conf,10000);
        } finally {
        }
    }

    @Test
    public void testEOFWithRemoteBlockReader() throws Exception {
        final Configuration conf = new Configuration(BASE_CONF);
        conf.setLong(HdfsClientConfigKeys.DFS_CLIENT_CACHE_READAHEAD, BLOCK_SIZE);
        testEOF(conf, 1);
        testEOF(conf, 14);
        testEOF(conf, 10000);
    }

    @Test
    public void testReadReservedPath() throws Exception {
        Configuration conf = new Configuration(BASE_CONF);
        try {
            FileSystem fs = getDefaultFS();
            fs.open(makeTestPath(".reserved/.inodes/file"));
            fail("Open a non existing file should fail.");
        } catch (FileNotFoundException e) {
            // Expected
        } finally {
        }
    }
}