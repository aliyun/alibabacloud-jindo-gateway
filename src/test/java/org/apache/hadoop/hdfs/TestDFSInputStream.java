package org.apache.hadoop.hdfs;

import com.aliyun.jindodata.gateway.JindoSingleClusterTestBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

public class TestDFSInputStream extends JindoSingleClusterTestBase {

    private void testSkipInner(Configuration configuration) throws IOException {
        DistributedFileSystem fs = (DistributedFileSystem) getFs(configuration);
        DFSClient client = fs.dfs;
        Path file = makeTestPath("testfile");
        int fileLength = 1 << 22;
        byte[] fileContent = new byte[fileLength];
        for (int i = 0; i < fileLength; i++) {
            fileContent[i] = (byte) (i % 133);
        }
        FSDataOutputStream fout = fs.create(file);
        fout.write(fileContent);
        fout.close();
        Random random = new Random();
        for (int i = 3; i < 18; i++) {
            DFSInputStream fin = client.open(makeTestPathStr("testfile"));
            for (long pos = 0; pos < fileLength;) {
                long skip = random.nextInt(1 << i) + 1;
                long skipped = fin.skip(skip);
                if (pos + skip >= fileLength) {
                    assertEquals(fileLength, pos + skipped);
                    break;
                } else {
                    assertEquals(skip, skipped);
                    pos += skipped;
                    int data = fin.read();
                    assertEquals((byte) (pos % 133), (byte) data);
                    pos += 1;
                }
            }
            fin.close();
        }
    }

    @Timeout(60)
    @Test
    public void testSkipWithRemoteBlockReader() throws IOException {
        Configuration conf = new Configuration();
        conf.setBoolean(HdfsClientConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADER, true);
        testSkipInner(conf);
    }

    @Timeout(60)
    @Test
    public void testSkipWithRemoteBlockReader2() throws IOException {
        Configuration conf = new Configuration();
        testSkipInner(conf);
    }

    @Test
    public void testSeekToNewSource() throws IOException {
        DistributedFileSystem fs = getDFS();
        Path path = makeTestPath("testfile");
        createFile(fs, path, 1024, (short) 3, 0);
        DFSInputStream fin = fs.dfs.open(makeTestPathStr("testfile"));
        try {
            fin.seekToNewSource(100);
            assertEquals(100, fin.getPos());
            DatanodeInfo firstNode = fin.getCurrentDatanode();
            assertNotNull(firstNode);
            fin.seekToNewSource(100);
            assertEquals(100, fin.getPos());
            // konna : only one datanode
            assertEquals(firstNode, fin.getCurrentDatanode());
        } finally {
            fin.close();
        }
    }

    @Test
    public void testOpenInfo() throws IOException {
        Configuration conf = new Configuration();
        conf.setInt(HdfsClientConfigKeys.Retry.TIMES_GET_LAST_BLOCK_LENGTH_KEY, 0);

        DistributedFileSystem fs = getDFS(conf);

        int chunkSize = 512;
        Random r = new Random(12345L);
        byte[] data = new byte[chunkSize];
        r.nextBytes(data);

        Path file = makeTestPath("testfile");
        try(FSDataOutputStream fout = fs.create(file)) {
            fout.write(data);
        }

        DfsClientConf dcconf = new DfsClientConf(conf);
        int retryTimesForGetLastBlockLength =
                dcconf.getRetryTimesForGetLastBlockLength();
        assertEquals(0, retryTimesForGetLastBlockLength);

        try(DFSInputStream fin = fs.dfs.open(makeTestPathStr("testfile"))) {
            long flen = fin.getFileLength();
            assertEquals(chunkSize, flen);

            long lastBlockBeingWrittenLength =
                    fin.getlastBlockBeingWrittenLengthForTesting();
            assertEquals(0, lastBlockBeingWrittenLength);
        }

    }
}