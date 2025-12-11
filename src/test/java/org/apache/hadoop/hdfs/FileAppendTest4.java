package org.apache.hadoop.hdfs;

import com.aliyun.jindodata.gateway.JindoSingleClusterTestBase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class FileAppendTest4 extends JindoSingleClusterTestBase {
    public static final Log LOG = LogFactory.getLog(FileAppendTest4.class);

    private static final int BYTES_PER_CHECKSUM = 4;
    private static final int PACKET_SIZE = BYTES_PER_CHECKSUM;
//    private static final int BLOCK_SIZE = 2 * PACKET_SIZE;
    private static final int BLOCK_SIZE = 1048576;
    private static final short REPLICATION = 3;
    private static Configuration conf;
    private static DistributedFileSystem fs;

    @BeforeEach
    public void before() throws IOException {
        conf = new Configuration(BASE_CONF);
        fs = (DistributedFileSystem) BASE_FS;
    }

    @Test
    public void testAppend() throws IOException {
        final int maxOldFileLen = 2 * BLOCK_SIZE + 1;
        final int maxFlushedBytes = BLOCK_SIZE;
        byte[] contents =
                DfsAppendTestUtil.initBuffer(maxOldFileLen + 2 * maxFlushedBytes);
        int step = (BLOCK_SIZE / 8 > 0) ? BLOCK_SIZE / 8 : 1;
        for (int oldFileLen = 0; oldFileLen <= maxOldFileLen; oldFileLen = next(oldFileLen, step, maxOldFileLen)) {
            for (int flushedBytes1 =
                 0; flushedBytes1 <= maxFlushedBytes; flushedBytes1 = next(flushedBytes1, step, maxFlushedBytes)) {
                for (int flushedBytes2 =
                     0; flushedBytes2 <= maxFlushedBytes; flushedBytes2 = next(flushedBytes2, step, maxFlushedBytes)) {
                    final int fileLen = oldFileLen + flushedBytes1 + flushedBytes2;
                    // create the initial file of oldFileLen
                    final Path p = makeTestPath(
                            "foo" + oldFileLen + "_" + flushedBytes1 + "_" + flushedBytes2);
                    LOG.info("Creating file " + p);
                    FSDataOutputStream out = fs.create(p, false, conf
                                    .getInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, 4096),
                            REPLICATION, BLOCK_SIZE);
                    out.write(contents, 0, oldFileLen);
                    out.close();

                    // append flushedBytes bytes to the file
                    out = fs.append(p);
                    out.write(contents, oldFileLen, flushedBytes1);
                    out.hflush();

                    // write another flushedBytes2 bytes to the file
                    out.write(contents, oldFileLen + flushedBytes1, flushedBytes2);
                    out.close();

                    // validate the file content
                    DfsAppendTestUtil.checkFullFile(fs, p, fileLen, contents, p.toString());
                    fs.delete(p, false);
                }
            }
        }
    }

    @Test
    public void tesAppendFile() throws IOException {
        Path path = makeTestPath("append.txt");
        byte[] content = "Hello, World!".getBytes(StandardCharsets.UTF_8);
        try (OutputStream out = fs.create(path, false, conf
                        .getInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, 4096),
                REPLICATION, BLOCK_SIZE)) {
        }
        try (FSDataOutputStream out = fs.append(path)) {
            for (int i = 0; i < 10000; i++)
                out.write(content);
        }
        System.out.println("File written successfully.");

        byte[] res = new byte[content.length];
        FSDataInputStream in = fs.open(path);
        in.readFully(0, res);
        in.close();
        Assertions.assertEquals(new String(content), new String(res));

        fs.truncate(path, content.length);
        in = fs.open(path);
        in.readFully(0, res);
        in.close();
        Assertions.assertEquals(new String(content), new String(res));
    }

    public int next(int current, int step, int limit) {
        if (current == limit) {
            return current + 1;
        }
        int next = current + step;
        if (next > limit) {
            next = limit;
        }
        return next;
    }
}
