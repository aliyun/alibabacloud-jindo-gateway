package org.apache.hadoop.hdfs;

import com.aliyun.jindodata.gateway.JindoMultiClusterTestBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.util.Random;

public class TestAppendDifferentChecksum extends JindoMultiClusterTestBase {
    private static final int SEGMENT_LENGTH = 1500;
    private static final long RANDOM_TEST_RUNTIME = 5000;

    @Test
    public void testSwitchAlgorithms() throws IOException {
        FileSystem fsWithCrc32 = createFsWithChecksum("CRC32", 512);
        FileSystem fsWithCrc32C = createFsWithChecksum("CRC32C", 512);

        Path p = makeTestPath("testSwitchAlgorithms");
        appendWithTwoFs(p, fsWithCrc32, fsWithCrc32C);
        // Regardless of which FS is used to read, it should pick up
        // the on-disk checksum!
        DfsAppendTestUtil.check(fsWithCrc32C, p, SEGMENT_LENGTH * 2);
        DfsAppendTestUtil.check(fsWithCrc32, p, SEGMENT_LENGTH * 2);
    }

    @Timeout(RANDOM_TEST_RUNTIME*2)
    @Test
    public void testAlgoSwitchRandomized() throws IOException {
        FileSystem fsWithCrc32 = createFsWithChecksum("CRC32", 512);
        FileSystem fsWithCrc32C = createFsWithChecksum("CRC32C", 512);

        Path p = makeTestPath("testAlgoSwitchRandomized");
        long seed = Time.now();
        System.out.println("seed: " + seed);
        Random r = new Random(seed);

        // Create empty to start
        IOUtils.closeStream(fsWithCrc32.create(p));

        long st = Time.now();
        int len = 0;
        while (Time.now() - st < RANDOM_TEST_RUNTIME) {
            int thisLen = r.nextInt(500);
            FileSystem fs = (r.nextBoolean() ? fsWithCrc32 : fsWithCrc32C);
            FSDataOutputStream stm = fs.append(p);
            try {
                DfsAppendTestUtil.write(stm, len, thisLen);
            } finally {
                stm.close();
            }
            len += thisLen;
        }

        DfsAppendTestUtil.check(fsWithCrc32, p, len);
        DfsAppendTestUtil.check(fsWithCrc32C, p, len);
    }

    private FileSystem createFsWithChecksum(String type, int bytes)
            throws IOException {
        Configuration conf = new Configuration(BASE_FS.getConf());
        conf.set(DFSConfigKeys.DFS_CHECKSUM_TYPE_KEY, type);
        conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, bytes);
        return FileSystem.get(conf);
    }


    private void appendWithTwoFs(Path p, FileSystem fs1, FileSystem fs2)
            throws IOException {
        FSDataOutputStream stm = fs1.create(p);
        try {
            DfsAppendTestUtil.write(stm, 0, SEGMENT_LENGTH);
        } finally {
            stm.close();
        }

        stm = fs2.append(p);
        try {
            DfsAppendTestUtil.write(stm, SEGMENT_LENGTH, SEGMENT_LENGTH);
        } finally {
            stm.close();
        }
    }
}
