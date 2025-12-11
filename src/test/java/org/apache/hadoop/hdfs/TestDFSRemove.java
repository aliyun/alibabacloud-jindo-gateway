package org.apache.hadoop.hdfs;

import com.aliyun.jindodata.gateway.JindoMultiClusterTestBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.DataOutputStream;
import java.io.IOException;

public class TestDFSRemove extends JindoMultiClusterTestBase {
    final Path dir = makeTestPath("test/remove/");

    static void createFile(FileSystem fs, Path f) throws IOException {
        DataOutputStream a_out = fs.create(f);
        a_out.writeBytes("something");
        a_out.close();
    }

    @Test
    public void testRemove() throws Exception {
        Configuration conf = new HdfsConfiguration();
        try {
            FileSystem fs = getDefaultFS();
            Assertions.assertTrue(fs.mkdirs(dir));
            {
                // Create 100 files
                final int fileCount = 100;
                for (int i = 0; i < fileCount; i++) {
                    Path a = new Path(dir, "a" + i);
                    createFile(fs, a);
                }
                // Remove 100 files
                for (int i = 0; i < fileCount; i++) {
                    Path a = new Path(dir, "a" + i);
                    fs.delete(a, false);
                }
                // wait 3 heartbeat intervals, so that all blocks are deleted.
                Thread.sleep(3 * DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT * 1000);
                // all blocks should be gone now.
            }

            fs.delete(dir, true);
        } finally {
        }
    }

    @Test
    public void testRemoveNonExist() throws Exception {
        FileSystem fs = getDefaultFS();
        boolean result = fs.delete(makeTestPath("NonExistFile"), true);
        Assertions.assertFalse(result);
    }
}
