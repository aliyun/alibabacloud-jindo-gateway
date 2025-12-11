package org.apache.hadoop.hdfs;

import com.aliyun.jindodata.gateway.JindoMultiClusterTestBase;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.ClosedChannelException;

import static org.junit.jupiter.api.Assertions.fail;

public class TestClose extends JindoMultiClusterTestBase {
    @Test
    public void testWriteAfterClose() throws IOException {
        try {
            final byte[] data = "foo".getBytes();

            FileSystem fs = BASE_FS;
            OutputStream out = fs.create(makeTestPath("test"));

            out.write(data);
            out.close();
            try {
                // Should fail.
                out.write(data);
                fail("Should not have been able to write more data after file is closed.");
            } catch (ClosedChannelException cce) {
                // We got the correct exception. Ignoring.
            }
            // Should succeed. Double closes are OK.
            out.close();
        } finally {
        }
    }
}