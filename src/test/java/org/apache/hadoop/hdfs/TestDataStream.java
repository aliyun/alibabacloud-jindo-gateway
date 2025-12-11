package org.apache.hadoop.hdfs;

import com.aliyun.jindodata.gateway.JindoMultiClusterTestBase;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Random;

public class TestDataStream extends JindoMultiClusterTestBase {

    static int PACKET_SIZE = 1024;

    @Test
    public void testDfsClient() throws IOException, InterruptedException {
//        LogCapturer logs = GenericTestUtils.LogCapturer.captureLogs(LogFactory
//                .getLog(DataStreamer.class));
        byte[] toWrite = new byte[PACKET_SIZE];
        new Random(1).nextBytes(toWrite);
        final Path path = makeTestPath("file1");
        final DistributedFileSystem dfs = (DistributedFileSystem)BASE_FS;
        FSDataOutputStream out = null;
        out = dfs.create(path, false);

        out.write(toWrite);
        out.write(toWrite);
        out.hflush();

        //Wait to cross slow IO warning threshold
        Thread.sleep(15 * 1000);

        out.write(toWrite);
        out.write(toWrite);
        out.hflush();

        //Wait for capturing logs in busy cluster
        Thread.sleep(5 * 1000);

        out.close();
//        logs.stopCapturing();
//        GenericTestUtils.assertDoesNotMatch(logs.getOutput(),
//                "Slow ReadProcessor read fields for block");
    }
}
