package org.apache.hadoop.hdfs;

import com.aliyun.jindodata.gateway.JindoSingleClusterTestBase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;

import static org.junit.jupiter.api.Assertions.*;

public class TestBasicFileOps extends JindoSingleClusterTestBase {
    private static final Log LOG = LogFactory.getLog(TestBasicFileOps.class);

    @Test
    public void TestBasicFileOps() throws InterruptedException {
        Configuration conf = new HdfsConfiguration();
        DistributedFileSystem dfs = null;
        try {
            dfs = (DistributedFileSystem) BASE_FS;
            InetSocketAddress addr =
                    new InetSocketAddress("localhost", getNameNodePort());
            DFSClient client = new DFSClient(addr, conf);
            for (int i = 0; i < 10; i ++) {
                Path file1 = makeTestPath("filestatus.dat");

                LOG.warn("sls create");
                FSDataOutputStream stream = createFile(dfs, file1, 3);
                assertNotNull(stream, "stream cannot be null");
                LOG.info("Created file filestatus.dat with one replicas.");
                LOG.info("Created file filestatus.dat with one replicas.");
                stream.write("1234567890".getBytes());
                // stream.flush();
                stream.close();

                DFSInputStream inputStream = dfs.getClient().open(makeTestPathStr("filestatus.dat"));
                long fileLength = inputStream.getFileLength();
                LOG.info("file length:" + inputStream.getFileLength());
                LOG.info("block size:" + inputStream.getAllBlocks());
                byte[] b = new byte[(int) fileLength];
                inputStream.read(b);
                LOG.info("read result:" + new String(b));

                try {
                    dfs.delete(file1, true);
                } catch (Exception e) {
                    LOG.warn("delete failed" + e);
                }
            }
        } catch (IOException ie) {
            fail(ie.getMessage());
        } finally {
        }
    }

    public static FSDataOutputStream createFile(FileSystem fileSys, Path name,
                                                int repl) throws IOException {
        LOG.info("createFile: Created " + name + " with " + repl + " replica.");
        FSDataOutputStream stm = fileSys.create(name, true,
                fileSys.getConf()
                        .getInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, 4096),
                (short) repl, 1024*1024);
        return stm;
    }
}