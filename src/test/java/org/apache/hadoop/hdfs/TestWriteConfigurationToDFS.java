package org.apache.hadoop.hdfs;

import com.aliyun.jindodata.gateway.JindoMultiClusterTestBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;

public class TestWriteConfigurationToDFS extends JindoMultiClusterTestBase {

    @Test
    public void testWriteConf() throws Exception {
        Configuration conf = new HdfsConfiguration(BASE_CONF);
        conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 4096);
        System.out.println("Setting conf in: " + System.identityHashCode(conf));
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 0);  // OK?
        FileSystem fs = null;
        OutputStream os = null;
        try {
            fs = getDefaultFS();
            Path filePath = makeTestPath("testWriteConf.xml");
            os = fs.create(filePath);
            StringBuilder longString = new StringBuilder();
            for (int i = 0; i < 100000; i++) {
                longString.append("hello");
            } // 500KB
            conf.set("foobar", longString.toString());
            conf.writeXml(os);
            os.close();
        } finally {
        }
    }
}
