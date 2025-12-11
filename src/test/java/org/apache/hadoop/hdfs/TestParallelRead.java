package org.apache.hadoop.hdfs;

import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;

public class TestParallelRead extends TestParallelReadUtil{

    @BeforeEach
    public void before() throws IOException {
        HdfsConfiguration conf = new HdfsConfiguration(BASE_CONF);
        conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY, false);
        conf.setBoolean(HdfsClientConfigKeys.DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC,
                false);
        // dfs.domain.socket.path should be ignored because the previous two keys
        // were set to false.  This is a regression test for HDFS-4473.
        conf.set(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY, "/will/not/be/created");

        try {
            setupCluster(1, conf);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
