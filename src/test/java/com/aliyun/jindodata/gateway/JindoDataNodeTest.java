package com.aliyun.jindodata.gateway;

import com.aliyun.jindodata.gateway.common.JindoMiniCluster;
import com.aliyun.jindodata.gateway.hdfs.datanode.JindoDataNode;
import com.aliyun.jindodata.gateway.hdfs.namenode.JindoNameNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.apache.hadoop.hdfs.DFSConfigKeys.*;
import static org.junit.jupiter.api.Assertions.*;

public class JindoDataNodeTest extends JindoTestBase{

    @BeforeAll
    public static void setUpTest() throws IOException {
        JindoMiniCluster.initSingleClusterConf(BASE_CONF);
    }

    @AfterAll
    public static void tearDownTest() {
    }

    @Test
    public void testStartDatanode() throws IOException, InterruptedException {
        Configuration dnConf = new HdfsConfiguration(BASE_CONF);

        BASE_CONF.set(DFS_DATANODE_IPC_ADDRESS_KEY, "127.0.0.1:0");
        BASE_CONF.set(DFS_DATANODE_ADDRESS_KEY, "127.0.0.1:0");

        JindoDataNode dn = JindoDataNode.createDataNode(dnConf);
        Thread.sleep(15000);

        JindoNameNode nn = createNameNode();
        Thread.sleep(15000);

        stopNameNode(nn);
        Thread.sleep(15000);

        stopDataNode(dn);
    }

    @Test
    public void testStart2DN() throws IOException, InterruptedException {
        JindoNameNode nn = createNameNode();

        {
            JindoDataNode dn1 = createDataNode();
            JindoDataNode dn2 = createDataNode();

            Thread.sleep(8000);
            assertEquals(2, nn.getNameSystem().getDatanodeManager().getDatanodeMap().size());

            stopDataNode(dn1);
            stopDataNode(dn2);
        }

        stopNameNode(nn);
    }
}
