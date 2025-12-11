package com.aliyun.jindodata.gateway.common;

import com.aliyun.jindodata.gateway.hdfs.datanode.JindoDataNode;
import com.aliyun.jindodata.gateway.hdfs.namenode.JindoNameNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_IPC_ADDRESS_KEY;

public class JindoMiniCluster {
    private static final Logger LOG = LoggerFactory.getLogger(JindoMiniCluster.class);
    private List<JindoNameNode> nameNodes = new ArrayList<>();
    private List<JindoDataNode> dataNodes = new ArrayList<>();
    private boolean running = false;

    JindoMiniCluster(int nnNum, int dnNum, Configuration conf) throws IOException {
        initMultiClusterConf(conf, nnNum, dnNum);
        for (int i = 1; i <= nnNum; i++) {
            Configuration nnConf = new Configuration(conf);
            nnConf.set("dfs.nameservice.id", conf.get("dfs.nameservices"));
            nnConf.set("dfs.ha.namenode.id", "nn" + i);
            nameNodes.add(new JindoNameNode(nnConf));
        }

        for (int i = 0; i < dnNum; i++) {
            Configuration dnConf = new HdfsConfiguration(conf);
            dnConf.set(DFS_DATANODE_IPC_ADDRESS_KEY, "127.0.0.1:0");
            dnConf.set(DFS_DATANODE_ADDRESS_KEY, "127.0.0.1:0");
            dataNodes.add(JindoDataNode.createDataNode(dnConf));
        }

        waitClusterUp();
    }

    JindoMiniCluster(Configuration conf) throws IOException {
        initSingleClusterConf(conf);
        {
            Configuration nnConf = new Configuration(conf);
            nameNodes.add(new JindoNameNode(nnConf));
        }

        {
            Configuration dnConf = new HdfsConfiguration(conf);
            dnConf.set(DFS_DATANODE_IPC_ADDRESS_KEY, "127.0.0.1:0");
            dnConf.set(DFS_DATANODE_ADDRESS_KEY, "127.0.0.1:0");
            dataNodes.add(JindoDataNode.createDataNode(dnConf));
        }

        waitClusterUp();
    }

    public void waitClusterUp() throws IOException {
        int i = 0;
        if (!this.nameNodes.isEmpty()) {
            while(!this.isClusterUp()) {
                try {
                    LOG.warn("Waiting for the Mini HDFS Cluster to start...");
                    Thread.sleep(3000L);
                } catch (InterruptedException var3) {
                }

                ++i;
                if (i > 10) {
                    LOG.error("Timed out waiting for Mini HDFS Cluster to start");
                    throw new IOException("Timed out waiting for Mini HDFS Cluster to start");
                }
            }
        }
        running = true;
    }

    private boolean isClusterUp() {
        for (JindoNameNode nameNode : nameNodes) {
            if (nameNode.getNameSystem().getDatanodeManager().getDatanodeMap().size() != dataNodes.size()) {
                return false;
            }
        }
        return true;
    }

    public static JindoMiniCluster startSingleCluster(Configuration conf) throws IOException {
        initSingleClusterConf(conf);
        return new JindoMiniCluster(conf);
    }

    public static JindoMiniCluster startMultiCluster(Configuration conf) throws IOException {
        initMultiClusterConf(conf, 3, 3);
        return new JindoMiniCluster(3, 3, conf);
    }

    public void stop() {
        running = false;
        for (JindoNameNode nameNode : nameNodes) {
            nameNode.stop();
            nameNode.join();
        }
        for (JindoDataNode dataNode : dataNodes) {
            dataNode.shutdown();
        }
        nameNodes.clear();
        dataNodes.clear();
    }

    public static void initSingleClusterConf(Configuration conf) {
        conf.set("fs.defaultFS", "hdfs://127.0.0.1:8020");
        
        LOG.info("Initialized single cluster configuration: fs.defaultFS=hdfs://127.0.0.1:8020");
    }

    public static void initMultiClusterConf(Configuration conf, int nnNum, int dnNum) {
        // config nameservices
        String nameservices = "jindo-gateway-cluster";
        conf.set("fs.defaultFS", "hdfs://" + nameservices);
        conf.set("dfs.nameservices", nameservices);
        
        // config namenode ids
        StringBuilder namenodeIds = new StringBuilder();
        for (int i = 1; i <= nnNum; i++) {
            if (i > 1) {
                namenodeIds.append(",");
            }
            namenodeIds.append("nn").append(i);
        }
        conf.set("dfs.ha.namenodes." + nameservices, namenodeIds.toString());
        
        // config every namenode ipc address
        int basePort = 9870;
        for (int i = 1; i <= nnNum; i++) {
            String nnId = "nn" + i;
            String rpcAddress = "127.0.0.1:" + (basePort + i - 1);
            conf.set("dfs.namenode.rpc-address." + nameservices + "." + nnId, rpcAddress);
        }
        
        // config failover proxy provider
        conf.set("dfs.client.failover.proxy.provider." + nameservices,
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        
        LOG.info("Initialized cluster configuration: nameservices={}, namenodes={}, nnNum={}, dnNum={}",
                nameservices, namenodeIds.toString(), nnNum, dnNum);
    }

    private void checkSingleNameNode() {
        if (nameNodes.size() != 1) {
            throw new IllegalArgumentException("Namenode index is needed");
        }
    }

    public int getNameNodePort() {
        checkSingleNameNode();
        return getNameNodePort(0);
    }

    public int getNameNodePort(int nnIndex) {
        return nameNodes.get(nnIndex).getNameNodeAddress().getPort();
    }

    public boolean isRunning() {
        return running;
    }
}
