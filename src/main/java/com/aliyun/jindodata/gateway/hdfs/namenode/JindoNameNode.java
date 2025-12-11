package com.aliyun.jindodata.gateway.hdfs.namenode;

import com.aliyun.jindodata.gateway.common.JfsRequestOptions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.security.SecureRandom;
import java.util.Arrays;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.*;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.server.namenode.NameNode.NAMENODE_SPECIFIC_KEYS;
import static org.apache.hadoop.hdfs.server.namenode.NameNode.NAMESERVICE_SPECIFIC_KEYS;
import static org.apache.hadoop.util.ExitUtil.terminate;

public class JindoNameNode {
    private JindoNameSystem nameSystem;
    private JindoNameNodeRpcServer rpcServer;
    private JfsRequestOptions jfsRequestOptions;
    private final String clientName = generateClientName();

    public static final Logger LOG =
            LoggerFactory.getLogger(JindoNameNode.class.getName());


    public JindoNameNode(Configuration conf) throws IOException {
        String nsId = DFSUtil.getNamenodeNameServiceId(conf);
        String namenodeId = HAUtil.getNameNodeId(conf, nsId);

        initializeGenericKeys(conf, nsId, namenodeId);
        initialize(conf);
    }

    public static void initializeGenericKeys(Configuration conf,
                                             String nameserviceId, String namenodeId) {
        if ((nameserviceId != null && !nameserviceId.isEmpty()) ||
                (namenodeId != null && !namenodeId.isEmpty())) {
            if (nameserviceId != null) {
                conf.set(DFS_NAMESERVICE_ID, nameserviceId);
            }
            if (namenodeId != null) {
                conf.set(DFS_HA_NAMENODE_ID_KEY, namenodeId);
            }

            DFSUtil.setGenericConf(conf, nameserviceId, namenodeId,
                    NAMENODE_SPECIFIC_KEYS);
            DFSUtil.setGenericConf(conf, nameserviceId, null,
                    NAMESERVICE_SPECIFIC_KEYS);
        }

        // If the RPC address is set use it to (re-)configure the default FS
        if (conf.get(DFS_NAMENODE_RPC_ADDRESS_KEY) != null) {
            URI defaultUri = URI.create(HdfsConstants.HDFS_URI_SCHEME + "://"
                    + conf.get(DFS_NAMENODE_RPC_ADDRESS_KEY));
            conf.set(FS_DEFAULT_NAME_KEY, defaultUri.toString());
            if (LOG.isDebugEnabled()) {
                LOG.debug("Setting " + FS_DEFAULT_NAME_KEY + " to {}", defaultUri.toString());
            }
        }
    }

    protected void initialize(Configuration conf) throws IOException {
        loadNameSystem(conf);

        initJfsRequestOptions(conf);
        rpcServer = createRpcServer(conf);
        startCommonServices(conf);
    }

    public void stop() {
        stopCommonServices();
    }

    public void join() {
        try {
            rpcServer.join();
        } catch (InterruptedException ie) {
            LOG.info("Caught interrupted exception ", ie);
        }
    }

    private void startCommonServices(Configuration conf) {
        nameSystem.activate();
        rpcServer.start();
        LOG.info("RPC up at: {}", getNameNodeAddress());

    }

    private void stopCommonServices() {
        if (rpcServer != null) rpcServer.stop();
        if (nameSystem != null) nameSystem.close();
    }

    private void loadNameSystem(Configuration conf) throws IOException {
        nameSystem = new JindoNameSystem(conf, this);
    }

    private JindoNameNodeRpcServer createRpcServer(Configuration conf) throws IOException {
        return new JindoNameNodeRpcServer(conf, this);
    }

    private void initJfsRequestOptions(Configuration conf) {
        jfsRequestOptions = new JfsRequestOptions();
        jfsRequestOptions.initWithConfiguration(conf);
        jfsRequestOptions.setClientName(clientName);
    }

    public static String generateClientName() {
        StringBuilder sb = new StringBuilder();

        // H{IP}
        sb.append("H");
        try {
            String localIp = InetAddress.getLocalHost().getHostAddress();
            sb.append(localIp);
        } catch (Exception e) {
            LOG.warn("Failed to get local IP address: {}", e.getMessage());
            sb.append("unknown");
        }

        // s{timestamp}
        sb.append("s");
        long nowSec = System.currentTimeMillis() / 1000;
        sb.append(nowSec);

        // r{random}
        sb.append("r");
        SecureRandom random = new SecureRandom();
        long randomValue = random.nextLong() & 0xFFFFFFFFL;
        sb.append(randomValue);

        // p{pid}
        sb.append("p");
        try {
            String processId = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
            sb.append(processId);
        } catch (Exception e) {
            LOG.warn("Failed to get process ID: {}", e.getMessage());
            sb.append("0");
        }

        // t{threadId}
        sb.append("t");
        long threadId = Thread.currentThread().getId();
        sb.append(threadId);

        String clientName = sb.toString();
        LOG.debug("Generated client name: {}", clientName);
        return clientName;
    }

    public JfsRequestOptions getJfsRequestOptions() {
        return jfsRequestOptions;
    }

    public JindoNameSystem getNameSystem() {
        return nameSystem;
    }

    public JindoNameNodeRpcServer getRpcServer() {
        return rpcServer;
    }

    public InetSocketAddress getNameNodeAddress() {
        return rpcServer.getRpcAddr();
    }

    public static UserGroupInformation getRemoteUser() throws IOException {
        UserGroupInformation ugi = Server.getRemoteUser();
        return (ugi != null) ? ugi : UserGroupInformation.getCurrentUser();
    }

    public static JindoNameNode createNameNode(String[] argv, Configuration conf) throws IOException {
        LOG.info("createNameNode {}", Arrays.asList(argv));
        if (conf == null)
            conf = new HdfsConfiguration();
        // Parse out some generic args into Configuration.
        GenericOptionsParser hParser = new GenericOptionsParser(conf, argv);

        return new JindoNameNode(conf);
    }

    public static void main(String[] argv) {
        try {
            StringUtils.startupShutdownMessage(JindoNameNode.class, argv, LOG);
            JindoNameNode namenode = createNameNode(argv, null);
            if (namenode != null) {
                namenode.join();
            }
        } catch (Throwable e) {
            LOG.error("Failed to start namenode.", e);
            terminate(1, e);
        }
    }
}
