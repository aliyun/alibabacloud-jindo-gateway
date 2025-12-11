package com.aliyun.jindodata.gateway.hdfs.datanode;

import com.aliyun.jindodata.gateway.common.JfsRequestOptions;
import com.aliyun.jindodata.gateway.common.JfsStatus;
import com.aliyun.jindodata.gateway.common.JfsUtil;
import com.aliyun.jindodata.gateway.io.JfsBlockUploadTaskGroup;
import com.aliyun.jindodata.gateway.io.JfsCloudBlock;
import com.google.protobuf.BlockingService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.ReconfigurationTaskStatus;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HDFSPolicyProvider;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.client.BlockReportOptions;
import org.apache.hadoop.hdfs.net.TcpPeerServer;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos;
import org.apache.hadoop.hdfs.protocolPB.*;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.datanode.*;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.apache.hadoop.hdfs.DFSConfigKeys.*;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY;
import static org.apache.hadoop.util.ExitUtil.terminate;

public class JindoDataNode implements ClientDatanodeProtocol {
    private static final Logger LOG = LoggerFactory.getLogger(JindoDataNode.class);
    private JindoBlockPoolManager blockPoolManager;
    private Configuration conf;
    volatile boolean shouldRun = true;
    private String clusterId = null;
    private JindoDataStorage storage = null;
    private InetSocketAddress streamingAddr;
    private String hostName;
    private int infoPort; // not used
    private int infoSecurePort; // not used
    private DatanodeID id;
    volatile boolean shutdownForUpgrade = false;
    private boolean shutdownInProgress = false;

    // For InterDataNodeProtocol
    public RPC.Server ipcServer;

    Daemon dataXceiverServer = null;
    JindoDataXceiverServer xserver = null;
    ThreadGroup threadGroup = null;

    // conf
    private int socketTimeout;
    private int socketWriteTimeout;
    private int socketKeepaliveTimeout;
    private int transferSocketRecvBufferSize;
    public long heartBeatInterval;
    private long xceiverStopTimeout;
    private long datanodeSlowIoWarningThresholdMs;

    // oss-hdfs conf
    private JfsRequestOptions requestOptions;

    public JindoDataNode(Configuration conf) throws IOException {
        this.conf = conf;
        initJfsRequestOptions(conf);
        try {
            hostName = DNS.getDefaultHost(null, null, false);
            LOG.info("Configured hostname is {}", hostName);
            startDataNode();
        } catch (IOException ie) {
            shutdown();
            throw ie;
        }
    }

    private void initJfsRequestOptions(Configuration conf) {
        requestOptions = new JfsRequestOptions();
        requestOptions.initWithConfiguration(conf);
        JfsBlockUploadTaskGroup.setUploadThreadPoolSize(requestOptions.getUploadThreads());
    }


    void startDataNode() throws IOException {
        initConf();

        storage = new JindoDataStorage();

        initDataXceiver();
        initIpcServer();

        blockPoolManager = new JindoBlockPoolManager(this);
        blockPoolManager.refreshNamenodes(conf);
    }

    private void initConf() {
        socketTimeout = conf.getInt(DFS_CLIENT_SOCKET_TIMEOUT_KEY,
                HdfsConstants.READ_TIMEOUT);
        socketWriteTimeout = conf.getInt(DFS_DATANODE_SOCKET_WRITE_TIMEOUT_KEY,
                HdfsConstants.WRITE_TIMEOUT);
        socketKeepaliveTimeout = conf.getInt(
                DFSConfigKeys.DFS_DATANODE_SOCKET_REUSE_KEEPALIVE_KEY,
                DFSConfigKeys.DFS_DATANODE_SOCKET_REUSE_KEEPALIVE_DEFAULT);
        transferSocketRecvBufferSize = conf.getInt(
                DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_KEY,
                DFSConfigKeys.DFS_DATANODE_TRANSFER_SOCKET_RECV_BUFFER_SIZE_DEFAULT);
        heartBeatInterval = conf.getLong(DFS_HEARTBEAT_INTERVAL_KEY,
                DFS_HEARTBEAT_INTERVAL_DEFAULT) * 1000L;
        xceiverStopTimeout = conf.getLong(
                DFS_DATANODE_XCEIVER_STOP_TIMEOUT_MILLIS_KEY,
                DFS_DATANODE_XCEIVER_STOP_TIMEOUT_MILLIS_DEFAULT);
        datanodeSlowIoWarningThresholdMs = conf.getLong(
                DFSConfigKeys.DFS_DATANODE_SLOW_IO_WARNING_THRESHOLD_KEY,
                DFSConfigKeys.DFS_DATANODE_SLOW_IO_WARNING_THRESHOLD_DEFAULT);
    }

    /** Instantiate a single datanode object. This must be run by invoking
     *  {@link DataNode#runDatanodeDaemon()} subsequently.
     */
    public static JindoDataNode instantiateDataNode(String[] args, Configuration conf) throws IOException {
        if (conf == null)
            conf = new HdfsConfiguration();

        if (args != null) {
            // parse generic hadoop options
            GenericOptionsParser hParser = new GenericOptionsParser(conf, args);
            args = hParser.getRemainingArgs();
        }

        return new JindoDataNode(conf);
    }

    public static JindoDataNode createDataNode(Configuration conf) throws IOException {
        return createDataNode(new String[0], conf);
    }

    public static JindoDataNode createDataNode(String[] args, Configuration conf) throws IOException {
        JindoDataNode dn = instantiateDataNode(args, conf);
        dn.runDatanodeDaemon();
        return dn;
    }

    void join() {
        while (shouldRun) {
            try {
                blockPoolManager.joinAll();
                if (blockPoolManager.getAllNamenodeThreads().isEmpty()) {
                    shouldRun = false;
                }
                // Terminate if shutdown is complete or 2 seconds after all BPs
                // are shutdown.
                synchronized(this) {
                    wait(2000);
                }
            } catch (InterruptedException ex) {
                LOG.warn("Received exception in Datanode#join: {}", String.valueOf(ex));
            }
        }
    }

    public static void main(String[] argv) {
        int errorCode = 0;
        try {
            StringUtils.startupShutdownMessage(JindoDataNode.class, argv, LOG);
            JindoDataNode datanode = createDataNode(argv, null);
            if (datanode != null) {
                datanode.join();
            } else {
                errorCode = 1;
            }
        } catch (Throwable e) {
            LOG.error("Exception in secureMain", e);
            terminate(1, e);
        } finally {
            // We need to terminate the process here because either shutdown was called
            // or some disk related conditions like volumes tolerated or volumes required
            // condition was not met. Also, In secure mode, control will go to Jsvc
            // and Datanode process hangs if it does not exit.
            LOG.warn("Exiting Datanode");
            terminate(errorCode);
        }
    }

    /** Start a single datanode daemon and wait for it to finish.
     *  If this thread is specifically interrupted, it will stop waiting.
     */
    public void runDatanodeDaemon() throws IOException {
        // start heart beat thread
        blockPoolManager.startAll();
        // start dataXceiveServer
        dataXceiverServer.start();
        // start rpc sever
        ipcServer.start();
    }

    /**
     * Remove the given block pool from the block scanner, dataset, and storage.
     */
    void shutdownBlockPool(JindoBPOfferService bpos) {
        blockPoolManager.remove(bpos);
    }

    /**
     * Connect to the NN. This is separated out for easier testing.
     */
    public DatanodeProtocolClientSideTranslatorPB connectToNN(InetSocketAddress nnAddr) throws IOException {
        return new DatanodeProtocolClientSideTranslatorPB(nnAddr, conf);
    }

    boolean shouldRun() {
        return shouldRun;
    }

    /**
     * One of the Block Pools has successfully connected to its NN.
     * This initializes the local storage for that block pool,
     * checks consistency of the NN's cluster ID, etc.
     *
     * If this is the first block pool to register, this also initializes
     * the datanode-scoped storage.
     *
     * @param bpos Block pool offer service
     * @throws IOException if the NN is inconsistent with the local storage.
     */
    void initBlockPool(JindoBPOfferService bpos) throws IOException {
        NamespaceInfo nsInfo = bpos.getNamespaceInfo();
        if (nsInfo == null) {
            throw new IOException("NamespaceInfo not found: Block pool " + bpos
                    + " should have retrieved namespace info before initBlockPool.");
        }

        setClusterId(nsInfo.clusterID, nsInfo.getBlockPoolID());

        // Register the new block pool with the BP manager.
        blockPoolManager.addBlockPool(bpos);

        // In the case that this is the first block pool to connect, initialize
        // the dataset, block scanners, etc.
        initStorage(nsInfo);
    }

    private synchronized void setClusterId(final String nsCid, final String bpid) throws IOException {
        if(clusterId != null && !clusterId.equals(nsCid)) {
            throw new IOException ("Cluster IDs not matched: dn cid=" + clusterId
                    + " but ns cid="+ nsCid + "; bpid=" + bpid);
        }
        // else
        clusterId = nsCid;
    }

    private void initStorage(final NamespaceInfo nsInfo) throws IOException {
        final String bpid = nsInfo.getBlockPoolID();
        LOG.info("Setting up storage: bpid={};" +
                        "nsInfo={}",
                bpid, nsInfo);

        // If this is a newly formatted DataNode then assign a new DatanodeUuid.
        checkDatanodeUuid();
    }

    /**
     * Verify that the DatanodeUuid has been initialized. If this is a new
     * datanode then we generate a new Datanode Uuid and persist it to disk.
     *
     * @throws IOException
     */
    synchronized void checkDatanodeUuid() throws IOException {
        if (storage.getDatanodeUuid() == null) {
            storage.setDatanodeUuid(generateUuid());
            LOG.info("Generated and persisted new Datanode UUID {}",
                    storage.getDatanodeUuid());
        }
    }

    public static String generateUuid() {
        return UUID.randomUUID().toString();
    }

    /**
     * Create a DatanodeRegistration for a specific block pool.
     * @param nsInfo the namespace info from the first part of the NN handshake
     */
    DatanodeRegistration createBPRegistration(NamespaceInfo nsInfo) {
        StorageInfo storageInfo = new StorageInfo(DataNodeLayoutVersion.CURRENT_LAYOUT_VERSION,
                nsInfo.getNamespaceID(), nsInfo.getClusterID(), nsInfo.getCTime(),
                HdfsServerConstants.NodeType.DATA_NODE);

        DatanodeID dnId = new DatanodeID(
                streamingAddr.getAddress().getHostAddress(), hostName,
                storage.getDatanodeUuid(), getXferPort(), getInfoPort(),
                infoSecurePort, getIpcPort());
        return new DatanodeRegistration(dnId, storageInfo,
                new ExportedBlockKeys(), VersionInfo.getVersion());
    }

    private void initDataXceiver() throws IOException {
        // find free port or use privileged port provided
        TcpPeerServer tcpPeerServer;
        int backlogLength = conf.getInt(
                CommonConfigurationKeysPublic.IPC_SERVER_LISTEN_QUEUE_SIZE_KEY,
                CommonConfigurationKeysPublic.IPC_SERVER_LISTEN_QUEUE_SIZE_DEFAULT);
        tcpPeerServer = new TcpPeerServer(socketWriteTimeout,
                getStreamingAddr(conf), backlogLength);
        if (transferSocketRecvBufferSize > 0) {
            tcpPeerServer.setReceiveBufferSize(
                    transferSocketRecvBufferSize);
        }
        streamingAddr = tcpPeerServer.getStreamingAddr();
        LOG.info("Opened streaming server at {}", streamingAddr);
        this.threadGroup = new ThreadGroup("dataXceiverServer");
        xserver = new JindoDataXceiverServer(tcpPeerServer, conf, this);
        this.dataXceiverServer = new Daemon(threadGroup, xserver);
        this.threadGroup.setDaemon(true); // auto destroy when empty
    }

    static InetSocketAddress getStreamingAddr(Configuration conf) {
        return NetUtils.createSocketAddr(
                conf.getTrimmed(DFS_DATANODE_ADDRESS_KEY, DFS_DATANODE_ADDRESS_DEFAULT));
    }

    public int getXferPort() {
        return streamingAddr.getPort();
    }

    public int getInfoPort() {
        return infoPort;
    }

    public int getIpcPort() {
        return ipcServer.getListenerAddress().getPort();
    }

    private void initIpcServer() throws IOException {
        InetSocketAddress ipcAddr = NetUtils.createSocketAddr(
                conf.getTrimmed(DFS_DATANODE_IPC_ADDRESS_KEY));

        // Add all the RPC protocols that the Datanode implements
        RPC.setProtocolEngine(conf, ClientDatanodeProtocolPB.class,
                ProtobufRpcEngine.class);
        ClientDatanodeProtocolServerSideTranslatorPB clientDatanodeProtocolXlator =
                new ClientDatanodeProtocolServerSideTranslatorPB(this);
        BlockingService service = ClientDatanodeProtocolProtos.ClientDatanodeProtocolService
                .newReflectiveBlockingService(clientDatanodeProtocolXlator);
        ipcServer = new RPC.Builder(conf)
                .setProtocol(ClientDatanodeProtocolPB.class)
                .setInstance(service)
                .setBindAddress(ipcAddr.getHostName())
                .setPort(ipcAddr.getPort())
                .setNumHandlers(
                        conf.getInt(DFS_DATANODE_HANDLER_COUNT_KEY,
                                DFS_DATANODE_HANDLER_COUNT_DEFAULT)).setVerbose(false)
                .build();

        LOG.info("Opened IPC server at {}", ipcServer.getListenerAddress());

        // set service-level authorization security policy
        if (conf.getBoolean(
                CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, false)) {
            ipcServer.refreshServiceAcl(conf, new HDFSPolicyProvider());
        }
    }


    /**
     * Check that the registration returned from a NameNode is consistent
     * with the information in the storage. If the storage is fresh/unformatted,
     * sets the storage ID based on this registration.
     * Also updates the block pool's state in the secret manager.
     */
    synchronized void bpRegistrationSucceeded(DatanodeRegistration bpRegistration) throws IOException {
        id = bpRegistration;

        if(!storage.getDatanodeUuid().equals(bpRegistration.getDatanodeUuid())) {
            throw new IOException("Inconsistent Datanode IDs. Name-node returned "
                    + bpRegistration.getDatanodeUuid()
                    + ". Expecting " + storage.getDatanodeUuid());
        }
    }

    /**
     * Shut down this instance of the datanode.
     * Returns only after shutdown is complete.
     * This method can only be called by the offerService thread.
     * Otherwise, deadlock might occur.
     */
    public void shutdown() {
        List<JindoBPOfferService> bposArray = (this.blockPoolManager == null)
                ? new ArrayList<>()
                : this.blockPoolManager.getAllNamenodeThreads();
        // If shutdown is not for restart, set shouldRun to false early.
        if (!shutdownForUpgrade) {
            shouldRun = false;
        }

        // When shutting down for restart, DataXceiverServer is interrupted
        // in order to avoid any further acceptance of requests, but the peers
        // for block writes are not closed until the clients are notified.
        if (dataXceiverServer != null) {
            try {
                xserver.sendOOBToPeers();
                ((JindoDataXceiverServer) this.dataXceiverServer.getRunnable()).kill();
                this.dataXceiverServer.interrupt();
            } catch (Exception e) {
                // Ignore, since the out of band messaging is advisory.
                LOG.trace("Exception interrupting DataXceiverServer: ", e);
            }
        }

        // Record the time of initial notification
        long timeNotified = Time.monotonicNow();

        // shouldRun is set to false here to prevent certain threads from exiting
        // before the restart prep is done.
        this.shouldRun = false;

        // wait for all data receiver threads to exit
        if (this.threadGroup != null) {
            int sleepMs = 2;
            while (true) {
                // When shutting down for restart, wait 2.5 seconds before forcing
                // termination of receiver threads.
                if (!this.shutdownForUpgrade ||
                        (this.shutdownForUpgrade && (Time.monotonicNow() - timeNotified
                                > 1000))) {
                    this.threadGroup.interrupt();
                    break;
                }
                LOG.info("Waiting for threadgroup to exit, active threads is " +
                        this.threadGroup.activeCount());
                if (this.threadGroup.activeCount() == 0) {
                    break;
                }
                try {
                    Thread.sleep(sleepMs);
                } catch (InterruptedException e) {}
                sleepMs = sleepMs * 3 / 2; // exponential backoff
                if (sleepMs > 200) {
                    sleepMs = 200;
                }
            }
            this.threadGroup = null;
        }
        if (this.dataXceiverServer != null) {
            // wait for dataXceiverServer to terminate
            try {
                this.dataXceiverServer.join();
            } catch (InterruptedException ie) {
            }
        }

        // IPC server needs to be shutdown late in the process, otherwise
        // shutdown command response won't get sent.
        if (ipcServer != null) {
            ipcServer.stop();
        }

        if(blockPoolManager != null) {
            try {
                this.blockPoolManager.shutDownAll(bposArray);
            } catch (InterruptedException ie) {
                LOG.warn("Received exception in BlockPoolManager#shutDownAll: ", ie);
            }
        }
    }

    /** Number of concurrent xceivers per node. */
    public int getXceiverCount() {
        return threadGroup == null ? 0 : threadGroup.activeCount();
    }

    /**
     * @return name useful for logging
     */
    public String getDisplayName() {
        // NB: our DatanodeID may not be set yet
        return hostName + ":" + getXferPort();
    }

    public int getSocketWriteTimeout() {
        return socketWriteTimeout;
    }

    public int getSocketKeepaliveTimeout() {
        return socketKeepaliveTimeout;
    }

    public int getSocketTimeout() {
        return socketTimeout;
    }

    public Configuration getConf() {
        return conf;
    }

    public DatanodeRegistration getDNRegistrationForBP(String bpid)
            throws IOException {
        JindoBPOfferService bpos = blockPoolManager.get(bpid);
        if(bpos==null || bpos.bpRegistration==null) {
            throw new IOException("cannot find BPOfferService for bpid="+bpid);
        }
        return bpos.bpRegistration;
    }

    public JfsRequestOptions getRequestOptions() {
        return requestOptions;
    }

    public long getXceiverStopTimeout() {
        return xceiverStopTimeout;
    }

    public long getDatanodeSlowIoWarningThresholdMs() {
        return datanodeSlowIoWarningThresholdMs;
    }

    @Override
    public long getReplicaVisibleLength(ExtendedBlock extendedBlock) throws IOException {
        JfsCloudBlock block = new JfsCloudBlock(extendedBlock, requestOptions);
        JfsStatus status = block.init();
        if (!status.isOk()) {
            LOG.error("getReplicaVisibleLength failed: {}", status.getMessage());
            JfsUtil.throwException(status);
        }
        return block.getRealSize();
    }

    @Override
    public void refreshNamenodes() throws IOException {
        throw new UnsupportedOperationException("refreshNamenodes not supported yet.");
    }

    @Override
    public void deleteBlockPool(String s, boolean b) throws IOException {
        throw new UnsupportedOperationException("deleteBlockPool not supported yet.");
    }

    @Override
    public BlockLocalPathInfo getBlockLocalPathInfo(ExtendedBlock extendedBlock, Token<BlockTokenIdentifier> token) throws IOException {
        throw new UnsupportedOperationException("getBlockLocalPathInfo not supported yet.");
    }

    @Override
    public HdfsBlocksMetadata getHdfsBlocksMetadata(String s, long[] longs, List<Token<BlockTokenIdentifier>> list) throws IOException {
        throw new UnsupportedOperationException("getHdfsBlocksMetadata not supported yet.");
    }

    @Override
    public void shutdownDatanode(boolean forUpgrade) throws IOException {
        LOG.info("shutdownDatanode command received (upgrade={}). Shutting down Datanode...", forUpgrade);

        // Shutdown can be called only once.
        if (shutdownInProgress) {
            throw new IOException("Shutdown already in progress.");
        }
        shutdownInProgress = true;
        shutdownForUpgrade = forUpgrade;

        // Asynchronously start the shutdown process so that the rpc response can be
        // sent back.
        Thread shutdownThread = new Thread("Async datanode shutdown thread") {
            @Override public void run() {
                if (!shutdownForUpgrade) {
                    // Delay the shutdown a bit if not doing for restart.
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ie) { }
                }
                shutdown();
            }
        };

        shutdownThread.setDaemon(true);
        shutdownThread.start();
    }

    @Override
    public void evictWriters() throws IOException {
        throw new UnsupportedOperationException("evictWriters not supported yet.");
    }

    @Override
    public DatanodeLocalInfo getDatanodeInfo() throws IOException {
        throw new UnsupportedOperationException("getDatanodeInfo not supported yet.");
    }

    @Override
    public void startReconfiguration() throws IOException {
        throw new UnsupportedOperationException("startReconfiguration not supported yet.");
    }

    @Override
    public ReconfigurationTaskStatus getReconfigurationStatus() throws IOException {
        throw new UnsupportedOperationException("getReconfigurationStatus not supported yet.");
    }

    @Override
    public List<String> listReconfigurableProperties() throws IOException {
        throw new UnsupportedOperationException("listReconfigurableProperties not supported yet.");
    }

    @Override
    public void triggerBlockReport(BlockReportOptions blockReportOptions) throws IOException {
        throw new UnsupportedOperationException("triggerBlockReport not supported yet.");
    }

    @Override
    public long getBalancerBandwidth() throws IOException {
        throw new UnsupportedOperationException("getBalancerBandwidth not supported yet.");
    }
}
