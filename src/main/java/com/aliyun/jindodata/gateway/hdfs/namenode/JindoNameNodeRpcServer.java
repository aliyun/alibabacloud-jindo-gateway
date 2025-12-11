package com.aliyun.jindodata.gateway.hdfs.namenode;

import com.aliyun.jindodata.gateway.call.*;
import com.aliyun.jindodata.gateway.common.JfsStatus;
import com.aliyun.jindodata.gateway.common.JfsUtil;
import com.aliyun.jindodata.gateway.hdfs.protocol.JindoNamenodeProtocols;
import com.google.protobuf.BlockingService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.AddBlockFlag;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.inotify.EventBatchList;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos;
import org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.protocol.*;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.*;

public class JindoNameNodeRpcServer implements JindoNamenodeProtocols {
    public static final Logger LOG =
            LoggerFactory.getLogger(JindoNameNodeRpcServer.class.getName());

    private String bindHost;
    private int port;
    InetSocketAddress rpcAddr;

    protected final JindoNameNode jnn;
    protected final JindoNameSystem nameSystem;

    /** The RPC server that listens to requests from clients */
    protected final RPC.Server clientRpcServer;
//    protected final InetSocketAddress clientRpcAddress;

    public JindoNameNodeRpcServer(Configuration conf, JindoNameNode jnn) throws IOException {
        // build client rpc server
        RPC.setProtocolEngine(conf, ClientNamenodeProtocolPB.class, ProtobufRpcEngine.class);
        ClientNamenodeProtocolServerSideTranslatorPB
                clientProtocolServerTranslator =
                new ClientNamenodeProtocolServerSideTranslatorPB(this);
        BlockingService clientNNPbService = ClientNamenodeProtocolProtos.ClientNamenodeProtocol.
                newReflectiveBlockingService(clientProtocolServerTranslator);

        rpcAddr = getServiceAddress(conf, true);
        bindHost = rpcAddr.getHostName();
        port = rpcAddr.getPort();
        int handlerCount =
                conf.getInt(DFS_NAMENODE_HANDLER_COUNT_KEY,
                        DFS_NAMENODE_HANDLER_COUNT_DEFAULT);

        clientRpcServer = new RPC.Builder(conf)
                .setProtocol(
                        org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolPB.class)
                .setInstance(clientNNPbService)
                .setBindAddress(bindHost)
                .setPort(port)
                .setNumHandlers(handlerCount)
                .setVerbose(false)
                .build();

        // add datanode protocol
        int maxDataLength = conf.getInt(IPC_MAXIMUM_DATA_LENGTH,
                IPC_MAXIMUM_DATA_LENGTH_DEFAULT);
        DatanodeProtocolServerSideTranslatorPB dnProtoPbTranslator =
                new DatanodeProtocolServerSideTranslatorPB(this, maxDataLength);
        BlockingService dnProtoPbService = DatanodeProtocolProtos.DatanodeProtocolService
                .newReflectiveBlockingService(dnProtoPbTranslator);
        DFSUtil.addPBProtocol(conf, DatanodeProtocolPB.class, dnProtoPbService,
                clientRpcServer);

        // NameNode
        this.jnn = jnn;
        nameSystem = jnn.getNameSystem();
    }

    void start() {
        clientRpcServer.start();
    }

    void join() throws InterruptedException {
        clientRpcServer.join();
    }

    void stop() {
        if (clientRpcServer != null) {
            clientRpcServer.stop();
        }
    }

    public InetSocketAddress getRpcAddr() {
        return rpcAddr;
    }

    public static InetSocketAddress getServiceAddress(Configuration conf,
                                                      boolean fallback) {
        String addr = conf.getTrimmed(DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY);
        if (addr == null || addr.isEmpty()) {
            return fallback ? DFSUtilClient.getNNAddress(conf) : null;
        }
        return DFSUtilClient.getNNAddress(addr);
    }

    @Override
    public LocatedBlocks getBlockLocations(String src,
                                           long offset,
                                           long length) throws IOException {
        return nameSystem.getBlockLocations(src, offset, length);
    }

    @Override
    public FsServerDefaults getServerDefaults() throws IOException {
        return nameSystem.getServerDefaults();
    }

    @Override
    public HdfsFileStatus create(String src, FsPermission masked,
                                 String clientName, EnumSetWritable<CreateFlag> flag,
                                 boolean createParent, short replication, long blockSize,
                                 CryptoProtocolVersion[] supportedVersions) throws IOException {
        return nameSystem.create(src, masked, clientName, flag, createParent, replication, blockSize, supportedVersions);
    }

    @Override
    public LastBlockWithStatus append(String src, String clientName,
                                      EnumSetWritable<CreateFlag> flag) throws IOException {
        return nameSystem.append(src, clientName, flag);
    }

    @Override
    public boolean setReplication(String s, short i) throws IOException {
        throw new UnsupportedOperationException("setReplication not supported yet.");
    }

    @Override
    public BlockStoragePolicy[] getStoragePolicies() throws IOException {
        throw new UnsupportedOperationException("getStoragePolicies not supported yet.");
    }

    @Override
    public void setStoragePolicy(String s, String s1) throws IOException {
        throw new UnsupportedOperationException("setStoragePolicy not supported yet.");
    }

    @Override
    public void unsetStoragePolicy(String s) throws IOException {
        throw new UnsupportedOperationException("unsetStoragePolicy not supported yet.");
    }

    @Override
    public BlockStoragePolicy getStoragePolicy(String s) throws IOException {
        throw new UnsupportedOperationException("getStoragePolicy not supported yet.");
    }

    @Override
    public void setPermission(String src, FsPermission fsPermission) throws IOException {
        nameSystem.setPermission(src, fsPermission);
    }

    @Override
    public void setOwner(String src, String username, String groupname) throws IOException {
        nameSystem.setOwner(src, username, groupname);
    }

    @Override
    public void abandonBlock(ExtendedBlock b, long fileId, String src,
                             String holder) throws IOException {
        nameSystem.abandonBlock(b, fileId, src, holder);
    }

    @Override
    public LocatedBlock addBlock(String src, String clientName,
                                 ExtendedBlock previous, DatanodeInfo[] excludedNodes, long fileId,
                                 String[] favoredNodes, EnumSet<AddBlockFlag> addBlockFlags) throws IOException {
        return nameSystem.addBlock(src, clientName, previous, excludedNodes, fileId, favoredNodes, addBlockFlags);
    }

    @Override
    public LocatedBlock getAdditionalDatanode(String s, long l, ExtendedBlock extendedBlock, DatanodeInfo[] datanodeInfos, String[] strings, DatanodeInfo[] datanodeInfos1, int i, String s1) throws IOException {
        throw new UnsupportedOperationException("getAdditionalDatanode not supported yet.");
    }

    @Override
    public boolean complete(String src, String clientName,
                            ExtendedBlock last,  long fileId) throws IOException {
        return nameSystem.complete(src, clientName, last, fileId);
    }

    @Override
    public void reportBadBlocks(LocatedBlock[] locatedBlocks) throws IOException {
        throw new UnsupportedOperationException("reportBadBlocks not supported yet.");
    }

    @Override
    public boolean rename(String src, String dst) throws IOException {
        return nameSystem.rename(src, dst);
    }

    @Override
    public void concat(String s, String[] strings) throws IOException {
        throw new UnsupportedOperationException("concat not supported yet.");
    }

    @Override
    public void rename2(String src, String dst, Options.Rename... options) throws IOException {
        nameSystem.rename2(src, dst, options);
    }

    @Override
    public boolean truncate(String src, long newLength, String clientName) throws IOException {
        return nameSystem.truncate(src, newLength, clientName);
    }

    @Override
    public boolean delete(String src, boolean recursive) throws IOException {
        return nameSystem.delete(src, recursive);
    }

    @Override
    public boolean mkdirs(String src, FsPermission masked, boolean createParent) throws IOException {
        return nameSystem.mkdirs(src, masked, createParent);
    }

    @Override
    public DirectoryListing getListing(String src, byte[] startAfter,
                                       boolean needLocation) throws IOException {
        return nameSystem.getListing(src, startAfter, needLocation);
    }

    @Override
    public SnapshottableDirectoryStatus[] getSnapshottableDirListing() throws IOException {
        throw new UnsupportedOperationException("getSnapshottableDirListing not supported yet.");
    }

    @Override
    public void renewLease(String clientName) throws IOException {
        nameSystem.renewLease(clientName);
    }

    @Override
    public boolean recoverLease(String src, String clientName) throws IOException {
        return nameSystem.recoverLease(src, clientName);
    }

    @Override
    public long[] getStats() throws IOException {
        LOG.info("Receive getStats call");
        return new long[]{Long.MAX_VALUE, 0, Long.MAX_VALUE, 0, 0, 0};
    }

    @Override
    public DatanodeInfo[] getDatanodeReport(HdfsConstants.DatanodeReportType datanodeReportType) throws IOException {
        return nameSystem.datanodeReport(datanodeReportType);
    }

    @Override
    public DatanodeStorageReport[] getDatanodeStorageReport(HdfsConstants.DatanodeReportType datanodeReportType) throws IOException {
        throw new UnsupportedOperationException("getDatanodeStorageReport not supported yet.");
    }

    @Override
    public long getPreferredBlockSize(String s) throws IOException {
        throw new UnsupportedOperationException("getPreferredBlockSize not supported yet.");
    }

    @Override
    public boolean setSafeMode(HdfsConstants.SafeModeAction safeModeAction, boolean b) throws IOException {
        throw new UnsupportedOperationException("setSafeMode not supported yet.");
    }

    @Override
    public void saveNamespace() throws IOException {
        throw new UnsupportedOperationException("saveNamespace not supported yet.");
    }

    @Override
    public long rollEdits() throws IOException {
        throw new UnsupportedOperationException("rollEdits not supported yet.");
    }

    @Override
    public boolean restoreFailedStorage(String s) throws IOException {
        throw new UnsupportedOperationException("restoreFailedStorage not supported yet.");
    }

    @Override
    public void refreshNodes() throws IOException {
        nameSystem.refreshNodes();
    }

    @Override
    public void finalizeUpgrade() throws IOException {
        throw new UnsupportedOperationException("finalizeUpgrade not supported yet.");
    }

    @Override
    public RollingUpgradeInfo rollingUpgrade(HdfsConstants.RollingUpgradeAction rollingUpgradeAction) throws IOException {
        throw new UnsupportedOperationException("rollingUpgrade not supported yet.");
    }

    @Override
    public CorruptFileBlocks listCorruptFileBlocks(String s, String s1) throws IOException {
        throw new UnsupportedOperationException("listCorruptFileBlocks not supported yet.");
    }

    @Override
    public void metaSave(String s) throws IOException {
        throw new UnsupportedOperationException("metaSave not supported yet.");
    }

    @Override
    public void setBalancerBandwidth(long l) throws IOException {
        throw new UnsupportedOperationException("setBalancerBandwidth not supported yet.");
    }

    @Override
    public HdfsFileStatus getFileInfo(String src) throws IOException {
        return nameSystem.getFileInfo(src);
    }

    @Override
    public boolean isFileClosed(String s) throws IOException {
        throw new UnsupportedOperationException("isFileClosed not supported yet.");
    }

    @Override
    public HdfsFileStatus getFileLinkInfo(String s) throws IOException {
        throw new UnsupportedOperationException("getFileLinkInfo not supported yet.");
    }

    @Override
    public ContentSummary getContentSummary(String src) throws IOException {
        return nameSystem.getContentSummary(src);
    }

    @Override
    public void setQuota(String s, long l, long l1, StorageType storageType) throws IOException {
        throw new UnsupportedOperationException("setQuota not supported yet.");
    }

    @Override
    public void fsync(String src, long fileId, String clientName,
                      long lastBlockLength) throws IOException {
        nameSystem.fsync(src, fileId, clientName, lastBlockLength);
    }

    @Override
    public void setTimes(String s, long l, long l1) throws IOException {
        throw new UnsupportedOperationException("setTimes not supported yet.");
    }

    @Override
    public void createSymlink(String s, String s1, FsPermission fsPermission, boolean b) throws IOException {
        throw new UnsupportedOperationException("setTimes not supported yet.");
    }

    @Override
    public String getLinkTarget(String s) throws IOException {
        throw new UnsupportedOperationException("getLinkTarget not supported yet.");
    }

    @Override
    public LocatedBlock updateBlockForPipeline(ExtendedBlock extendedBlock, String clientName) throws IOException {
        LOG.info("updateBlockForPipeline is not supported for DLS, just mock with blockInfo unchanged");
        return new LocatedBlock(extendedBlock, new DatanodeInfo[0]);
    }

    @Override
    public void updatePipeline(String clientName, ExtendedBlock oldBlock,
                               ExtendedBlock newBlock, DatanodeID[] newNodes, String[] newStorageIDs) throws IOException {
        throw new UnsupportedOperationException("updatePipeline not supported for Jindo-Gateway.");
    }

    @Override
    public Token<DelegationTokenIdentifier> getDelegationToken(Text text) throws IOException {
        throw new UnsupportedOperationException("getDelegationToken not supported yet.");
    }

    @Override
    public long renewDelegationToken(Token<DelegationTokenIdentifier> token) throws IOException {
        throw new UnsupportedOperationException("renewDelegationToken not supported yet.");
    }

    @Override
    public void cancelDelegationToken(Token<DelegationTokenIdentifier> token) throws IOException {
        throw new UnsupportedOperationException("cancelDelegationToken not supported yet.");
    }

    @Override
    public DataEncryptionKey getDataEncryptionKey() throws IOException {
        throw new UnsupportedOperationException("getDataEncryptionKey not supported yet.");
    }

    @Override
    public String createSnapshot(String s, String s1) throws IOException {
        throw new UnsupportedOperationException("createSnapshot not supported yet.");
    }

    @Override
    public void deleteSnapshot(String s, String s1) throws IOException {
        throw new UnsupportedOperationException("deleteSnapshot not supported yet.");
    }

    @Override
    public void renameSnapshot(String s, String s1, String s2) throws IOException {
        throw new UnsupportedOperationException("renameSnapshot not supported yet.");
    }

    @Override
    public void allowSnapshot(String s) throws IOException {
        throw new UnsupportedOperationException("allowSnapshot not supported yet.");
    }

    @Override
    public void disallowSnapshot(String s) throws IOException {
        throw new UnsupportedOperationException("disallowSnapshot not supported yet.");
    }

    @Override
    public SnapshotDiffReport getSnapshotDiffReport(String s, String s1, String s2) throws IOException {
        throw new UnsupportedOperationException("getSnapshotDiffReport not supported yet.");
    }

    @Override
    public long addCacheDirective(CacheDirectiveInfo cacheDirectiveInfo, EnumSet<CacheFlag> enumSet) throws IOException {
        throw new UnsupportedOperationException("addCacheDirective not supported yet.");
    }

    @Override
    public void modifyCacheDirective(CacheDirectiveInfo cacheDirectiveInfo, EnumSet<CacheFlag> enumSet) throws IOException {
        throw new UnsupportedOperationException("modifyCacheDirective not supported yet.");
    }

    @Override
    public void removeCacheDirective(long l) throws IOException {
        throw new UnsupportedOperationException("removeCacheDirective not supported yet.");
    }

    @Override
    public BatchedRemoteIterator.BatchedEntries<CacheDirectiveEntry> listCacheDirectives(long l, CacheDirectiveInfo cacheDirectiveInfo) throws IOException {
        throw new UnsupportedOperationException("listCacheDirectives not supported yet.");
    }

    @Override
    public void addCachePool(CachePoolInfo cachePoolInfo) throws IOException {
        throw new UnsupportedOperationException("addCachePool not supported yet.");
    }

    @Override
    public void modifyCachePool(CachePoolInfo cachePoolInfo) throws IOException {
        throw new UnsupportedOperationException("modifyCachePool not supported yet.");
    }

    @Override
    public void removeCachePool(String s) throws IOException {
        throw new UnsupportedOperationException("removeCachePool not supported yet.");
    }

    @Override
    public BatchedRemoteIterator.BatchedEntries<CachePoolEntry> listCachePools(String s) throws IOException {
        throw new UnsupportedOperationException("listCachePools not supported yet.");
    }

    @Override
    public void modifyAclEntries(String s, List<AclEntry> list) throws IOException {
        throw new UnsupportedOperationException("modifyAclEntries not supported yet.");
    }

    @Override
    public void removeAclEntries(String s, List<AclEntry> list) throws IOException {
        throw new UnsupportedOperationException("removeAclEntries not supported yet.");
    }

    @Override
    public void removeDefaultAcl(String s) throws IOException {
        throw new UnsupportedOperationException("removeDefaultAcl not supported yet.");
    }

    @Override
    public void removeAcl(String s) throws IOException {
        throw new UnsupportedOperationException("removeAcl not supported yet.");
    }

    @Override
    public void setAcl(String s, List<AclEntry> list) throws IOException {
        throw new UnsupportedOperationException("setAcl not supported yet.");
    }

    @Override
    public AclStatus getAclStatus(String s) throws IOException {
        throw new UnsupportedOperationException("getAclStatus not supported yet.");
    }

    @Override
    public void createEncryptionZone(String s, String s1) throws IOException {
        throw new UnsupportedOperationException("createEncryptionZone not supported yet.");
    }

    @Override
    public EncryptionZone getEZForPath(String s) throws IOException {
        throw new UnsupportedOperationException("getEZForPath not supported yet.");
    }

    @Override
    public BatchedRemoteIterator.BatchedEntries<EncryptionZone> listEncryptionZones(long l) throws IOException {
        throw new UnsupportedOperationException("listEncryptionZones not supported yet.");
    }

    @Override
    public void setXAttr(String s, XAttr xAttr, EnumSet<XAttrSetFlag> enumSet) throws IOException {
        throw new UnsupportedOperationException("setXAttr not supported yet.");
    }

    @Override
    public List<XAttr> getXAttrs(String s, List<XAttr> list) throws IOException {
        throw new UnsupportedOperationException("getXAttrs not supported yet.");
    }

    @Override
    public List<XAttr> listXAttrs(String s) throws IOException {
        throw new UnsupportedOperationException("listXAttrs not supported yet.");
    }

    @Override
    public void removeXAttr(String s, XAttr xAttr) throws IOException {
        throw new UnsupportedOperationException("removeXAttr not supported yet.");
    }

    @Override
    public void checkAccess(String s, FsAction fsAction) throws IOException {
        throw new UnsupportedOperationException("checkAccess not supported yet.");
    }

    @Override
    public long getCurrentEditLogTxid() throws IOException {
        throw new UnsupportedOperationException("getCurrentEditLogTxid not supported yet.");
    }

    @Override
    public EventBatchList getEditsFromTxid(long l) throws IOException {
        throw new UnsupportedOperationException("getEditsFromTxid not supported yet.");
    }

    @Override
    public QuotaUsage getQuotaUsage(String s) throws IOException {
        throw new UnsupportedOperationException("getQuotaUsage not supported yet.");
    }

    @Override
    public BatchedRemoteIterator.BatchedEntries<OpenFileEntry> listOpenFiles(long l) throws IOException {
        throw new UnsupportedOperationException("listOpenFiles not supported yet.");
    }

    @Override
    public DatanodeRegistration registerDatanode(DatanodeRegistration nodeReg) throws IOException {
        nameSystem.registerDatanode(nodeReg);
        return nodeReg;
    }

    @Override
    public HeartbeatResponse sendHeartbeat(DatanodeRegistration nodeReg,
                                           StorageReport[] report, long dnCacheCapacity, long dnCacheUsed,
                                           int xmitsInProgress, int xceiverCount,
                                           int failedVolumes, VolumeFailureSummary volumeFailureSummary,
                                           boolean requestFullBlockReportLease) throws IOException {
        return nameSystem.handleHeartbeat(nodeReg, report, dnCacheCapacity,
                dnCacheUsed, xmitsInProgress, xceiverCount, failedVolumes, volumeFailureSummary);
    }

    @Override
    public DatanodeCommand blockReport(DatanodeRegistration datanodeRegistration, String s, StorageBlockReport[] storageBlockReports, BlockReportContext blockReportContext) throws IOException {
        throw new UnsupportedOperationException("blockReport not supported yet.");
    }

    @Override
    public DatanodeCommand cacheReport(DatanodeRegistration datanodeRegistration, String s, List<Long> list) throws IOException {
        throw new UnsupportedOperationException("cacheReport not supported yet.");
    }

    @Override
    public void blockReceivedAndDeleted(DatanodeRegistration datanodeRegistration, String s, StorageReceivedDeletedBlocks[] storageReceivedDeletedBlocks) throws IOException {
        throw new UnsupportedOperationException("blockReceivedAndDeleted not supported yet.");
    }

    @Override
    public void errorReport(DatanodeRegistration datanodeRegistration, int i, String s) throws IOException {
        throw new UnsupportedOperationException("errorReport not supported yet.");
    }

    @Override
    public NamespaceInfo versionRequest() throws IOException {
        LOG.info("Receive versionRequest call");
        return nameSystem.getNamespaceInfo();
    }

    @Override
    public void commitBlockSynchronization(ExtendedBlock extendedBlock, long l, long l1, boolean b, boolean b1, DatanodeID[] datanodeIDS, String[] strings) throws IOException {
        throw new UnsupportedOperationException("commitBlockSynchronization not supported yet.");
    }
}
