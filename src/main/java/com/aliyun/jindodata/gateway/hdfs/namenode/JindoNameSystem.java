package com.aliyun.jindodata.gateway.hdfs.namenode;

import com.aliyun.jindodata.gateway.call.*;
import com.aliyun.jindodata.gateway.common.JfsStatus;
import com.aliyun.jindodata.gateway.common.JfsUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.hdfs.AddBlockFlag;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.protocol.*;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.util.DataChecksum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.*;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CHECKSUM_TYPE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CHECKSUM_TYPE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_REPLICATION_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_REPLICATION_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY;
import static org.apache.hadoop.hdfs.protocol.HdfsConstants.HOT_STORAGE_POLICY_ID;

public class JindoNameSystem {
    private static final Logger LOG =
            LoggerFactory.getLogger(JindoNameSystem.class.getName());
    private int namespaceId = 0;
    private String clusterId = "default-cluster-id";
    private String blockPoolId = "default-block-pool";
    private long cTime = 0;
    private int layoutVersion = 0;

    private final ReadWriteLock rwLock;
    private final JindoDatanodeManager datanodeManager;
    private volatile boolean fsRunning = true;
    private final FsServerDefaults serverDefaults;
    private final JindoNameNode nn;

    public JindoNameSystem(Configuration conf, JindoNameNode nn) throws IOException {
        rwLock = new ReentrantReadWriteLock();
        this.nn = nn;
        datanodeManager = new JindoDatanodeManager(conf, this);

        namespaceId = conf.getInt("namenode.namespace.id", 0);
        clusterId = conf.get("namenode.cluster.id", "default-cluster-id");
        blockPoolId = conf.get("namenode.block-pool.id", "default-block-pool");
        cTime = conf.getLong("namenode.ctime", 0);
        layoutVersion = conf.getInt("namenode.layout-version", 0);

        // Get the checksum type from config
        String checksumTypeStr = conf.get(DFS_CHECKSUM_TYPE_KEY, DFS_CHECKSUM_TYPE_DEFAULT);
        DataChecksum.Type checksumType;
        try {
            checksumType = DataChecksum.Type.valueOf(checksumTypeStr);
        } catch (IllegalArgumentException iae) {
            throw new IOException("Invalid checksum type in "
                    + DFS_CHECKSUM_TYPE_KEY + ": " + checksumTypeStr);
        }
        serverDefaults = new FsServerDefaults(
                conf.getLongBytes(DFS_BLOCK_SIZE_KEY, DFS_BLOCK_SIZE_DEFAULT),
                conf.getInt(DFS_BYTES_PER_CHECKSUM_KEY, DFS_BYTES_PER_CHECKSUM_DEFAULT),
                conf.getInt(DFS_CLIENT_WRITE_PACKET_SIZE_KEY, DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT),
                (short) conf.getInt(DFS_REPLICATION_KEY, DFS_REPLICATION_DEFAULT),
                conf.getInt(IO_FILE_BUFFER_SIZE_KEY, IO_FILE_BUFFER_SIZE_DEFAULT),
                conf.getBoolean(DFS_ENCRYPT_DATA_TRANSFER_KEY, DFS_ENCRYPT_DATA_TRANSFER_DEFAULT),
                conf.getLong(FS_TRASH_INTERVAL_KEY, FS_TRASH_INTERVAL_DEFAULT),
                checksumType,
                "",
                HOT_STORAGE_POLICY_ID);
    }

    public NamespaceInfo getNamespaceInfo() {
        return new NamespaceInfo(namespaceId, clusterId, blockPoolId, cTime, HAServiceProtocol.HAServiceState.ACTIVE);
    }

    public void registerDatanode(DatanodeRegistration nodeReg) throws DisallowedDatanodeException {
        LOG.info("Received datanode heartbeat from {}", nodeReg);
        rwLock.writeLock().lock();
        try {
            datanodeManager.registerDatanode(nodeReg);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public HeartbeatResponse handleHeartbeat(DatanodeRegistration nodeReg,
                                             StorageReport[] reports, long cacheCapacity, long cacheUsed,
                                             int xceiverCount, int xmitsInProgress, int failedVolumes,
                                             VolumeFailureSummary volumeFailureSummary) throws DisallowedDatanodeException {
        LOG.info("Received heartbeat from {}", nodeReg);
        rwLock.readLock().lock();
        try {
            DatanodeCommand[] cmds = datanodeManager.handleHeartbeat(nodeReg,
                    reports, cacheCapacity, cacheUsed, xceiverCount, failedVolumes, volumeFailureSummary);

            //create ha status
            final NNHAStatusHeartbeat haState = new NNHAStatusHeartbeat(
                    HAServiceProtocol.HAServiceState.ACTIVE,
                    0);

            return new HeartbeatResponse(cmds, haState, null,
                    0);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public JindoDatanodeManager getDatanodeManager() {
        return datanodeManager;
    }

    public boolean isRunning() {
        return fsRunning;
    }

    public void activate() {
        rwLock.writeLock().lock();
        datanodeManager.activate();
        rwLock.writeLock().unlock();
    }

    public void close() {
        fsRunning = false;

        rwLock.writeLock().lock();
        datanodeManager.close();
        rwLock.writeLock().unlock();
    }

    public void writeLock() {
        rwLock.writeLock().lock();
    }

    public void writeUnlock() {
        rwLock.writeLock().unlock();
    }

    public void readLock() {
        rwLock.readLock().lock();
    }

    public void readUnlock() {
        rwLock.readLock().unlock();
    }

    public FsServerDefaults getServerDefaults() {
        LOG.info("Receive getServerDefaults call");
        return serverDefaults;
    }

    public LocatedBlocks getBlockLocations(String src,
                                           long offset,
                                           long length) throws IOException {
        LOG.info("Receive getBlockLocations call: src={}, offset={}, length={}", src, offset, length);
        JfsGetBlockLocationsCall call = new JfsGetBlockLocationsCall();
        call.setPath(src);
        call.setOffset(offset);
        call.setLength(length);
        JfsStatus status = call.execute(nn.getJfsRequestOptions());

        if (!status.isOk()) {
            LOG.error("getBlockLocations failed: {}", status.getMessage());
            JfsUtil.throwException(status);
        }

        LocatedBlocks locatedBlocks = call.getBlocks();
        for (LocatedBlock locatedBlock : locatedBlocks.getLocatedBlocks()) {
            try {
                DatanodeInfoWithStorage[] newLocs = datanodeManager.chooseRandomNodes(3, null);
                JfsUtil.setFieldValue(locatedBlock, "locs", newLocs);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                LOG.error("Failed to modify locs field via reflection: {}", e.getMessage(), e);
                throw new IOException("Failed to modify block locations", e);
            }
        }

        if (locatedBlocks.getLastLocatedBlock() != null) {
            try {
                DatanodeInfoWithStorage[] newLocs = datanodeManager.chooseRandomNodes(3, null);
                JfsUtil.setFieldValue(locatedBlocks.getLastLocatedBlock(), "locs", newLocs);
            } catch (NoSuchFieldException |IllegalAccessException e) {
                LOG.error("Failed to modify locs field via reflection: {}", e.getMessage(), e);
                throw new IOException("Failed to modify block locations", e);
            }
        }

        return locatedBlocks;
    }

    public HdfsFileStatus create(String src, FsPermission masked,
                                 String clientName, EnumSetWritable<CreateFlag> flag,
                                 boolean createParent, short replication, long blockSize,
                                 CryptoProtocolVersion[] supportedVersions) throws IOException {
        LOG.info("Receive create call: src={}, masked={}, clientName={}, flag={}," +
                        " createParent={}, replication={}, blockSize={}, supportedVersions={}",
                src, masked, clientName, flag, createParent, replication, blockSize, supportedVersions);
        int flagNum = 0;
        for (CreateFlag flag1 : flag.get()) {
            flagNum |= (1 << flag1.ordinal());
        }
        JfsCreateFileCall call = new JfsCreateFileCall();
        call.setPath(src);
        call.setClientName(clientName);
        call.setFlag(flagNum);
        call.setPermission(masked.toShort());
        call.setReplication(replication);
        call.setBlockSize(blockSize);
        call.setCreateParent(createParent);
        JfsStatus status = call.execute(nn.getJfsRequestOptions());

        if (!status.isOk()) {
            LOG.error("create failed: {}", status.getMessage());
            JfsUtil.throwException(status);
        }

        return call.getFileStatus();
    }

    public LastBlockWithStatus append(String src, String clientName,
                                      EnumSetWritable<CreateFlag> flag) throws IOException {
        LOG.info("Receive append call: src={}, clientName={}, flag={}", src, clientName, flag);
        JfsAppendFileCall call = new JfsAppendFileCall();
        call.setPath(src);
        call.setClientName(clientName);
        // not set flag
        JfsStatus status = call.execute(nn.getJfsRequestOptions());
        if (!status.isOk()) {
            LOG.error("append failed: {}", status.getMessage());
            JfsUtil.throwException(status);
        }
        return new LastBlockWithStatus(null, call.getFileStatus());
    }

    public void setPermission(String src, FsPermission fsPermission) throws IOException {
        LOG.info("Receive setPermission call: src={}, fsPermission={}", src, fsPermission);
        JfsSetPermissionCall call = new JfsSetPermissionCall();
        call.setPath(src);
        call.setPermission(fsPermission.toShort());
        JfsStatus status = call.execute(nn.getJfsRequestOptions());
        if (!status.isOk()) {
            LOG.error("setPermission failed: {}", status.getMessage());
            JfsUtil.throwException(status);
        }
    }

    public void setOwner(String src, String username, String groupname) throws IOException {
        LOG.info("Receive setOwner call: src={}, username={}, groupname={}", src, username, groupname);
        JfsSetOwnerCall call = new JfsSetOwnerCall();
        call.setPath(src);
        call.setUser(username);
        call.setGroup(groupname);
        JfsStatus status = call.execute(nn.getJfsRequestOptions());
        if (!status.isOk()) {
            LOG.error("setOwner failed: {}", status.getMessage());
            JfsUtil.throwException(status);
        }
    }

    public void abandonBlock(ExtendedBlock b, long fileId, String src,
                             String holder) throws IOException {
        LOG.info("Receive abandonBlock call: b={}, fileId={}, src={}, holder={}", b, fileId, src, holder);
        JfsAbandonBlockCall call = new JfsAbandonBlockCall();
        call.setBlock(b);
        call.setFileId(fileId);
        call.setPath(src);
        call.setClientName(holder);
        JfsStatus status = call.execute(nn.getJfsRequestOptions());
        if (!status.isOk()) {
            LOG.error("abandonBlock failed: {}", status.getMessage());
            JfsUtil.throwException(status);
        }
    }

    public LocatedBlock addBlock(String src, String clientName,
                                 ExtendedBlock previous, DatanodeInfo[] excludedNodes, long fileId,
                                 String[] favoredNodes, EnumSet<AddBlockFlag> addBlockFlags) throws IOException {
        LOG.info("Receive addBlock call: src={}, clientName={}, previous={}, excludedNodes={}, fileId={}," +
                        " favoredNodes={}, addBlockFlags={}",
                src, clientName, previous, excludedNodes, fileId, favoredNodes, addBlockFlags);
        JfsAddBlockCall call = new JfsAddBlockCall();
        call.setPath(src);
        call.setClientName(clientName);
        call.setPrevious(previous);
        call.setFileId(fileId);
        JfsStatus status = call.execute(nn.getJfsRequestOptions());
        if (!status.isOk()) {
            LOG.error("addBlock failed: {}", status.getMessage());
            JfsUtil.throwException(status);
        }

        if (call.getLocatedBlock() != null) {
            try {
                DatanodeInfoWithStorage[] newLocs = datanodeManager.chooseRandomNodes(3, excludedNodes);
                insertLocationsToBlock(call.getLocatedBlock(), newLocs);
            } catch (NoSuchFieldException |IllegalAccessException e) {
                LOG.error("Failed to modify locs field via reflection: {}", e.getMessage(), e);
                throw new IOException("Failed to modify block locations", e);
            }
        }

        return call.getLocatedBlock();
    }

    /**
     *  only for addBlock
     */
    private void insertLocationsToBlock(LocatedBlock lb, DatanodeInfoWithStorage[] locs)
            throws NoSuchFieldException, IllegalAccessException {
        String[] storageIds = new String[locs.length];
        StorageType[] storageTypes = new StorageType[locs.length];
        for (int i=0; i< locs.length; ++i) {
            storageIds[i] = locs[i].getStorageID();
            storageTypes[i] = locs[i].getStorageType();
        }
        JfsUtil.setFieldValue(lb, "locs", locs);
        JfsUtil.setFieldValue(lb, "storageIDs", storageIds);
        JfsUtil.setFieldValue(lb, "storageTypes", storageTypes);
    }

    public boolean complete(String src, String clientName,
                            ExtendedBlock last,  long fileId) throws IOException {
        LOG.info("Receive complete call: src={}, clientName={}, last={}, fileId={}", src, clientName, last, fileId);
        JfsCompleteFileCall call = new JfsCompleteFileCall();
        call.setPath(src);
        call.setClientName(clientName);
        call.setBlock(last);
        call.setFileId(fileId);
        JfsStatus status = call.execute(nn.getJfsRequestOptions());
        if (!status.isOk()) {
            LOG.error("complete failed: {}", status.getMessage());
            JfsUtil.throwException(status);
        }
        return call.getResult();
    }

    public boolean rename(String src, String dst) throws IOException {
        LOG.info("Receive rename call: src {}, dst {}", src, dst);
        JfsRenameCall call = new JfsRenameCall();
        call.setSrcPath(src);
        call.setDstPath(dst);
        JfsStatus status = call.execute(nn.getJfsRequestOptions());
        if (!status.isOk()) {
            LOG.error("rename failed: {}", status.getMessage());
            JfsUtil.throwException(status);
        }
        return call.getResult();
    }

    public void rename2(String src, String dst, Options.Rename... options) throws IOException {
        LOG.info("Receive rename2 call: src {}, dst {}", src, dst);
        // FileSystem Default implementation
        // not atomic!
        Path srcPath = new Path(src);
        Path dstPath = new Path(dst);
        final HdfsFileStatus srcStatus = getFileInfo(src);
        if (srcStatus == null) {
            throw new FileNotFoundException("rename source " + src + " not found.");
        }

        boolean overwrite = false;
        if (null != options) {
            for (Options.Rename option : options) {
                if (option == Options.Rename.OVERWRITE) {
                    overwrite = true;
                    break;
                }
            }
        }

        HdfsFileStatus dstStatus;
        try {
            dstStatus = getFileInfo(dst);
        } catch (IOException e) {
            dstStatus = null;
        }
        if (dstStatus != null) {
            if (srcStatus.isDir() != dstStatus.isDir()) {
                throw new IOException("Source " + src + " Destination " + dst
                        + " both should be either file or directory");
            }
            if (!overwrite) {
                throw new FileAlreadyExistsException("rename destination " + dst
                        + " already exists.");
            }
            // Delete the destination that is a file or an empty directory
            if (dstStatus.isDir()) {
                DirectoryListing list = getListing(dst, new byte[0], false);
                if (list != null && list.getPartialListing().length != 0) {
                    throw new IOException(
                            "rename cannot overwrite non empty destination directory " + dst);
                }
            }
            delete(dst, false);
        } else {
            final Path parent = dstPath.getParent();
            final HdfsFileStatus parentStatus = getFileInfo(parent.toString());
            if (parentStatus == null) {
                throw new FileNotFoundException("rename destination parent " + parent
                        + " not found.");
            }
            if (!parentStatus.isDir()) {
                throw new ParentNotDirectoryException("rename destination parent " + parent
                        + " is a file.");
            }
        }
        if (!rename(src, dst)) {
            throw new IOException("rename from " + src + " to " + dst + " failed.");
        }
    }

    public boolean truncate(String src, long newLength, String clientName) throws IOException {
        LOG.info("Receive truncate call: src {}, newLength {}, clientName {}", src, newLength, clientName);
        JfsTruncateFileCall call = new JfsTruncateFileCall();
        call.setPath(src);
        call.setSize(newLength);
        call.setClientName(clientName);
        JfsStatus status = call.execute(nn.getJfsRequestOptions());
        if (!status.isOk()) {
            LOG.error("truncate failed: {}", status.getMessage());
            JfsUtil.throwException(status);
        }
        return call.getTruncateResult();
    }

    public boolean delete(String src, boolean recursive) throws IOException {
        LOG.info("Receive delete call: src {}, recursive {}", src, recursive);
        JfsDeleteCall call = new JfsDeleteCall();
        call.setPath(src);
        call.setRecursive(recursive);
        JfsStatus status = call.execute(nn.getJfsRequestOptions());
        if (!status.isOk()) {
            if (status.getCode() == JfsStatus.FILE_NOT_FOUND_ERROR) {
                return call.getResult();
            }
            LOG.error("delete failed: {}", status.getMessage());
            JfsUtil.throwException(status);
        }
        return call.getResult();
    }

    public boolean mkdirs(String src, FsPermission masked, boolean createParent) throws IOException {
        LOG.info("Receive mkdirs call: src {}, masked {}, createParent {}", src, masked, createParent);
        JfsMkdirsCall call = new JfsMkdirsCall();
        call.setPath(src);
        call.setPermission(masked.toShort());
        call.setCreateParent(createParent);
        JfsStatus status = call.execute(nn.getJfsRequestOptions());

        if (!status.isOk()) {
            LOG.error("mkdirs failed: {}", status.getMessage());
            JfsUtil.throwException(status);
        }

        return call.getResult();
    }

    public DirectoryListing getListing(String src, byte[] startAfter,
                                       boolean needLocation) throws IOException {
        LOG.info("Receive getListing call: src {}, startAfter {}, needLocation {}", src, startAfter, needLocation);
        JfsGetListingCall call = new JfsGetListingCall();
        call.setPath(src);
        call.setMarker(new String(startAfter, StandardCharsets.UTF_8));
        call.setNeedLocation(needLocation);
        JfsStatus status = call.execute(nn.getJfsRequestOptions());

        if (!status.isOk()) {
            LOG.error("getListing failed: {}", status.getMessage());
            JfsUtil.throwException(status);
        }

        return call.getDirectoryListing();
    }

    public void renewLease(String clientName) throws IOException {
        LOG.info("Receive renewLease call: clientName {}", clientName);
        JfsRenewLeaseCall call = new JfsRenewLeaseCall();
        call.setClientName(clientName);
        JfsStatus status = call.execute(nn.getJfsRequestOptions());
        if (!status.isOk()) {
            LOG.error("renewLease failed: {}", status.getMessage());
            JfsUtil.throwException(status);
        }
    }

    public boolean recoverLease(String src, String clientName) throws IOException {
        LOG.info("Receive recoverLease call: src {}, clientName {}", src, clientName);
        JfsRecoveryLeaseCall call = new JfsRecoveryLeaseCall();
        call.setPath(src);
        call.setClientName(clientName);
        JfsStatus status = call.execute(nn.getJfsRequestOptions());
        if (!status.isOk()) {
            LOG.error("recoverLease failed: {}", status.getMessage());
            JfsUtil.throwException(status);
        }
        return call.getResult();
    }

    DatanodeInfo[] datanodeReport(final HdfsConstants.DatanodeReportType type) {
        LOG.info("Receive datanodeReport call: type {}", type);
        readLock();
        try {
            final List<DatanodeDescriptor> results = datanodeManager.getDatanodeListForReport(type);

            DatanodeInfo[] arr = new DatanodeInfo[results.size()];
            for (int i = 0; i < arr.length; i++) {
                arr[i] = new DatanodeInfo(results.get(i));
            }
            return arr;
        } finally {
            readUnlock();
        }
    }

    void refreshNodes() throws IOException {
        LOG.info("Receive refreshNodes call");
        datanodeManager.refreshNodes(new HdfsConfiguration());
    }

    public HdfsFileStatus getFileInfo(String src) throws IOException {
        LOG.info("Receive getFileInfo call: src {}", src);
        JfsGetFileInfoCall call = new JfsGetFileInfoCall();
        call.setPath(src);
        JfsStatus status = call.execute(nn.getJfsRequestOptions());
        if (!status.isOk()) {
            if (status.getCode() == JfsStatus.FILE_NOT_FOUND_ERROR) {
                return null;
            }
            LOG.error("getFileInfo failed: {}", status.getMessage());
            JfsUtil.throwException(status);
        }
        return call.getFileStatus();
    }

    public ContentSummary getContentSummary(String src) throws IOException {
        LOG.info("Receive getContentSummary call: src {}", src);
        JfsGetContentSummaryCall call = new JfsGetContentSummaryCall();
        call.setPath(src);
        JfsStatus status = call.execute(nn.getJfsRequestOptions());
        if (!status.isOk()) {
            LOG.error("getContentSummary failed: {}", status.getMessage());
            JfsUtil.throwException(status);
        }
        return call.getSummary();
    }

    public void fsync(String src, long fileId, String clientName,
                      long lastBlockLength) throws IOException {
        LOG.info("Receive fsync call: src {}, fileId {}, clientName {}, lastBlockLength {}",
                src, fileId, clientName, lastBlockLength);
        JfsFsyncFileCall call = new JfsFsyncFileCall();
        call.setPath(src);
        call.setFileId(fileId);
        call.setClientName(clientName);
        call.setLastBlockLength(lastBlockLength);
        JfsStatus status = call.execute(nn.getJfsRequestOptions());
        if (!status.isOk()) {
            LOG.error("fsync failed: {}", status.getMessage());
            JfsUtil.throwException(status);
        }
    }
}
