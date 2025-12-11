package com.aliyun.jindodata.gateway.hdfs.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.HostConfigManager;
import org.apache.hadoop.hdfs.server.blockmanagement.HostFileManager;
import org.apache.hadoop.hdfs.server.protocol.*;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.util.Time.monotonicNow;

public class JindoDatanodeManager {
    public static final Logger LOG =
            LoggerFactory.getLogger(JindoDatanodeManager.class.getName());
    private final Map<String, DatanodeDescriptor> datanodeMap
            = new HashMap<>();
    private final Host2NodesMap host2DatanodeMap = new Host2NodesMap();

    private final JindoNameSystem nameSystem;
    private final JindoHeartbeatManager heartbeatManager;
    private volatile long heartbeatIntervalSeconds;
    private volatile int heartbeatRecheckInterval;
    private long heartbeatExpireInterval;
    private final int defaultXferPort;
    private final int defaultInfoPort;
    private final int defaultInfoSecurePort;
    private final int defaultIpcPort;
    /** The interval for judging stale DataNodes for read/write */
    private final long staleInterval;

    /** Read include/exclude files. */
    private HostConfigManager hostConfigManager;

    public JindoDatanodeManager(Configuration conf, JindoNameSystem jindoNameSystem) {
        this.nameSystem = jindoNameSystem;
        this.heartbeatManager = new JindoHeartbeatManager(conf, jindoNameSystem);
        this.hostConfigManager = ReflectionUtils.newInstance(
                conf.getClass(DFSConfigKeys.DFS_NAMENODE_HOSTS_PROVIDER_CLASSNAME_KEY,
                        HostFileManager.class, HostConfigManager.class), conf);

        try {
            this.hostConfigManager.refresh();
        } catch (IOException e) {
            LOG.error("error reading hosts files: ", e);
        }

        heartbeatIntervalSeconds = conf.getTimeDuration(
                DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY,
                DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT, TimeUnit.SECONDS);
        heartbeatRecheckInterval = conf.getInt(
                DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
                DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_DEFAULT); // 5 minutes
        this.heartbeatExpireInterval = 2 * heartbeatRecheckInterval
                + 10 * 1000 * heartbeatIntervalSeconds;
        staleInterval = conf.getLong(
                DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY,
                DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_DEFAULT);

        this.defaultXferPort = NetUtils.createSocketAddr(
                conf.getTrimmed(DFSConfigKeys.DFS_DATANODE_ADDRESS_KEY,
                        DFSConfigKeys.DFS_DATANODE_ADDRESS_DEFAULT)).getPort();
        this.defaultInfoPort = NetUtils.createSocketAddr(
                conf.getTrimmed(DFSConfigKeys.DFS_DATANODE_HTTP_ADDRESS_KEY,
                        DFSConfigKeys.DFS_DATANODE_HTTP_ADDRESS_DEFAULT)).getPort();
        this.defaultInfoSecurePort = NetUtils.createSocketAddr(
                conf.getTrimmed(DFSConfigKeys.DFS_DATANODE_HTTPS_ADDRESS_KEY,
                        DFSConfigKeys.DFS_DATANODE_HTTPS_ADDRESS_DEFAULT)).getPort();
        this.defaultIpcPort = NetUtils.createSocketAddr(
                conf.getTrimmed(DFSConfigKeys.DFS_DATANODE_IPC_ADDRESS_KEY,
                        DFSConfigKeys.DFS_DATANODE_IPC_ADDRESS_DEFAULT)).getPort();
    }

    public void registerDatanode(DatanodeRegistration nodeReg) throws DisallowedDatanodeException {
        InetAddress dnAddress = Server.getRemoteIp();
        if (dnAddress != null) {
            // Mostly called inside an RPC, update ip and peer hostname
            String hostname = dnAddress.getHostName();
            String ip = dnAddress.getHostAddress();
            // update node registration with the ip and hostname from rpc request
            nodeReg.setIpAddr(ip);
            nodeReg.setPeerHostName(hostname);
        }

        // Checks if the node is not on the hosts list
        // or is on excludes list.  If true, then
        // it will be disallowed from registering.
        if (!hostConfigManager.isIncluded(nodeReg) || hostConfigManager.isExcluded(nodeReg)) {
            throw new DisallowedDatanodeException(nodeReg);
        }

        DatanodeDescriptor nodeS = getDatanode(nodeReg.getDatanodeUuid());
        DatanodeDescriptor nodeN = host2DatanodeMap.getDatanodeByXferAddr(
                nodeReg.getIpAddr(), nodeReg.getXferPort());

        if (nodeN != null && nodeN != nodeS) {
            LOG.info("BLOCK* registerDatanode: {}", nodeN);
            // nodeN previously served a different data storage,
            // which is not served by anybody anymore.
            removeDatanode(nodeN);
            // physically remove node from datanodeMap
            wipeDatanode(nodeN);
            nodeN = null;
        }

        if (nodeS != null) {
            if (nodeN == nodeS) {
                // The same datanode has been just restarted to serve the same data
                // storage. We do not need to remove old data blocks, the delta will
                // be calculated on the next block report from the datanode
                if(LOG.isDebugEnabled()) {
                    LOG.debug("BLOCK* registerDatanode: "
                            + "node restarted.");
                }
            } else {
                // nodeS is found
                /* The registering datanode is a replacement node for the existing
                data storage, which from now on will be served by a new node.
                If this message repeats, both nodes might have same storageID
                by (insanely rare) random chance. User needs to restart one of the
                nodes with its data cleared (or user can just remove the StorageID
                value in "VERSION" file under the data directory of the datanode,
                but this is might not work if VERSION file format has changed
                */
                LOG.info("BLOCK* registerDatanode: {} is replaced by {} with the same storageID {}",
                        nodeS, nodeReg, nodeReg.getDatanodeUuid());
            }

            boolean success = false;
            try {
                nodeS.updateRegInfo(nodeReg);
                nodeS.setSoftwareVersion(nodeReg.getSoftwareVersion());
                nodeS.setDisallowed(false); // Node is in the include list

                // also treat the registration message as a heartbeat
                heartbeatManager.register(nodeS);
                success = true;
            } finally {
                if (!success) {
                    removeDatanode(nodeS);
                    wipeDatanode(nodeS);
                }
            }
            return;
        }

        DatanodeDescriptor nodeDescr
                = new DatanodeDescriptor(nodeReg, NetworkTopology.DEFAULT_RACK);
        boolean success = false;
        try{
            nodeDescr.setSoftwareVersion(nodeReg.getSoftwareVersion());
            // register new datanode
            addDatanode(nodeDescr);
            heartbeatManager.addDatanode(nodeDescr);
            success = true;
        } finally {
            if (!success) {
                removeDatanode(nodeDescr);
                wipeDatanode(nodeDescr);
            }
        }
    }

    /** Add a datanode. */
    void addDatanode(final DatanodeDescriptor node) {
        // To keep host2DatanodeMap consistent with datanodeMap,
        // remove  from host2DatanodeMap the datanodeDescriptor removed
        // from datanodeMap before adding node to host2DatanodeMap.
        synchronized(this) {
            host2DatanodeMap.remove(datanodeMap.put(node.getDatanodeUuid(), node));
        }

        host2DatanodeMap.add(node);

        if (LOG.isDebugEnabled()) {
            LOG.debug("{}.addDatanode: node {} is added to datanodeMap.",
                    getClass().getSimpleName(), node);
        }
    }

    /** Get a datanode descriptor given corresponding DatanodeUUID */
    public DatanodeDescriptor getDatanode(final String datanodeUuid) {
        if (datanodeUuid == null) {
            return null;
        }
        synchronized (this) {
            return datanodeMap.get(datanodeUuid);
        }
    }

    /**
     * Remove a datanode descriptor.
     * @param nodeInfo datanode descriptor.
     */
    private void removeDatanode(DatanodeDescriptor nodeInfo) {
        heartbeatManager.removeDatanode(nodeInfo);
        if (LOG.isDebugEnabled()) {
            LOG.debug("remove datanode {}", nodeInfo);
        }
    }

    /** Physically remove node from datanodeMap. */
    private void wipeDatanode(final DatanodeID node) {
        final String key = node.getDatanodeUuid();
        synchronized (this) {
            host2DatanodeMap.remove(datanodeMap.remove(key));
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("{}.wipeDatanode({}): storage {} is removed from datanodeMap.",
                    getClass().getSimpleName(), node, key);
        }
    }

    public DatanodeCommand[] handleHeartbeat(DatanodeRegistration nodeReg,
                                             StorageReport[] reports, long cacheCapacity, long cacheUsed,
                                             int xceiverCount, int failedVolumes,
                                             VolumeFailureSummary volumeFailureSummary) throws DisallowedDatanodeException {
        final DatanodeDescriptor nodeinfo;
        try {
            nodeinfo = getDatanode(nodeReg);
        } catch (UnregisteredNodeException e) {
            return new DatanodeCommand[]{RegisterCommand.REGISTER};
        }

        // Check if this datanode should actually be shutdown instead.
        if (nodeinfo != null && nodeinfo.isDisallowed()) {
            setDatanodeDead(nodeinfo);
            throw new DisallowedDatanodeException(nodeinfo);
        }
        if (nodeinfo == null || !nodeinfo.isRegistered()) {
            return new DatanodeCommand[]{RegisterCommand.REGISTER};
        }
        heartbeatManager.updateHeartbeat(nodeinfo, reports, cacheCapacity,
                cacheUsed, xceiverCount, failedVolumes, volumeFailureSummary);

        return new DatanodeCommand[0];
    }

    public DatanodeDescriptor getDatanode(DatanodeID nodeID)
            throws UnregisteredNodeException {
        final DatanodeDescriptor node = getDatanode(nodeID.getDatanodeUuid());
        if (node == null)
            return null;
        if (!node.getXferAddr().equals(nodeID.getXferAddr())) {
            final UnregisteredNodeException e = new UnregisteredNodeException(
                    nodeID, node);
            LOG.error("BLOCK* NameSystem.getDatanode: {}", e.getLocalizedMessage());
            throw e;
        }
        return node;
    }

    private void setDatanodeDead(DatanodeDescriptor node) {
        node.setLastUpdate(0);
        node.setLastUpdateMonotonic(0);
    }

    public Map getDatanodeMap() {
        return datanodeMap;
    }

    public void activate() {
        heartbeatManager.activate();
    }

    public void close() {
        heartbeatManager.close();
    }

    /** Is the datanode dead? */
    boolean isDatanodeDead(DatanodeDescriptor node) {
        return (node.getLastUpdateMonotonic() <
                (monotonicNow() - heartbeatExpireInterval));
    }

    long getStaleInterval() {
        return staleInterval;
    }

    /** Remove a dead datanode. */
    void removeDeadDatanode(final DatanodeID nodeID) {
        DatanodeDescriptor d;
        try {
            d = getDatanode(nodeID);
        } catch(IOException e) {
            d = null;
        }
        if (d != null && isDatanodeDead(d)) {
            LOG.info("BLOCK* removeDeadDatanode: lost heartbeat from {}", d);
            removeDatanode(d);
        }
    }

    DatanodeInfoWithStorage[] chooseRandomNodes(int numNodes, DatanodeInfo[] excludeNodes) {
        LOG.debug("Get random datanodes with exclude nodes {}", excludeNodes == null ? "0" : excludeNodes.length);
        if (excludeNodes != null && excludeNodes.length > 0) {
            for (DatanodeInfo excludeNode : excludeNodes) {
                LOG.debug("Exclude node {}", excludeNode);
            }
        }

        nameSystem.readLock();

        try {
            // Collect all live (non-dead) datanodes
            List<DatanodeDescriptor> liveDatanodes = new ArrayList<>();
            synchronized (this) {
                for (DatanodeDescriptor dn : datanodeMap.values()) {
                    if (!isDatanodeDead(dn)) {
                        liveDatanodes.add(dn);
                    }
                }
            }

            // Shuffle the list to randomize
            Collections.shuffle(liveDatanodes);

            // Collect result nodes, skipping excluded ones
            List<DatanodeInfoWithStorage> resultNodes = new ArrayList<>();
            for (DatanodeDescriptor dn : liveDatanodes) {
                // Check if this node is in the exclude list
                boolean isExcluded = false;
                if (excludeNodes != null && excludeNodes.length > 0) {
                    for (DatanodeInfo excludeNode : excludeNodes) {
                        if (dn.getDatanodeUuid().equals(excludeNode.getDatanodeUuid())) {
                            isExcluded = true;
                            LOG.debug("Node {} is in the exclude list, skip", dn);
                            break;
                        }
                    }
                }

                if (!isExcluded) {
                    // HDFS 3.2.1 needs storageID
                    resultNodes.add(new DatanodeInfoWithStorage(dn, "default-storage-id", StorageType.DEFAULT));
                    LOG.debug("Add random node {}", dn);
                    if (resultNodes.size() >= numNodes) {
                        break;
                    }
                }
            }

            // Return the result or warn if not enough nodes found
            if (resultNodes.isEmpty()) {
                LOG.warn("Failed to find any nodes while {} demanded", numNodes);
                return new DatanodeInfoWithStorage[0];
            }
            if (resultNodes.size() < numNodes) {
                LOG.warn("Failed to find {} nodes, only {} nodes found", numNodes, resultNodes.size());
            }

            return resultNodes.toArray(new DatanodeInfoWithStorage[0]);
        } finally {
            nameSystem.readUnlock();
        }
    }

    public List<DatanodeDescriptor> getDatanodeListForReport(
      final HdfsConstants.DatanodeReportType type) {
        final boolean listLiveNodes =
                type == HdfsConstants.DatanodeReportType.ALL ||
                        type == HdfsConstants.DatanodeReportType.LIVE;
        final boolean listDeadNodes =
                type == HdfsConstants.DatanodeReportType.ALL ||
                        type == HdfsConstants.DatanodeReportType.DEAD;
        final boolean listDecommissioningNodes =
                type == HdfsConstants.DatanodeReportType.ALL ||
                        type == HdfsConstants.DatanodeReportType.DECOMMISSIONING;

        ArrayList<DatanodeDescriptor> nodes;
        final HostSet foundNodes = new HostSet();
        final Iterable<InetSocketAddress> includedNodes =
                hostConfigManager.getIncludes();

        synchronized (datanodeMap) {
            nodes = new ArrayList<>(datanodeMap.size());
            for (DatanodeDescriptor dn : datanodeMap.values()) {
                final boolean isDead = isDatanodeDead(dn);
                final boolean isDecommissioning = dn.isDecommissionInProgress();

                if (((listLiveNodes && !isDead) ||
                        (listDeadNodes && isDead) ||
                        (listDecommissioningNodes && isDecommissioning)) &&
                        hostConfigManager.isIncluded(dn)) {
                    nodes.add(dn);
                }

                foundNodes.add(dn.getResolvedAddress());
            }
        }
        Collections.sort(nodes);

        if (listDeadNodes) {
            for (InetSocketAddress addr : includedNodes) {
                if (foundNodes.matchedBy(addr)) {
                    continue;
                }
                // The remaining nodes are ones that are referenced by the hosts
                // files but that we do not know about, ie that we have never
                // head from. Eg. an entry that is no longer part of the cluster
                // or a bogus entry was given in the hosts files
                //
                // If the host file entry specified the xferPort, we use that.
                // Otherwise, we guess that it is the default xfer port.
                // We can't ask the DataNode what it had configured, because it's
                // dead.
                DatanodeDescriptor dn = new DatanodeDescriptor(new DatanodeID(addr
                        .getAddress().getHostAddress(), addr.getHostName(), "",
                        addr.getPort() == 0 ? defaultXferPort : addr.getPort(),
                        defaultInfoPort, defaultInfoSecurePort, defaultIpcPort));
                setDatanodeDead(dn);
                if (hostConfigManager.isExcluded(dn)) {
                    dn.setDecommissioned();
                }
                nodes.add(dn);
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("getDatanodeListForReport with includedNodes = {}, excludedNodes = {}, foundNodes = {}, nodes = {}",
                    hostConfigManager.getIncludes(), hostConfigManager.getExcludes(), foundNodes, nodes);
        }
        return nodes;
    }

    public void refreshNodes(final Configuration conf) throws IOException {
        refreshHostsReader(conf);
        nameSystem.writeLock();
        try {
            refreshDatanodes();
        } finally {
            nameSystem.writeUnlock();
        }
    }

    private void refreshHostsReader(Configuration conf) throws IOException {
        // Reread the conf to get dfs.hosts and dfs.hosts.exclude filenames.
        // Update the file names and refresh internal includes and excludes list.
        if (conf == null) {
            conf = new HdfsConfiguration();
        }
        {
            final String includeFilePath = conf.get(DFSConfigKeys.DFS_HOSTS,"");
            final String excludeFilePath = conf.get(DFSConfigKeys.DFS_HOSTS_EXCLUDE,"");
            if (!includeFilePath.isEmpty()) {
                if (!new File(includeFilePath).exists()) {
                    LOG.warn("Include file {} does not exist.", includeFilePath);
                    conf.set(DFSConfigKeys.DFS_HOSTS, "");
                }
            }
            if (!excludeFilePath.isEmpty()) {
                if (!new File(excludeFilePath).exists()) {
                    LOG.warn("Exclude file {} does not exist.", excludeFilePath);
                    conf.set(DFSConfigKeys.DFS_HOSTS_EXCLUDE, "");
                }
            }
        }
        this.hostConfigManager.setConf(conf);
        this.hostConfigManager.refresh();
    }

    private void refreshDatanodes() {
        for (DatanodeDescriptor node : datanodeMap.values()) {
            // Check if not include.
            if (!hostConfigManager.isIncluded(node)) {
                node.setDisallowed(true); // include list exits and node is not in
            } else {
                // include list not exists or node is in include list, check exclude list
                node.setDisallowed(hostConfigManager.isExcluded(node));
            }
        }
    }
}
