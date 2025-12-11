package com.aliyun.jindodata.gateway.hdfs.datanode;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.hdfs.server.protocol.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class JindoBPOfferService {
    private static final Logger LOG = LoggerFactory.getLogger(JindoBPOfferService.class);
    private final String nameserviceId;
    private volatile String bpId;
    private final JindoDataNode dn;
    private final ReentrantReadWriteLock mReadWriteLock =
            new ReentrantReadWriteLock();
    private final Lock mReadLock  = mReadWriteLock.readLock();
    private final Lock mWriteLock = mReadWriteLock.writeLock();
    private long lastActiveClaimTxId = -1;

    /**
     * The registration information for this block pool.
     * This is assigned after the second phase of the
     * handshake.
     */
    volatile DatanodeRegistration bpRegistration;

    /**
     * In gateway, Active NN is not necessary.
     * So this field doesn't matter.
     */
    private JindoBPServiceActor bpServiceToActive = null;

    /**
     * The list of all actors for namenodes in this nameservice, regardless
     * of their active or standby states.
     */
    private final List<JindoBPServiceActor> bpServices =
            new CopyOnWriteArrayList<JindoBPServiceActor>();

    /**
     * Information about the namespace that this service
     * is registering with. This is assigned after
     * the first phase of the handshake.
     */
    NamespaceInfo bpNSInfo;

    public JindoBPOfferService(String nameserviceId, List<InetSocketAddress> nnAddrs, JindoDataNode dn) {
        Preconditions.checkArgument(!nnAddrs.isEmpty(),
                "Must pass at least one NN.");
        this.nameserviceId = nameserviceId;
        this.dn = dn;

        for (int i = 0; i < nnAddrs.size(); ++i) {
            this.bpServices.add(new JindoBPServiceActor(nnAddrs.get(i), this));
        }
    }

    void start() {
        for (JindoBPServiceActor actor : bpServices) {
            actor.start();
        }
    }

    void stop() {
        for (JindoBPServiceActor actor : bpServices) {
            actor.stop();
        }
    }

    void join() {
        for (JindoBPServiceActor actor : bpServices) {
            actor.join();
        }
    }

    // konna : Not used for now
    void refreshNNList(ArrayList<InetSocketAddress> addrs) {
        Set<InetSocketAddress> oldAddrs = Sets.newHashSet();
        for (JindoBPServiceActor actor : bpServices) {
            oldAddrs.add(actor.getNNSocketAddress());
        }
        Set<InetSocketAddress> newAddrs = Sets.newHashSet(addrs);

        // Process added NNs
        Set<InetSocketAddress> addedNNs = Sets.difference(newAddrs, oldAddrs);
        for (InetSocketAddress addedNN : addedNNs) {
            JindoBPServiceActor actor = new JindoBPServiceActor(addedNN, this);
            actor.start();
            bpServices.add(actor);
        }

        // Process removed NNs
        Set<InetSocketAddress> removedNNs = Sets.difference(oldAddrs, newAddrs);
        for (InetSocketAddress removedNN : removedNNs) {
            for (JindoBPServiceActor actor : bpServices) {
                if (actor.getNNSocketAddress().equals(removedNN)) {
                    actor.stop();
                    shutdownActor(actor);
                    break;
                }
            }
        }
    }

    void shutdownActor(JindoBPServiceActor actor) {
        writeLock();
        try {
            bpServices.remove(actor);

            if (bpServices.isEmpty()) {
                dn.shutdownBlockPool(this);
            }
        } finally {
            writeUnlock();
        }
    }

    // utility methods to acquire and release read lock and write lock
    void readLock() {
        mReadLock.lock();
    }

    void readUnlock() {
        mReadLock.unlock();
    }

    void writeLock() {
        mWriteLock.lock();
    }

    void writeUnlock() {
        mWriteLock.unlock();
    }

    public JindoDataNode getDataNode() {
        return dn;
    }

    NamespaceInfo setNamespaceInfo(NamespaceInfo nsInfo) throws IOException {
        writeLock();
        try {
            NamespaceInfo old = bpNSInfo;
            if (bpNSInfo != null && nsInfo != null) {
                checkNSEquality(bpNSInfo.getBlockPoolID(), nsInfo.getBlockPoolID(),
                        "Blockpool ID");
                checkNSEquality(bpNSInfo.getNamespaceID(), nsInfo.getNamespaceID(),
                        "Namespace ID");
                checkNSEquality(bpNSInfo.getClusterID(), nsInfo.getClusterID(),
                        "Cluster ID");
            }
            bpNSInfo = nsInfo;
            // cache the block pool id for lock-free access.
            bpId = (nsInfo != null) ? nsInfo.getBlockPoolID() : null;
            return old;
        } finally {
            writeUnlock();
        }
    }

    /**
     * Called by the BPServiceActors when they handshake to a NN.
     * If this is the first NN connection, this sets the namespace info
     * for this BPOfferService. If it's a connection to a new NN, it
     * verifies that this namespace matches (eg to prevent a misconfiguration
     * where a StandbyNode from a different cluster is specified)
     */
    void verifyAndSetNamespaceInfo(JindoBPServiceActor actor, NamespaceInfo nsInfo)
            throws IOException {
        writeLock();

        if(nsInfo.getState() == HAServiceProtocol.HAServiceState.ACTIVE
                && bpServiceToActive == null) {
            LOG.info("Acknowledging ACTIVE Namenode during handshake" + actor);
            bpServiceToActive = actor;
        }

        try {
            if (setNamespaceInfo(nsInfo) == null) {
                boolean success = false;

                // Now that we know the namespace ID, etc, we can pass this to the DN.
                // The DN can now initialize its local storage if we are the
                // first BP to handshake, etc.
                try {
                    dn.initBlockPool(this);
                    success = true;
                } finally {
                    if (!success) {
                        // The datanode failed to initialize the BP. We need to reset
                        // the namespace info so that other BPService actors still have
                        // a chance to set it, and re-initialize the datanode.
                        setNamespaceInfo(null);
                    }
                }
            }
        } finally {
            writeUnlock();
        }
    }

    String getNameserviceId() {
        return nameserviceId;
    }

    NamespaceInfo getNamespaceInfo() {
        readLock();
        try {
            return bpNSInfo;
        } finally {
            readUnlock();
        }
    }

    String getBlockPoolId(boolean quiet) {
        // avoid lock contention unless the registration hasn't completed.
        String id = bpId;
        if (id != null) {
            return id;
        }
        readLock();
        try {
            if (bpNSInfo != null) {
                return bpNSInfo.getBlockPoolID();
            } else {
                if (!quiet) {
                    LOG.warn("Block pool ID needed, but service not yet registered with "
                            + "NN, trace:", new Exception());
                }
                return null;
            }
        } finally {
            readUnlock();
        }
    }

    String getBlockPoolId() {
        return getBlockPoolId(false);
    }

    DatanodeRegistration createRegistration() {
        writeLock();
        try {
            Preconditions.checkState(bpNSInfo != null,
                    "getRegistration() can only be called after initial handshake");
            return dn.createBPRegistration(bpNSInfo);
        } finally {
            writeUnlock();
        }
    }

    void registrationSucceeded(DatanodeRegistration reg) throws IOException {
        writeLock();
        try {
            if (bpRegistration != null) {
                checkNSEquality(bpRegistration.getStorageInfo().getNamespaceID(),
                        reg.getStorageInfo().getNamespaceID(), "namespace ID");
                checkNSEquality(bpRegistration.getStorageInfo().getClusterID(),
                        reg.getStorageInfo().getClusterID(), "cluster ID");
            }
            bpRegistration = reg;

            dn.bpRegistrationSucceeded(bpRegistration);
        } finally {
            writeUnlock();
        }
    }

    boolean hasBlockPoolId() {
        return getBlockPoolId(true) != null;
    }

    boolean isAlive() {
        for (JindoBPServiceActor actor : bpServices) {
            if (actor.isAlive()) {
                return true;
            }
        }
        return false;
    }

    boolean shouldRetryInit() {
        if (hasBlockPoolId()) {
            // One of the namenode registered successfully. lets continue retry for
            // other.
            return true;
        }
        return isAlive();
    }

    /**
     * Verify equality of two namespace-related fields, throwing
     * an exception if they are unequal.
     */
    private static void checkNSEquality(
            Object ourID, Object theirID,
            String idHelpText) throws IOException {
        if (!ourID.equals(theirID)) {
            throw new IOException(idHelpText + " mismatch: " +
                    "previously connected to " + idHelpText + " " + ourID +
                    " but now connected to " + idHelpText + " " + theirID);
        }
    }

    void updateActorStatesFromHeartbeat(
            JindoBPServiceActor actor,
            NNHAStatusHeartbeat nnHaState) {
        writeLock();
        try {
            final long txid = nnHaState.getTxId();

            final boolean nnClaimsActive =
                    nnHaState.getState() == HAServiceProtocol.HAServiceState.ACTIVE;
            final boolean bposThinksActive = bpServiceToActive == actor;
            final boolean isMoreRecentClaim = txid > lastActiveClaimTxId;

            if (nnClaimsActive && !bposThinksActive) {
                LOG.info("Namenode " + actor + " trying to claim ACTIVE state with " +
                        "txid=" + txid);
                if (!isMoreRecentClaim) {
                    // Split-brain scenario - an NN is trying to claim active
                    // state when a different NN has already claimed it with a higher
                    // txid.
                    LOG.warn("NN " + actor + " tried to claim ACTIVE state at txid=" +
                            txid + " but there was already a more recent claim at txid=" +
                            lastActiveClaimTxId);
                    return;
                } else {
                    if (bpServiceToActive == null) {
                        LOG.info("Acknowledging ACTIVE Namenode " + actor);
                    } else {
                        LOG.info("Namenode " + actor + " taking over ACTIVE state from " +
                                bpServiceToActive + " at higher txid=" + txid);
                    }
                    bpServiceToActive = actor;
                }
            } else if (!nnClaimsActive && bposThinksActive) {
                LOG.info("Namenode " + actor + " relinquishing ACTIVE state with " +
                        "txid=" + nnHaState.getTxId());
                bpServiceToActive = null;
            }

            if (bpServiceToActive == actor) {
                assert txid >= lastActiveClaimTxId;
                lastActiveClaimTxId = txid;
            }
        } finally {
            writeUnlock();
        }
    }

    boolean processCommandFromActor(DatanodeCommand cmd,
                                    JindoBPServiceActor actor) throws IOException {
        assert bpServices.contains(actor);
        if (cmd == null) {
            return true;
        }
        /*
         * Datanode Registration can be done asynchronously here. No need to hold
         * the lock. for more info refer HDFS-5014
         */
        if (DatanodeProtocol.DNA_REGISTER == cmd.getAction()) {
            // namenode requested a registration - at start or if NN lost contact
            // Just logging the claiming state is OK here instead of checking the
            // actor state by obtaining the lock
            LOG.info("DatanodeCommand action : DNA_REGISTER from " + actor.nnAddr
                    + " with " + actor.state + " state");
            actor.reRegister();
            return false;
        }
        writeLock();
        try {
            if (actor == bpServiceToActive) {
                return processCommandFromActive(cmd, actor);
            } else {
                return processCommandFromStandby(cmd, actor);
            }
        } finally {
            writeUnlock();
        }
    }

    /**
     * This method should handle all commands from Active namenode except
     * DNA_REGISTER which should be handled earlier itself.
     *
     * @param cmd
     * @return true if further processing may be required or false otherwise.
     * @throws IOException
     */
    private boolean processCommandFromActive(DatanodeCommand cmd,
                                             JindoBPServiceActor actor) throws IOException {
        final BlockCommand bcmd =
                cmd instanceof BlockCommand? (BlockCommand)cmd: null;
        final BlockIdCommand blockIdCmd =
                cmd instanceof BlockIdCommand ? (BlockIdCommand)cmd: null;

        switch(cmd.getAction()) {
            case DatanodeProtocol.DNA_TRANSFER:
            case DatanodeProtocol.DNA_INVALIDATE:
            case DatanodeProtocol.DNA_CACHE:
            case DatanodeProtocol.DNA_UNCACHE:
            case DatanodeProtocol.DNA_SHUTDOWN:
            case DatanodeProtocol.DNA_FINALIZE:
            case DatanodeProtocol.DNA_RECOVERBLOCK:
            case DatanodeProtocol.DNA_ACCESSKEYUPDATE:
            case DatanodeProtocol.DNA_BALANCERBANDWIDTHUPDATE:
                LOG.warn("NN should not send any DatanodeCommand action: " + cmd.getAction());
                break;
            default:
                LOG.warn("Unknown DatanodeCommand action: " + cmd.getAction());
        }
        return true;
    }

    /**
     * This method should handle commands from Standby namenode except
     * DNA_REGISTER which should be handled earlier itself.
     */
    private boolean processCommandFromStandby(DatanodeCommand cmd,
                                              JindoBPServiceActor actor) throws IOException {
        switch(cmd.getAction()) {
            case DatanodeProtocol.DNA_ACCESSKEYUPDATE:
            case DatanodeProtocol.DNA_TRANSFER:
            case DatanodeProtocol.DNA_INVALIDATE:
            case DatanodeProtocol.DNA_SHUTDOWN:
            case DatanodeProtocol.DNA_FINALIZE:
            case DatanodeProtocol.DNA_RECOVERBLOCK:
            case DatanodeProtocol.DNA_BALANCERBANDWIDTHUPDATE:
            case DatanodeProtocol.DNA_CACHE:
            case DatanodeProtocol.DNA_UNCACHE:
                LOG.warn("NN should not send any DatanodeCommand action: " + cmd.getAction());
                break;
            default:
                LOG.warn("Unknown DatanodeCommand action: " + cmd.getAction());
        }
        return true;
    }
}
