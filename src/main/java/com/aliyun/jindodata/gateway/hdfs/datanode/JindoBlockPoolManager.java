package com.aliyun.jindodata.gateway.hdfs.datanode;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

public class JindoBlockPoolManager {
    private static final Logger LOG = LoggerFactory.getLogger(JindoBlockPoolManager.class);
    private final JindoDataNode dn;
    private final Map<String, JindoBPOfferService> bpByNameserviceId =
            Maps.newHashMap();
    private final Map<String, JindoBPOfferService> bpByBlockPoolId =
            Maps.newHashMap();
    private final List<JindoBPOfferService> offerServices =
            new CopyOnWriteArrayList<>();

    //This lock is used only to ensure exclusion of refreshNamenodes
    private final Object refreshNamenodesLock = new Object();

    public JindoBlockPoolManager(JindoDataNode dn) {
        this.dn = dn;
    }

    void refreshNamenodes(Configuration conf) throws IOException {
        LOG.info("Refresh request received for nameservices: {}", conf.get(DFSConfigKeys.DFS_NAMESERVICES));

        Map<String, Map<String, InetSocketAddress>> newAddressMap = null;

        try {
            newAddressMap =
                    DFSUtil.getNNServiceRpcAddressesForCluster(conf);
        } catch (IOException ioe) {
            LOG.warn("Unable to get NameNode addresses.");
        }

        if (newAddressMap == null || newAddressMap.isEmpty()) {
            throw new IOException("No services to connect, missing NameNode " +
                    "address.");
        }

        synchronized (refreshNamenodesLock) {
            doRefreshNamenodes(newAddressMap);
        }
    }

    private void doRefreshNamenodes(Map<String, Map<String, InetSocketAddress>> addrMap) throws IOException {
        assert Thread.holdsLock(refreshNamenodesLock);

        Set<String> toRefresh = Sets.newLinkedHashSet();
        Set<String> toAdd = Sets.newLinkedHashSet();
        Set<String> toRemove;

        synchronized (this) {
            // Step 1. For each of the new nameservices, figure out whether
            // it's an update of the set of NNs for an existing NS,
            // or an entirely new nameservice.
            for (String nameserviceId : addrMap.keySet()) {
                if (bpByNameserviceId.containsKey(nameserviceId)) {
                    toRefresh.add(nameserviceId);
                } else {
                    toAdd.add(nameserviceId);
                }
            }

            // Step 2. Any nameservices we currently have but are no longer present
            // need to be removed.
            toRemove = Sets.newHashSet(Sets.difference(
                    bpByNameserviceId.keySet(), addrMap.keySet()));

            assert toRefresh.size() + toAdd.size() ==
                    addrMap.size() :
                    "toAdd: " + Joiner.on(",").useForNull("<default>").join(toAdd) +
                            "  toRemove: " + Joiner.on(",").useForNull("<default>").join(toRemove) +
                            "  toRefresh: " + Joiner.on(",").useForNull("<default>").join(toRefresh);

            // Step 3. Start new nameservices
            if (!toAdd.isEmpty()) {
                LOG.info("Starting BPOfferServices for nameservices: " +
                        Joiner.on(",").useForNull("<default>").join(toAdd));

                for (String nsToAdd : toAdd) {
                    Map<String, InetSocketAddress> nnIdToAddr = addrMap.get(nsToAdd);
                    ArrayList<InetSocketAddress> addrs =
                            Lists.newArrayListWithCapacity(nnIdToAddr.size());
                    for (String nnId : nnIdToAddr.keySet()) {
                        addrs.add(nnIdToAddr.get(nnId));
                    }
                    JindoBPOfferService bpos = createBPOS(nsToAdd, addrs);
                    bpByNameserviceId.put(nsToAdd, bpos);
                    offerServices.add(bpos);
                }
            }
            startAll();
        }

        // Step 4. Shut down old nameservices. This happens outside
        // of the synchronized(this) lock since they need to call
        // back to .remove() from another thread
        // konna : Not used for now
        if (!toRemove.isEmpty()) {
            LOG.info("Stopping BPOfferServices for nameservices: {}",
                    Joiner.on(",").useForNull("<default>").join(toRemove));

            for (String nsToRemove : toRemove) {
                JindoBPOfferService bpos = bpByNameserviceId.get(nsToRemove);
                bpos.stop();
                bpos.join();
                // they will call remove on their own
            }
        }

        // Step 5. Update nameservices whose NN list has changed
        // konna : Not used for now
        if (!toRefresh.isEmpty()) {
            LOG.info("Refreshing list of NNs for nameservices: {}",
                    Joiner.on(",").useForNull("<default>").join(toRefresh));

            for (String nsToRefresh : toRefresh) {
                JindoBPOfferService bpos = bpByNameserviceId.get(nsToRefresh);
                Map<String, InetSocketAddress> nnIdToAddr = addrMap.get(nsToRefresh);
                ArrayList<InetSocketAddress> addrs =
                        Lists.newArrayListWithCapacity(nnIdToAddr.size());
                for (String nnId : nnIdToAddr.keySet()) {
                    addrs.add(nnIdToAddr.get(nnId));
                }
                try {
                    UserGroupInformation.getLoginUser()
                            .doAs(new PrivilegedExceptionAction<Object>() {
                                @Override
                                public Object run() throws Exception {
                                    bpos.refreshNNList(addrs);
                                    return null;
                                }
                            });
                } catch (InterruptedException ex) {
                    IOException ioe = new IOException();
                    ioe.initCause(ex.getCause());
                    throw ioe;
                }
            }
        }
    }

    /**
     * Extracted out for test purposes.
     */
    protected JindoBPOfferService createBPOS(
            final String nameserviceId,
            List<InetSocketAddress> nnAddrs) {
        return new JindoBPOfferService(nameserviceId, nnAddrs, dn);
    }

    synchronized void startAll() throws IOException {
        try {
            UserGroupInformation.getLoginUser().doAs(
                    new PrivilegedExceptionAction<Object>() {
                        @Override
                        public Object run() throws Exception {
                            for (JindoBPOfferService bpos : offerServices) {
                                bpos.start();
                            }
                            return null;
                        }
                    });
        } catch (InterruptedException ex) {
            throw new IOException(ex.getCause());
        }
    }

    void shutDownAll(List<JindoBPOfferService> bposList) throws InterruptedException {
        for (JindoBPOfferService bpos : bposList) {
            bpos.stop(); //interrupts the threads
        }
        //now join
        for (JindoBPOfferService bpos : bposList) {
            bpos.join();
        }
    }

    void joinAll() {
        for (JindoBPOfferService bpos: this.getAllNamenodeThreads()) {
            bpos.join();
        }
    }

    synchronized void remove(JindoBPOfferService t) {
        offerServices.remove(t);
        if (t.hasBlockPoolId()) {
            // It's possible that the block pool never successfully registered
            // with any NN, so it was never added it to this map
            bpByBlockPoolId.remove(t.getBlockPoolId());
        }

        boolean removed = false;
        for (Iterator<JindoBPOfferService> it = bpByNameserviceId.values().iterator();
             it.hasNext() && !removed;) {
            JindoBPOfferService bpos = it.next();
            if (bpos == t) {
                it.remove();
                LOG.info("Removed " + bpos);
                removed = true;
            }
        }

        if (!removed) {
            LOG.warn("Couldn't remove BPOS " + t + " from bpByNameserviceId map");
        }
    }

    synchronized void addBlockPool(JindoBPOfferService bpos) {
        Preconditions.checkArgument(offerServices.contains(bpos),
                "Unknown BPOS: %s", bpos);
        if (bpos.getBlockPoolId() == null) {
            throw new IllegalArgumentException("Null blockpool id");
        }
        bpByBlockPoolId.put(bpos.getBlockPoolId(), bpos);
    }

    /**
     * Returns a list of BPOfferService objects. The underlying list
     * implementation is a CopyOnWriteArrayList so it can be safely
     * iterated while BPOfferServices are being added or removed.
     *
     * Caution: The BPOfferService returned could be shutdown any time.
     */
    synchronized List<JindoBPOfferService> getAllNamenodeThreads() {
        return Collections.unmodifiableList(offerServices);
    }

    synchronized JindoBPOfferService get(String bpid) {
        return bpByBlockPoolId.get(bpid);
    }
}
