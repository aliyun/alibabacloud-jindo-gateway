package com.aliyun.jindodata.gateway.hdfs.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.server.protocol.VolumeFailureSummary;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class JindoHeartbeatManager {
    static final Logger LOG = LoggerFactory.getLogger(JindoHeartbeatManager.class);

    /** The time period to check for expired datanodes */
    private final long heartbeatRecheckInterval;
    private final JindoNameSystem namesystem;

    private final List<DatanodeDescriptor> datanodes = new ArrayList<>();
    /** Heartbeat monitor thread */
    private final Daemon heartbeatThread = new Daemon(new Monitor());

    JindoHeartbeatManager(Configuration conf, JindoNameSystem jindoNameSystem) {
        namesystem = jindoNameSystem;
        heartbeatRecheckInterval = conf.getInt(
                DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
                DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_DEFAULT); // 5 min
    }

    synchronized void register(final DatanodeDescriptor d) {
        if (!d.isAlive()) {
            addDatanode(d);

            //update its timestamp
            d.updateHeartbeatState(StorageReport.EMPTY_ARRAY, 0L, 0L, 0, 0, null);
        }
    }

    synchronized void addDatanode(final DatanodeDescriptor d) {
        // update in-service node count
        datanodes.add(d);
        d.setAlive(true);
    }

    synchronized void removeDatanode(DatanodeDescriptor node) {
        if (node.isAlive()) {
            datanodes.remove(node);
            node.setAlive(false);
        }
    }

    synchronized void updateHeartbeat(final DatanodeDescriptor node,
                                      StorageReport[] reports, long cacheCapacity, long cacheUsed,
                                      int xceiverCount, int failedVolumes,
                                      VolumeFailureSummary volumeFailureSummary) {
        node.updateHeartbeat(reports, cacheCapacity, cacheUsed, xceiverCount,
                failedVolumes, volumeFailureSummary);
    }

    public void activate() {
        heartbeatThread.start();
    }

    public void close() {
        heartbeatThread.interrupt();
        try {
            // This will no effect if the thread hasn't yet been started.
            heartbeatThread.join(3000);
        } catch (InterruptedException ignored) {
        }
    }

    void heartbeatCheck() {
        final JindoDatanodeManager dm = namesystem.getDatanodeManager();
        boolean allAlive = false;
        while (!allAlive) {
            // locate the first dead node.
            DatanodeDescriptor dead = null;

            // check the number of stale nodes
            int numOfStaleNodes = 0;
            synchronized(this) {
                for (DatanodeDescriptor d : datanodes) {
                    if (dead == null && dm.isDatanodeDead(d)) {
                        dead = d;
                    }
                    if (d.isStale(dm.getStaleInterval())) {
                        numOfStaleNodes++;
                    }
                }
                if (numOfStaleNodes > 0) {
                    LOG.error("Found {} stale DataNodes", numOfStaleNodes);
                }
            }

            allAlive = dead == null;
            if (dead != null) {
                // acquire the fsnamesystem lock, and then remove the dead node.
                namesystem.writeLock();
                try {
                    dm.removeDeadDatanode(dead);
                } finally {
                    namesystem.writeUnlock();
                }
            }
        }
    }

    /** Periodically check heartbeat and update block key */
    private class Monitor implements Runnable {
        private long lastHeartbeatCheck;

        @Override
        public void run() {
            while(namesystem.isRunning()) {
                try {
                    final long now = Time.monotonicNow();
                    if (lastHeartbeatCheck + heartbeatRecheckInterval < now) {
                        heartbeatCheck();
                        lastHeartbeatCheck = now;
                    }
                } catch (Exception e) {
                    LOG.error("Exception while checking heartbeat", e);
                }
                try {
                    Thread.sleep(5000);  // 5 seconds
                } catch (InterruptedException ignored) {
                }
            }
        }
    }
}
