package com.aliyun.jindodata.gateway.hdfs.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.net.PeerServer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Daemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.channels.AsynchronousCloseException;
import java.util.HashMap;

public class JindoDataXceiverServer implements Runnable{
    private static final Logger LOG = LoggerFactory.getLogger(JindoDataXceiverServer.class);

    private final PeerServer peerServer;
    private final JindoDataNode datanode;
    private final HashMap<Peer, Thread> peers = new HashMap<Peer, Thread>();
    private final HashMap<Peer, JindoDataXceiver> peersXceiver = new HashMap<>();
    private boolean closed = false;

    int maxXceiverCount;

    public JindoDataXceiverServer(PeerServer peerServer, Configuration conf, JindoDataNode dataNode) {
        this.peerServer = peerServer;
        this.datanode = dataNode;
        this.maxXceiverCount =
                conf.getInt(DFSConfigKeys.DFS_DATANODE_MAX_RECEIVER_THREADS_KEY,
                        DFSConfigKeys.DFS_DATANODE_MAX_RECEIVER_THREADS_DEFAULT);

    }

    @Override
    public void run() {
        Peer peer = null;
        while (datanode.shouldRun && !datanode.shutdownForUpgrade) {
            try {
                peer = peerServer.accept();

                // Make sure the xceiver count is not exceeded
                int curXceiverCount = datanode.getXceiverCount();
                if (curXceiverCount > maxXceiverCount) {
                    throw new IOException("Xceiver count " + curXceiverCount
                            + " exceeds the limit of concurrent xcievers: "
                            + maxXceiverCount);
                }

                new Daemon(datanode.threadGroup,
                        JindoDataXceiver.create(peer, datanode, this))
                        .start();
            } catch (SocketTimeoutException ignored) {
                // wake up to see if should continue to run
            } catch (AsynchronousCloseException ace) {
                // another thread closed our listener socket - that's expected during shutdown,
                // but not in other circumstances
                if (datanode.shouldRun && !datanode.shutdownForUpgrade) {
                    LOG.warn("{}:DataXceiverServer: ", datanode.getDisplayName(), ace);
                }
            } catch (IOException ie) {
                IOUtils.cleanup(null, peer);
                LOG.warn("{}:DataXceiverServer: ", datanode.getDisplayName(), ie);
            } catch (OutOfMemoryError ie) {
                IOUtils.cleanup(null, peer);
                // DataNode can run out of memory if there is too many transfers.
                // Log the event, Sleep for 30 seconds, other transfers may complete by
                // then.
                LOG.error("DataNode is out of memory. Will retry in 30 seconds.", ie);
                try {
                    Thread.sleep(30 * 1000);
                } catch (InterruptedException e) {
                    // ignore
                }
            } catch (Throwable te) {
                LOG.error("{}:DataXceiverServer: Exiting due to: ", datanode.getDisplayName(), te);
                datanode.shouldRun = false;
            }
        }

        // Close the server to stop reception of more requests.
        try {
            peerServer.close();
            closed = true;
        } catch (IOException ie) {
            LOG.warn("{} :DataXceiverServer: close exception", datanode.getDisplayName(), ie);
        }

        // if in restart prep stage, notify peers before closing them.
        if (datanode.shutdownForUpgrade) {
            restartNotifyPeers();
            // Each thread needs some time to process it. If a thread needs
            // to send an OOB message to the client, but blocked on network for
            // long time, we need to force its termination.
            LOG.info("Shutting down DataXceiverServer before restart");
            // Allow roughly up to 2 seconds.
            for (int i = 0; getNumPeers() > 0 && i < 10; i++) {
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    // ignore
                }
            }
        }
        // Close all peers.
        closeAllPeers();
    }

    // Notify all peers of the shutdown and restart.
    // datanode.shouldRun should still be true and datanode.restarting should
    // be set true before calling this method.
    synchronized void restartNotifyPeers() {
        assert (datanode.shouldRun && datanode.shutdownForUpgrade);
        for (Thread t : peers.values()) {
            // interrupt each and every DataXceiver thread.
            t.interrupt();
        }
    }

    // Return the number of peers.
    synchronized int getNumPeers() {
        return peers.size();
    }

    // Close all peers and clear the map.
    synchronized void closeAllPeers() {
        LOG.info("Closing all peers.");
        for (Peer p : peers.keySet()) {
            IOUtils.cleanup(null, p);
        }
        peers.clear();
        peersXceiver.clear();
    }

    synchronized void closePeer(Peer peer) {
        peers.remove(peer);
        peersXceiver.remove(peer);
        IOUtils.cleanup(null, peer);
    }

    public synchronized void sendOOBToPeers() {
        if (!datanode.shutdownForUpgrade) {
            return;
        }

        for (Peer p : peers.keySet()) {
            try {
                peersXceiver.get(p).sendOOB();
            } catch (IOException e) {
                LOG.warn("Got error when sending OOB message.", e);
            } catch (InterruptedException e) {
                LOG.warn("Interrupted when sending OOB message.");
            }
        }
    }

    void kill() {
        assert (!datanode.shouldRun || datanode.shutdownForUpgrade) :
                "shoudRun should be set to false or restarting should be true"
                        + " before killing";
        try {
            this.peerServer.close();
            this.closed = true;
        } catch (IOException ie) {
            LOG.warn("{}:DataXceiverServer.kill(): ", datanode.getDisplayName(), ie);
        }
    }

    synchronized void addPeer(Peer peer, Thread t, JindoDataXceiver xceiver)
            throws IOException {
        if (closed) {
            throw new IOException("Server closed.");
        }
        peers.put(peer, t);
        peersXceiver.put(peer, xceiver);
    }
}
