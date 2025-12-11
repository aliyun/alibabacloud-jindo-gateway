package com.aliyun.jindodata.gateway.hdfs.datanode;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.hdfs.protocol.UnregisteredNodeException;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.common.IncorrectVersionException;
import org.apache.hadoop.hdfs.server.protocol.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import static org.apache.hadoop.util.Time.monotonicNow;

public class JindoBPServiceActor implements Runnable{
    private static final Logger LOG = LoggerFactory.getLogger(JindoBPServiceActor.class);
    final InetSocketAddress nnAddr;
    HAServiceProtocol.HAServiceState state;

    final JindoBPOfferService bpos;

    enum RunningState {
        CONNECTING, INIT_FAILED, RUNNING, EXITED, FAILED;
    }
    private volatile RunningState runningState = RunningState.CONNECTING;

    Thread bpThread;
    DatanodeProtocolClientSideTranslatorPB bpNamenode;
    private final JindoDataNode dn;
    private volatile boolean shouldServiceRun = true;
    private DatanodeRegistration bpRegistration;
    private final Scheduler scheduler;

    public JindoBPServiceActor(InetSocketAddress nnAddr, JindoBPOfferService bpos) {
        this.bpos = bpos;
        this.dn = bpos.getDataNode();
        this.nnAddr = nnAddr;
        scheduler = new Scheduler(dn.heartBeatInterval);
    }

    public InetSocketAddress getNNSocketAddress() {
        return nnAddr;
    }

    public void start() {
        if ((bpThread != null) && (bpThread.isAlive())) {
            //Thread is started already
            return;
        }
        bpThread = new Thread(this);
        bpThread.setDaemon(true); // needed for JUnit testing
        bpThread.start();
    }

    void stop() {
        shouldServiceRun = false;
        if (bpThread != null) {
            bpThread.interrupt();
        }
    }

    public void join() {
        try {
            if (bpThread != null) {
                bpThread.join();
            }
        } catch (InterruptedException ie) { }
    }

    private boolean shouldRun() {
        return shouldServiceRun && dn.shouldRun();
    }

    private void sleepAndLogInterrupts(int millis,
                                       String stateString) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ie) {
            LOG.info("BPOfferService {} interrupted while {}", this, stateString);
        }
    }

    /**
     * Perform the first part of the handshake with the NameNode.
     * This calls <code>versionRequest</code> to determine the NN's
     * namespace and version info. It automatically retries until
     * the NN responds or the DN is shutting down.
     *
     * @return the NamespaceInfo
     */
    NamespaceInfo retrieveNamespaceInfo() throws IOException {
        NamespaceInfo nsInfo = null;
        while (shouldRun()) {
            try {
                nsInfo = bpNamenode.versionRequest();
                LOG.debug("{} received versionRequest response: {}", this, nsInfo);
                break;
            } catch(SocketTimeoutException e) {  // namenode is busy
                LOG.warn("Problem connecting to server: {}", nnAddr, e);
            } catch(IOException e) {  // namenode is not available
                LOG.warn("Problem connecting to server: {}", nnAddr, e);
            }

            // try again in a second
            sleepAndLogInterrupts(5000, "requesting version info from NN");
        }

        if (nsInfo == null) {
            throw new IOException("DN shut down before block pool connected");
        }
        return nsInfo;
    }

    private String formatThreadName(
            final String action,
            final InetSocketAddress addr) {
        String bpId = bpos.getBlockPoolId(true);
        final String prefix = bpId != null ? bpId : bpos.getNameserviceId();
        return prefix + " " + action + " to " + addr;
    }

    void register(NamespaceInfo nsInfo) throws IOException {
        // The handshake() phase loaded the block pool storage
        // off disk - so update the bpRegistration object from that info
        DatanodeRegistration newBpRegistration = bpos.createRegistration();

        LOG.info(this + " beginning handshake with NN");

        while (shouldRun()) {
            try {
                // Use returned registration from namenode with updated fields
                newBpRegistration = bpNamenode.registerDatanode(newBpRegistration);
                newBpRegistration.setNamespaceInfo(nsInfo);
                bpRegistration = newBpRegistration;
                break;
            } catch(EOFException e) {  // namenode might have just restarted
                LOG.info("Problem connecting to server: {} :{}", nnAddr, e.getLocalizedMessage());
                sleepAndLogInterrupts(1000, "connecting to server");
            } catch(SocketTimeoutException e) {  // namenode is busy
                LOG.info("Problem connecting to server: {}", nnAddr);
                sleepAndLogInterrupts(1000, "connecting to server");
            }
        }

        LOG.info("Block pool {} successfully registered with NN", this);
        bpos.registrationSucceeded(bpRegistration);
    }

    private void connectToNNAndHandshake() throws IOException {
        // get NN proxy
        bpNamenode = dn.connectToNN(nnAddr);

        // First phase of the handshake with NN - get the namespace
        // info.
        NamespaceInfo nsInfo = retrieveNamespaceInfo();

        // Verify that this matches the other NN in this HA pair.
        // This also initializes our block pool in the DN if we are
        // the first NN connection for this BP.
        bpos.verifyAndSetNamespaceInfo(this, nsInfo);

        /* set thread name again to include NamespaceInfo when it's available. */
        this.bpThread.setName(formatThreadName("heartbeating", nnAddr));

        // Second phase of the handshake with the NN.
        register(nsInfo);
    }

    private boolean shouldRetryInit() {
        return shouldRun() && bpos.shouldRetryInit();
    }

    boolean isAlive() {
        if (!shouldServiceRun || !bpThread.isAlive()) {
            return false;
        }
        return runningState == RunningState.RUNNING
                || runningState == RunningState.CONNECTING;
    }

    boolean processCommand(DatanodeCommand[] cmds) {
        if (cmds != null) {
            for (DatanodeCommand cmd : cmds) {
                try {
                    if (!bpos.processCommandFromActor(cmd, this)) {
                        return false;
                    }
                } catch (IOException ioe) {
                    LOG.warn("Error processing datanode Command", ioe);
                }
            }
        }
        return true;
    }

    synchronized void waitTillNextIBR(long waitTime) {
        if (waitTime > 0) {
            try {
                wait(waitTime);
            } catch (InterruptedException ie) {
                LOG.warn(getClass().getSimpleName() + " interrupted");
            }
        }
    }

    private void sleepAfterException() {
        try {
            long sleepTime = Math.min(1000, dn.heartBeatInterval);
            Thread.sleep(sleepTime);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Main loop for each BP thread. Run until shutdown,
     * forever calling remote NameNode functions.
     */
    private void offerService() {
        LOG.info("For namenode " + nnAddr + " using"
                + "; heartBeatInterval=" + dn.heartBeatInterval);

        //
        // Now loop for a long time....
        //
        while (shouldRun()) {
            try {
                final long startTime = scheduler.monotonicNow();

                //
                // Every so often, send heartbeat or block-report
                //
                final boolean sendHeartbeat = scheduler.isHeartbeatDue(startTime);
                HeartbeatResponse resp = null;
                if (sendHeartbeat) {
                    resp = sendHeartBeat();
                    assert resp != null;

                    // If the state of this NN has changed (eg STANDBY->ACTIVE)
                    // then let the BPOfferService update itself.
                    //
                    // Important that this happens before processCommand below,
                    // since the first heartbeat to a new active might have commands
                    // that we should actually process.
                    bpos.updateActorStatesFromHeartbeat(
                            this, resp.getNameNodeHaState());
                    state = resp.getNameNodeHaState().getState();

                    long startProcessCommands = monotonicNow();
                    if (!processCommand(resp.getCommands()))
                        continue;
                    long endProcessCommands = monotonicNow();
                    if (endProcessCommands - startProcessCommands > 2000) {
                        LOG.info("Took {}ms to process {} commands from NN",
                                endProcessCommands - startProcessCommands, resp.getCommands().length);
                    }

                }

                // There is no work to do;  sleep until hearbeat timer elapses,
                // or work arrives, and then iterate again.
                waitTillNextIBR(scheduler.getHeartbeatWaitTime());
            } catch(RemoteException re) {
                String reClass = re.getClassName();
                if (UnregisteredNodeException.class.getName().equals(reClass) ||
                        DisallowedDatanodeException.class.getName().equals(reClass) ||
                        IncorrectVersionException.class.getName().equals(reClass)) {
                    LOG.warn("{} is shutting down", this, re);
                    shouldServiceRun = false;
                    return;
                }
                LOG.warn("RemoteException in offerService", re);
                sleepAfterException();
            } catch (IOException e) {
                LOG.warn("IOException in offerService", e);
                sleepAfterException();
            }
        } // while (shouldRun())
    } // offerService

    HeartbeatResponse sendHeartBeat()
            throws IOException {
        scheduler.scheduleNextHeartbeat();

        scheduler.updateLastHeartbeatTime(monotonicNow());
        return bpNamenode.sendHeartbeat(bpRegistration,
                new StorageReport[0],
                0,
                0,
                0,
                0,
                0,
                null,
                false);
    }

    //Cleanup method to be called by current thread before exiting.
    private synchronized void cleanUp() {

        shouldServiceRun = false;
        IOUtils.cleanup(null, bpNamenode);
        bpos.shutdownActor(this);
    }

    /**
     * No matter what kind of exception we get, keep retrying to offerService().
     * That's the loop that connects to the NameNode and provides basic DataNode
     * functionality.
     *
     * Only stop when "shouldRun" or "shouldServiceRun" is turned off, which can
     * happen either at shutdown or due to refreshNamenodes.
     */
    @Override
    public void run() {
        LOG.info("{} starting to offer service", this);

        try {
            while (true) {
                // init stuff
                try {
                    // setup storage
                    connectToNNAndHandshake();
                    break;
                } catch (IOException ioe) {
                    // Initial handshake, storage recovery or registration failed
                    runningState = RunningState.INIT_FAILED;
                    if (shouldRetryInit()) {
                        // Retry until all namenode's of BPOS failed initialization
                        LOG.error("Initialization failed for {} {}",
                                this, ioe.getLocalizedMessage());
                        sleepAndLogInterrupts(5000, "initializing");
                    } else {
                        runningState = RunningState.FAILED;
                        LOG.error("Initialization failed for {}. Exiting. ", this, ioe);
                        return;
                    }
                }
            }

            runningState = RunningState.RUNNING;

            while (shouldRun()) {
                try {
                    offerService();
                } catch (Exception ex) {
                    LOG.error("Exception in BPOfferService for {}", this, ex);
                    sleepAndLogInterrupts(5000, "offering service");
                }
            }
            runningState = RunningState.EXITED;
        } catch (Throwable ex) {
            LOG.warn("Unexpected exception in block pool {}", this, ex);
            runningState = RunningState.FAILED;
        } finally {
            LOG.warn("Ending block pool service for: {}", this);
            cleanUp();
        }
    }

    /**
     * Utility class that wraps the timestamp computations for scheduling
     * heartbeats and block reports.
     */
    static class Scheduler {
        @VisibleForTesting
        volatile long nextHeartbeatTime = monotonicNow();

        @VisibleForTesting
        volatile long lastHeartbeatTime = monotonicNow();

        private final long heartbeatIntervalMs;

        Scheduler(long heartbeatIntervalMs) {
            this.heartbeatIntervalMs = heartbeatIntervalMs;
        }

        long scheduleHeartbeat() {
            nextHeartbeatTime = monotonicNow();
            return nextHeartbeatTime;
        }

        long scheduleNextHeartbeat() {
            // Numerical overflow is possible here and is okay.
            nextHeartbeatTime = monotonicNow() + heartbeatIntervalMs;
            return nextHeartbeatTime;
        }

        void updateLastHeartbeatTime(long heartbeatTime) {
            lastHeartbeatTime = heartbeatTime;
        }

        long getLastHearbeatTime() {
            return (monotonicNow() - lastHeartbeatTime)/1000;
        }


        boolean isHeartbeatDue(long startTime) {
            return (nextHeartbeatTime - startTime <= 0);
        }


        long getHeartbeatWaitTime() {
            return nextHeartbeatTime - monotonicNow();
        }

        /**
         * Wrapped for testing.
         * @return
         */
        @VisibleForTesting
        public long monotonicNow() {
            return Time.monotonicNow();
        }
    }

    void reRegister() throws IOException {
        if (shouldRun()) {
            // re-retrieve namespace info to make sure that, if the NN
            // was restarted, we still match its version (HDFS-2120)
            NamespaceInfo nsInfo = retrieveNamespaceInfo();
            // and re-register
            register(nsInfo);
            scheduler.scheduleHeartbeat();
        }
    }
}
