package com.aliyun.jindodata.gateway.hdfs.datanode;

import com.aliyun.jindodata.gateway.hdfs.blockIO.JindoBlockOutputStream;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketHeader;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketReceiver;
import org.apache.hadoop.hdfs.protocol.datatransfer.PipelineAck;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.LinkedList;

import static org.apache.hadoop.hdfs.protocol.datatransfer.PipelineAck.ECN.DISABLED;

public class JindoBlockReceiver implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(JindoBlockReceiver.class);
    private final ExtendedBlock block;
    private final StorageType storageType;
    private final DataInputStream in;
    private final String inAddr;
    private final String myAddr;
    private final BlockConstructionStage stage;
    private final long newGs;
    private final long minBytesRcvd;
    private final long maxBytesRcvd;
    private final String clientname;
    private final DatanodeInfo srcDataNode;
    private final JindoDataNode datanode;
    private final DataChecksum requestedChecksum;
    private final CachingStrategy cachingStrategy;
    private final boolean allowLazyPersist;
    private final boolean pinning;
    private final long datanodeSlowLogThresholdMs;
    private final long responseInterval;
    private JindoReplicaHandler replicaHandler;
    private JindoReplicaBeingWritten replicaInfo;
    private JindoReplicaOutputStreams streams;
    private DataChecksum clientChecksum;
    private DataChecksum diskChecksum;
    private JindoBlockOutputStream out = null;
    private final PacketReceiver packetReceiver = new PacketReceiver(false);
    private Daemon responder = null;

    private static enum PacketResponderType {
        NON_PIPELINE, LAST_IN_PIPELINE, HAS_DOWNSTREAM_IN_PIPELINE
    }

    public JindoBlockReceiver(ExtendedBlock block,
                              StorageType storageType,
                              DataInputStream in,
                              String inAddr,
                              String myAddr,
                              BlockConstructionStage stage,
                              long newGs,
                              long minBytesRcvd,
                              long maxBytesRcvd,
                              String clientname,
                              DatanodeInfo srcDataNode,
                              JindoDataNode datanode,
                              DataChecksum requestedChecksum,
                              CachingStrategy cachingStrategy,
                              boolean allowLazyPersist,
                              boolean pinning) throws IOException {
        try {
            this.block = block;
            this.storageType = storageType;
            this.in = in;
            this.inAddr = inAddr;
            this.myAddr = myAddr;
            this.stage = stage;
            this.newGs = newGs;
            this.minBytesRcvd = minBytesRcvd;
            this.maxBytesRcvd = maxBytesRcvd;
            this.clientname = clientname;
            this.srcDataNode = srcDataNode;
            this.datanode = datanode;
            this.requestedChecksum = requestedChecksum;
            this.cachingStrategy = cachingStrategy;
            this.allowLazyPersist = allowLazyPersist;
            this.pinning = pinning;
            this.datanodeSlowLogThresholdMs = datanode.getDatanodeSlowIoWarningThresholdMs();
            this.responseInterval = (long) (datanode.getSocketTimeout() * 0.5);

            if (LOG.isDebugEnabled()) {
                LOG.debug(getClass().getSimpleName() + ": " + block
                    + "\n storageType=" + storageType + ", inAddr=" + inAddr
                    + ", myAddr=" + myAddr + "\n stage=" + stage + ", newGs=" + newGs
                    + ", minBytesRcvd=" + minBytesRcvd
                    + ", maxBytesRcvd=" + maxBytesRcvd + "\n clientname=" + clientname
                    + ", srcDataNode=" + srcDataNode
                    + ", datanode=" + datanode.getDisplayName()
                    + "\n requestedChecksum=" + requestedChecksum
                    + "\n cachingStrategy=" + cachingStrategy
                    + "\n allowLazyPersist=" + allowLazyPersist + ", pinning=" + pinning
                    + ", responseInterval=" + responseInterval
                );
            }

            {
                // open remote block writer
                switch (stage) {
                    case PIPELINE_SETUP_CREATE:
                      replicaHandler = createRbw(block);
                      break;
                    case PIPELINE_SETUP_STREAMING_RECOVERY:
                      replicaHandler = recoverRbw(
                          block, newGs);
                      break;
                    case PIPELINE_SETUP_APPEND:
                    case PIPELINE_SETUP_APPEND_RECOVERY:
                    case TRANSFER_RBW:
                    case TRANSFER_FINALIZED:
                    default:
                        throw new IOException("Unsupported stage " + stage +
                          " while receiving block " + block + " from " + inAddr);
                }
            }

            replicaInfo = replicaHandler.getReplica();
            streams = replicaInfo.createStreams(requestedChecksum, datanode.getRequestOptions());
            clientChecksum = requestedChecksum;
            diskChecksum = streams.getChecksum();
            this.out = streams.getDataOut();
        } catch(IOException ioe) {
            IOUtils.closeStream(this);
            throw ioe;
        }
    }

    private void verifyChunks(ByteBuffer dataBuf, ByteBuffer checksumBuf)
        throws IOException {
        try {
            clientChecksum.verifyChunkedSums(dataBuf, checksumBuf, clientname, 0);
        } catch (ChecksumException ce) {
            PacketHeader header = packetReceiver.getHeader();
            String specificOffset = "specific offsets are:"
                    + " offsetInBlock = " + header.getOffsetInBlock()
                    + " offsetInPacket = " + ce.getPos();
            LOG.warn("Checksum error in block "
                    + block + " from " + inAddr
                    + ", " + specificOffset, ce);
            throw new IOException("Unexpected checksum mismatch while writing "
                    + block + " from " + inAddr);
        }
    }

    private int receivePacket() throws IOException {
        // read the next packet
        packetReceiver.receiveNextPacket(in);

        PacketHeader header = packetReceiver.getHeader();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Receiving one packet for block " + block +
                    ": " + header);
        }

        // Sanity check the header
        if (header.getDataLen() < 0) {
            throw new IOException("Got wrong length during writeBlock(" + block +
                    ") from " + inAddr + " at offset " +
                    header.getOffsetInBlock() + ": " +
                    header.getDataLen());
        }

        long offsetInBlock = header.getOffsetInBlock();
        long seqno = header.getSeqno();
        boolean lastPacketInBlock = header.isLastPacketInBlock();
        final int len = header.getDataLen();
        boolean syncBlock = header.getSyncBlock();

        // update received bytes
        final long firstByteInBlock = offsetInBlock;
        offsetInBlock += len;

        ByteBuffer dataBuf = packetReceiver.getDataSlice();
        ByteBuffer checksumBuf = packetReceiver.getChecksumSlice();

        if (lastPacketInBlock || len == 0) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Receiving an empty packet or the end of the block " + block);
            }
        } else {
            final int checksumLen = diskChecksum.getChecksumSize(len);
            final int checksumReceivedLen = checksumBuf.capacity();

            if (checksumReceivedLen > 0 && checksumReceivedLen != checksumLen) {
                throw new IOException("Invalid checksum length: received length is "
                        + checksumReceivedLen + " but expected length is " + checksumLen);
            }

            if (checksumReceivedLen > 0) {
                try {
                    verifyChunks(dataBuf, checksumBuf);
                } catch (IOException ioe) {
                    // checksum error detected locally. there is no reason to continue.
                    if (responder != null) {
                        try {
                            ((JindoPacketResponder) responder.getRunnable()).enqueue(seqno,
                                    lastPacketInBlock, offsetInBlock,
                                    DataTransferProtos.Status.ERROR_CHECKSUM);
                            // Wait until the responder sends back the response
                            // and interrupt this thread.
                            Thread.sleep(3000);
                        } catch (InterruptedException e) {
                        }
                    }
                    throw new IOException("Terminating due to a checksum error." + ioe);
                }
            }
        }
        // if sync was requested, put in queue for pending acks here
        // (after the fsync finished)
        if (responder != null) {
            byte[] buf = new byte[len];
            System.arraycopy(dataBuf.array(), dataBuf.arrayOffset() + dataBuf.position(),
                    buf, 0, len);
            ((JindoPacketResponder) responder.getRunnable()).enqueue(seqno,
                    lastPacketInBlock, offsetInBlock, DataTransferProtos.Status.SUCCESS,
                    buf, syncBlock);
        }

        return lastPacketInBlock ? -1 : len;
    }

    void receiveBlock(DataOutputStream replyOut) throws IOException {
        boolean responderClosed = false;
        try {
            {
                responder = new Daemon(datanode.threadGroup,
                        new JindoPacketResponder(replyOut, out, this));
                responder.start(); // start thread to processes responses
            }

            while (receivePacket() >= 0) { /* Receive until the last packet */ }

            // wait for all outstanding packet responses. And then
            // indicate responder to gracefully shutdown.
            // Mark that responder has been closed for future processing
            if (responder != null) {
                ((JindoPacketResponder) responder.getRunnable()).close();
                responderClosed = true;
            }

        } catch (IOException ioe) {
            LOG.info("Exception for " + block, ioe);
            throw ioe;
        } finally {
            // Clear the previous interrupt state of this thread.
            Thread.interrupted();

            // If a shutdown for restart was initiated, upstream needs to be notified.
            // There is no need to do anything special if the responder was closed
            // normally.
            if (!responderClosed) { // Data transfer was not complete.
                if (responder != null) {
                    responder.interrupt();
                }
                IOUtils.closeStream(this);
            }
            if (responder != null) {
                try {
                    responder.interrupt();
                    // join() on the responder should timeout a bit earlier than the
                    // configured deadline. Otherwise, the join() on this thread will
                    // likely timeout as well.
                    long joinTimeout = datanode.getXceiverStopTimeout();
                    joinTimeout = joinTimeout > 1 ? joinTimeout * 8 / 10 : joinTimeout;
                    responder.join(joinTimeout);
                    if (responder.isAlive()) {
                        String msg = "Join on responder thread " + responder
                                + " timed out";
                        LOG.warn(msg + "\n" + StringUtils.getStackTrace(responder));
                        throw new IOException(msg);
                    }
                } catch (InterruptedException e) {
                    responder.interrupt();
                    throw new IOException("Interrupted receiveBlock");
                }
                responder = null;
            }
        }
    }

    private JindoReplicaHandler createRbw(ExtendedBlock block)
            throws IOException {
        JindoReplicaBeingWritten replicaInfo = new JindoReplicaBeingWritten(block);
        return new JindoReplicaHandler(replicaInfo);
    }

    private JindoReplicaHandler recoverRbw(ExtendedBlock block, long newGs)
            throws IOException {
        JindoReplicaBeingWritten replicaInfo = new JindoReplicaBeingWritten(block, newGs);
        return new JindoReplicaHandler(replicaInfo);
    }

    public void sendOOB() throws IOException, InterruptedException {
    }

    @Override
    public void close() throws IOException {
        out = null;
    }

    class JindoPacketResponder implements Runnable, Closeable {
        private JindoBlockOutputStream out;
        private final PacketResponderType type;
        private final DataOutputStream upstreamOut;
        private final String myString;
        private boolean sending = false;
        private volatile boolean running = true;
        private final LinkedList<Packet> ackQueue = new LinkedList<Packet>();
        private final JindoBlockReceiver receiver;
        private final Thread receiverThread = Thread.currentThread();

        @Override
        public String toString() {
            return myString;
        }

        public JindoPacketResponder(DataOutputStream upstreamOut,
                                    JindoBlockOutputStream out,
                                    JindoBlockReceiver receiver) {
            this.upstreamOut = upstreamOut;
            this.out = out;
            type = PacketResponderType.LAST_IN_PIPELINE;
            final StringBuilder b = new StringBuilder(getClass().getSimpleName())
                    .append(": ").append(block).append(", type=").append(type);
            this.myString = b.toString();
            this.receiver = receiver;
        }

        private boolean isRunning() {
            // When preparing for a restart, it should continue to run until
            // interrupted by the receiver thread.
            return running && datanode.shouldRun;
        }

        @Override
        public void close() throws IOException {
            synchronized (ackQueue) {
                while (isRunning() && !ackQueue.isEmpty()) {
                    try {
                        ackQueue.wait();
                    } catch (InterruptedException e) {
                        running = false;
                        Thread.currentThread().interrupt();
                    }
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug(myString + ": closing");
                }
                running = false;
                ackQueue.notifyAll();
            }

            synchronized (this) {
                running = false;
                notifyAll();
            }
        }

        Packet waitForAckHead(long seqno) throws InterruptedException {
            synchronized (ackQueue) {
                while (isRunning() && ackQueue.isEmpty()) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("{}: seqno={} waiting for local datanode to finish write.", myString, seqno);
                    }
                    ackQueue.wait();
                }
                return isRunning() ? ackQueue.getFirst() : null;
            }
        }

        @Override
        public void run() {
            boolean lastPacketInBlock = false;
            final long startTime = LOG.isInfoEnabled() ? System.nanoTime() : 0;
            while (isRunning() && !lastPacketInBlock) {
                long totalAckTimeNanos = 0;
                boolean isInterrupted = false;
                try {
                    Packet pkt = null;
                    long expected = -2;
                    PipelineAck ack = new PipelineAck();
                    long seqno = PipelineAck.UNKOWN_SEQNO;
                    try {
                        pkt = waitForAckHead(seqno);
                        if (!isRunning()) {
                            break;
                        }
                        expected = pkt.seqno;
                        lastPacketInBlock = pkt.lastPacketInBlock;

                        LOG.debug("Receive pkt : {}", pkt);
                    } catch (InterruptedException ine) {
                        isInterrupted = true;
                    }

                    if (Thread.interrupted() || isInterrupted) {
                        /*
                         * The receiver thread cancelled this thread. We could also check
                         * any other status updates from the receiver thread (e.g. if it is
                         * ok to write to replyOut). It is prudent to not send any more
                         * status back to the client because this datanode has a problem.
                         * The upstream datanode will detect that this datanode is bad, and
                         * rightly so.
                         *
                         * The receiver thread can also interrupt this thread for sending
                         * an out-of-band response upstream.
                         */
                        LOG.info("{}: Thread is interrupted.", myString);
                        running = false;
                        continue;
                    }

                    if (pkt.ackStatus != DataTransferProtos.Status.SUCCESS) {
                        LOG.warn("Received not success pkt. Send ack to client {}", pkt.ackStatus);
                        sendAckUpstream(ack, expected, totalAckTimeNanos,
                                pkt.offsetInBlock,
                                PipelineAck.combineHeader(DISABLED, pkt.ackStatus));
                        throw new IOException("Received not success pkg. Send ack to client " + pkt.ackStatus);
                    }
                    try {
                        processPacket(pkt, startTime);
                    } catch (IOException e) {
                        LOG.warn("Process packet {} Error. Send ack to client ERROR", pkt);
                        sendAckUpstream(ack, expected, totalAckTimeNanos,
                                pkt.offsetInBlock,
                                PipelineAck.combineHeader(DISABLED, DataTransferProtos.Status.ERROR));
                        throw e;
                    }

                    DataTransferProtos.Status myStatus = pkt.ackStatus;
                    sendAckUpstream(ack, expected, totalAckTimeNanos,
                            pkt.offsetInBlock,
                            PipelineAck.combineHeader(DISABLED, myStatus));
                    // remove the packet from the ack queue
                    removeAckHead();
                } catch (IOException e) {
                    LOG.warn("IOException in BlockReceiver.run(): ", e);
                    if (running) {
                        LOG.info(myString, e);
                        running = false;
                        if (!Thread.interrupted()) { // failure not caused by interruption
                            receiverThread.interrupt();
                        }
                    }
                } catch (Throwable e) {
                    if (running) {
                        LOG.info(myString, e);
                        running = false;
                        receiverThread.interrupt();
                    }
                }
            }
            LOG.info("{} terminating", myString);
        }

        private void processPacket(Packet pkt, long startTime) throws IOException {
            if (pkt.lastPacketInBlock) {
                if (out != null) {
                    out.close();
                    out = null;
                }
                finalizeBlock(startTime);
            } else {
                out.write(pkt.offsetInBlock, pkt.dataBuf);
                if (pkt.syncBlock) {
                    out.flush();
                }
            }
        }

        private void finalizeBlock(long startTime) throws IOException {
            receiver.close();
            if (LOG.isInfoEnabled()) {
                long endTime = LOG.isInfoEnabled() ? System.nanoTime() : 0;
                LOG.info("Received {} size {} from {} costs {} ns",
                    block, block.getNumBytes(), inAddr, endTime - startTime);
            } else {
                LOG.info("Received {} size {} from {}",
                    block, block.getNumBytes(), inAddr);
            }
        }

        private void removeAckHead() {
            synchronized (ackQueue) {
                ackQueue.removeFirst();
                ackQueue.notifyAll();
            }
        }

        public void enqueue(long seqno, boolean lastPacketInBlock,
                            long offsetInBlock, DataTransferProtos.Status ackStatus) {
            final Packet p = new Packet(seqno, lastPacketInBlock, offsetInBlock,
                    System.nanoTime(), ackStatus);
            if (LOG.isDebugEnabled()) {
                LOG.debug("{}: enqueue {}", myString, p);
            }
            synchronized (ackQueue) {
                if (running) {
                    ackQueue.addLast(p);
                    ackQueue.notifyAll();
                }
            }
        }

        public void enqueue(long seqno, boolean lastPacketInBlock,
                            long offsetInBlock, DataTransferProtos.Status ackStatus,
                            byte[] dataBuf, boolean syncBlock) {
            final Packet p = new Packet(seqno, lastPacketInBlock, offsetInBlock,
                    System.nanoTime(), ackStatus, dataBuf, syncBlock);
            if (LOG.isDebugEnabled()) {
                LOG.debug("{}: enqueue {}", myString, p);
            }
            synchronized (ackQueue) {
                if (running) {
                    ackQueue.addLast(p);
                    ackQueue.notifyAll();
                }
            }
        }

        private void sendAckUpstream(PipelineAck ack, long seqno,
                            long totalAckTimeNanos, long offsetInBlock,
                            int myHeader) throws IOException {
            try {
                // Wait for other sender to finish. Unless there is an OOB being sent,
                // the responder won't have to wait.
                synchronized (this) {
                    while (sending) {
                        wait();
                    }
                    sending = true;
                }

                try {
                    if (!running) return;
                    sendAckUpstreamUnprotected(ack, seqno, totalAckTimeNanos,
                            offsetInBlock, myHeader);
                } finally {
                    synchronized (this) {
                        sending = false;
                        notify();
                    }
                }
            } catch (InterruptedException ie) {
                // The responder was interrupted. Make it go down without
                // interrupting the receiver(writer) thread.
                running = false;
            }
        }

        private void sendAckUpstreamUnprotected(PipelineAck ack, long seqno,
                                                long totalAckTimeNanos, long offsetInBlock, int myHeader)
            throws IOException {
          final int[] replies;
          if (ack == null) {
            // A new OOB response is being sent from this node. Regardless of
            // downstream nodes, reply should contain one reply.
            replies = new int[] { myHeader };
          } else {
            short ackLen = type == PacketResponderType.LAST_IN_PIPELINE ? 0 : ack
                .getNumOfReplies();
            replies = new int[ackLen + 1];
            replies[0] = myHeader;
            for (int i = 0; i < ackLen; ++i) {
              replies[i + 1] = ack.getHeaderFlag(i);
            }
            // If the mirror has reported that it received a corrupt packet,
            // do self-destruct to mark myself bad, instead of making the
            // mirror node bad. The mirror is guaranteed to be good without
            // corrupt data on disk.
            if (ackLen > 0 && PipelineAck.getStatusFromHeader(replies[1]) ==
              DataTransferProtos.Status.ERROR_CHECKSUM) {
              throw new IOException("Shutting down writer and responder "
                  + "since the down streams reported the data sent by this "
                  + "thread is corrupt");
            }
          }
          PipelineAck replyAck = new PipelineAck(seqno, replies,
              totalAckTimeNanos);
          // send my ack back to upstream datanode
          long begin = Time.monotonicNow();
          replyAck.write(upstreamOut);
          upstreamOut.flush();
          long duration = Time.monotonicNow() - begin;
          if (duration > datanodeSlowLogThresholdMs) {
            LOG.warn("Slow PacketResponder send ack to upstream took " + duration
                + "ms (threshold=" + datanodeSlowLogThresholdMs + "ms), " + myString
                + ", replyAck=" + replyAck);
          } else if (LOG.isDebugEnabled()) {
            LOG.debug(myString + ", replyAck=" + replyAck);
          }

          // If a corruption was detected in the received data, terminate after
          // sending ERROR_CHECKSUM back.
          DataTransferProtos.Status myStatus = PipelineAck.getStatusFromHeader(myHeader);
          if (myStatus == DataTransferProtos.Status.ERROR_CHECKSUM) {
            throw new IOException("Shutting down writer and responder "
                + "due to a checksum error in received data. The error "
                + "response has been sent upstream.");
          }
        }
    }

    private static class Packet {
        final long seqno;
        final boolean lastPacketInBlock;
        final long offsetInBlock;
        final long ackEnqueueNanoTime;
        final DataTransferProtos.Status ackStatus;
        byte[] dataBuf;
        boolean syncBlock;

        Packet(long seqno, boolean lastPacketInBlock, long offsetInBlock,
               long ackEnqueueNanoTime, DataTransferProtos.Status ackStatus) {
            this.seqno = seqno;
            this.lastPacketInBlock = lastPacketInBlock;
            this.offsetInBlock = offsetInBlock;
            this.ackEnqueueNanoTime = ackEnqueueNanoTime;
            this.ackStatus = ackStatus;
        }

        Packet(long seqno, boolean lastPacketInBlock, long offsetInBlock,
               long ackEnqueueNanoTime, DataTransferProtos.Status ackStatus,
               byte[] dataBuf, boolean syncBlock) {
            this.seqno = seqno;
            this.lastPacketInBlock = lastPacketInBlock;
            this.offsetInBlock = offsetInBlock;
            this.ackEnqueueNanoTime = ackEnqueueNanoTime;
            this.ackStatus = ackStatus;
            this.dataBuf = dataBuf;
            this.syncBlock = syncBlock;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(seqno=" + seqno
                    + ", lastPacketInBlock=" + lastPacketInBlock
                    + ", offsetInBlock=" + offsetInBlock
                    + ", ackEnqueueNanoTime=" + ackEnqueueNanoTime
                    + ", ackStatus=" + ackStatus
                    + ", dataBuf=" + (dataBuf == null ? "null" : dataBuf.length)
                    + ", syncBlock=" + syncBlock
                    + ")";
        }
    }
}
