package com.aliyun.jindodata.gateway.hdfs.datanode;

import com.aliyun.jindodata.gateway.hdfs.blockIO.JindoBlockCrcInputStream;
import com.google.protobuf.ByteString;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.*;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.datanode.*;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitShm;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.channels.ClosedChannelException;
import java.util.Arrays;

import static com.aliyun.jindodata.gateway.common.JfsConstant.BYTES_PER_CHECKSUM_DEFAULT;
import static org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status.ERROR;
import static org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status.SUCCESS;

public class JindoDataXceiver extends Receiver implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(JindoDataXceiver.class);

    private Peer peer;
    private final String remoteAddress; // address of remote side
    private final String localAddress;  // local address of this daemon
    private final JindoDataNode datanode;
    private final JindoDataXceiverServer dataXceiverServer;
    private final InputStream socketIn;
    private OutputStream socketOut;
    private JindoBlockReceiver blockReceiver = null;
    private final int ioFileBufferSize;
    private final int smallBufferSize;
    private Thread xceiver = null;

    protected JindoDataXceiver(Peer peer, JindoDataNode datanode,
                               JindoDataXceiverServer dataXceiverServer) throws IOException {
        super(null);
        this.peer = peer;
        this.socketIn = peer.getInputStream();
        this.socketOut = peer.getOutputStream();
        this.datanode = datanode;
        this.dataXceiverServer = dataXceiverServer;
        remoteAddress = peer.getRemoteAddressString();
        localAddress = peer.getLocalAddressString();
        this.ioFileBufferSize = DFSUtilClient.getIoFileBufferSize(datanode.getConf());
        this.smallBufferSize = DFSUtilClient.getSmallBufferSize(datanode.getConf());

        if (LOG.isDebugEnabled()) {
            LOG.debug("Number of active connections is: {}", datanode.getXceiverCount());
        }
    }

    public static Runnable create(Peer peer, JindoDataNode dn, JindoDataXceiverServer dataXceiverServer) throws IOException {
        return new JindoDataXceiver(peer, dn, dataXceiverServer);
    }

    public void sendOOB() throws IOException, InterruptedException {
        JindoBlockReceiver br = getCurrentBlockReceiver();
        if (br == null) {
            return;
        }
        // This doesn't need to be in a critical section. Althogh the client
        // can resue the connection to issue a different request, trying sending
        // an OOB through the recently closed block receiver is harmless.
        LOG.info("Sending OOB to peer: {}", peer);
        br.sendOOB();
    }

    @Override
    public void run() {
        int opsProcessed = 0;
        Op op = null;

        try {
            synchronized (this) {
                xceiver = Thread.currentThread();
            }
            dataXceiverServer.addPeer(peer, Thread.currentThread(), this);
            peer.setWriteTimeout(datanode.getSocketWriteTimeout());

            super.initialize(new DataInputStream(socketIn));

            // We process requests in a loop, and stay around for a short timeout.
            // This optimistic behaviour allows the other end to reuse connections.
            // Setting keepalive timeout to 0 disable this behavior.
            do {
                try {
                    if (opsProcessed != 0) {
                        assert datanode.getSocketKeepaliveTimeout() > 0;
                        peer.setReadTimeout(datanode.getSocketKeepaliveTimeout());
                    } else {
                        peer.setReadTimeout(datanode.getSocketTimeout());
                    }
                    op = readOp();
                } catch (InterruptedIOException ignored) {
                    // Time out while we wait for client rpc
                    break;
                } catch (EOFException | ClosedChannelException e) {
                    // Since we optimistically expect the next op, it's quite normal to
                    // get EOF here.
                    LOG.debug("Cached {} closing after {} ops.  " +
                            "This message is usually benign.", peer, opsProcessed);
                    break;
                } catch (IOException err) {
                    LOG.error("{}:DataXceiver error processing", datanode.getDisplayName(), err);
                    throw err;
                }

                // restore normal timeout
                if (opsProcessed != 0) {
                    peer.setReadTimeout(datanode.getSocketTimeout());
                }

                processOp(op);
                ++opsProcessed;
            } while ((peer != null) &&
                    (!peer.isClosed() && datanode.getSocketKeepaliveTimeout() > 0));
        } catch (Throwable t) {
            String s = datanode.getDisplayName() + ":DataXceiver error processing "
                    + ((op == null) ? "unknown" : op.name()) + " operation "
                    + " src: " + remoteAddress + " dst: " + localAddress;
            if (op == Op.WRITE_BLOCK && t instanceof ReplicaAlreadyExistsException) {
                // For WRITE_BLOCK, it is okay if the replica already exists since
                // client and replication may write the same block to the same datanode
                // at the same time.
                if (LOG.isTraceEnabled()) {
                    LOG.trace(s, t);
                } else {
                    LOG.info("{};", s, t);
                }
            } else if (op == Op.READ_BLOCK && t instanceof SocketTimeoutException) {
                String s1 =
                        "Likely the client has stopped reading, disconnecting it";
                s1 += " (" + s + ")";
                if (LOG.isTraceEnabled()) {
                    LOG.trace(s1, t);
                } else {
                    LOG.info("{};", s1, t);
                }
            } else if (t instanceof SecretManager.InvalidToken) {
                // The InvalidToken exception has already been logged in
                // checkAccess() method and this is not a server error.
                if (LOG.isTraceEnabled()) {
                    LOG.trace(s, t);
                }
            } else {
                LOG.error(s, t);
            }
        } finally {
            if (LOG.isDebugEnabled()) {
                LOG.debug("{}:Number of active connections is: {}",
                        datanode.getDisplayName(), datanode.getXceiverCount());
            }
            if (peer != null) {
                dataXceiverServer.closePeer(peer);
                IOUtils.closeStream(in);
            }
        }
    }

    private OutputStream getOutputStream() {
        return socketOut;
    }

    DataOutputStream getBufferedOutputStream() {
        return new DataOutputStream(
                new BufferedOutputStream(getOutputStream(), smallBufferSize));
    }

    private void writeSuccessWithChecksumInfo(JindoBlockSender blockSender,
                                              DataOutputStream out) throws IOException {

        DataTransferProtos.ReadOpChecksumInfoProto ckInfo = DataTransferProtos.ReadOpChecksumInfoProto.newBuilder()
                .setChecksum(DataTransferProtoUtil.toProto(blockSender.getChecksum()))
                .setChunkOffset(blockSender.getOffset())
                .build();

        DataTransferProtos.BlockOpResponseProto response = DataTransferProtos.BlockOpResponseProto.newBuilder()
                .setStatus(SUCCESS)
                .setReadOpChecksumInfo(ckInfo)
                .build();
        response.writeDelimitedTo(out);
        out.flush();
    }

    private void sendResponse(DataTransferProtos.Status status,
                              String message) throws IOException {
        writeResponse(status, message, getOutputStream());
    }

    private static void writeResponse(DataTransferProtos.Status status, String message, OutputStream out)
            throws IOException {
        DataTransferProtos.BlockOpResponseProto.Builder response = DataTransferProtos.BlockOpResponseProto.newBuilder()
                .setStatus(status);
        if (message != null) {
            response.setMessage(message);
        }
        response.build().writeDelimitedTo(out);
        out.flush();
    }

    @Override
    public void readBlock(final ExtendedBlock block,
                          final Token<BlockTokenIdentifier> blockToken,
                          final String clientName,
                          final long blockOffset,
                          final long length,
                          final boolean sendChecksum,
                          final CachingStrategy cachingStrategy) throws IOException {
        long read = 0;
        OutputStream baseStream = getOutputStream();
        DataOutputStream out = getBufferedOutputStream();

        JindoBlockSender blockSender = null;
        DatanodeRegistration dnR =
                datanode.getDNRegistrationForBP(block.getBlockPoolId());
        LOG.info("Read block {}, offset {}, length {}, from client {}, remote address {}\n" +
                        "datanode {}",
                block, blockOffset, length, clientName, remoteAddress, dnR);
        try {
            try {
                blockSender = new JindoBlockSender(block, blockOffset, length,
                        true, false, sendChecksum, datanode);
            } catch (IOException e) {
                String msg = "opReadBlock " + block + " received exception " + e;
                LOG.info(msg);
                sendResponse(ERROR, msg);
                throw e;
            }

            // send op status
            writeSuccessWithChecksumInfo(blockSender, new DataOutputStream(getOutputStream()));

            long beginRead = LOG.isDebugEnabled() ? Time.monotonicNow() : 0;
            read = blockSender.sendBlock(out, baseStream); // send data
            if (LOG.isDebugEnabled()) {
                long duration = Time.monotonicNow() - beginRead;
                LOG.debug("Read block {} from {} of {} bytes in {} msecs", block, remoteAddress, read, duration);
            }
            if (blockSender.didSendEntireByteRange()) {
                // If we sent the entire range, then we should expect the client
                // to respond with a Status enum.
                try {
                    DataTransferProtos.ClientReadStatusProto stat = DataTransferProtos.ClientReadStatusProto.parseFrom(
                            PBHelperClient.vintPrefixed(in));
                    if (!stat.hasStatus()) {
                        LOG.warn("Client {} did not send a valid status code after reading. Will close connection.", peer.getRemoteAddressString());
                        IOUtils.closeStream(out);
                    }
                } catch (IOException ioe) {
                    LOG.debug("Error reading client status response. Will close connection.", ioe);
                    IOUtils.closeStream(out);
                }
            } else {
                IOUtils.closeStream(out);
            }
        } catch (SocketException ignored) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("{}:Ignoring exception while serving {} to {}", dnR, block, remoteAddress, ignored);
            }
            // Its ok for remote side to close the connection anytime.
            IOUtils.closeStream(out);
        } catch (IOException ioe) {
            /* What exactly should we do here?
             * Earlier version shutdown() datanode if there is disk error.
             */
            if (!(ioe instanceof SocketTimeoutException)) {
                LOG.warn("{}:Got exception while serving {} to {}", dnR, block, remoteAddress, ioe);
            }
            throw ioe;
        } finally {
            IOUtils.closeStream(blockSender);
        }
    }

    private void checkValid(final ExtendedBlock block,
                            final String clientname,
                            final DatanodeInfo[] targets,
                            final DatanodeInfo srcDataNode,
                            final BlockConstructionStage stage,
                            final int pipelineSize,
                            final long minBytesRcvd,
                            final long maxBytesRcvd,
                            final long latestGenerationStamp,
                            final boolean pinning) throws IOException {
        final boolean isDatanode = clientname.isEmpty();
        final boolean isClient = !isDatanode;
        final boolean isTransfer = stage == BlockConstructionStage.TRANSFER_RBW
                || stage == BlockConstructionStage.TRANSFER_FINALIZED;

        if (LOG.isDebugEnabled()) {
            LOG.debug("opWriteBlock: stage=" + stage + ", clientname=" + clientname
                    + "\n  block  =" + block + ", newGs=" + latestGenerationStamp
                    + ", bytesRcvd=[" + minBytesRcvd + ", " + maxBytesRcvd + "]"
                    + "\n  targets=" + Arrays.asList(targets)
                    + "; pipelineSize=" + pipelineSize + ", srcDataNode=" + srcDataNode
                    + ", pinning=" + pinning);
            LOG.debug("isDatanode=" + isDatanode
                    + ", isClient=" + isClient
                    + ", isTransfer=" + isTransfer);
            LOG.debug("writeBlock receive buf size " + peer.getReceiveBufferSize() +
                    " tcp no delay " + peer.getTcpNoDelay());
        }

        if (isTransfer)
            throw new IOException("Transfer is not supported.");
        if (stage == BlockConstructionStage.PIPELINE_SETUP_APPEND)
            throw new IOException("Pipeline setup append is not supported.");
        if (stage == BlockConstructionStage.PIPELINE_SETUP_APPEND_RECOVERY)
            throw new IOException("Pipeline setup append recovery is not supported.");
        if (stage == BlockConstructionStage.PIPELINE_CLOSE_RECOVERY)
            throw new IOException("Pipeline close recovery is not supported.");
        if (stage == BlockConstructionStage.PIPELINE_SETUP_STREAMING_RECOVERY)
            LOG.info("Recovering pipeline! Other target worker will do nothing.");
        if (isDatanode)
            throw new IOException("Gateway only accept client request.");
    }

    private synchronized void setCurrentBlockReceiver(JindoBlockReceiver br) {
        blockReceiver = br;
    }

    private synchronized JindoBlockReceiver getCurrentBlockReceiver() {
        return blockReceiver;
    }

    @Override
    public void writeBlock(final ExtendedBlock block,
                           final StorageType storageType,
                           final Token<BlockTokenIdentifier> blockToken,
                           final String clientname,
                           final DatanodeInfo[] targets,
                           final StorageType[] targetStorageTypes,
                           final DatanodeInfo srcDataNode,
                           final BlockConstructionStage stage,
                           final int pipelineSize,
                           final long minBytesRcvd,
                           final long maxBytesRcvd,
                           final long latestGenerationStamp,
                           DataChecksum requestedChecksum,
                           CachingStrategy cachingStrategy,
                           boolean allowLazyPersist,
                           final boolean pinning,
                           final boolean[] targetPinnings) throws IOException {

        checkValid(block, clientname, targets, srcDataNode, stage,
                pipelineSize, minBytesRcvd, maxBytesRcvd, latestGenerationStamp, pinning);
        // reply to upstream datanode or client
        final DataOutputStream replyOut = getBufferedOutputStream();
        LOG.info("Receiving {} src: {} dest: {}", block, remoteAddress, localAddress);

        try {
            // open a block receiver
            setCurrentBlockReceiver(new JindoBlockReceiver(block, storageType, in,
                    peer.getRemoteAddressString(),
                    peer.getLocalAddressString(),
                    stage, latestGenerationStamp, minBytesRcvd, maxBytesRcvd,
                    clientname, srcDataNode, datanode, requestedChecksum,
                    cachingStrategy, allowLazyPersist, pinning));

            // send connect-ack to source for clients and not transfer-RBW/Finalized
            {
                DataTransferProtos.BlockOpResponseProto.newBuilder()
                        .setStatus(SUCCESS)
                        .setFirstBadLink("")
                        .build()
                        .writeDelimitedTo(replyOut);
                replyOut.flush();
            }

            // receive the block and mirror to the next target
            if (blockReceiver != null) {
                blockReceiver.receiveBlock(replyOut);
            }
        } catch (IOException ioe) {
            LOG.info("opWriteBlock {} received exception ", block, ioe);
            throw ioe;
        } finally {
            // close all opened streams
            IOUtils.closeStream(replyOut);
            IOUtils.closeStream(blockReceiver);
            setCurrentBlockReceiver(null);
        }
    }

    @Override
    public void transferBlock(final ExtendedBlock blk,
                              final Token<BlockTokenIdentifier> blockToken,
                              final String clientName,
                              final DatanodeInfo[] targets,
                              final StorageType[] targetStorageTypes) throws IOException {
        throw new UnsupportedOperationException("transferBlock not supported yet.");
    }

    @Override
    public void requestShortCircuitFds(ExtendedBlock extendedBlock, Token<BlockTokenIdentifier> token, ShortCircuitShm.SlotId slotId, int i, boolean b) throws IOException {
        throw new UnsupportedOperationException("requestShortCircuitFds not supported yet.");
    }

    @Override
    public void releaseShortCircuitFds(ShortCircuitShm.SlotId slotId) throws IOException {
        throw new UnsupportedOperationException("releaseShortCircuitFds not supported yet.");
    }

    @Override
    public void requestShortCircuitShm(String s) throws IOException {
        throw new UnsupportedOperationException("requestShortCircuitShm not supported yet.");
    }

    @Override
    public void replaceBlock(final ExtendedBlock block,
                             final StorageType storageType,
                             final Token<BlockTokenIdentifier> blockToken,
                             final String delHint,
                             final DatanodeInfo proxySource) throws IOException {
        throw new UnsupportedOperationException("replaceBlock not supported yet.");
    }

    @Override
    public void copyBlock(ExtendedBlock extendedBlock, Token<BlockTokenIdentifier> token) throws IOException {
        throw new UnsupportedOperationException("copyBlock not supported yet.");
    }

    @Override
    public void blockChecksum(final ExtendedBlock block,
                              final Token<BlockTokenIdentifier> blockToken) throws IOException {
        final DataOutputStream out = new DataOutputStream(
                getOutputStream());
        long requestLength = block.getNumBytes();

        try {
            long crcPerBlock = requestLength / BYTES_PER_CHECKSUM_DEFAULT;
            byte[] crcBuf;
            try(JindoBlockCrcInputStream crcIn = new JindoBlockCrcInputStream(block,
                datanode.getRequestOptions())) {
                crcBuf = crcIn.getCrcBytes(requestLength);
            }
            final MD5Hash md5 = MD5Hash.digest(crcBuf);
            //write reply
            DataTransferProtos.BlockOpResponseProto.newBuilder()
                    .setStatus(SUCCESS)
                    .setChecksumResponse(DataTransferProtos.OpBlockChecksumResponseProto.newBuilder()
                            .setBytesPerCrc(BYTES_PER_CHECKSUM_DEFAULT)
                            .setCrcPerBlock(crcPerBlock <= 0 ? 1 : crcPerBlock)
                            .setMd5(ByteString.copyFrom(md5.getDigest()))
                            .setCrcType(PBHelperClient.convert(DataChecksum.Type.CRC32C)))
                    .build()
                    .writeDelimitedTo(out);
            out.flush();
        } catch (IOException ioe) {
            LOG.info("blockChecksum {} received exception {}",
                    block, ioe.toString());
            throw ioe;
        } finally {
            IOUtils.closeStream(out);
        }
    }
}