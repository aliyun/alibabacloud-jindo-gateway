package com.aliyun.jindodata.gateway.hdfs.datanode;

import com.aliyun.jindodata.gateway.hdfs.blockIO.JindoBlockInputStream;
import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketHeader;
import org.apache.hadoop.hdfs.server.datanode.ChunkChecksum;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.ReadaheadPool;
import org.apache.hadoop.util.DataChecksum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;

import static com.aliyun.jindodata.gateway.common.JfsConstant.CHUNK_SIZE;
import static org.apache.hadoop.util.DataChecksum.Type.CRC32C;
import static org.apache.hadoop.util.DataChecksum.Type.NULL;

public class JindoBlockSender implements java.io.Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(JindoBlockSender.class);
    private static final int MIN_BUFFER_WITH_TRANSFERTO = 64*1024;
    private static final int IO_FILE_BUFFER_SIZE;
    static {
        HdfsConfiguration conf = new HdfsConfiguration();
        IO_FILE_BUFFER_SIZE = DFSUtilClient.getIoFileBufferSize(conf);
    }
    private static final int TRANSFERTO_BUFFER_SIZE = Math.max(
            IO_FILE_BUFFER_SIZE, MIN_BUFFER_WITH_TRANSFERTO);

    /** the block to read from */
    private final ExtendedBlock block;
    /** Stream to read block data from */
    private JindoBlockInputStream blockIn;
    /** updated while using transferTo() */
    private long blockInPosition = -1;
    /** Stream to read checksum */
    private DataInputStream checksumIn;
    /** Checksum utility */
    private final DataChecksum checksum;
    /** Initial position to read */
    private long initialOffset;
    /** Current position of read */
    private long offset;
    /** Position of last byte to read from block file */
    private final long endOffset;
    /** Number of bytes in chunk used for computing checksum */
    private final int chunkSize;
    /** Number bytes of checksum computed for a chunk */
    private final int checksumSize;
    /** If true, failure to read checksum is ignored */
    private final boolean corruptChecksumOk;
    /** Sequence number of packet being sent */
    private long seqno;
    /** Set to true once entire requested byte range has been sent to the client */
    private boolean sentEntireByteRange;
    /** When true, verify checksum while reading from checksum file */
    private final boolean verifyChecksum;
    private volatile ChunkChecksum lastChunkChecksum = null;
    private JindoDataNode datanode;
    private ReadaheadPool.ReadaheadRequest curReadahead;    // TODO


    public JindoBlockSender(ExtendedBlock block, long startOffset, long length, boolean corruptChecksumOk,
                            boolean verifyChecksum, boolean sendChecksum, JindoDataNode datanode) throws IOException {
        try {
            this.block = block;
            this.corruptChecksumOk = corruptChecksumOk;
            this.verifyChecksum = verifyChecksum;
            this.datanode = datanode;

            if (verifyChecksum) {
                // To simplify implementation, callers may not specify verification
                // without sending.
                Preconditions.checkArgument(sendChecksum,
                        "If verifying checksum, currently must also send it.");
            }

            // gateway does not check replica VS block
            long replicaVisibleLength = block.getNumBytes();

            DataChecksum csum = null;
            if (sendChecksum) {
                LOG.info("Initialize CRC32C data checksum");
                csum = DataChecksum.newDataChecksum(CRC32C, (int) CHUNK_SIZE);
            } else {
                LOG.info("Initialize NULL data checksum");
                csum = DataChecksum.newDataChecksum(NULL, (int) CHUNK_SIZE);
            }

            /*
             * If chunkSize is very large, then the metadata file is mostly
             * corrupted. For now just truncate bytesPerchecksum to blockLength.
             */
            int size = csum.getBytesPerChecksum();
            if (size > 10 * 1024 * 1024 && size > replicaVisibleLength) {
                csum = DataChecksum.newDataChecksum(csum.getChecksumType(),
                        Math.max((int) replicaVisibleLength, 10 * 1024 * 1024));
                size = csum.getBytesPerChecksum();
            }
            chunkSize = size;
            checksum = csum;
            checksumSize = checksum.getChecksumSize();
            length = length < 0 ? replicaVisibleLength : length;

            // end is either last byte on disk or the length for which we have a
            // checksum
            long end = block.getNumBytes();
            if (startOffset < 0 || startOffset > end
                    || (length + startOffset) > end) {
                String msg = " Offset " + startOffset + " and length " + length
                        + " don't match block " + block + " ( blockLen " + end + " )";
                LOG.warn(datanode.getDNRegistrationForBP(block.getBlockPoolId()) +
                        ":sendBlock() : " + msg);
                throw new IOException(msg);
            }

            // Ensure read offset is position at the beginning of chunk
            offset = startOffset - (startOffset % chunkSize);
            if (length >= 0) {
                // Ensure endOffset points to end of chunk.
                long tmpLen = startOffset + length;
                if (tmpLen % chunkSize != 0) {
                    tmpLen += (chunkSize - tmpLen % chunkSize);
                }
                if (tmpLen < end) {
                    // will use on-disk checksum here since the end is a stable chunk
                    end = tmpLen;
                }
            }
            endOffset = end;

            seqno = 0;
            if (LOG.isDebugEnabled()) {
                LOG.debug("replica={}", block);
            }
            blockIn = JindoBlockInputStream.getBlockInputStream(block, checksum, offset, datanode.getRequestOptions());
        } catch (IOException ioe) {
            IOUtils.closeStream(this);
            IOUtils.closeStream(blockIn);
            throw ioe;
        }
    }


    @Override
    public void close() throws IOException {
        if (curReadahead != null) {
            curReadahead.cancel();
        }

        IOException ioe = null;
        if(checksumIn!=null) {
            try {
                checksumIn.close(); // close checksum file
            } catch (IOException e) {
                ioe = e;
            }
            checksumIn = null;
        }
        if(blockIn!=null) {
            try {
                blockIn.close(); // close data file
            } catch (IOException e) {
                ioe = e;
            }
            blockIn = null;
        }
        // throw IOException if there is any
        if(ioe!= null) {
            throw ioe;
        }
    }

    long sendBlock(DataOutputStream out, OutputStream baseStream) throws IOException {
        return doSendBlock(out, baseStream);
    }

    private long doSendBlock(DataOutputStream out, OutputStream baseStream) throws IOException {
        if (out == null) {
            throw new IOException( "out stream is null" );
        }
        long totalRead = 0;
        OutputStream streamForSendChunks = out;

        // Trigger readahead of beginning of file if configured.
        manageOsCache();

        final long startTime = LOG.isDebugEnabled() ? System.nanoTime() : 0;
        try {
            int maxChunksPerPacket;
            int pktBufSize = PacketHeader.PKT_MAX_HEADER_LEN;
            maxChunksPerPacket = Math.max(1,
                    numberOfChunks(IO_FILE_BUFFER_SIZE));
            // Packet size includes both checksum and data
            pktBufSize += (chunkSize + checksumSize) * maxChunksPerPacket;
            ByteBuffer pktBuf = ByteBuffer.allocate(pktBufSize);

            while (endOffset > offset && !Thread.currentThread().isInterrupted()) {
                manageOsCache();
                long len = sendPacket(pktBuf, maxChunksPerPacket, streamForSendChunks);
                offset += len;
                totalRead += len + (numberOfChunks(len) * checksumSize);
                seqno++;
            }
            // If this thread was interrupted, then it did not send the full block.
            if (!Thread.currentThread().isInterrupted()) {
                try {
                    // send an empty packet to mark the end of the block
                    sendPacket(pktBuf, maxChunksPerPacket, streamForSendChunks);
                    out.flush();
                } catch (IOException e) { //socket error
                    throw ioeToSocketException(e);
                }

                sentEntireByteRange = true;
            }
        } finally {
            if (LOG.isDebugEnabled()) {
                final long endTime = System.nanoTime();
                LOG.debug("Sent {} bytes of block {}, cost {} ns", totalRead, block, endTime - startTime);
            }
            close();
        }
        return totalRead;
    }

    private int sendPacket(ByteBuffer pkt, int maxChunks, OutputStream out) throws IOException {
        int dataLen = (int) Math.min(endOffset - offset,
                (chunkSize * (long) maxChunks));

        int numChunks = numberOfChunks(dataLen); // Number of chunks be sent in the packet
        int checksumDataLen = numChunks * checksumSize;
        int packetLen = dataLen + checksumDataLen + 4;
        boolean lastDataPacket = offset + dataLen == endOffset && dataLen > 0;

        // The packet buffer is organized as follows:
        // _______HHHHCCCCD?D?D?D?
        //        ^   ^
        //        |   \ checksumOff
        //        \ headerOff
        // _ padding, since the header is variable-length
        // H = header and length prefixes
        // C = checksums
        // D? = data, if transferTo is false.

        int headerLen = writePacketHeader(pkt, dataLen, packetLen);

        // Per above, the header doesn't start at the beginning of the
        // buffer
        int headerOff = pkt.position() - headerLen;

        int checksumOff = pkt.position();
        byte[] buf = pkt.array();
        int dataOff = checksumOff + checksumDataLen;

        blockIn.readFully(buf, pkt.position(), dataLen);

        if (verifyChecksum) {   // always false for now
            verifyChecksum(buf, dataOff, dataLen, numChunks, checksumOff);
        }

        try {
            // normal transfer
            out.write(buf, headerOff, dataOff + dataLen - headerOff);
        } catch (IOException e) {
            if (e instanceof SocketTimeoutException) {
                /*
                 * writing to client timed out.  This happens if the client reads
                 * part of a block and then decides not to read the rest (but leaves
                 * the socket open).
                 *
                 * Reporting of this case is done in DataXceiver#run
                 */
            } else {
                /* Exception while writing to the client. Connection closure from
                 * the other end is mostly the case and we do not care much about
                 * it. But other things can go wrong, especially in transferTo(),
                 * which we do not want to ignore.
                 *
                 * The message parsing below should not be considered as a good
                 * coding example. NEVER do it to drive a program logic. NEVER.
                 * It was done here because the NIO throws an IOException for EPIPE.
                 */
                String ioem = e.getMessage();
                if (!ioem.startsWith("Broken pipe") && !ioem.startsWith("Connection reset")) {
                    LOG.error("BlockSender.sendChunks() exception: ", e);
                }
            }
            throw ioeToSocketException(e);
        }

        return dataLen;
    }

    public void verifyChecksum(final byte[] buf, final int dataOffset,
                               final int datalen, final int numChunks, final int checksumOffset)
            throws ChecksumException {
        int dOff = dataOffset;
        int cOff = checksumOffset;
        int dLeft = datalen;

        for (int i = 0; i < numChunks; i++) {
            checksum.reset();
            int dLen = Math.min(dLeft, chunkSize);
            checksum.update(buf, dOff, dLen);
            if (!checksum.compare(buf, cOff)) {
                long failedPos = offset + datalen - dLeft;
                throw new ChecksumException("Checksum failed at " + failedPos, failedPos);
            }
            dLeft -= dLen;
            dOff += dLen;
            cOff += checksumSize;
        }
    }

    private static IOException ioeToSocketException(IOException ioe) {
        if (ioe.getClass().equals(IOException.class)) {
            // "se" could be a new class in stead of SocketException.
            IOException se = new SocketException("Original Exception : " + ioe);
            se.initCause(ioe);
            /* Change the stacktrace so that original trace is not truncated
             * when printed.*/
            se.setStackTrace(ioe.getStackTrace());
            return se;
        }
        // otherwise just return the same exception.
        return ioe;
    }

    private int writePacketHeader(ByteBuffer pkt, int dataLen, int packetLen) {
        pkt.clear();
        // both syncBlock and syncPacket are false
        PacketHeader header = new PacketHeader(packetLen, offset, seqno,
                (dataLen == 0), dataLen, false);

        int size = header.getSerializedSize();
        pkt.position(PacketHeader.PKT_MAX_HEADER_LEN - size);
        header.putInBuffer(pkt);
        return size;
    }

    private void manageOsCache() throws IOException {
        // TODO
    }

    private int numberOfChunks(long datalen) {
        return (int) ((datalen + chunkSize - 1)/chunkSize);
    }

    DataChecksum getChecksum() {
        return checksum;
    }

    long getOffset() {
        return offset;
    }

    boolean didSendEntireByteRange() {
        return sentEntireByteRange;
    }
}
