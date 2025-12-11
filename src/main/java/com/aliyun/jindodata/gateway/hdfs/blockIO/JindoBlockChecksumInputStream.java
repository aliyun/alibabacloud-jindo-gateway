package com.aliyun.jindodata.gateway.hdfs.blockIO;

import com.aliyun.jindodata.gateway.common.JfsRequestOptions;
import com.aliyun.jindodata.gateway.common.JfsStatus;
import com.aliyun.jindodata.gateway.io.JfsCloudBlock;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.util.DataChecksum;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class JindoBlockChecksumInputStream extends JindoBlockInputStream {
    private static final Logger LOG = LoggerFactory.getLogger(JindoBlockChecksumInputStream.class);

    protected boolean crcDataAvailable = false;
    private long crcDataReadCnt = 0;

    protected DataChecksum checksum;
    protected int chunkSize;
    protected int checksumSize;

    protected long totalChunks = -1;
    private static final long CRC_BUFFER_META_SIZE = 128 * 1024;
    private static final long CRC_BUFFER_DATA_SIZE = CRC_BUFFER_META_SIZE * 128;
    protected static final long CHUNKS_IN_BUFFER = CRC_BUFFER_META_SIZE / 4;
    
    private long currentStartChunkIndex = -1;
    private byte[] crcBuf = null;

    public JindoBlockChecksumInputStream(ExtendedBlock block, JfsRequestOptions options, DataChecksum checksum) {
        super(block, options);
        this.checksum = checksum;
        this.chunkSize = checksum.getBytesPerChecksum();
        this.checksumSize = checksum.getChecksumSize();
    }

    @Override
    public JfsStatus openAndSeek(long offset) throws IOException {
        cloudBlock = new JfsCloudBlock(block, options);
        JfsStatus status = cloudBlock.init();
        if (!status.isOk()) {
            return status;
        }

        if (cloudBlock.getType() == JfsCloudBlock.JfsCloudBlockType.JfsCloudBlockType_Composed) {
            crcDataAvailable = false;
        } else {
            crcDataAvailable = true;
        }
        
        totalChunks = (block.getNumBytes() - 1) / chunkSize + 1;
        LOG.info("Initialize cloud block with checksum for block {}, crc data available {}, total chunks {}",
                block, crcDataAvailable, totalChunks);
        
        openAndSeekBlockReader(offset);
        return JfsStatus.OK();
    }

    @Override
    public void readFully(@NotNull byte[] buf, int off, int dataLen) throws IOException {
        if (crcDataAvailable) {
            readFullyWithCrc(buf, off, dataLen);
        } else {
            readFullyCalcCrc(buf, off, dataLen);
        }
    }

    private void readFullyWithCrc(byte[] buf, int off, int dataLen) throws IOException {
        long numChunks = (dataLen + chunkSize - 1) / chunkSize; // Number of chunks to send in the data packet
        long numChunksLeft = numChunks;
        long chunkIndex = getPosition() / chunkSize;
        int checksumOff = off;
        
        while (numChunksLeft > 0) {
            long startChunkIndex = chunkIndex / CHUNKS_IN_BUFFER * CHUNKS_IN_BUFFER;
            if (crcBuf == null || startChunkIndex != currentStartChunkIndex) {
                long chunks = Math.min(totalChunks - startChunkIndex, CHUNKS_IN_BUFFER);
                LOG.debug("Start to buffer more checksum from chunk {}, chunk count {}, block {}",
                        startChunkIndex, chunks, block);

                crcBuf = new byte[(int) (chunks * 4)];
                JfsStatus status = cloudBlock.readChecksumData(crcBuf, startChunkIndex, chunks);
                if (!status.isOk()) {
                    LOG.warn("Failed to buffer more checksum, {}", status);
                    throw new IOException("Failed to buffer checksum: " + status.getMessage());
                }
                currentStartChunkIndex = startChunkIndex;
                crcDataReadCnt++;
            }

            long chunksRemainingInBuffer = currentStartChunkIndex + CHUNKS_IN_BUFFER - chunkIndex;
            long readChunks = Math.min(chunksRemainingInBuffer, numChunksLeft);
            int offsetInBuffer = (int) ((chunkIndex - currentStartChunkIndex) * 4);
            int readSize = (int) (readChunks * 4);
            System.arraycopy(crcBuf, offsetInBuffer, buf, checksumOff, readSize);

            chunkIndex += readChunks;
            numChunksLeft -= readChunks;
            checksumOff += readSize;
        }

        int checksumDataLen = (int) (numChunks * checksumSize);
        int dataOff = off + checksumDataLen;
        readDataFully(buf, dataOff, dataLen);
    }

    /**
     * calc crc
     */
    private void readFullyCalcCrc(byte[] buf, int off, int dataLen) throws IOException {
        long numChunks = (dataLen + chunkSize - 1) / chunkSize;
        int checksumDataLen = (int) (numChunks * checksumSize);
        int dataOff = off + checksumDataLen;

        readDataFully(buf, dataOff, dataLen);

        calcChecksum(buf, dataOff, dataLen, buf, off);
    }

    private void calcChecksum(byte[] dataBuf, int dataOff, int dataLen, byte[] crcBuf, int crcOff) {
        int numChunks = (dataLen + chunkSize - 1) / chunkSize;
        LOG.debug("Send checksum by calculating, total chunks {}", numChunks);
        
        int dLeft = dataLen;
        int currentDataOff = dataOff;
        int currentCrcOff = crcOff;
        
        for (int i = 0; i < numChunks; i++) {
            checksum.reset();
            int dLen = Math.min(dLeft, chunkSize);
            checksum.update(dataBuf, currentDataOff, dLen);

            long checksumVal = checksum.getValue();
            ByteBuffer buffer = ByteBuffer.allocate(4);
            buffer.order(ByteOrder.BIG_ENDIAN);
            buffer.putInt((int) checksumVal);
            System.arraycopy(buffer.array(), 0, crcBuf, currentCrcOff, 4);
            
            dLeft -= dLen;
            currentDataOff += dLen;
            currentCrcOff += checksumSize;
        }
    }

    @Override
    public void close() throws IOException {
        if (crcDataAvailable) {
            LOG.info("Read cloud block {} with available checksum data, read total size {}, crc data read count {}",
                    block, getDataReadSize(), crcDataReadCnt);
        } else {
            LOG.info("Read cloud block {} without available checksum data, read total size {}",
                    block, getDataReadSize());
        }
        
        super.close();
        currentStartChunkIndex = -1;
        crcBuf = null;
        crcDataReadCnt = 0;
    }
}
