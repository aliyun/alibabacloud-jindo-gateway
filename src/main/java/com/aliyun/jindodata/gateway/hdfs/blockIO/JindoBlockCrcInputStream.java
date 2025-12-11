package com.aliyun.jindodata.gateway.hdfs.blockIO;

import com.aliyun.jindodata.gateway.common.JfsRequestOptions;
import com.aliyun.jindodata.gateway.common.JfsStatus;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.util.DataChecksum;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static com.aliyun.jindodata.gateway.common.JfsConstant.CHUNK_SIZE;
import static org.apache.hadoop.util.DataChecksum.Type.CRC32C;

public class JindoBlockCrcInputStream extends JindoBlockChecksumInputStream {
    private static final Logger LOG = LoggerFactory.getLogger(JindoBlockCrcInputStream.class);

    public JindoBlockCrcInputStream(ExtendedBlock block, JfsRequestOptions options) throws IOException {
        super(block, options, DataChecksum.newDataChecksum(CRC32C, (int) CHUNK_SIZE));
        openAndSeek(0);
    }

    @Override
    public void readFully(@NotNull byte[] buf, int off, int dataLen) throws IOException {
        throw new UnsupportedOperationException("JindoBlockCrcInputStream only used for getCrcBytes");
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

    public byte[] getCrcBytes(long requestedLength) throws IOException {
        if (requestedLength == 0) {
            return new byte[0];
        }
        long numChunks = (requestedLength + chunkSize - 1) / chunkSize;
        int crcBytesLen = (int) (numChunks * checksumSize);
        byte[] result = new byte[crcBytesLen];
        
        if (crcDataAvailable) {
            getCrcBytesFromMeta(result, numChunks);
        } else {
            getCrcBytesByCalc(result, requestedLength, numChunks);
        }
        
        return result;
    }

    private void getCrcBytesFromMeta(byte[] result, long numChunks) throws IOException {
        long numChunksLeft = numChunks;
        long chunkIndex = 0;
        int resultOff = 0;
        
        while (numChunksLeft > 0) {
            long chunksToRead = Math.min(numChunksLeft, CHUNKS_IN_BUFFER);
            byte[] tempCrcBuf = new byte[(int) (chunksToRead * 4)];
            
            JfsStatus status = cloudBlock.readChecksumData(tempCrcBuf, chunkIndex, chunksToRead);
            if (!status.isOk()) {
                LOG.warn("Failed to read checksum data from chunk {}, count {}", chunkIndex, chunksToRead);
                throw new IOException("Failed to read checksum: " + status.getMessage());
            }

            System.arraycopy(tempCrcBuf, 0, result, resultOff, tempCrcBuf.length);
            
            chunkIndex += chunksToRead;
            numChunksLeft -= chunksToRead;
            resultOff += tempCrcBuf.length;
        }
        
        LOG.debug("Read {} chunks CRC data from meta file for block {}", numChunks, block);
    }

    private void getCrcBytesByCalc(byte[] result, long requestedLength, long numChunks) throws IOException {
        byte[] dataBuf = new byte[(int) requestedLength];

        readDataFully(dataBuf, 0, (int) requestedLength);

        calcChecksum(dataBuf, 0, (int) requestedLength, result, 0);

        LOG.debug("Calculated {} chunks CRC data for block {}", numChunks, block);
    }
}
