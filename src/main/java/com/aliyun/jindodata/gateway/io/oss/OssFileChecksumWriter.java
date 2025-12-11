package com.aliyun.jindodata.gateway.io.oss;

import com.aliyun.jindodata.gateway.common.JfsRequestOptions;
import com.aliyun.jindodata.gateway.io.JfsOssBackend;
import org.apache.hadoop.util.DataChecksum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static com.aliyun.jindodata.gateway.common.JfsConstant.BYTES_PER_CHECKSUM_DEFAULT;
import static org.apache.hadoop.util.DataChecksum.Type.CRC32C;

public class OssFileChecksumWriter implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(OssFileChecksumWriter.class);

    private static final byte[] CHECKSUM_HEADER_DEFAULT = new byte[]{
        0, 1, 2, 0, 0, 2, 0  // version: 0,1  ;  type: 2;   bytes per checksum(512):  0,0,2,0
    };
    private static final int TMP_BUFFER_SIZE = 16 * 1024; // 16KB

    private final JfsOssBackend ossBackend;
    private final long generation;
    private final String fileName;
    private final JfsRequestOptions requestOptions;

    private OssFileWriter writer;

    private final DataChecksum dataChecksum = DataChecksum.newDataChecksum(CRC32C, BYTES_PER_CHECKSUM_DEFAULT);

    private final byte[] crcBuf = new byte[BYTES_PER_CHECKSUM_DEFAULT];
    private int crcBufOffset = 0;

    private final byte[] buffer = new byte[TMP_BUFFER_SIZE];
    private int posInBuffer = 0;

    private int lastCrcBufOffset = 0;
    private int lastPosInBuffer = 0;
    private long lastCrc64 = 0;
    private boolean flushed = false;
    

    public OssFileChecksumWriter(JfsOssBackend ossBackend, long generation,
                                 String cloudBlockPath, JfsRequestOptions requestOptions) {
        this.ossBackend = ossBackend;
        this.generation = generation;
        this.requestOptions = requestOptions;

        this.fileName = cloudBlockPath + "_" + generation + ".meta";
        
        LOG.debug("DataChecksumWriter initialized for {}", fileName);
    }

    public void write(byte[] buf, int length) throws IOException {
        write(buf, 0, length);
    }

    public void write(byte[] buf, int off, int length) throws IOException {
        if (writer == null) {
            writer = new OssFileWriter(ossBackend, fileName, requestOptions);
            writer.write(CHECKSUM_HEADER_DEFAULT, CHECKSUM_HEADER_DEFAULT.length);
        }

        if (crcBufOffset != 0) {
            int remainingLen = BYTES_PER_CHECKSUM_DEFAULT - crcBufOffset;
            int bufLen = Math.min(remainingLen, length);
            System.arraycopy(buf, off, crcBuf, crcBufOffset, bufLen);
            crcBufOffset += bufLen;
            off += bufLen;
            length -= bufLen;
        }
        
        if (crcBufOffset > BYTES_PER_CHECKSUM_DEFAULT) {
            throw new IOException("Bug: crc buffer exceeds boundary");
        }

        if (crcBufOffset == BYTES_PER_CHECKSUM_DEFAULT) {
            writeCrcBuf(crcBuf, 0, crcBufOffset);
            crcBufOffset = 0;
        }

        while (length >= BYTES_PER_CHECKSUM_DEFAULT) {
            writeCrcBuf(buf, off, BYTES_PER_CHECKSUM_DEFAULT);
            off += BYTES_PER_CHECKSUM_DEFAULT;
            length -= BYTES_PER_CHECKSUM_DEFAULT;
        }

        if (length > 0) {
            System.arraycopy(buf, off, crcBuf, 0, length);
            crcBufOffset = length;
        }
    }

    private void writeCrcBuf(byte[] buf, int off, int length) throws IOException {
        dataChecksum.reset();
        dataChecksum.update(buf, off, length);
        int checksumVal = (int) dataChecksum.getValue();

        ByteBuffer byteBuffer = ByteBuffer.allocate(4);
        byteBuffer.order(ByteOrder.BIG_ENDIAN);
        byteBuffer.putInt(checksumVal);
        byte[] checksumBytes = byteBuffer.array();

        System.arraycopy(checksumBytes, 0, buffer, posInBuffer, 4);
        posInBuffer += 4;

        if (posInBuffer >= TMP_BUFFER_SIZE) {
            writer.write(buffer, TMP_BUFFER_SIZE);
            posInBuffer = 0;
        }
    }

    @Override
    public void close() throws IOException {
        if (flushed) {
            return;
        }
        
        if (writer == null) {
            return;
        }

        if (crcBufOffset > 0) {
            writeCrcBuf(crcBuf, 0, crcBufOffset);
            crcBufOffset = 0;
        }

        if (posInBuffer > 0) {
            writer.write(buffer, posInBuffer);
            posInBuffer = 0;
        }
        
        writer.close();
        LOG.debug("DataChecksumWriter closed for {}", fileName);
    }

    public void flush() throws IOException {
        if (writer == null) {
            return;
        }

        lastCrcBufOffset = crcBufOffset;
        lastPosInBuffer = posInBuffer;

        if (crcBufOffset > 0) {
            writeCrcBuf(crcBuf, 0, crcBufOffset);
            crcBufOffset = 0;
        }

        if (posInBuffer > 0) {
            writer.write(buffer, posInBuffer);
            posInBuffer = 0;
        }
        
        writer.flush();
        flushed = true;
        
        LOG.debug("DataChecksumWriter flushed for {}", fileName);
    }

    public void reOpen() throws IOException {
        if (writer == null) {
            throw new IOException("Writer not initialized");
        }

        flushed = false;

        posInBuffer = lastPosInBuffer;
        crcBufOffset = lastCrcBufOffset;

        long offsetToEOF;
        if (crcBufOffset > 0) {
            offsetToEOF = posInBuffer + 4;
        } else {
            offsetToEOF = posInBuffer;
        }
        
        writer.reOpen(offsetToEOF);
        
        LOG.debug("DataChecksumWriter reopened for {}", fileName);
    }

    public boolean isFlushed() {
        return flushed;
    }
}
