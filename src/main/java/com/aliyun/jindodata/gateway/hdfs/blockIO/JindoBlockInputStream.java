package com.aliyun.jindodata.gateway.hdfs.blockIO;

import com.aliyun.jindodata.gateway.common.JfsRequestOptions;
import com.aliyun.jindodata.gateway.common.JfsStatus;
import com.aliyun.jindodata.gateway.io.JfsCloudBlock;
import com.aliyun.jindodata.gateway.io.JfsNormalBlockReader;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.util.DataChecksum;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

public abstract class JindoBlockInputStream implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(JindoBlockInputStream.class);

    private InputStream blockReader;

    private long currentPos = 0;

    private long dataReadSize = 0;

    protected ExtendedBlock block;

    private volatile boolean closed = false;

    protected JfsCloudBlock cloudBlock;
    protected JfsRequestOptions options;

    public JindoBlockInputStream(ExtendedBlock block, JfsRequestOptions options) {
        this.block = block;
        this.options = options;
    }

    public static JindoBlockInputStream getBlockInputStream(ExtendedBlock block, DataChecksum checksum,
                                                            long offset, JfsRequestOptions options) throws IOException {
        JindoBlockInputStream stream = null;
        if (checksum.getChecksumSize() > 0) {
            stream = new JindoBlockChecksumInputStream(block, options, checksum);
        } else {
            stream = new JindoBlockDefaultInputStream(block, options);
        }
        JfsStatus jfsStatus =  stream.openAndSeek(offset);
        if (!jfsStatus.isOk()) {
            throw new IOException(jfsStatus.getMessage());
        }
        return stream;
    }

    public abstract JfsStatus openAndSeek(long offset) throws IOException;

    public abstract void readFully(@NotNull byte[] buf, int off, int len) throws IOException;

    public void readDataFully(@NotNull byte[] buf, int off, int len) throws IOException {
        checkNotClosed();
        if (blockReader == null) {
            throw new IOException("Block reader is not initialized");
        }
        if (off < 0 || len < 0 || off + len > buf.length) {
            throw new IndexOutOfBoundsException();
        }

        int totalLen = len;
        int result = -1;
        while (len > 0) {
            result = blockReader.read(buf, off, len);
            if (result <= 0) {
                throw new IOException("Failed to read fully: got " + result +
                    " bytes but expected " + len + " remaining bytes");
            }
            off += result;
            len -= result;
        }
        currentPos += totalLen;
        dataReadSize += totalLen;
        LOG.debug("Read {} bytes fully from block input stream", len);
    }

    public void skipFully(long len) throws IOException {
        checkNotClosed();
        if (blockReader == null) {
            throw new IOException("Block reader is not initialized");
        }
        if (len < 0) {
            throw new IllegalArgumentException("Length cannot be negative: " + len);
        }
        if (len == 0) {
            return;
        }

        long remaining = len;
        while (remaining > 0) {
            long skipped = blockReader.skip(remaining);
            if (skipped <= 0) {
                throw new IOException("Failed to skip: got " + skipped + 
                    " bytes but expected to skip " + remaining + " bytes");
            }
            remaining -= skipped;
        }
        currentPos += len;
        LOG.debug("Skipped {} bytes in block input stream", len);
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        if (blockReader != null) {
            blockReader.close();
            blockReader = null;
        }
        currentPos = 0;
        dataReadSize = 0;
        LOG.debug("Closed block input stream for block {}", block);
    }

    public void openAndSeekBlockReader(long offset) throws IOException {
        blockReader = new JfsNormalBlockReader(cloudBlock, options, 0);
        currentPos = 0;
        skipFully(offset);
    }

    public long getPosition() {
        return currentPos;
    }

    public long getDataReadSize() {
        return dataReadSize;
    }

    public ExtendedBlock getBlock() {
        return block;
    }

    private void checkNotClosed() throws IOException {
        if (closed) {
            throw new IOException("Stream is closed");
        }
    }
}
