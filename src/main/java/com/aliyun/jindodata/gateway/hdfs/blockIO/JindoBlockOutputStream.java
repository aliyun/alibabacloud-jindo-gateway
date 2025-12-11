package com.aliyun.jindodata.gateway.hdfs.blockIO;

import com.aliyun.jindodata.gateway.common.JfsRequestOptions;
import com.aliyun.jindodata.gateway.io.JfsComposedBlockWriter;
import org.apache.hadoop.hdfs.protocol.Block;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.nio.ByteBuffer;

public class JindoBlockOutputStream implements Closeable, Flushable {
    private static final Logger LOG = LoggerFactory.getLogger(JindoBlockOutputStream.class);
    private long currentPos = 0;
    private Block block;
    private JfsComposedBlockWriter dlsWriter;

    public JindoBlockOutputStream(Block block, JfsRequestOptions options) {
        this.block = block;
        dlsWriter = new JfsComposedBlockWriter(block, options);
    }

    public void init() {
        if (currentPos == 0) {
            dlsWriter.init();
        }
    }

    public void write(long offsetInBlock, byte[] buf) throws IOException {
        write(offsetInBlock, buf, 0);
    }

    /**
     * @param dataOffsetInBuf : data offset in buf
     */
    public void write(long offsetInBlock, byte[] buf, int dataOffsetInBuf) throws IOException {
        if (offsetInBlock <= currentPos) {
            LOG.info("Packet offset {} smaller than current pos {}, maybe resend packet",
                    offsetInBlock, currentPos);
            return;
        }
        long startOffset = offsetInBlock - (buf.length - dataOffsetInBuf);
        if (startOffset > currentPos) {
            throw new IOException("Received an out-of-sequence packet for " + block
                    + " at offset " + startOffset + ". Expecting packet starting at " + currentPos);
        }
        long readOffsetInBuffer = currentPos - startOffset + dataOffsetInBuf;
        int actualLength = (int) (offsetInBlock - currentPos);
        dlsWriter.write(buf, (int) readOffsetInBuffer, actualLength);
        currentPos = offsetInBlock;
    }

    @Override
    public void close() throws IOException {
        dlsWriter.close();
    }

    @Override
    public void flush() throws IOException {
        dlsWriter.flush();
    }
}
