package com.aliyun.jindodata.gateway.io;

import com.aliyun.jindodata.gateway.common.JfsRequestOptions;
import com.aliyun.jindodata.gateway.common.JfsStatus;

import java.io.IOException;
import java.io.InputStream;

public class JfsNormalBlockReader extends InputStream {
    private long curOffset = 0;
    private JfsCloudBlock cloudBlock;
    private JfsRequestOptions options;
    private byte[] buffer;
    private final int BUFFER_SIZE = 1024 * 1024;
    private long bufferStart = Long.MIN_VALUE;

    public JfsNormalBlockReader(JfsCloudBlock cloudBlock, JfsRequestOptions options,
                                long startOffset) {
        this.cloudBlock = cloudBlock;
        this.options = options;
        this.curOffset = startOffset;
        this.buffer = new byte[BUFFER_SIZE];
    }

    @Override
    public int read() throws IOException {
        if (curOffset >= cloudBlock.totalSize) {
            return -1;
        }
        if (curOffset >= bufferStart && curOffset < bufferStart + BUFFER_SIZE) {
            return buffer[(int) ((curOffset++) - bufferStart)] & 0xFF;
        }
        int readSize = BUFFER_SIZE;
        if (curOffset + readSize > cloudBlock.totalSize) {
            readSize = (int) (cloudBlock.totalSize - curOffset);
        }
        JfsStatus status = cloudBlock.read(buffer, curOffset, readSize);
        if (!status.isOk()) {
            if (status.getCode() == JfsStatus.EOF_ERROR) {
                return -1;
            } else {
                throw new IOException(status.getMessage());
            }
        }
        bufferStart = curOffset;
        return buffer[(int) ((curOffset++) - bufferStart)] & 0xFF;
    }

//    @Override
//    public int read(@NotNull byte[] b, int off, int len) throws IOException {
//        if (len > BUFFER_SIZE) {
//
//        }
//
//        JfsStatus status = cloudBlock.read(b, curOffset, len, off);
//        if (!status.isOk()) {
//            if (status.getCode() == JfsStatus.EOF_ERROR) {
//                return -1;
//            } else {
//                throw new IOException(status.getMessage());
//            }
//        }
//        return len;
//    }

    @Override
    public long skip(long n) throws IOException {
        curOffset += n;
        return n;
    }

    @Override
    public void close() throws IOException {
        buffer = null;
    }
}
