package com.aliyun.jindodata.gateway.io;

import com.aliyun.jindodata.gateway.common.JfsRequestOptions;
import com.aliyun.jindodata.gateway.common.JfsUtil;
import com.aliyun.jindodata.gateway.io.oss.OssFileChecksumWriter;
import com.aliyun.jindodata.gateway.io.oss.OssFileWriter;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

public class JfsComposedBlockWriter implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(JfsComposedBlockWriter.class);

    private static final int FLUSH_MERGE_MAX = 8 * 1024 * 1024; // 8MB

    private final JfsOssBackend ossBackend;
    private final Block block;
    private final JfsRequestOptions requestOptions;

    private String ossPathPrefix;
    private long blockSize = 0;

    private OssFileWriter ossWriter;
    private int blockSliceId = 0;
    private OssFileChecksumWriter checksumWriter;

    private long lastSliceSize = 0;
    private long flushMergeThresholdSize = 0;
    private boolean enableFlushMerge = false;

    public JfsComposedBlockWriter(Block block,
                                   JfsRequestOptions requestOptions) {
        this.ossBackend = new JfsOssBackend(requestOptions);
        this.block = block;
        this.requestOptions = requestOptions;
        this.flushMergeThresholdSize = requestOptions.getFlushMergeThreshold();
    }

    public void init() {
        long blockId = block.getBlockId();
        ossPathPrefix = JfsUtil.concatPath(requestOptions.getBackendLocation(), String.valueOf(blockId)) + "/";

        if (flushMergeThresholdSize > 0) {
            if (flushMergeThresholdSize > FLUSH_MERGE_MAX) {
                LOG.warn("Does not support flush merge {} Bytes, greater than 8MB", flushMergeThresholdSize);
                flushMergeThresholdSize = FLUSH_MERGE_MAX;
            }
            LOG.info("Flush merge threshold size = {} Bytes", flushMergeThresholdSize);
            enableFlushMerge = true;
        } else {
            LOG.info("Did not enable merge when flush");
        }
        
        LOG.debug("JfsComposedBlockWriter init {}", ossPathPrefix);
    }

    public void write(byte[] buf, int length) throws IOException {
        write(buf, 0, length);
    }

    public void write(byte[] buf, int off, int length) throws IOException {
        if (ossWriter == null || ossWriter.isFlushed()) {
            createBlockSlice();
        }

        ossWriter.write(buf, off, length);
        checksumWriter.write(buf, off, length);

        blockSize += length;
        block.setNumBytes(blockSize);

        if (enableFlushMerge) {
            lastSliceSize += length;
        }
    }

    public void flush() throws IOException {
        if (ossWriter == null) {
            return;
        }

        if (enableFlushMerge && lastSliceSize < flushMergeThresholdSize) {
            ossWriter.flush();
            checksumWriter.flush();
            LOG.debug("Flushed slice[{}] with size {} (below threshold, kept for merge)", 
                     blockSliceId - 1, lastSliceSize);
        } else {
            ossWriter.close();
            ossWriter = null;
            checksumWriter.close();
            checksumWriter = null;
            LOG.debug("Closed slice[{}] with size {}", blockSliceId - 1, lastSliceSize);
        }
    }

    @Override
    public void close() throws IOException {
        if (ossWriter == null || ossWriter.isFlushed()) {
            return;
        }
        
        ossWriter.close();
        checksumWriter.close();
        
        LOG.debug("JfsComposedBlockWriter closed, total size: {}", blockSize);
    }

    public long getBlockSize() {
        return blockSize;
    }

    private void createBlockSlice() throws IOException {
        boolean mergeExistSlice = false;

        if (enableFlushMerge) {
            if (lastSliceSize < flushMergeThresholdSize && lastSliceSize > 0) {
                blockSliceId--;
                mergeExistSlice = true;
                LOG.debug("Merging into existing slice[{}]", blockSliceId);
            } else {
                lastSliceSize = 0;
                LOG.debug("Start track slice[{}]", blockSliceId);
            }
        }

        String ossPath = ossPathPrefix + blockSliceId;
        
        if (mergeExistSlice) {
            ossWriter.reOpen(0);
            checksumWriter.reOpen();
            LOG.debug("Reopened slice local file for merge");
        } else {
            long generationStamp = block.getGenerationStamp();
            ossWriter = new OssFileWriter(ossBackend, ossPath, requestOptions);
            checksumWriter = new OssFileChecksumWriter(ossBackend, generationStamp, ossPath, requestOptions);
        }
        
        blockSliceId++;
    }
}
