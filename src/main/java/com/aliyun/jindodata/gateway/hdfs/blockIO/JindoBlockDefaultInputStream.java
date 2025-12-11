package com.aliyun.jindodata.gateway.hdfs.blockIO;

import com.aliyun.jindodata.gateway.common.JfsRequestOptions;
import com.aliyun.jindodata.gateway.common.JfsStatus;
import com.aliyun.jindodata.gateway.io.JfsCloudBlock;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;

public class JindoBlockDefaultInputStream extends JindoBlockInputStream{

    public JindoBlockDefaultInputStream(ExtendedBlock block, JfsRequestOptions options) {
        super(block, options);
    }

    @Override
    public JfsStatus openAndSeek(long offset) throws IOException {
        cloudBlock = new JfsCloudBlock(block, options);
        JfsStatus status = cloudBlock.init();
        if (!status.isOk()) {
            return status;
        }
        openAndSeekBlockReader(offset);
        return JfsStatus.OK();
    }

    @Override
    public void readFully(@NotNull byte[] buf, int off, int len) throws IOException {
        readDataFully(buf, off, len);
    }
}
