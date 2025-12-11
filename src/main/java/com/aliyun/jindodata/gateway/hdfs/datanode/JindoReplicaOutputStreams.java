package com.aliyun.jindodata.gateway.hdfs.datanode;

import com.aliyun.jindodata.gateway.common.JfsRequestOptions;
import com.aliyun.jindodata.gateway.hdfs.blockIO.JindoBlockOutputStream;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.util.DataChecksum;

import java.io.IOException;

public class JindoReplicaOutputStreams {
    private DataChecksum checksum;
    private Block block;
    private JindoBlockOutputStream dataOut;
    private JfsRequestOptions options;

    public JindoReplicaOutputStreams(ExtendedBlock block, DataChecksum requestedChecksum, JfsRequestOptions options) {
        this.checksum = requestedChecksum;
        this.block = block.getLocalBlock();
        this.options = options;
    }

    public void init() throws IOException {
        dataOut = new JindoBlockOutputStream(block, options);
        dataOut.init();
    }

    public DataChecksum getChecksum() {
        return checksum;
    }

    public JindoBlockOutputStream getDataOut() {
        return dataOut;
    }
}
