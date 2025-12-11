package com.aliyun.jindodata.gateway.hdfs.datanode;

import com.aliyun.jindodata.gateway.common.JfsRequestOptions;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.util.DataChecksum;

import java.io.IOException;

public class JindoReplicaBeingWritten {
    private final ExtendedBlock block;

    public JindoReplicaBeingWritten(ExtendedBlock block) {
        this.block = block;
    }

    public JindoReplicaBeingWritten(ExtendedBlock block, long newGs) {
        this.block = block;
    }

    public JindoReplicaOutputStreams createStreams(DataChecksum requestedChecksum, JfsRequestOptions options) throws IOException {
        JindoReplicaOutputStreams streams = new JindoReplicaOutputStreams(block, requestedChecksum, options);
        streams.init();
        return streams;
    }
}
