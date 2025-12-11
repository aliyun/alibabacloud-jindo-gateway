package com.aliyun.jindodata.gateway.hdfs.datanode;

public class JindoReplicaHandler {
    private final JindoReplicaBeingWritten replicaInfo;

    public JindoReplicaHandler(JindoReplicaBeingWritten replicaInfo) {
        this.replicaInfo = replicaInfo;
    }

    public JindoReplicaBeingWritten getReplica() {
        return replicaInfo;
    }
}
