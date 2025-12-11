package com.aliyun.jindodata.gateway.hdfs.datanode;

public class JindoDataStorage {
    private volatile String datanodeUuid = null;

    public String getDatanodeUuid() {
        return datanodeUuid;
    }

    public void setDatanodeUuid(String newDatanodeUuid) {
        this.datanodeUuid = newDatanodeUuid;
    }
}
