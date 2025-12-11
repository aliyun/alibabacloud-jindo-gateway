package com.aliyun.jindodata.gateway.io;

import com.aliyun.jindodata.gateway.io.oss.OssFileStatus;

public class JfsBlockSlice {
    int index;
    String ossPath;
    long size;
    OssFileStatus crcFileInfo;

    public JfsBlockSlice(int index, String ossPath, long size) {
        this.index = index;
        this.ossPath = ossPath;
        this.size = size;
        this.crcFileInfo = null;
    }

    public OssFileStatus getCrcFileName() {
        return crcFileInfo;
    }

    public void setCrcFileInfo(OssFileStatus crcFileInfo) {
        this.crcFileInfo = crcFileInfo;
    }
}
