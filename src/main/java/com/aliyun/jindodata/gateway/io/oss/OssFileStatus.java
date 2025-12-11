package com.aliyun.jindodata.gateway.io.oss;

import static com.aliyun.jindodata.gateway.common.JfsConstant.FILE_TYPE_FILE;
import static com.aliyun.jindodata.gateway.common.JfsConstant.FILE_TYPE_UNKNOWN;

public class OssFileStatus {
    String path;
    long size;
    int fileType = FILE_TYPE_UNKNOWN;

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public int getFileType() {
        return fileType;
    }

    public void setFileType(int fileType) {
        this.fileType = fileType;
    }

    public boolean isFile() {
        return fileType == FILE_TYPE_FILE;
    }
}
