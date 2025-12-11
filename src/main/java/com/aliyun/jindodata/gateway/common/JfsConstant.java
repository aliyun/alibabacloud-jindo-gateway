package com.aliyun.jindodata.gateway.common;

public class JfsConstant {
    public static final int JFS_BACKEND_TYPE_CLOUD = 1;
    public static final String OSS_DEFAULT_BACKEND_LOCATION = ".dlsdata";
    public static final int BYTES_PER_CHECKSUM_DEFAULT = 512;
    public static final long CHUNK_SIZE = 512;
    public static final long FLUSH_MERGE_SIZE_DEFAULT = 1024 * 1024;
    public static final String TMP_DATA_DIRS_DEFAULT = "/tmp/jindo-gateway/tmp/data1,/tmp/jindo-gateway/tmp/data2";

    // ==================== file type ====================
    public static final int FILE_TYPE_UNKNOWN = 0;
    public static final int FILE_TYPE_DIRECTORY = 1;
    public static final int FILE_TYPE_FILE = 2;
    public static final int FILE_TYPE_SYMLINK = 3;
    
    // ==================== Checksum ====================
    public static final int CHECKSUM_HEADER_SIZE = 7;
    public static final int CHECKSUM_BYTES_PER_CHECKSUM_SIZE = 4;
}
