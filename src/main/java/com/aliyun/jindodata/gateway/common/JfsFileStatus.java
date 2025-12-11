package com.aliyun.jindodata.gateway.common;

import java.util.List;

import static com.aliyun.jindodata.gateway.common.JfsConstant.*;

public class JfsFileStatus {
    public static final int STORAGE_STATE_UNKNOWN = 0;

    public static final int FS_STATE_UNKNOWN = 0;

    public static final int STORAGE_POLICY_UNSPECIFIED = 0;

    private int fileType = FILE_TYPE_UNKNOWN;
    private String fileId;
    private long fileSize = 0;
    private String name;
    private String owner;
    private String ownerGroup;
    private long mtime = 0;
    private long atime = 0;
    private int storageState = STORAGE_STATE_UNKNOWN;
    private String symlink;
    private long blockSize = 0;
    // ==================== extended ====================
    private JfsFilePermission permission;
    private String backendLocation;
    private int storagePolicy = STORAGE_POLICY_UNSPECIFIED;
    private int fsState = FS_STATE_UNKNOWN;
    private int childrenNum = 0;
    private long deltaGeneration = 0;
    private short blockReplication = 0;

    public JfsFileStatus() {
    }

    public JfsFileStatus(String name, int fileType, long fileSize) {
        this.name = name;
        this.fileType = fileType;
        this.fileSize = fileSize;
    }

    public JfsFileStatus(String fileId, String name, int fileType, long fileSize, String owner, long mtime) {
        this.fileId = fileId;
        this.name = name;
        this.fileType = fileType;
        this.fileSize = fileSize;
        this.owner = owner;
        this.mtime = mtime;
    }

    public int getFileType() {
        return fileType;
    }

    public String getFileId() {
        return fileId;
    }

    public long getFileSize() {
        return fileSize;
    }

    public String getName() {
        return name;
    }

    public String getOwner() {
        return owner;
    }

    public String getOwnerGroup() {
        return ownerGroup;
    }

    public long getMtime() {
        return mtime;
    }

    public long getAtime() {
        return atime;
    }

    public int getStorageState() {
        return storageState;
    }

    public String getSymlink() {
        return symlink;
    }

    public long getBlockSize() {
        return blockSize;
    }

    public JfsFilePermission getPermission() {
        return permission;
    }

    public String getBackendLocation() {
        return backendLocation;
    }

    public int getStoragePolicy() {
        return storagePolicy;
    }

    public int getFsState() {
        return fsState;
    }

    public int getChildrenNum() {
        return childrenNum;
    }

    public long getDeltaGeneration() {
        return deltaGeneration;
    }

    public short getBlockReplication() {
        return blockReplication;
    }

    public void setFileType(int fileType) {
        this.fileType = fileType;
    }

    public void setFileId(String fileId) {
        this.fileId = fileId;
    }

    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public void setOwnerGroup(String ownerGroup) {
        this.ownerGroup = ownerGroup;
    }

    public void setMtime(long mtime) {
        this.mtime = mtime;
    }

    public void setAtime(long atime) {
        this.atime = atime;
    }

    public void setStorageState(int storageState) {
        this.storageState = storageState;
    }

    public void setSymlink(String symlink) {
        this.symlink = symlink;
    }

    public void setBlockSize(long blockSize) {
        this.blockSize = blockSize;
    }

    public void setPermission(JfsFilePermission permission) {
        this.permission = permission;
    }

    public void setBackendLocation(String backendLocation) {
        this.backendLocation = backendLocation;
    }

    public void setStoragePolicy(int storagePolicy) {
        this.storagePolicy = storagePolicy;
    }

    public void setFsState(int fsState) {
        this.fsState = fsState;
    }

    public void setChildrenNum(int childrenNum) {
        this.childrenNum = childrenNum;
    }

    public void setDeltaGeneration(long deltaGeneration) {
        this.deltaGeneration = deltaGeneration;
    }

    public void setBlockReplication(short blockReplication) {
        this.blockReplication = blockReplication;
    }

    public boolean isDir() {
        return fileType == FILE_TYPE_DIRECTORY;
    }

    public boolean isFile() {
        return fileType == FILE_TYPE_FILE;
    }

    public boolean isSymlink() {
        return fileType == FILE_TYPE_SYMLINK;
    }

    public String getFileTypeString() {
        switch (fileType) {
            case FILE_TYPE_FILE:
                return "file";
            case FILE_TYPE_DIRECTORY:
                return "directory";
            case FILE_TYPE_SYMLINK:
                return "symlink";
            case FILE_TYPE_UNKNOWN:
            default:
                return "unknown";
        }
    }

    public long getFileIdAsLong() {
        if (fileId == null || fileId.isEmpty()) {
            return -1;
        }
        try {
            return Long.parseLong(fileId);
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    @Override
    public String toString() {
        return "JfsFileStatus{" +
                "name='" + name + '\'' +
                ", fileType=" + getFileTypeString() +
                ", fileSize=" + fileSize +
                ", fileId='" + fileId + '\'' +
                ", owner='" + owner + '\'' +
                ", mtime=" + mtime +
                '}';
    }
}
