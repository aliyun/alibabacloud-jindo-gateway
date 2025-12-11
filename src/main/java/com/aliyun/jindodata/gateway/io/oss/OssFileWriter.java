package com.aliyun.jindodata.gateway.io.oss;

import com.aliyun.jindodata.gateway.common.JfsRequestOptions;
import com.aliyun.jindodata.gateway.common.JfsStatus;
import com.aliyun.jindodata.gateway.io.JfsBlockUploadTaskGroup;
import com.aliyun.jindodata.gateway.io.JfsOssBackend;
import com.aliyun.oss.model.PartETag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;
import java.util.ArrayList;
import java.util.zip.CRC32;

public class OssFileWriter implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(OssFileWriter.class);

    private static final long LOCAL_PART_FILE_SIZE = 8L * 1024 * 1024; // 8MB
    private static final String OSS_WRITER_TMP_FILE_PREFIX = "jfs-tmp-";

    private final JfsOssBackend ossBackend;
    private final String ossPath;
    private final JfsRequestOptions requestOptions;
    private final int unfinishedTaskMax;

    private final String[] localDataDirs;
    private final String localPrefixKey;
    private String localPartFile;
    private FileOutputStream writer;
    private int currentIndex = 0;
    private long currentOffset = 0;

    private String uploadId;
    private JfsBlockUploadTaskGroup uploadTaskGroup;
    private final List<File> tempFiles = new ArrayList<>();

    private boolean failed = false;
    private boolean closed = false;
    private boolean flushed = false;
    private boolean cleanSimpleLocalFile = true; // Whether to clean up local file for simple upload

    private long clientCrc64 = 0;
    private final CRC32 crc32 = new CRC32();

    public OssFileWriter(JfsOssBackend ossBackend, String ossPath,
                         JfsRequestOptions requestOptions) {
        this.ossBackend = ossBackend;
        this.ossPath = ossPath;
        this.requestOptions = requestOptions;
        this.localDataDirs = requestOptions.getTmpDataDirs();
        this.unfinishedTaskMax = requestOptions.getMaxPendingUploadCount();

        long hashVal = Math.abs(ossPath.hashCode());
        this.localPrefixKey = OSS_WRITER_TMP_FILE_PREFIX + hashVal + "-" 
                + System.currentTimeMillis() + "-";

        this.uploadTaskGroup = new JfsBlockUploadTaskGroup(ossBackend, ossPath, 
                this.unfinishedTaskMax, requestOptions);
        
        LOG.info("OSS writer initialized successfully for {}", ossPath);
    }

    public void write(byte[] buf, int size) throws IOException {
        write(buf, 0, size);
    }

    public void write(byte[] buf, int off, int len) throws IOException {
        if (failed) {
            throw new IOException("OSS writer has already failed");
        }
        
        if (closed) {
            throw new IOException("OSS writer has been closed");
        }

        if (uploadTaskGroup.hasFailure()) {
            failed = true;
            throw new IOException("Some upload tasks failed");
        }

        if (writer == null) {
            if (!initTempWriter()) {
                failed = true;
                throw new IOException("Failed to open local temp file at any local path");
            }
        }

        try {
            writer.write(buf, off, len);
        } catch (IOException e) {
            failed = true;
            LOG.error("Failed to write local path {}", localPartFile, e);
            throw new IOException("Failed to write local path: " + e.getMessage(), e);
        }

        currentOffset += len;
        if (currentOffset >= LOCAL_PART_FILE_SIZE) {
            try {
                writer.close();
            } catch (IOException e) {
                failed = true;
                LOG.error("Failed to close local path {}", localPartFile, e);
                throw new IOException("Failed to close local path: " + e.getMessage(), e);
            }
            writer = null;
            
            tryToInitUploadId();
            submitTask();
            currentOffset = 0;
        }
        
        LOG.debug("OSS writer wrote {} bytes to {}", len, ossPath);
    }

    public void flush() throws IOException {
        if (failed) {
            throw new IOException("OSS writer has already failed");
        }

        if (flushed) {
            LOG.info("OSS writer has already flushed");
        }
        
        LOG.info("OSS writer flush for {}", ossPath);

        if (uploadId == null || uploadId.isEmpty()) {
            cleanSimpleLocalFile = false;
            simpleUpload();
            flushed = true;
            return;
        }

        cleanUp();
        throw new IOException("Local file should be smaller than 8MB when needing to be flushed. Something wrong happened");
    }

    public void reOpen(long offsetToEOF) throws IOException {
        if (failed) {
            throw new IOException("OSS writer has already failed");
        }
        
        if (closed) {
            throw new IOException("OSS writer has been closed");
        }

        flushed = false;
        cleanSimpleLocalFile = true;

        if (localPartFile == null) {
            throw new IOException("No local part file to reopen");
        }
        
        File file = new File(localPartFile);
        if (!file.exists()) {
            throw new IOException("Local part file does not exist: " + localPartFile);
        }
        
        try {
            currentOffset -= offsetToEOF;

            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            raf.seek(currentOffset);
            raf.close();

            writer = new FileOutputStream(file, true);
            
            LOG.debug("Reopened writer for {} at offset {}", ossPath, currentOffset);
        } catch (IOException e) {
            failed = true;
            LOG.error("Failed to reopen local path {}", localPartFile, e);
            throw new IOException("Failed to reopen local path: " + e.getMessage(), e);
        }
    }

    public boolean isFlushed() {
        return flushed;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        
        closed = true;
        
        if (failed) {
            cleanUp();
            throw new IOException("OSS writer has already failed");
        }
        
        LOG.info("OSS writer close for {}", ossPath);
        
        try {
            if (uploadId == null || uploadId.isEmpty()) {
                simpleUpload();
                return;
            }

            if (writer != null) {
                writer.close();
                writer = null;
                tryToInitUploadId();
                submitTask();
            }
            
            waitForAllPartUploaded();
        } finally {
            cleanUp();
        }
    }

    private String getLocalPath(int dirIndex) {
        String dir = localDataDirs[dirIndex];
        String resultPath = dir + File.separator + localPrefixKey + currentIndex;
        LOG.debug("Start writing local path {}", resultPath);
        return resultPath;
    }

    private boolean initTempWriter() {
        int startIndex = (int) (System.currentTimeMillis() % localDataDirs.length);
        for (int i = 0; i < localDataDirs.length; i++) {
            int dirIndex = (startIndex + i) % localDataDirs.length;
            localPartFile = getLocalPath(dirIndex);
            
            try {
                File file = new File(localPartFile);
                file.getParentFile().mkdirs();
                writer = new FileOutputStream(file);
                tempFiles.add(file);
                currentIndex++;
                currentOffset = 0;
                return true;
            } catch (IOException e) {
                LOG.info("Failed to open local path {}, error: {}", localPartFile, e.getMessage());
            }
        }
        return false;
    }

    private void simpleUpload() throws IOException {
        if (flushed) {
            return;
        }
        
        if (writer != null) {
            writer.close();
            writer = null;

            JfsStatus status = ossBackend.put(ossPath, localPartFile);
            if (!status.isOk()) {
                failed = true;
                throw new IOException("Failed to upload file: " + status.getMessage());
            }
            
            LOG.info("Simple upload completed for {}", ossPath);

            if (cleanSimpleLocalFile) {
                File file = new File(localPartFile);
                if (file.delete()) {
                    LOG.debug("Deleted simple upload local file: {}", localPartFile);
                } else {
                    LOG.warn("Failed to delete simple upload local file: {}", localPartFile);
                }
            }
        } else {
            JfsStatus status = ossBackend.put(ossPath, new byte[0]);
            if (!status.isOk()) {
                failed = true;
                throw new IOException("Failed to upload empty file: " + status.getMessage());
            }
            
            LOG.info("Empty file uploaded for {}", ossPath);
        }
    }

    private void waitForAllPartUploaded() throws IOException {
        List<PartETag> partETags = new ArrayList<>();
        JfsStatus status;
        try {
            status = uploadTaskGroup.waitForTasksComplete(partETags);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            abortUpload();
            throw new IOException("Upload interrupted", e);
        }
        
        if (!status.isOk() || uploadTaskGroup.hasFailure()) {
            abortUpload();
            throw new IOException("Some parts failed to upload: " + status.getMessage());
        }

        JfsStatus completeStatus = ossBackend.completeUpload(ossPath, uploadId, partETags);
        if (!completeStatus.isOk()) {
            LOG.warn("Failed to complete upload {}, upload id {}", ossPath, uploadId);
            abortUpload();
            throw new IOException("Failed to complete upload: " + completeStatus.getMessage());
        }
        
        LOG.info("Multipart upload completed for {}, upload id {}", ossPath, uploadId);
    }

    private void submitTask() {
        uploadTaskGroup.submitTask(localPartFile, currentIndex, currentOffset, uploadId);
    }

    private void tryToInitUploadId() throws IOException {
        if (uploadId != null && !uploadId.isEmpty()) {
            return;
        }
        
        JfsStatus status = ossBackend.initUpload(ossPath);
        if (!status.isOk()) {
            LOG.warn("Failed to init upload {}", ossPath);
            throw new IOException("Failed to init upload: " + status.getMessage());
        }

        uploadId = (String) status.getResult();
        
        LOG.info("Init upload for {}, upload id {}", ossPath, uploadId);
    }

    private void abortUpload() {
        if (uploadId != null && !uploadId.isEmpty()) {
            JfsStatus status = ossBackend.abortUpload(ossPath, uploadId);
            if (!status.isOk()) {
                LOG.warn("Failed to abort upload {}, upload id {}", ossPath, uploadId);
            } else {
                LOG.warn("Aborting upload for {}, upload id {}", ossPath, uploadId);
            }
        }
    }

    private void cleanUp() {
        if (writer != null) {
            try {
                writer.close();
            } catch (IOException e) {
                LOG.warn("Failed to close writer", e);
            }
            writer = null;
        }

        for (File file : tempFiles) {
            if (file.exists()) {
                if (file.delete()) {
                    LOG.debug("Deleted temp file: {}", file.getPath());
                } else {
                    LOG.warn("Failed to delete temp file: {}", file.getPath());
                }
            }
        }
        tempFiles.clear();

        if (!cleanSimpleLocalFile && localPartFile != null) {
            File file = new File(localPartFile);
            if (file.exists()) {
                if (file.delete()) {
                    LOG.debug("Deleted unflushed simple local file: {}", localPartFile);
                } else {
                    LOG.warn("Failed to delete unflushed simple local file: {}", localPartFile);
                }
            }
        }
    }
}
