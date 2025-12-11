package com.aliyun.jindodata.gateway.io;

import com.aliyun.jindodata.gateway.common.JfsStatus;
import com.aliyun.oss.model.PartETag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class JfsBlockUploadTask implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(JfsBlockUploadTask.class);
    
    private final String localPartFile;
    private final int localFileOffset;
    private final boolean cleanLocalFile;
    private final int partNum;
    private final long length;
    private final String uploadId;
    private final JfsBlockUploadTaskGroup taskGroup;
    
    private volatile boolean finished = false;
    private volatile int errCode = 0;
    private volatile String errMsg = null;
    private volatile PartETag partETag = null;
    private final CountDownLatch latch = new CountDownLatch(1);
    
    public JfsBlockUploadTask(String localPartFile, int localFileOffset, boolean cleanLocalFile,
                              int partNum, long length, String uploadId, JfsBlockUploadTaskGroup taskGroup) {
        this.localPartFile = localPartFile;
        this.localFileOffset = localFileOffset;
        this.cleanLocalFile = cleanLocalFile;
        this.partNum = partNum;
        this.length = length;
        this.uploadId = uploadId;
        this.taskGroup = taskGroup;
    }
    
    @Override
    public void run() {
        doRun();
    }
    
    private void doRun() {
        if (taskGroup == null) {
            LOG.warn("Task group has already been released, just abort this task");
            errCode = -1;
            errMsg = "Task group has already been released";
            latch.countDown();
            return;
        }
        
        LOG.debug("Upload part {} of {}, upload id {}", partNum, taskGroup.getOssPath(), uploadId);
        
        if (uploadId == null || uploadId.isEmpty()) {
            LOG.warn("Failed to upload part, uploadId could not be null.");
            finish(false);
            return;
        }

        // Upload part to OSS
        JfsStatus status = taskGroup.getBackend().uploadPart(
                taskGroup.getOssPath(), 
                uploadId, 
                partNum, 
                length, 
                localPartFile);
        // Clean local file if needed
        if (cleanLocalFile) {
            File file = new File(localPartFile);
            if (!file.delete()) {
                LOG.warn("Failed to delete local file {}", localPartFile);
            }
        }
        
        if (!status.isOk()) {
            errCode = status.getCode();
            errMsg = status.getMessage();
            LOG.warn("Failed to upload {} to OSS, errMsg: {}", localPartFile, errMsg);
            finish(false);
            return;
        }

        partETag = new PartETag(partNum, (String) status.getResult());
        
        LOG.debug("Successfully upload part {} of {}, upload id {}, length {}",
                partNum, taskGroup.getOssPath(), uploadId, length);
        finish(true);
    }
    
    private void finish(boolean successful) {
        finished = successful;
        taskGroup.onTaskFinished(partNum, successful);
        latch.countDown();
    }
    
    public boolean waitForComplete(long timeoutMs) throws InterruptedException {
        return latch.await(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
    }
    
    // Getters
    public boolean isFinished() {
        return finished;
    }
    
    public int getErrCode() {
        return errCode;
    }
    
    public String getErrMsg() {
        return errMsg;
    }
    
    public int getPartNum() {
        return partNum;
    }
    
    public PartETag getPartETag() {
        return partETag;
    }
}
