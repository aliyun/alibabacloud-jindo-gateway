package com.aliyun.jindodata.gateway.io;

import com.aliyun.jindodata.gateway.common.JfsRequestOptions;
import com.aliyun.jindodata.gateway.common.JfsStatus;
import com.aliyun.oss.model.PartETag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class JfsBlockUploadTaskGroup {
    private static final Logger LOG = LoggerFactory.getLogger(JfsBlockUploadTaskGroup.class);

    private static volatile ExecutorService uploadThreadPool = null;
    private static final Object POOL_LOCK = new Object();
    private static int UPLOAD_THREAD_POOL_SIZE = 16;
    
    private final JfsOssBackend backend;
    private final String ossPath;
    private final int unfinishedTaskMax;
    private final JfsRequestOptions requestOptions;
    
    private final List<JfsBlockUploadTask> tasks = new ArrayList<>();
//    private final ConcurrentHashMap<Integer, Boolean> finishedTasks = new ConcurrentHashMap<>();
    private final AtomicBoolean failureFlag = new AtomicBoolean(false);
    private final AtomicInteger unfinishedCount = new AtomicInteger(0);
    
    public JfsBlockUploadTaskGroup(JfsOssBackend backend, String ossPath, 
                                   int unfinishedTaskMax, JfsRequestOptions requestOptions) {
        this.backend = backend;
        this.ossPath = ossPath;
        this.unfinishedTaskMax = unfinishedTaskMax;
        this.requestOptions = requestOptions;
    }

    public static ExecutorService getUploadThreadPool() {
        if (uploadThreadPool == null) {
            synchronized (POOL_LOCK) {
                if (uploadThreadPool == null) {
                    uploadThreadPool = Executors.newFixedThreadPool(UPLOAD_THREAD_POOL_SIZE,
                            r -> {
                                Thread t = new Thread(r);
                                t.setName("block-upload-thread-" + t.getId());
                                t.setDaemon(true);
                                return t;
                            });
                    LOG.info("Created upload thread pool with {} threads", UPLOAD_THREAD_POOL_SIZE);
                }
            }
        }
        return uploadThreadPool;
    }

    public static void shutdownUploadThreadPool() {
        synchronized (POOL_LOCK) {
            if (uploadThreadPool != null) {
                uploadThreadPool.shutdown();
                try {
                    if (!uploadThreadPool.awaitTermination(60, TimeUnit.SECONDS)) {
                        uploadThreadPool.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    uploadThreadPool.shutdownNow();
                    Thread.currentThread().interrupt();
                }
                uploadThreadPool = null;
                LOG.info("Upload thread pool shutdown");
            }
        }
    }

    public void submitTask(String localPartFile, int partNum, long length, String uploadId) {
        submitTask(localPartFile, 0, true, partNum, length, uploadId);
    }

    public void submitTask(String localPartFile, int localFileOffset, boolean cleanLocalPart,
                          int partNum, long length, String uploadId) {
//        finishedTasks.remove(partNum);
        JfsBlockUploadTask task = new JfsBlockUploadTask(localPartFile, localFileOffset, 
                cleanLocalPart, partNum, length, uploadId, this);
        
        synchronized (tasks) {
            tasks.add(task);
        }
        
        unfinishedCount.incrementAndGet();
        LOG.debug("Submit oss upload task for part number {}, length {}", partNum, length);

        long start = System.currentTimeMillis();
        boolean waited = false;
        while (true) {
            int unfinished = getUnfinishedTasks();
            if (unfinished <= unfinishedTaskMax) {
                break;
            }
            if (System.currentTimeMillis() - start > 60000) {
                LOG.info("Wait for async upload timeout, will do sync upload for part number {}", partNum);
                task.run();
                return;
            }
            waited = true;
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        
        if (waited) {
            long waitTime = System.currentTimeMillis() - start;
            LOG.info("Unfinished upload task number exceeds max number {}, waited for {}ms before uploading part number {}",
                    unfinishedTaskMax, waitTime, partNum);
        }

        getUploadThreadPool().submit(task);
    }

    public int getUnfinishedTasks() {
        return unfinishedCount.get();
    }

    void onTaskFinished(int partNum, boolean successful) {
//        finishedTasks.put(partNum, successful);
        unfinishedCount.decrementAndGet();
        if (!successful) {
            failureFlag.set(true);
        }
    }

    public JfsStatus waitForTasksComplete(List<PartETag> partETags) throws InterruptedException {
        synchronized (tasks) {
            for (JfsBlockUploadTask task : tasks) {
                if (!task.isFinished()) {
                    boolean completed = task.waitForComplete(10 * 60 * 1000); // 10 minutes timeout
                    if (!completed) {
                        String errorMsg = String.format("Wait upload timeout for part number %d for oss path %s",
                                task.getPartNum(), ossPath);
                        LOG.warn(errorMsg);
                        return JfsStatus.ioError(errorMsg);
                    }
                }
                
                if (task.getErrCode() != 0) {
                    LOG.warn("Task failed for part number {}, error: {}", 
                            task.getPartNum(), task.getErrMsg());
                    return JfsStatus.ioError(task.getErrMsg());
                } else {
                    LOG.debug("Task completed for part number {} for oss path {}", 
                            task.getPartNum(), ossPath);
                    if (task.getPartETag() != null) {
                        partETags.add(task.getPartETag());
                    }
                }
            }
        }
        
        return JfsStatus.OK();
    }

    public boolean hasFailure() {
        return failureFlag.get();
    }

    public static void setUploadThreadPoolSize(int size) {
        UPLOAD_THREAD_POOL_SIZE = size;
    }

    // Getters
    public JfsOssBackend getBackend() {
        return backend;
    }

    public String getOssPath() {
        return ossPath;
    }

    public JfsRequestOptions getRequestOptions() {
        return requestOptions;
    }
}
