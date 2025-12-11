package com.aliyun.jindodata.gateway.call;

import com.aliyun.jindodata.gateway.common.JfsRequestOptions;
import com.aliyun.jindodata.gateway.common.JfsStatus;
import com.aliyun.jindodata.gateway.http.reponse.JfsFsyncFileResponse;
import com.aliyun.jindodata.gateway.http.request.JfsFsyncFileRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JfsFsyncFileCall extends JfsBaseCall {
    private static final Logger LOG = LoggerFactory.getLogger(JfsFsyncFileCall.class);

    public JfsFsyncFileCall() {
        request = new JfsFsyncFileRequest();
        response = new JfsFsyncFileResponse();
    }

    @Override
    public JfsStatus execute(JfsRequestOptions requestOptions) {
        long startTime = System.currentTimeMillis();
        String path = getPath();
        long fileId = getFileId();
        long lastBlockLength = getLastBlockLength();
        String clientName = getClientName();
        String bucket = requestOptions.getBucket();

        LOG.info("FlushFile for {} fileId {} lastBlockLength {} clientName {} from bucket {}",
                path, fileId, lastBlockLength, clientName, bucket);

        JfsStatus status = super.execute(requestOptions);

        if (status.isOk()) {
            long elapsed = System.currentTimeMillis() - startTime;
            String requestId = response.getRequestId();
            String serverTime = response.getServerTime();

            LOG.info("[RequestId: {}] Successfully FlushFile for {} fileId {} lastBlockLength {} clientName {} " +
                            "from bucket {} dur {}ms ossServerElapsed {}",
                    requestId, path, fileId, lastBlockLength, clientName, bucket, elapsed, serverTime);
        } else {
            String requestId = response.getRequestId();
            LOG.warn("[RequestId: {}] Failed to NS META FlushFile for {} fileId {} lastBlockLength {} clientName {} " +
                            "from bucket {}, errorCode: {}, errorMessage: {}",
                    requestId, path, fileId, lastBlockLength, clientName, bucket, status.getCode(), status.getMessage());
        }

        return status;
    }

    public void setPath(String path) {
        request.setPath(path);
    }

    public String getPath() {
        return request.getPath();
    }

    public void setClientName(String clientName) {
        request.setClientName(clientName);
    }

    public String getClientName() {
        return request.getClientName();
    }

    public void setLastBlockLength(long lastBlockLength) {
        ((JfsFsyncFileRequest) request).setLastBlockLength(lastBlockLength);
    }

    public long getLastBlockLength() {
        return ((JfsFsyncFileRequest) request).getLastBlockLength();
    }

    public void setFileId(long fileId) {
        ((JfsFsyncFileRequest) request).setFileId(fileId);
    }

    public long getFileId() {
        return ((JfsFsyncFileRequest) request).getFileId();
    }
}
