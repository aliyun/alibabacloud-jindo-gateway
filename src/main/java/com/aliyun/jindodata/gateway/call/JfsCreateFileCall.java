package com.aliyun.jindodata.gateway.call;

import com.aliyun.jindodata.gateway.common.JfsRequestOptions;
import com.aliyun.jindodata.gateway.common.JfsStatus;
import com.aliyun.jindodata.gateway.common.JfsUtil;
import com.aliyun.jindodata.gateway.http.reponse.JfsCreateFileResponse;
import com.aliyun.jindodata.gateway.http.request.JfsCreateFileRequest;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JfsCreateFileCall extends JfsBaseCall {
    private static final Logger LOG = LoggerFactory.getLogger(JfsCreateFileCall.class);

    private HdfsFileStatus fileStatus;

    public JfsCreateFileCall() {
        request = new JfsCreateFileRequest();
        response = new JfsCreateFileResponse();
    }

    @Override
    public JfsStatus execute(JfsRequestOptions requestOptions) {
        long startTime = System.currentTimeMillis();
        JfsCreateFileRequest req = (JfsCreateFileRequest) request;
        
        String path = req.getPath();
        long blockSize = req.getBlockSize();
        boolean createParent = req.isCreateParent();
        int replication = req.getReplication();
        String clientName = req.getClientName();
        String flag = req.getFlag();
        String bucket = requestOptions.getBucket();

        LOG.info("createFile path {} blockSize {} createParent {} replication {} clientName {} flag {} from bucket {}",
                path, blockSize, createParent, replication, clientName, flag, bucket);

        JfsStatus status = super.execute(requestOptions);

        if (status.isOk()) {
            long elapsed = System.currentTimeMillis() - startTime;
            String requestId = response.getRequestId();
            String serverTime = response.getServerTime();

            LOG.info("[RequestId: {}] Successfully createFile path {} blockSize {} createParent {} " +
                            "replication {} clientName {} flag {} from bucket {} dur {}ms ossServerElapsed {}",
                    requestId, path, blockSize, createParent, replication, clientName, flag, bucket, elapsed, serverTime);
        } else {
            String requestId = response.getRequestId();
            LOG.warn("[RequestId: {}] Failed to createFile path {} blockSize {} createParent {} " +
                            "replication {} clientName {} flag {} from bucket {}, errorCode: {}, errorMessage: {}",
                    requestId, path, blockSize, createParent, replication, clientName, flag, bucket,
                    status.getCode(), status.getMessage());
        }

        return status;
    }

    @Override
    protected void processResponse() {
        fileStatus = JfsUtil.convert2HdfsFileStatus(((JfsCreateFileResponse) response).getFileStatus());
    }

    public void setClientName(String clientName) {
        request.setClientName(clientName);
    }

    public void setPath(String path) {
        request.setPath(path);
    }

    public void setBlockSize(long blockSize) {
        ((JfsCreateFileRequest) request).setBlockSize(blockSize);
    }

    public void setCreateParent(boolean createParent) {
        ((JfsCreateFileRequest) request).setCreateParent(createParent);
    }

    public void setReplication(int replication) {
        ((JfsCreateFileRequest) request).setReplication(replication);
    }

    public void setFlag(int flag) {
        ((JfsCreateFileRequest) request).setFlag(flag);
    }

    public void setFlag(String flag) {
        ((JfsCreateFileRequest) request).setFlag(flag);
    }

    public void setPermission(short permission) {
        ((JfsCreateFileRequest) request).setPermission(permission);
    }

    public void setPermission(int permission) {
        ((JfsCreateFileRequest) request).setPermission(permission);
    }

    public void setPermission(String permission) {
        ((JfsCreateFileRequest) request).setPermission(permission);
    }

    public HdfsFileStatus getFileStatus() {
        return fileStatus;
    }
}
