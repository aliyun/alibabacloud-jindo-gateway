package com.aliyun.jindodata.gateway.call;

import com.aliyun.jindodata.gateway.common.JfsFileStatus;
import com.aliyun.jindodata.gateway.common.JfsRequestOptions;
import com.aliyun.jindodata.gateway.common.JfsStatus;
import com.aliyun.jindodata.gateway.common.JfsUtil;
import com.aliyun.jindodata.gateway.http.reponse.JfsAppendFileResponse;
import com.aliyun.jindodata.gateway.http.request.JfsAppendFileRequest;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JfsAppendFileCall extends JfsBaseCall {
    private static final Logger LOG = LoggerFactory.getLogger(JfsAppendFileCall.class);

    private HdfsFileStatus fileStatus;

    public JfsAppendFileCall() {
        request = new JfsAppendFileRequest();
        response = new JfsAppendFileResponse();
    }

    @Override
    public JfsStatus execute(JfsRequestOptions requestOptions) {
        long startTime = System.currentTimeMillis();
        String path = request.getPath();
        String clientName = request.getClientName();
        String bucket = requestOptions.getBucket();

        LOG.info("appendFile path {} clientName {} from bucket {}", path, clientName, bucket);

        JfsStatus status = super.execute(requestOptions);

        if (status.isOk()) {
            long elapsed = System.currentTimeMillis() - startTime;
            String requestId = response.getRequestId();
            String serverTime = response.getServerTime();

            LOG.info("[RequestId: {}] Successfully appendFile path {} clientName {} from bucket {} dur {}ms ossServerElapsed {}",
                    requestId, path, clientName, bucket, elapsed, serverTime);
        } else {
            String requestId = response.getRequestId();
            LOG.warn("[RequestId: {}] Failed to appendFile path {} clientName {} from bucket {}, errorCode: {}, errorMessage: {}",
                    requestId, path, clientName, bucket, status.getCode(), status.getMessage());
        }

        return status;
    }

    @Override
    protected void processResponse() {
        JfsFileStatus jfsFileStatus = getJfsFileStatus();
        if (jfsFileStatus != null) {
            fileStatus = JfsUtil.convert2HdfsFileStatus(jfsFileStatus);
        }
    }

    public void setPath(String path) {
        request.setPath(path);
    }

    public void setClientName(String clientName) {
        request.setClientName(clientName);
    }

    public void setFlag(int flag) {
        ((JfsAppendFileRequest) request).setFlag(flag);
    }

    public JfsFileStatus getJfsFileStatus() {
        return ((JfsAppendFileResponse) response).getFileStatus();
    }

    public HdfsFileStatus getFileStatus() {
        return fileStatus;
    }

    public LocatedBlock getLastBlock() {
        return ((JfsAppendFileResponse) response).getLastBlock();
    }
}
