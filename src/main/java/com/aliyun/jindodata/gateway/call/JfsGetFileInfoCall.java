package com.aliyun.jindodata.gateway.call;

import com.aliyun.jindodata.gateway.common.JfsFileStatus;
import com.aliyun.jindodata.gateway.common.JfsRequestOptions;
import com.aliyun.jindodata.gateway.common.JfsStatus;
import com.aliyun.jindodata.gateway.common.JfsUtil;
import com.aliyun.jindodata.gateway.http.reponse.JfsGetFileInfoResponse;
import com.aliyun.jindodata.gateway.http.request.JfsGetFileInfoRequest;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JfsGetFileInfoCall extends JfsBaseCall {
    private static final Logger LOG = LoggerFactory.getLogger(JfsGetFileInfoCall.class);

    private HdfsFileStatus fileStatus;

    public JfsGetFileInfoCall() {
        request = new JfsGetFileInfoRequest();
        response = new JfsGetFileInfoResponse();
    }

    @Override
    public JfsStatus execute(JfsRequestOptions requestOptions) {
        if (!getPath().startsWith("/")) {
            return JfsStatus.invalidPath("Absolute path required : " + getPath());
        }

        long startTime = System.currentTimeMillis();
        String path = getPath();
        String bucket = requestOptions.getBucket();

        LOG.info("GetFileInfo path {} from bucket {}", path, bucket);

        JfsStatus status = super.execute(requestOptions);

        if (status.isOk()) {
            long elapsed = System.currentTimeMillis() - startTime;
            String requestId = response.getRequestId();
            String serverTime = response.getServerTime();

            LOG.info("[RequestId: {}] Successfully getFileInfo path {} from bucket {} dur {}ms ossServerElapsed {}",
                    requestId, path, bucket, elapsed, serverTime);
        } else {
            String requestId = response.getRequestId();
            LOG.warn("[RequestId: {}] Failed to getFileInfo path {} from bucket {}, errorCode: {}, errorMessage: {}",
                    requestId, path, bucket, status.getCode(), status.getMessage());
        }

        return status;
    }

    @Override
    protected void processResponse() {
        fileStatus = JfsUtil.convert2HdfsFileStatus(((JfsGetFileInfoResponse) response).getFileStatus());
    }

    public void setPath(String path) {
        request.setPath(path);
    }

    public String getPath() {
        return request.getPath();
    }

    public JfsFileStatus getJfsFileStatus() {
        return ((JfsGetFileInfoResponse) response).getFileStatus();
    }

    public HdfsFileStatus getFileStatus() {
        return fileStatus;
    }
}
