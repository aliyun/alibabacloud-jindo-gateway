package com.aliyun.jindodata.gateway.call;

import com.aliyun.jindodata.gateway.common.JfsRequestOptions;
import com.aliyun.jindodata.gateway.common.JfsStatus;
import com.aliyun.jindodata.gateway.http.reponse.JfsDeleteResponse;
import com.aliyun.jindodata.gateway.http.request.JfsDeleteRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JfsDeleteCall extends JfsBaseCall {
    private static final Logger LOG = LoggerFactory.getLogger(JfsDeleteCall.class);

    public JfsDeleteCall() {
        request = new JfsDeleteRequest();
        response = new JfsDeleteResponse();
    }

    @Override
    public JfsStatus execute(JfsRequestOptions requestOptions) {
        long startTime = System.currentTimeMillis();
        String path = getPath();
        boolean recursive = isRecursive();
        String bucket = requestOptions.getBucket();

        LOG.info("Delete path {} recursive {} from bucket {}", path, recursive, bucket);

        JfsStatus status = super.execute(requestOptions);

        if (status.isOk()) {
            long elapsed = System.currentTimeMillis() - startTime;
            String requestId = response.getRequestId();
            String serverTime = response.getServerTime();

            LOG.info("[RequestId: {}] Successfully delete path {} from bucket {} dur {}ms ossServerElapsed {}",
                    requestId, path, bucket, elapsed, serverTime);
        } else {
            String requestId = response.getRequestId();
            LOG.warn("[RequestId: {}] Failed to delete path {} from bucket {}, errorCode: {}, errorMessage: {}",
                    requestId, path, bucket, status.getCode(), status.getMessage());
        }

        return status;
    }

    public void setPath(String path) {
        request.setPath(path);
    }

    public String getPath() {
        return request.getPath();
    }

    public void setRecursive(boolean recursive) {
        ((JfsDeleteRequest) request).setRecursive(recursive);
    }

    public boolean isRecursive() {
        return ((JfsDeleteRequest) request).isRecursive();
    }

    public boolean getResult() {
        return ((JfsDeleteResponse) response).getResult();
    }
}
