package com.aliyun.jindodata.gateway.call;

import com.aliyun.jindodata.gateway.common.JfsRequestOptions;
import com.aliyun.jindodata.gateway.common.JfsStatus;
import com.aliyun.jindodata.gateway.http.reponse.JfsMkdirsResponse;
import com.aliyun.jindodata.gateway.http.request.JfsMkdirsRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JfsMkdirsCall extends JfsBaseCall {
    private static final Logger LOG = LoggerFactory.getLogger(JfsMkdirsCall.class);

    public JfsMkdirsCall() {
        request = new JfsMkdirsRequest();
        response = new JfsMkdirsResponse();
    }

    @Override
    public JfsStatus execute(JfsRequestOptions requestOptions) {
        long startTime = System.currentTimeMillis();
        String path = request.getPath();
        String bucket = requestOptions.getBucket();

        LOG.info("mkdirs {} from bucket {}", path, bucket);

        JfsStatus status = super.execute(requestOptions);

        if (status.isOk()) {
            long elapsed = System.currentTimeMillis() - startTime;
            String requestId = response.getRequestId();
            String serverTime = response.getServerTime();

            LOG.info("[RequestId: {}] Successfully mkdirs {} from bucket {} dur {}ms ossServerElapsed {}",
                    requestId, path, bucket, elapsed, serverTime);
        } else {
            String requestId = response.getRequestId();
            LOG.warn("[RequestId: {}] Failed to mkdirs {} from bucket {}, errorCode: {}, errorMessage: {}",
                    requestId, path, bucket, status.getCode(), status.getMessage());
        }

        return status;
    }

    public void setPath(String path) {
        ((JfsMkdirsRequest) request).setPath(path);
    }

    public void setPermission(int permission) {
        ((JfsMkdirsRequest) request).setPermission(permission);
    }

    public void setPermission(String permission) {
        ((JfsMkdirsRequest) request).setPermission(permission);
    }

    public void setCreateParent(boolean createParent) {
        ((JfsMkdirsRequest) request).setCreateParent(createParent);
    }

    public boolean getResult() {
        return ((JfsMkdirsResponse) response).getResult();
    }
}
