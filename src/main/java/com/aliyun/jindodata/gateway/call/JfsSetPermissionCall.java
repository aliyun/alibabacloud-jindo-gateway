package com.aliyun.jindodata.gateway.call;

import com.aliyun.jindodata.gateway.common.JfsRequestOptions;
import com.aliyun.jindodata.gateway.common.JfsStatus;
import com.aliyun.jindodata.gateway.http.reponse.JfsSetPermissionResponse;
import com.aliyun.jindodata.gateway.http.request.JfsSetPermissionRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JfsSetPermissionCall extends JfsBaseCall {
    private static final Logger LOG = LoggerFactory.getLogger(JfsSetPermissionCall.class);

    public JfsSetPermissionCall() {
        request = new JfsSetPermissionRequest();
        response = new JfsSetPermissionResponse();
    }

    @Override
    public JfsStatus execute(JfsRequestOptions requestOptions) {
        long startTime = System.currentTimeMillis();
        JfsSetPermissionRequest req = (JfsSetPermissionRequest) request;
        
        String path = req.getPath();
        String permission = req.getPermission();
        String bucket = requestOptions.getBucket();

        LOG.info("set permission path {} permission {} from bucket {}", path, permission, bucket);

        JfsStatus status = super.execute(requestOptions);

        if (status.isOk()) {
            long elapsed = System.currentTimeMillis() - startTime;
            String requestId = response.getRequestId();
            String serverTime = response.getServerTime();

            LOG.info("[RequestId: {}] Successfully set permission path {} {} from bucket {} dur {}ms ossServerElapsed {}",
                    requestId, path, permission, bucket, elapsed, serverTime);
        } else {
            String requestId = response.getRequestId();
            LOG.warn("[RequestId: {}] Failed to set permission path {} from bucket {}, errorCode: {}, errorMessage: {}",
                    requestId, path, bucket, status.getCode(), status.getMessage());
        }

        return status;
    }

    public void setPath(String path) {
        request.setPath(path);
    }

    public void setPermission(int permission) {
        ((JfsSetPermissionRequest) request).setPermission(permission);
    }

    public void setPermission(short permission) {
        ((JfsSetPermissionRequest) request).setPermission(permission);
    }

    public void setPermission(String permission) {
        ((JfsSetPermissionRequest) request).setPermission(permission);
    }
}
