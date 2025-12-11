package com.aliyun.jindodata.gateway.call;

import com.aliyun.jindodata.gateway.common.JfsRequestOptions;
import com.aliyun.jindodata.gateway.common.JfsStatus;
import com.aliyun.jindodata.gateway.http.reponse.JfsSetOwnerResponse;
import com.aliyun.jindodata.gateway.http.request.JfsSetOwnerRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JfsSetOwnerCall extends JfsBaseCall {
    private static final Logger LOG = LoggerFactory.getLogger(JfsSetOwnerCall.class);

    public JfsSetOwnerCall() {
        request = new JfsSetOwnerRequest();
        response = new JfsSetOwnerResponse();
    }

    @Override
    public JfsStatus execute(JfsRequestOptions requestOptions) {
        long startTime = System.currentTimeMillis();
        JfsSetOwnerRequest req = (JfsSetOwnerRequest) request;
        
        String path = req.getPath();
        String owner = req.getUser();
        String group = req.getGroup();
        String bucket = requestOptions.getBucket();

        LOG.info("set owner {} group {} for path {} from bucket {}", owner, group, path, bucket);

        JfsStatus status = super.execute(requestOptions);

        if (status.isOk()) {
            long elapsed = System.currentTimeMillis() - startTime;
            String requestId = response.getRequestId();
            String serverTime = response.getServerTime();

            LOG.info("[RequestId: {}] Successfully set owner path {} from bucket {} dur {}ms ossServerElapsed {}",
                    requestId, path, bucket, elapsed, serverTime);
        } else {
            String requestId = response.getRequestId();
            LOG.warn("[RequestId: {}] Failed to set owner {} for path {} from bucket {}, errorCode: {}, errorMessage: {}",
                    requestId, owner, path, bucket, status.getCode(), status.getMessage());
        }

        return status;
    }

    public void setPath(String path) {
        request.setPath(path);
    }

    public void setUser(String user) {
        ((JfsSetOwnerRequest) request).setUser(user);
    }

    public void setGroup(String group) {
        ((JfsSetOwnerRequest) request).setGroup(group);
    }
}
