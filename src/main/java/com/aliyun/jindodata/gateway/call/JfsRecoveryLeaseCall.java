package com.aliyun.jindodata.gateway.call;

import com.aliyun.jindodata.gateway.common.JfsRequestOptions;
import com.aliyun.jindodata.gateway.common.JfsStatus;
import com.aliyun.jindodata.gateway.http.reponse.JfsRecoveryLeaseResponse;
import com.aliyun.jindodata.gateway.http.request.JfsRecoveryLeaseRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JfsRecoveryLeaseCall extends JfsBaseCall {
    private static final Logger LOG = LoggerFactory.getLogger(JfsRecoveryLeaseCall.class);

    public JfsRecoveryLeaseCall() {
        request = new JfsRecoveryLeaseRequest();
        response = new JfsRecoveryLeaseResponse();
    }

    @Override
    public JfsStatus execute(JfsRequestOptions requestOptions) {
        long startTime = System.currentTimeMillis();
        String path = getPath();
        String clientName = getClientName();
        String bucket = requestOptions.getBucket();

        LOG.info("RecoveryLease for {} clientName {} from bucket {}", path, clientName, bucket);

        JfsStatus status = super.execute(requestOptions);

        if (status.isOk()) {
            long elapsed = System.currentTimeMillis() - startTime;
            String requestId = response.getRequestId();
            String serverTime = response.getServerTime();

            LOG.info("[RequestId: {}] Successfully RecoveryLease {} clientName {} from bucket {} dur {}ms ossServerElapsed {}",
                    requestId, path, clientName, bucket, elapsed, serverTime);
        } else {
            String requestId = response.getRequestId();
            LOG.warn("[RequestId: {}] Failed to NS META RecoveryLease for {} clientName {} from bucket {}, errorCode: {}, errorMessage: {}",
                    requestId, path, clientName, bucket, status.getCode(), status.getMessage());
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

    public boolean getResult() {
        return ((JfsRecoveryLeaseResponse) response).getResult();
    }
}
