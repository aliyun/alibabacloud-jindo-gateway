package com.aliyun.jindodata.gateway.call;

import com.aliyun.jindodata.gateway.common.JfsRequestOptions;
import com.aliyun.jindodata.gateway.common.JfsStatus;
import com.aliyun.jindodata.gateway.http.reponse.JfsRenewLeaseResponse;
import com.aliyun.jindodata.gateway.http.request.JfsRenewLeaseRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JfsRenewLeaseCall extends JfsBaseCall {
    private static final Logger LOG = LoggerFactory.getLogger(JfsRenewLeaseCall.class);

    private boolean result = false;

    public JfsRenewLeaseCall() {
        request = new JfsRenewLeaseRequest();
        response = new JfsRenewLeaseResponse();
    }

    @Override
    public JfsStatus execute(JfsRequestOptions requestOptions) {
        long startTime = System.currentTimeMillis();
        String clientName = getClientName();
        String bucket = requestOptions.getBucket();

        LOG.info("RenewLease clientName {} from bucket {}", clientName, bucket);

        JfsStatus status = super.execute(requestOptions);

        if (status.isOk()) {
            long elapsed = System.currentTimeMillis() - startTime;
            String requestId = response.getRequestId();
            String serverTime = response.getServerTime();

            result = true;
            LOG.info("[RequestId: {}] Successfully RenewLease clientName {} from bucket {} dur {}ms ossServerElapsed {}",
                    requestId, clientName, bucket, elapsed, serverTime);
        } else {
            String requestId = response.getRequestId();
            LOG.warn("[RequestId: {}] Failed to NS META RenewLease clientName {} from bucket {}, errorCode: {}, errorMessage: {}",
                    requestId, clientName, bucket, status.getCode(), status.getMessage());
        }

        return status;
    }

    public void setClientName(String clientName) {
        request.setClientName(clientName);
    }

    public String getClientName() {
        return request.getClientName();
    }

    public boolean getResult() {
        return result;
    }
}
