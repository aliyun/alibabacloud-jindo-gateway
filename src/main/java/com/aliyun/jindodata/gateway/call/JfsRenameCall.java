package com.aliyun.jindodata.gateway.call;

import com.aliyun.jindodata.gateway.common.JfsRequestOptions;
import com.aliyun.jindodata.gateway.common.JfsStatus;
import com.aliyun.jindodata.gateway.http.reponse.JfsRenameResponse;
import com.aliyun.jindodata.gateway.http.request.JfsRenameRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JfsRenameCall extends JfsBaseCall {
    private static final Logger LOG = LoggerFactory.getLogger(JfsRenameCall.class);

    public JfsRenameCall() {
        request = new JfsRenameRequest();
        response = new JfsRenameResponse();
    }

    @Override
    public JfsStatus execute(JfsRequestOptions requestOptions) {
        long startTime = System.currentTimeMillis();
        JfsRenameRequest req = (JfsRenameRequest) request;

        String srcPath = req.getSrcPath();
        String dstPath = req.getDstPath();
        String bucket = requestOptions.getBucket();

        LOG.info("Rename from {} to {} from bucket {}", srcPath, dstPath, bucket);

        JfsStatus status = super.execute(requestOptions);

        if (status.isOk()) {
            long elapsed = System.currentTimeMillis() - startTime;
            String requestId = response.getRequestId();
            String serverTime = response.getServerTime();

            LOG.info("[RequestId: {}] Successfully Rename {} to {} from bucket {} dur {}ms ossServerElapsed {}",
                    requestId, srcPath, dstPath, bucket, elapsed, serverTime);
        } else {
            String requestId = response.getRequestId();
            LOG.warn("[RequestId: {}] Failed to Rename from {} to {} from bucket {}, errorCode: {}, errorMessage: {}",
                    requestId, srcPath, dstPath, bucket, status.getCode(), status.getMessage());
        }

        return status;
    }

    public void setSrcPath(String srcPath) {
        ((JfsRenameRequest) request).setSrcPath(srcPath);
    }

    public void setDstPath(String dstPath) {
        ((JfsRenameRequest) request).setDstPath(dstPath);
    }

    public boolean getResult() {
        return ((JfsRenameResponse) response).getResult();
    }
}
