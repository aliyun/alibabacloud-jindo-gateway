package com.aliyun.jindodata.gateway.call;

import com.aliyun.jindodata.gateway.common.JfsRequestOptions;
import com.aliyun.jindodata.gateway.common.JfsStatus;
import com.aliyun.jindodata.gateway.http.reponse.JfsTruncateFileResponse;
import com.aliyun.jindodata.gateway.http.request.JfsTruncateFileRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JfsTruncateFileCall extends JfsBaseCall {
    private static final Logger LOG = LoggerFactory.getLogger(JfsTruncateFileCall.class);

    public JfsTruncateFileCall() {
        request = new JfsTruncateFileRequest();
        response = new JfsTruncateFileResponse();
    }

    @Override
    public JfsStatus execute(JfsRequestOptions requestOptions) {
        long startTime = System.currentTimeMillis();
        String path = getPath();
        long size = getSize();
        String clientName = getClientName();
        String bucket = requestOptions.getBucket();

        LOG.info("TruncateFile for {} size {} clientName {} from bucket {}",
                path, size, clientName, bucket);

        JfsStatus status = super.execute(requestOptions);

        if (status.isOk()) {
            long elapsed = System.currentTimeMillis() - startTime;
            String requestId = response.getRequestId();
            String serverTime = response.getServerTime();

            LOG.info("[RequestId: {}] Successfully TruncateFile {} size {} clientName {} " +
                            "from bucket {} dur {}ms ossServerElapsed {}",
                    requestId, path, size, clientName, bucket, elapsed, serverTime);
        } else {
            String requestId = response.getRequestId();
            LOG.warn("[RequestId: {}] Failed to NS META TruncateFile for {} size {} clientName {} " +
                            "from bucket {}, errorCode: {}, errorMessage: {}",
                    requestId, path, size, clientName, bucket, status.getCode(), status.getMessage());
        }

        return status;
    }

    public void setPath(String path) {
        request.setPath(path);
    }

    public String getPath() {
        return request.getPath();
    }

    public void setSize(long size) {
        ((JfsTruncateFileRequest) request).setSize(size);
    }

    public long getSize() {
        return ((JfsTruncateFileRequest) request).getSize();
    }

    public void setClientName(String clientName) {
        request.setClientName(clientName);
    }

    public String getClientName() {
        return request.getClientName();
    }

    public boolean getResult() {
        return ((JfsTruncateFileResponse) response).getResult();
    }

    public boolean getTruncateResult() {
        return ((JfsTruncateFileResponse) response).getTruncateResult();
    }
}
