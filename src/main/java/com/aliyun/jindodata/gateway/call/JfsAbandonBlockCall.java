package com.aliyun.jindodata.gateway.call;

import com.aliyun.jindodata.gateway.common.JfsRequestOptions;
import com.aliyun.jindodata.gateway.common.JfsStatus;
import com.aliyun.jindodata.gateway.http.reponse.JfsAbandonBlockResponse;
import com.aliyun.jindodata.gateway.http.request.JfsAbandonBlockRequest;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JfsAbandonBlockCall extends JfsBaseCall {
    private static final Logger LOG = LoggerFactory.getLogger(JfsAbandonBlockCall.class);

    public JfsAbandonBlockCall() {
        request = new JfsAbandonBlockRequest();
        response = new JfsAbandonBlockResponse();
    }

    @Override
    public JfsStatus execute(JfsRequestOptions requestOptions) {
        long startTime = System.currentTimeMillis();
        JfsAbandonBlockRequest req = (JfsAbandonBlockRequest) request;

        String path = req.getPath();
        ExtendedBlock block = req.getBlock();
        long fileId = req.getFileId();
        String clientName = req.getClientName();
        String bucket = requestOptions.getBucket();

        String blockStr = (block != null) ? block.toString() : "";
        LOG.info("AbandonBlock path {} block {} fileId {} clientName {} from bucket {}",
                path, blockStr, fileId, clientName, bucket);

        JfsStatus status = super.execute(requestOptions);

        if (status.isOk()) {
            long elapsed = System.currentTimeMillis() - startTime;
            String requestId = response.getRequestId();
            String serverTime = response.getServerTime();

            LOG.info("[RequestId: {}] Successfully AbandonBlock path {} block {} fileId {} clientName {} " +
                            "from bucket {} dur {}ms ossServerElapsed {}",
                    requestId, path, blockStr, fileId, clientName, bucket, elapsed, serverTime);
        } else {
            String requestId = response.getRequestId();
            LOG.warn("[RequestId: {}] Failed to AbandonBlock path {} block {} fileId {} clientName {} " +
                            "from bucket {}, errorCode: {}, errorMessage: {}",
                    requestId, path, blockStr, fileId, clientName, bucket,
                    status.getCode(), status.getMessage());
        }

        return status;
    }

    public void setPath(String path) {
        request.setPath(path);
    }

    public void setClientName(String clientName) {
        request.setClientName(clientName);
    }

    public void setFileId(long fileId) {
        ((JfsAbandonBlockRequest) request).setFileId(fileId);
    }

    public void setBlock(ExtendedBlock block) {
        ((JfsAbandonBlockRequest) request).setBlock(block);
    }
}
