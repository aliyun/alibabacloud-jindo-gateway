package com.aliyun.jindodata.gateway.call;

import com.aliyun.jindodata.gateway.common.JfsRequestOptions;
import com.aliyun.jindodata.gateway.common.JfsStatus;
import com.aliyun.jindodata.gateway.http.reponse.JfsCompleteFileResponse;
import com.aliyun.jindodata.gateway.http.request.JfsCompleteFileRequest;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JfsCompleteFileCall extends JfsBaseCall {
    private static final Logger LOG = LoggerFactory.getLogger(JfsCompleteFileCall.class);

    public JfsCompleteFileCall() {
        request = new JfsCompleteFileRequest();
        response = new JfsCompleteFileResponse();
    }

    @Override
    public JfsStatus execute(JfsRequestOptions requestOptions) {
        long startTime = System.currentTimeMillis();
        JfsCompleteFileRequest req = (JfsCompleteFileRequest) request;

        ExtendedBlock block = req.getBlock();
        long fileId = req.getFileId();
        String clientName = req.getClientName();
        String path = req.getPath();
        String bucket = requestOptions.getBucket();

        String blockStr = (block != null) ? block.toString() : "";
        LOG.info("CompleteFile for {} fileId {} block {} clientName {} from bucket {}",
                path, fileId, blockStr, clientName, bucket);

        JfsStatus status = super.execute(requestOptions);

        if (status.isOk()) {
            long elapsed = System.currentTimeMillis() - startTime;
            String requestId = response.getRequestId();
            String serverTime = response.getServerTime();

            LOG.info("[RequestId: {}] Successfully CompleteFile {} fileId {} block {} clientName {} " +
                            "from bucket {} dur {}ms ossServerElapsed {}",
                    requestId, path, fileId, blockStr, clientName, bucket, elapsed, serverTime);
        } else {
            String requestId = response.getRequestId();
            LOG.warn("[RequestId: {}] Failed to NS META CompleteFile for {} fileId {} block {} clientName {} " +
                            "from bucket {}, errorCode: {}, errorMessage: {}",
                    requestId, path, fileId, blockStr, clientName, bucket,
                    status.getCode(), status.getMessage());
        }

        return status;
    }

    public void setClientName(String clientName) {
        request.setClientName(clientName);
    }

    public void setBlock(ExtendedBlock block) {
        ((JfsCompleteFileRequest) request).setBlock(block);
    }

    public void setFileId(long fileId) {
        ((JfsCompleteFileRequest) request).setFileId(fileId);
    }

    public void setPath(String path) {
        request.setPath(path);
    }

    public boolean getResult() {
        return ((JfsCompleteFileResponse) response).getResult();
    }
}
