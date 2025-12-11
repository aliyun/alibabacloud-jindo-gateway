package com.aliyun.jindodata.gateway.call;

import com.aliyun.jindodata.gateway.common.JfsRequestOptions;
import com.aliyun.jindodata.gateway.common.JfsStatus;
import com.aliyun.jindodata.gateway.http.reponse.JfsGetBlockLocationsResponse;
import com.aliyun.jindodata.gateway.http.request.JfsGetBlockLocationsRequest;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JfsGetBlockLocationsCall extends JfsBaseCall {
    private static final Logger LOG = LoggerFactory.getLogger(JfsGetBlockLocationsCall.class);

    public JfsGetBlockLocationsCall() {
        request = new JfsGetBlockLocationsRequest();
        response = new JfsGetBlockLocationsResponse();
    }

    @Override
    public JfsStatus execute(JfsRequestOptions requestOptions) {
        long startTime = System.currentTimeMillis();
        String path = getPath();
        long offset = getOffset();
        long length = getLength();
        String bucket = requestOptions.getBucket();

        LOG.info("GetBlockLocations for {} offset {} length {} from bucket {}",
                path, offset, length, bucket);

        JfsStatus status = super.execute(requestOptions);

        if (status.isOk()) {
            long elapsed = System.currentTimeMillis() - startTime;
            String requestId = response.getRequestId();
            String serverTime = response.getServerTime();

            LOG.info("[RequestId: {}] Successfully GetBlockLocations {} offset {} length {} " +
                            "from bucket {} dur {}ms ossServerElapsed {}",
                    requestId, path, offset, length, bucket, elapsed, serverTime);
        } else {
            String requestId = response.getRequestId();
            LOG.warn("[RequestId: {}] Failed to NS META GetBlockLocations for {} offset {} length {} " +
                            "from bucket {}, errorCode: {}, errorMessage: {}",
                    requestId, path, offset, length, bucket, status.getCode(), status.getMessage());
        }
        return status;
    }

    public void setPath(String path) {
        request.setPath(path);
    }

    public String getPath() {
        return request.getPath();
    }

    public void setOffset(long offset) {
        ((JfsGetBlockLocationsRequest) request).setOffset(offset);
    }

    public long getOffset() {
        return ((JfsGetBlockLocationsRequest) request).getOffset();
    }

    public void setLength(long length) {
        ((JfsGetBlockLocationsRequest) request).setLength(length);
    }

    public long getLength() {
        return ((JfsGetBlockLocationsRequest) request).getLength();
    }

    /**
     * blocks in result do not contain datanode info,
     * should add later.
     */
    public LocatedBlocks getBlocks() {
        return ((JfsGetBlockLocationsResponse) response).getLocatedBlocks();
    }
}
