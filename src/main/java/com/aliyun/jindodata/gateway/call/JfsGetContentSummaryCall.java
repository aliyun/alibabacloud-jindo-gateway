package com.aliyun.jindodata.gateway.call;

import com.aliyun.jindodata.gateway.common.JfsRequestOptions;
import com.aliyun.jindodata.gateway.common.JfsStatus;
import com.aliyun.jindodata.gateway.http.reponse.JfsGetContentSummaryResponse;
import com.aliyun.jindodata.gateway.http.request.JfsGetContentSummaryRequest;
import org.apache.hadoop.fs.ContentSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JfsGetContentSummaryCall extends JfsBaseCall {
    private static final Logger LOG = LoggerFactory.getLogger(JfsGetContentSummaryCall.class);

    private ContentSummary contentSummary;

    public JfsGetContentSummaryCall() {
        request = new JfsGetContentSummaryRequest();
        response = new JfsGetContentSummaryResponse();
    }

    @Override
    public JfsStatus execute(JfsRequestOptions requestOptions) {
        long startTime = System.currentTimeMillis();
        String path = getPath();
        String bucket = requestOptions.getBucket();

        LOG.info("GetContentSummary path {} from bucket {}", path, bucket);

        JfsStatus status = super.execute(requestOptions);

        if (status.isOk()) {
            long elapsed = System.currentTimeMillis() - startTime;
            String requestId = response.getRequestId();
            String serverTime = response.getServerTime();

            LOG.info("[RequestId: {}] Successfully getContentSummary path {} from bucket {} dur {}ms ossServerElapsed {}",
                    requestId, path, bucket, elapsed, serverTime);
        } else {
            String requestId = response.getRequestId();
            LOG.warn("[RequestId: {}] Failed to getContentSummary path {} from bucket {}, errorCode: {}, errorMessage: {}",
                    requestId, path, bucket, status.getCode(), status.getMessage());
        }

        return status;
    }

    @Override
    protected void processResponse() {
        JfsGetContentSummaryResponse resp = (JfsGetContentSummaryResponse) response;
        contentSummary = new ContentSummary.Builder()
                .length(resp.getLength())
                .fileCount(resp.getFileCount())
                .directoryCount(resp.getDirectoryCount())
                .quota(resp.getQuota())
                .spaceConsumed(resp.getSpaceConsumed())
                .spaceQuota(resp.getSpaceQuota())
                .snapshotLength(resp.getSnapshotLength())
                .snapshotFileCount(resp.getSnapshotFileCount())
                .snapshotDirectoryCount(resp.getSnapshotDirectoryCount())
                .snapshotSpaceConsumed(resp.getSnapshotSpaceConsumed())
                .build();
    }

    public void setPath(String path) {
        request.setPath(path);
    }

    public String getPath() {
        return request.getPath();
    }

    public ContentSummary getSummary() {
        return contentSummary;
    }

    public JfsGetContentSummaryResponse getResponse() {
        return (JfsGetContentSummaryResponse) response;
    }
}
