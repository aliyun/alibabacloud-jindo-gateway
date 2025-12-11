package com.aliyun.jindodata.gateway.call;

import com.aliyun.jindodata.gateway.common.JfsFileStatus;
import com.aliyun.jindodata.gateway.common.JfsRequestOptions;
import com.aliyun.jindodata.gateway.common.JfsStatus;
import com.aliyun.jindodata.gateway.common.JfsUtil;
import com.aliyun.jindodata.gateway.http.reponse.JfsGetListingResponse;
import com.aliyun.jindodata.gateway.http.request.JfsGetListingRequest;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class JfsGetListingCall extends JfsBaseCall{
    private static final Logger LOG = LoggerFactory.getLogger(JfsGetListingCall.class);
    private DirectoryListing directoryListing;
    private int maxKeys = -1;
    private String startAfter;
    private boolean needLocation;

    public JfsGetListingCall() {
        request = new JfsGetListingRequest();
        response = new JfsGetListingResponse();
    }

    @Override
    public JfsStatus execute(JfsRequestOptions requestOptions) {
        long startTime = System.currentTimeMillis();
        String path = getPath();
        String bucket = requestOptions.getBucket();

        LOG.info("GetListing path {} from bucket {}", path, bucket);

        JfsStatus status = super.execute(requestOptions);

        if (status.isOk()) {
            long elapsed = System.currentTimeMillis() - startTime;
            String requestId = response.getRequestId();
            String serverTime = response.getServerTime();

            LOG.info("[RequestId: {}] Successfully getListing path {} from bucket {} dur {}ms ossServerElapsed {}",
                    requestId, path, bucket, elapsed, serverTime);
        } else {
            String requestId = response.getRequestId();
            LOG.warn("[RequestId: {}] Failed to getListing path {} from bucket {}, errorCode: {}, errorMessage: {}",
                    requestId, path, bucket, status.getCode(), status.getMessage());
        }

        return status;
    }

    @Override
    protected void processResponse() {
        toHdfs();
    }

    private void toHdfs() {
        List<JfsFileStatus> fileStatuses = getFileStatuses();
        int remainingEntries = isTruncated() ? 1 : 0;
        HdfsFileStatus[] partialListing;
        if (fileStatuses == null || fileStatuses.isEmpty()) {
            partialListing = new HdfsFileStatus[0];
            directoryListing = new DirectoryListing(partialListing, remainingEntries);
            return;
        }

        int numOfListing = Math.min(fileStatuses.size(), maxKeys <= 0 ? Integer.MAX_VALUE : maxKeys);
        partialListing = new HdfsFileStatus[numOfListing];
        int count = 0;
        for (JfsFileStatus fileStatus : fileStatuses) {
            String name = fileStatus.getName();
            if (startAfter.isEmpty() || name.isEmpty() || !startAfter.equals(name)) {
                if (count >= numOfListing) {
                    ++remainingEntries;
                    break;
                }
                if (needLocation) {
                    if (fileStatus.isFile()) {
                        JfsGetBlockLocationsCall blockLocationsCall = getGetBlockLocationsCall(name, fileStatus.getFileSize());
                        JfsStatus status =  blockLocationsCall.execute(requestOptions);

                        if (!status.isOk()) {
                            LOG.error("getBlockLocations failed: {}", status.getMessage());
                            throw new RuntimeException(status.getMessage());
                        }

                        partialListing[count] = JfsUtil.convert2HdfsLocatedFileStatus(fileStatus,
                                blockLocationsCall.getBlocks());
                    } else {
                        partialListing[count] = JfsUtil.convert2HdfsFileStatus(fileStatus);
                    }
                } else {
                    partialListing[count] = JfsUtil.convert2HdfsFileStatus(fileStatus);
                }
                ++count;
            }
        }
        directoryListing = new DirectoryListing(partialListing, remainingEntries);
    }

    private @NotNull JfsGetBlockLocationsCall getGetBlockLocationsCall(String name, long size) {
        String childPath = request.getPath();
        if (!childPath.endsWith("/")) {
            childPath += "/" + name;
        } else {
            childPath += name;
        }

        JfsGetBlockLocationsCall blockLocationsCall = new JfsGetBlockLocationsCall();
        blockLocationsCall.setPath(childPath);
        blockLocationsCall.setOffset(0);
        blockLocationsCall.setLength(size);
        return blockLocationsCall;
    }

    public void setPath(String path) {
        request.setPath(path);
    }

    public String getPath() {
        return request.getPath();
    }

    public void setMaxKeys(int maxKeys) {
        this.maxKeys = maxKeys;
        // if hdfs want maxKeys > 0, Dls should return one more.
        if (maxKeys > 0) {
            ((JfsGetListingRequest) request).setMaxKeys(maxKeys + 1);
        } else {
            ((JfsGetListingRequest) request).setMaxKeys(maxKeys);
        }
    }

    public void setMarker(String marker) {
        startAfter = marker;
        ((JfsGetListingRequest) request).setMarker(marker);
    }

    public void setNeedLocation(boolean needLocation) {
        this.needLocation = needLocation;
        // OSS-HDFS will never return block locations,
        // so we get block locations after get listing.
//        ((JfsGetListingRequest) request).setNeedLocation(needLocation);
    }

    public String getNextMarker() {
        return ((JfsGetListingResponse) response).getNextMarker();
    }

    public boolean isTruncated() {
        return ((JfsGetListingResponse) response).isTruncated();
    }

    public List<JfsFileStatus> getFileStatuses() {
        return ((JfsGetListingResponse) response).getFileStatuses();
    }

    public DirectoryListing getDirectoryListing() {
        return directoryListing;
    }
}
