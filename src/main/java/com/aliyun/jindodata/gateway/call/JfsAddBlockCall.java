package com.aliyun.jindodata.gateway.call;

import com.aliyun.jindodata.gateway.common.JfsRequestOptions;
import com.aliyun.jindodata.gateway.common.JfsStatus;
import com.aliyun.jindodata.gateway.http.reponse.JfsAddBlockResponse;
import com.aliyun.jindodata.gateway.http.request.JfsAddBlockRequest;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfoWithStorage;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JfsAddBlockCall extends JfsBaseCall {
    private static final Logger LOG = LoggerFactory.getLogger(JfsAddBlockCall.class);

    private LocatedBlock locatedBlock;

    public JfsAddBlockCall() {
        request = new JfsAddBlockRequest();
        response = new JfsAddBlockResponse();
    }

    @Override
    public JfsStatus execute(JfsRequestOptions requestOptions) {
        long startTime = System.currentTimeMillis();
        JfsAddBlockRequest req = (JfsAddBlockRequest) request;

        String path = req.getPath();
        long fileId = req.getFileId();
        String clientName = req.getClientName();
        String bucket = requestOptions.getBucket();

        LOG.info("AddBlock path {} fileId {} clientName {} from bucket {}",
                path,  fileId, clientName, bucket);

        JfsStatus status = super.execute(requestOptions);

        if (status.isOk()) {
            long elapsed = System.currentTimeMillis() - startTime;
            String requestId = response.getRequestId();
            String serverTime = response.getServerTime();

            LOG.info("[RequestId: {}] Successfully AddBlock path {} fileId {} clientName {} " +
                            "from bucket {} dur {}ms ossServerElapsed {}",
                    requestId, path, fileId, clientName, bucket, elapsed, serverTime);
        } else {
            String requestId = response.getRequestId();
            LOG.warn("[RequestId: {}] Failed to AddBlock path {} fileId {} clientName {} " +
                            "from bucket {}, errorCode: {}, errorMessage: {}",
                    requestId, path, fileId, clientName, bucket,
                    status.getCode(), status.getMessage());
        }

        // real locs will be filled in JindoNameSystem
        DatanodeInfoWithStorage[] locs = new DatanodeInfoWithStorage[0];
        String[] storageIds = new String[0];
        StorageType[] storageTypes = new StorageType[0];
        DatanodeInfo[] cacheLocs = new DatanodeInfo[0];

        locatedBlock = new LocatedBlock(getExtendedBlock(), locs, storageIds, storageTypes, getStartOffset(), false, cacheLocs);
        Token<BlockTokenIdentifier> blockToken = getBlockToken();
        if (null != blockToken) {
            locatedBlock.setBlockToken(blockToken);
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
        ((JfsAddBlockRequest) request).setFileId(fileId);
    }

    public void setPrevious(ExtendedBlock previous) {
        ((JfsAddBlockRequest) request).setPrevious(previous);
    }

    public ExtendedBlock getExtendedBlock() {
        return ((JfsAddBlockResponse) response).getExtendedBlock();
    }

    public Token<BlockTokenIdentifier> getBlockToken() {
        return ((JfsAddBlockResponse) response).getBlockToken();
    }

    public long getStartOffset() {
        return ((JfsAddBlockResponse) response).getStartOffset();
    }

    public LocatedBlock getLocatedBlock() {
        return locatedBlock;
    }
}
