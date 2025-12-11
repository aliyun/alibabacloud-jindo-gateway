package com.aliyun.jindodata.gateway.http.request;

import com.aliyun.jindodata.gateway.common.JfsRequestOptions;

public class JfsFsyncFileRequest extends JfsAbstractHttpRequest {
    private static final String REQUEST_TYPE = "fsyncFile";
    private static final String PATH_KEY = "path";
    private static final String FILEID_KEY = "fileId";
    private static final String CLIENTNAME_KEY = "clientName";
    private static final String BLOCK_LENGTH_KEY = "lastBlockLength";

    private long lastBlockLength = -1;
    private long fileId = 0;

    public JfsFsyncFileRequest() {
        super();
        setQueryParam(NS_DFS, "");
    }

    @Override
    public void prepareRequest(JfsRequestOptions options) {
        initRequestWithOptions(options);
        initRequestXml(REQUEST_TYPE);

        requestXml.addRequestParameter(PATH_KEY, encodePath(path));
        requestXml.addRequestParameter(FILEID_KEY, fileId);
        requestXml.addRequestParameter(CLIENTNAME_KEY, clientName);
        requestXml.addRequestParameter(BLOCK_LENGTH_KEY, lastBlockLength);

        setBody(requestXml.getXmlString());
    }

    public long getLastBlockLength() {
        return lastBlockLength;
    }

    public void setLastBlockLength(long lastBlockLength) {
        this.lastBlockLength = lastBlockLength;
    }

    public long getFileId() {
        return fileId;
    }

    public void setFileId(long fileId) {
        this.fileId = fileId;
    }
}
