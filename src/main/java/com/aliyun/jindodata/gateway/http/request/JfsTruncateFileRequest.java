package com.aliyun.jindodata.gateway.http.request;

import com.aliyun.jindodata.gateway.common.JfsRequestOptions;

public class JfsTruncateFileRequest extends JfsAbstractHttpRequest {
    private static final String REQUEST_TYPE = "truncateFile";
    private static final String PATH_KEY = "path";
    private static final String SIZE_KEY = "size";
    private static final String CLIENTNAME_KEY = "clientName";

    private long size = 0;

    public JfsTruncateFileRequest() {
        super();
        setQueryParam(NS_DFS, "");
    }

    @Override
    public void prepareRequest(JfsRequestOptions options) {
        initRequestWithOptions(options);
        initRequestXml(REQUEST_TYPE);

        requestXml.addRequestParameter(PATH_KEY, encodePath(path));
        requestXml.addRequestParameter(SIZE_KEY, size);
        requestXml.addRequestParameter(CLIENTNAME_KEY, clientName);

        setBody(requestXml.getXmlString());
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }
}
