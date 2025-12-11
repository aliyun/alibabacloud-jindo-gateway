package com.aliyun.jindodata.gateway.http.request;

import com.aliyun.jindodata.gateway.common.JfsRequestOptions;

public class JfsGetBlockLocationsRequest extends JfsAbstractHttpRequest {
    private static final String REQUEST_TYPE = "getBlockLocations";
    private static final String PATH_KEY = "path";
    private static final String OFFSET_KEY = "offset";
    private static final String LENGTH_KEY = "length";

    private long offset = 0;
    private long length = 0;

    public JfsGetBlockLocationsRequest() {
        super();
        setQueryParam(NS_DFS, "");
    }

    @Override
    public void prepareRequest(JfsRequestOptions options) {
        initRequestWithOptions(options);
        initRequestXml(REQUEST_TYPE);

        requestXml.addRequestParameter(PATH_KEY, encodePath(path));
        requestXml.addRequestParameter(OFFSET_KEY, offset);
        requestXml.addRequestParameter(LENGTH_KEY, length);

        setBody(requestXml.getXmlString());
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public long getLength() {
        return length;
    }

    public void setLength(long length) {
        this.length = length;
    }
}
