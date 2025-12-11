package com.aliyun.jindodata.gateway.http.request;

import com.aliyun.jindodata.gateway.common.JfsRequestOptions;

public class JfsDeleteRequest extends JfsAbstractHttpRequest {
    private static final String REQUEST_TYPE = "remove";
    private static final String PATH_KEY = "path";
    private static final String RECURSIVE_KEY = "recursive";

    private boolean recursive = false;

    public JfsDeleteRequest() {
        super();
        setQueryParam(NS_DFS, "");
    }

    @Override
    public void prepareRequest(JfsRequestOptions options) {
        initRequestWithOptions(options);
        initRequestXml(REQUEST_TYPE);

        requestXml.addRequestParameter(PATH_KEY, encodePath(path));
        requestXml.addRequestParameter(RECURSIVE_KEY, recursive);

        setBody(requestXml.getXmlString());
    }

    public boolean isRecursive() {
        return recursive;
    }

    public void setRecursive(boolean recursive) {
        this.recursive = recursive;
    }
}
