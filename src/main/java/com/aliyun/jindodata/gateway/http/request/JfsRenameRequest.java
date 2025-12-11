package com.aliyun.jindodata.gateway.http.request;

import com.aliyun.jindodata.gateway.common.JfsRequestOptions;

public class JfsRenameRequest extends JfsAbstractHttpRequest {
    private static final String REQUEST_TYPE = "rename";
    private static final String SOURCE_KEY = "source";
    private static final String DESTINATION_KEY = "destination";

    private String srcPath = "";
    private String dstPath = "";

    public JfsRenameRequest() {
        super();
        setQueryParam(NS_DFS, "");
    }

    @Override
    public void prepareRequest(JfsRequestOptions options) {
        initRequestWithOptions(options);
        initRequestXml(REQUEST_TYPE);

        requestXml.addRequestParameter(SOURCE_KEY, encodePath(srcPath));
        requestXml.addRequestParameter(DESTINATION_KEY, encodePath(dstPath));

        setBody(requestXml.getXmlString());
    }

    public String getSrcPath() {
        return srcPath;
    }

    public void setSrcPath(String srcPath) {
        this.srcPath = srcPath;
    }

    public String getDstPath() {
        return dstPath;
    }

    public void setDstPath(String dstPath) {
        this.dstPath = dstPath;
    }
}
