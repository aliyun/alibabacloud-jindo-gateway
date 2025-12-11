package com.aliyun.jindodata.gateway.http.request;

import com.aliyun.jindodata.gateway.common.JfsRequestOptions;

public class JfsAppendFileRequest extends JfsAbstractHttpRequest {
    private static final String REQUEST_TYPE = "appendFile";
    private static final String PATH_KEY = "path";
    private static final String CLIENTNAME_KEY = "clientName";
    private static final String FLAG_KEY = "flag";

    private int flag = 0;

    public JfsAppendFileRequest() {
        super();
        setQueryParam(NS_DFS, "");
    }

    @Override
    public void prepareRequest(JfsRequestOptions options) {
        initRequestWithOptions(options);
        initRequestXml(REQUEST_TYPE);

        requestXml.addRequestParameter(PATH_KEY, encodePath(path));
        requestXml.addRequestParameter(CLIENTNAME_KEY, clientName);
        requestXml.addRequestParameter(FLAG_KEY, flag);

        setBody(requestXml.getXmlString());
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }
}
