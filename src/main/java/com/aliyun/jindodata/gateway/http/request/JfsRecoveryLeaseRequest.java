package com.aliyun.jindodata.gateway.http.request;

import com.aliyun.jindodata.gateway.common.JfsRequestOptions;

public class JfsRecoveryLeaseRequest extends JfsAbstractHttpRequest {
    private static final String REQUEST_TYPE = "recoveryLease";
    private static final String PATH_KEY = "path";
    private static final String CLIENTNAME_KEY = "clientName";

    public JfsRecoveryLeaseRequest() {
        super();
        setQueryParam(NS_DFS, "");
    }

    @Override
    public void prepareRequest(JfsRequestOptions options) {
        initRequestWithOptions(options);
        initRequestXml(REQUEST_TYPE);

        requestXml.addRequestParameter(PATH_KEY, encodePath(path));
        requestXml.addRequestParameter(CLIENTNAME_KEY, clientName);

        setBody(requestXml.getXmlString());
    }
}
