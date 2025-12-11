package com.aliyun.jindodata.gateway.http.request;

import com.aliyun.jindodata.gateway.common.JfsRequestOptions;

public class JfsRenewLeaseRequest extends JfsAbstractHttpRequest {
    private static final String REQUEST_TYPE = "renewLease";
    private static final String CLIENTNAME_KEY = "clientName";

    public JfsRenewLeaseRequest() {
        super();
        setQueryParam(NS_DFS, "");
    }

    @Override
    public void prepareRequest(JfsRequestOptions options) {
        initRequestWithOptions(options);
        initRequestXml(REQUEST_TYPE);

        requestXml.addRequestParameter(CLIENTNAME_KEY, clientName);

        setBody(requestXml.getXmlString());
    }
}
