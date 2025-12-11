package com.aliyun.jindodata.gateway.http.request;

import com.aliyun.jindodata.gateway.common.JfsRequestOptions;

public class JfsListAccessPoliciesRequest extends JfsAbstractHttpRequest {
    private static final String REQUEST_TYPE = "listAccessPolicies";

    public JfsListAccessPoliciesRequest() {
        super();
        setQueryParam(NS_DFS, "");
    }

    @Override
    public void prepareRequest(JfsRequestOptions options) {
        initRequestWithOptions(options);
        initRequestXml(REQUEST_TYPE);

        setBody(requestXml.getXmlString());
    }
}
