package com.aliyun.jindodata.gateway.http.reponse;

import com.aliyun.jindodata.gateway.common.JfsStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class JfsListAccessPoliciesResponse extends JfsAbstractHttpResponse {
    private static final Logger LOG = LoggerFactory.getLogger(JfsListAccessPoliciesResponse.class);

    private List<String> accessPolicies;

    @Override
    public JfsStatus parseXml(String xml) {
        accessPolicies = new ArrayList<>();
        return responseXml.getAccessPolicies(accessPolicies);
    }

    public List<String> getAccessPolicies() {
        return accessPolicies;
    }
}
