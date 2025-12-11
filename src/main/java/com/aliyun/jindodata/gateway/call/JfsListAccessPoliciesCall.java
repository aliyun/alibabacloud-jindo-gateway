package com.aliyun.jindodata.gateway.call;

import com.aliyun.jindodata.gateway.http.reponse.JfsListAccessPoliciesResponse;
import com.aliyun.jindodata.gateway.http.request.JfsListAccessPoliciesRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class JfsListAccessPoliciesCall extends JfsBaseCall{
    private static final Logger LOG = LoggerFactory.getLogger(JfsListAccessPoliciesCall.class);


    public JfsListAccessPoliciesCall () {
        request = new JfsListAccessPoliciesRequest();
        response = new JfsListAccessPoliciesResponse();
    }

    public List<String> getAccessPolicies() {
        return ((JfsListAccessPoliciesResponse) response).getAccessPolicies();
    }
}
