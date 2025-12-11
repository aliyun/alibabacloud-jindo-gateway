package com.aliyun.jindodata.gateway.http.reponse;

import com.aliyun.jindodata.gateway.common.JfsResponseXml;
import com.aliyun.jindodata.gateway.common.JfsStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

public class JfsRecoveryLeaseResponse extends JfsAbstractHttpResponse {
    private static final Logger LOG = LoggerFactory.getLogger(JfsRecoveryLeaseResponse.class);

    private boolean result = false;

    @Override
    public JfsStatus parseXml(String xml) {
        Element response = responseXml.getResponseNode();
        try {
            result = JfsResponseXml.getNodeBool(response, "result", false, true);
        } catch (Exception e) {
            LOG.warn("Failed to parse recovery lease response", e);
            return JfsStatus.corruption("Failed to parse recovery lease response: " + e.getMessage());
        }

        return JfsStatus.OK();
    }

    public boolean getResult() {
        return result;
    }

    public void setResult(boolean result) {
        this.result = result;
    }
}
