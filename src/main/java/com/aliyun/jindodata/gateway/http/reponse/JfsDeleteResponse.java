package com.aliyun.jindodata.gateway.http.reponse;

import com.aliyun.jindodata.gateway.common.JfsResponseXml;
import com.aliyun.jindodata.gateway.common.JfsStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

public class JfsDeleteResponse extends JfsAbstractHttpResponse {
    private static final Logger LOG = LoggerFactory.getLogger(JfsDeleteResponse.class);

    private boolean deleteResult = false;

    @Override
    public JfsStatus parseXml(String xml) {
        Element response = responseXml.getResponseNode();
        try {
            deleteResult = JfsResponseXml.getNodeBool(response, "result", false, true);
        } catch (Exception e) {
            LOG.warn("Failed to parse delete response", e);
            return JfsStatus.corruption("Failed to parse delete response: " + e.getMessage());
        }

        return JfsStatus.OK();
    }

    public boolean getResult() {
        return deleteResult;
    }
}
