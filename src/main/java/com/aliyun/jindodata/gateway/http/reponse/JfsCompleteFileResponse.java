package com.aliyun.jindodata.gateway.http.reponse;

import com.aliyun.jindodata.gateway.common.JfsResponseXml;
import com.aliyun.jindodata.gateway.common.JfsStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import java.io.IOException;

public class JfsCompleteFileResponse extends JfsAbstractHttpResponse {
    private static final Logger LOG = LoggerFactory.getLogger(JfsCompleteFileResponse.class);

    private boolean result = false;

    @Override
    public JfsStatus parseXml(String xml) {
        Element response = responseXml.getResponseNode();

        try {
            result = JfsResponseXml.getNodeBool(response, "result", false, true);
        } catch (IOException e) {
            LOG.warn("Failed to parse complete file response", e);
            return JfsStatus.corruption("Failed to parse complete file response: " + e.getMessage());
        }

        return JfsStatus.OK();
    }

    public boolean getResult() {
        return result;
    }
}
