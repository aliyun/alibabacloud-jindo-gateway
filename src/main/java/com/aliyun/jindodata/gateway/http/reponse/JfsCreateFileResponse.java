package com.aliyun.jindodata.gateway.http.reponse;

import com.aliyun.jindodata.gateway.common.JfsFileStatus;
import com.aliyun.jindodata.gateway.common.JfsResponseXml;
import com.aliyun.jindodata.gateway.common.JfsStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import java.io.IOException;

public class JfsCreateFileResponse extends JfsAbstractHttpResponse {
    private static final Logger LOG = LoggerFactory.getLogger(JfsCreateFileResponse.class);

    private JfsFileStatus fileStatus;

    @Override
    public JfsStatus parseXml(String xml) {
        Element response = responseXml.getResponseNode();

        try {
            fileStatus = JfsResponseXml.getJfsFileStatus(response);
        } catch (IOException e) {
            LOG.warn("Failed to parse create file response", e);
            return JfsStatus.corruption("Failed to parse create file response: " + e.getMessage());
        }

        return JfsStatus.OK();
    }

    public JfsFileStatus getFileStatus() {
        return fileStatus;
    }
}
