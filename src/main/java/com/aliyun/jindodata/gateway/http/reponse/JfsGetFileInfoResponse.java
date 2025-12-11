package com.aliyun.jindodata.gateway.http.reponse;

import com.aliyun.jindodata.gateway.common.JfsFileStatus;
import com.aliyun.jindodata.gateway.common.JfsStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class JfsGetFileInfoResponse extends JfsAbstractHttpResponse {
    private static final Logger LOG = LoggerFactory.getLogger(JfsGetFileInfoResponse.class);

    private JfsFileStatus fileStatus;

    public JfsGetFileInfoResponse() {
        fileStatus = new JfsFileStatus();
    }

    @Override
    public JfsStatus parseXml(String xml) {
        try {
            fileStatus = responseXml.getJfsFileStatus();
        } catch (IOException e) {
            LOG.warn("Failed to parse get file info response", e);
            return JfsStatus.corruption("Failed to parse get file info response: " + e.getMessage());
        }

        return JfsStatus.OK();
    }

    public JfsFileStatus getFileStatus() {
        return fileStatus;
    }
}
