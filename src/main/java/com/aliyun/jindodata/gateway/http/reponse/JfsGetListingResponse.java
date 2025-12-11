package com.aliyun.jindodata.gateway.http.reponse;

import com.aliyun.jindodata.gateway.common.JfsFileStatus;
import com.aliyun.jindodata.gateway.common.JfsResponseXml;
import com.aliyun.jindodata.gateway.common.JfsStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class JfsGetListingResponse extends JfsAbstractHttpResponse {
    private static final Logger LOG = LoggerFactory.getLogger(JfsGetListingResponse.class);

    private String nextMarker;
    private boolean isTruncated;
    private List<JfsFileStatus> fileStatuses;


    @Override
    public JfsStatus parseXml(String xml) {
        fileStatuses = new ArrayList<>();
        Element response = responseXml.getResponseNode();

        try {
            nextMarker = JfsResponseXml.getPath(response, "nextMarker", "", true);
            isTruncated = JfsResponseXml.getNodeBool(response, "isTruncated", false, true);
            responseXml.getJfsFileStatuses(fileStatuses);
        } catch (IOException e) {
            LOG.warn("Failed to parse get listing response", e);
            return JfsStatus.corruption("Failed to parse get listing response: " + e.getMessage());
        }

        return JfsStatus.OK();
    }

    public String getNextMarker() {
        return nextMarker;
    }

    public boolean isTruncated() {
        return isTruncated;
    }

    public List<JfsFileStatus> getFileStatuses() {
        return fileStatuses;
    }
}
