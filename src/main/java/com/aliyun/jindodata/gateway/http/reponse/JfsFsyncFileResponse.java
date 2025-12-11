package com.aliyun.jindodata.gateway.http.reponse;

import com.aliyun.jindodata.gateway.common.JfsStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JfsFsyncFileResponse extends JfsAbstractHttpResponse {
    private static final Logger LOG = LoggerFactory.getLogger(JfsFsyncFileResponse.class);

    @Override
    public JfsStatus parseXml(String xml) {
        // FsyncFile response has no specific fields to parse
        return JfsStatus.OK();
    }
}
