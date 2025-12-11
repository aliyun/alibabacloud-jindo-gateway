package com.aliyun.jindodata.gateway.http.reponse;

import com.aliyun.jindodata.gateway.common.JfsStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JfsSetPermissionResponse extends JfsAbstractHttpResponse {
    private static final Logger LOG = LoggerFactory.getLogger(JfsSetPermissionResponse.class);

    @Override
    public JfsStatus parseXml(String xml) {
        return JfsStatus.OK();
    }
}
