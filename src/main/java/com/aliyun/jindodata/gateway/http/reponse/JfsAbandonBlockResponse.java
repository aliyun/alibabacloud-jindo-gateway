package com.aliyun.jindodata.gateway.http.reponse;

import com.aliyun.jindodata.gateway.common.JfsStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JfsAbandonBlockResponse extends JfsAbstractHttpResponse {
    private static final Logger LOG = LoggerFactory.getLogger(JfsAbandonBlockResponse.class);

    @Override
    public JfsStatus parseXml(String xml) {
        return JfsStatus.OK();
    }
}
