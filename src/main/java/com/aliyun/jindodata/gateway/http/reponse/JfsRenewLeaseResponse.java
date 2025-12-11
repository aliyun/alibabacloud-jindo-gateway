package com.aliyun.jindodata.gateway.http.reponse;

import com.aliyun.jindodata.gateway.common.JfsStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JfsRenewLeaseResponse extends JfsAbstractHttpResponse {
    private static final Logger LOG = LoggerFactory.getLogger(JfsRenewLeaseResponse.class);

    @Override
    public JfsStatus parseXml(String xml) {
        // RenewLease response has no specific fields to parse
        return JfsStatus.OK();
    }
}
