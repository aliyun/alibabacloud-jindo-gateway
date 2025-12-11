package com.aliyun.jindodata.gateway.http.reponse;

import com.aliyun.jindodata.gateway.common.JfsStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class JfsGetBlockLocationsResponse extends JfsAbstractHttpResponse {
    private static final Logger LOG = LoggerFactory.getLogger(JfsGetBlockLocationsResponse.class);

    private LocatedBlocks locatedBlocks;

    @Override
    public JfsStatus parseXml(String xml) {
        try {
            locatedBlocks = responseXml.getLocatedBlocks();
        } catch (IOException e) {
            LOG.warn("Failed to parse get block locations response", e);
            return JfsStatus.corruption("Failed to parse get block locations response: " + e.getMessage());
        }

        return JfsStatus.OK();
    }


    public LocatedBlocks getLocatedBlocks() {
        return locatedBlocks;
    }
}
