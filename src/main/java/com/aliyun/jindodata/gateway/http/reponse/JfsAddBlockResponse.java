package com.aliyun.jindodata.gateway.http.reponse;

import com.aliyun.jindodata.gateway.common.JfsResponseXml;
import com.aliyun.jindodata.gateway.common.JfsStatus;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import java.io.IOException;

public class JfsAddBlockResponse extends JfsAbstractHttpResponse {
    private static final Logger LOG = LoggerFactory.getLogger(JfsAddBlockResponse.class);

    private ExtendedBlock extendedBlock;
    private Token<BlockTokenIdentifier> blockToken;
    private long startOffset = -1;

    @Override
    public JfsStatus parseXml(String xml) {
        Element response = responseXml.getResponseNode();

        try {
            Element blockNode = JfsResponseXml.getNode(response, "extendedBlock");
            if (blockNode != null) {
                extendedBlock = JfsResponseXml.getExtendedBlock(blockNode);
            } else {
                LOG.warn("extendedBlock node not found in response");
                return JfsStatus.corruption("cannot parse status for add block");
            }

            Element blockTokenNode = JfsResponseXml.getNode(response, "blockToken");
            if (blockTokenNode != null) {
                blockToken = JfsResponseXml.getBlockToken(blockTokenNode);
            }

            startOffset = JfsResponseXml.getNodeLong(response, "offset", -1, true);

        } catch (IOException e) {
            LOG.warn("Failed to parse add block response", e);
            return JfsStatus.corruption("Failed to parse add block response: " + e.getMessage());
        }

        return JfsStatus.OK();
    }

    public ExtendedBlock getExtendedBlock() {
        return extendedBlock;
    }

    public Token<BlockTokenIdentifier> getBlockToken() {
        return blockToken;
    }

    public long getStartOffset() {
        return startOffset;
    }
}
