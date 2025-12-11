package com.aliyun.jindodata.gateway.http.reponse;

import com.aliyun.jindodata.gateway.common.JfsFileStatus;
import com.aliyun.jindodata.gateway.common.JfsResponseXml;
import com.aliyun.jindodata.gateway.common.JfsStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import java.io.IOException;

public class JfsAppendFileResponse extends JfsAbstractHttpResponse {
    private static final Logger LOG = LoggerFactory.getLogger(JfsAppendFileResponse.class);

    private JfsFileStatus fileStatus;
    private LocatedBlock lastBlock;

    @Override
    public JfsStatus parseXml(String xml) {
        Element response = responseXml.getResponseNode();

        try {
            fileStatus = JfsResponseXml.getJfsFileStatus(response);

            Element lastBlockNode = JfsResponseXml.getNode(response, "lastBlock");
            if (lastBlockNode != null) {
                lastBlock = JfsResponseXml.getLocatedBlock(lastBlockNode);
            }
        } catch (IOException e) {
            LOG.warn("Failed to parse append file response", e);
            return JfsStatus.corruption("Failed to parse append file response: " + e.getMessage());
        }

        return JfsStatus.OK();
    }

    public JfsFileStatus getFileStatus() {
        return fileStatus;
    }

    public LocatedBlock getLastBlock() {
        return lastBlock;
    }
}
