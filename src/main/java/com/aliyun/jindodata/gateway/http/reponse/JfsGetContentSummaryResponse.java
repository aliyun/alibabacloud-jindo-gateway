package com.aliyun.jindodata.gateway.http.reponse;

import com.aliyun.jindodata.gateway.common.JfsResponseXml;
import com.aliyun.jindodata.gateway.common.JfsStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

public class JfsGetContentSummaryResponse extends JfsAbstractHttpResponse {
    private static final Logger LOG = LoggerFactory.getLogger(JfsGetContentSummaryResponse.class);

    private long length = -1;
    private long fileCount = -1;
    private long directoryCount = -1;
    private long quota = -1;
    private long spaceConsumed = -1;
    private long spaceQuota = -1;
    private long snapshotLength = -1;
    private long snapshotFileCount = -1;
    private long snapshotDirectoryCount = -1;
    private long snapshotSpaceConsumed = -1;

    @Override
    public JfsStatus parseXml(String xml) {
        Element response = responseXml.getResponseNode();
        if (response == null) {
            return JfsStatus.corruption("Missing response");
        }

        Element summary = JfsResponseXml.getNode(response, "summary");
        if (summary == null) {
            LOG.warn("Missing summary");
            return JfsStatus.corruption("Missing summary");
        }

        try {
            length = JfsResponseXml.getNodeLong(summary, "length", -1, true);
            fileCount = JfsResponseXml.getNodeLong(summary, "fileCount", -1, true);
            directoryCount = JfsResponseXml.getNodeLong(summary, "directoryCount", -1, true);
            quota = JfsResponseXml.getNodeLong(summary, "quota", -1, true);
            spaceConsumed = JfsResponseXml.getNodeLong(summary, "spaceConsumed", -1, true);
            spaceQuota = JfsResponseXml.getNodeLong(summary, "spaceQuota", -1, true);
            snapshotLength = JfsResponseXml.getNodeLong(summary, "snapshotLength", -1, true);
            snapshotFileCount = JfsResponseXml.getNodeLong(summary, "snapshotFileCount", -1, true);
            snapshotDirectoryCount = JfsResponseXml.getNodeLong(summary, "snapshotDirectoryCount", -1, true);
            snapshotSpaceConsumed = JfsResponseXml.getNodeLong(summary, "snapshotSpaceConsumed", -1, true);
        } catch (Exception e) {
            LOG.warn("Failed to parse content summary response", e);
            return JfsStatus.corruption("Failed to parse content summary response: " + e.getMessage());
        }

        return JfsStatus.OK();
    }

    public long getLength() {
        return length;
    }

    public void setLength(long length) {
        this.length = length;
    }

    public long getFileCount() {
        return fileCount;
    }

    public void setFileCount(long fileCount) {
        this.fileCount = fileCount;
    }

    public long getDirectoryCount() {
        return directoryCount;
    }

    public void setDirectoryCount(long directoryCount) {
        this.directoryCount = directoryCount;
    }

    public long getQuota() {
        return quota;
    }

    public void setQuota(long quota) {
        this.quota = quota;
    }

    public long getSpaceConsumed() {
        return spaceConsumed;
    }

    public void setSpaceConsumed(long spaceConsumed) {
        this.spaceConsumed = spaceConsumed;
    }

    public long getSpaceQuota() {
        return spaceQuota;
    }

    public void setSpaceQuota(long spaceQuota) {
        this.spaceQuota = spaceQuota;
    }

    public long getSnapshotLength() {
        return snapshotLength;
    }

    public void setSnapshotLength(long snapshotLength) {
        this.snapshotLength = snapshotLength;
    }

    public long getSnapshotFileCount() {
        return snapshotFileCount;
    }

    public void setSnapshotFileCount(long snapshotFileCount) {
        this.snapshotFileCount = snapshotFileCount;
    }

    public long getSnapshotDirectoryCount() {
        return snapshotDirectoryCount;
    }

    public void setSnapshotDirectoryCount(long snapshotDirectoryCount) {
        this.snapshotDirectoryCount = snapshotDirectoryCount;
    }

    public long getSnapshotSpaceConsumed() {
        return snapshotSpaceConsumed;
    }

    public void setSnapshotSpaceConsumed(long snapshotSpaceConsumed) {
        this.snapshotSpaceConsumed = snapshotSpaceConsumed;
    }
}
