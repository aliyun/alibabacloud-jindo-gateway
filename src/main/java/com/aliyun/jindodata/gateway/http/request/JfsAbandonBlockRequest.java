package com.aliyun.jindodata.gateway.http.request;

import com.aliyun.jindodata.gateway.common.JfsRequestOptions;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;

public class JfsAbandonBlockRequest extends JfsAbstractHttpRequest {
    private static final String REQUEST_TYPE = "abandonBlock";
    private static final String PATH_KEY = "src";
    private static final String CLIENTNAME_KEY = "clientName";
    private static final String B_KEY = "b";
    private static final String FILE_ID_KEY = "fileId";

    private ExtendedBlock block = null;
    private long fileId = 0;

    public JfsAbandonBlockRequest() {
        super();
        setQueryParam(NS_DFS, "");
    }

    @Override
    public void prepareRequest(JfsRequestOptions options) {
        initRequestWithOptions(options);
        initRequestXml(REQUEST_TYPE);

        requestXml.addRequestParameter(PATH_KEY, encodePath(path));
        requestXml.addRequestParameter(CLIENTNAME_KEY, clientName);
        requestXml.addRequestParameter(B_KEY, block);
        requestXml.addRequestParameter(FILE_ID_KEY, fileId);

        setBody(requestXml.getXmlString());
    }

    public ExtendedBlock getBlock() {
        return block;
    }

    public void setBlock(ExtendedBlock block) {
        this.block = block;
    }

    public long getFileId() {
        return fileId;
    }

    public void setFileId(long fileId) {
        this.fileId = fileId;
    }
}
