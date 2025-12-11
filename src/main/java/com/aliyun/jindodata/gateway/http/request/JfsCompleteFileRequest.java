package com.aliyun.jindodata.gateway.http.request;

import com.aliyun.jindodata.gateway.common.JfsRequestOptions;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;

public class JfsCompleteFileRequest extends JfsAbstractHttpRequest {
    private static final String REQUEST_TYPE = "completeFile";
    private static final String PATH_KEY = "path";
    private static final String CLIENTNAME_KEY = "clientName";
    private static final String FILEID_KEY = "fileId";
    private static final String BLOCK_KEY = "lastBlock";

    private ExtendedBlock block = null;
    private long fileId = 0;

    public JfsCompleteFileRequest() {
        super();
        setQueryParam(NS_DFS, "");
    }

    @Override
    public void prepareRequest(JfsRequestOptions options) {
        initRequestWithOptions(options);
        initRequestXml(REQUEST_TYPE);

        requestXml.addRequestParameter(PATH_KEY, encodePath(path));
        requestXml.addRequestParameter(CLIENTNAME_KEY, clientName);
        requestXml.addRequestParameter(FILEID_KEY, fileId);
        requestXml.addRequestParameter(BLOCK_KEY, block);

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
