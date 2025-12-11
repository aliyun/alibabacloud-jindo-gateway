package com.aliyun.jindodata.gateway.http.request;

import com.aliyun.jindodata.gateway.common.JfsRequestOptions;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;

import static com.aliyun.jindodata.gateway.common.JfsConstant.JFS_BACKEND_TYPE_CLOUD;

public class JfsAddBlockRequest extends JfsAbstractHttpRequest {
    private static final String REQUEST_TYPE = "addBlock";
    private static final String PATH_KEY = "path";
    private static final String CLIENTNAME_KEY = "clientName";
    private static final String PREVIOUS_KEY = "previous";
    private static final String FILE_ID_KEY = "fileId";
    private static final String BACKEND_TYPE_KEY = "backendType";

    private ExtendedBlock previous = null;
    private long fileId = 0;
    private int backendType = JFS_BACKEND_TYPE_CLOUD; // Default is CLOUD type

    public JfsAddBlockRequest() {
        super();
        setQueryParam(NS_DFS, "");
    }

    @Override
    public void prepareRequest(JfsRequestOptions options) {
        initRequestWithOptions(options);
        initRequestXml(REQUEST_TYPE);

        requestXml.addRequestParameter(PATH_KEY, encodePath(path));
        requestXml.addRequestParameter(CLIENTNAME_KEY, clientName);
        requestXml.addRequestParameter(PREVIOUS_KEY, previous);
        requestXml.addRequestParameter(FILE_ID_KEY, fileId);
        requestXml.addRequestParameter(BACKEND_TYPE_KEY, backendType);

        setBody(requestXml.getXmlString());
    }

    public ExtendedBlock getPrevious() {
        return previous;
    }

    public void setPrevious(ExtendedBlock previous) {
        this.previous = previous;
    }

    public long getFileId() {
        return fileId;
    }

    public void setFileId(long fileId) {
        this.fileId = fileId;
    }

    public int getBackendType() {
        return backendType;
    }

    public void setBackendType(int backendType) {
        this.backendType = backendType;
    }
}
