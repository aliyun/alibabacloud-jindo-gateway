package com.aliyun.jindodata.gateway.http.request;

import com.aliyun.jindodata.gateway.common.JfsRequestOptions;

public class JfsCreateFileRequest extends JfsAbstractHttpRequest {
    private static final String REQUEST_TYPE = "createFile";
    private static final String PATH_KEY = "path";
    private static final String CLIENTNAME_KEY = "clientName";
    private static final String BLOCKSIZE_KEY = "blockSize";
    private static final String REPLICATION_KEY = "replication";
    private static final String CREATEPARENT_KEY = "createParent";
    private static final String FLAG_KEY = "flag";
    private static final String PERMISSION_KEY = "permission";

    private String flag = "";
    private long blockSize = 0;
    private int replication = 0;
    private boolean createParent = false;
    private String permission = "";

    public JfsCreateFileRequest() {
        super();
        setQueryParam(NS_DFS, "");
    }

    @Override
    public void prepareRequest(JfsRequestOptions options) {
        initRequestWithOptions(options);
        initRequestXml(REQUEST_TYPE);

        requestXml.addRequestParameter(PATH_KEY, encodePath(path));
        requestXml.addRequestParameter(CLIENTNAME_KEY, clientName);
        requestXml.addRequestParameter(BLOCKSIZE_KEY, blockSize);
        requestXml.addRequestParameter(REPLICATION_KEY, replication);
        requestXml.addRequestParameter(CREATEPARENT_KEY, createParent);
        requestXml.addRequestParameter(FLAG_KEY, flag);
        requestXml.addRequestParameter(PERMISSION_KEY, permission);

        setBody(requestXml.getXmlString());
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    public void setFlag(int flag) {
        this.flag = String.valueOf(flag);
    }

    public long getBlockSize() {
        return blockSize;
    }

    public void setBlockSize(long blockSize) {
        this.blockSize = blockSize;
    }

    public int getReplication() {
        return replication;
    }

    public void setReplication(int replication) {
        this.replication = replication;
    }

    public boolean isCreateParent() {
        return createParent;
    }

    public void setCreateParent(boolean createParent) {
        this.createParent = createParent;
    }

    public String getPermission() {
        return permission;
    }

    public void setPermission(String permission) {
        this.permission = permission;
    }

    public void setPermission(int permission) {
        this.permission = String.valueOf(permission);
    }

    public void setPermission(short permission) {
        this.permission = String.valueOf(permission);
    }
}
