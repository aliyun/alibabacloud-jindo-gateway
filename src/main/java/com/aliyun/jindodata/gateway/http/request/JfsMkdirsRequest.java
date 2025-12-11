package com.aliyun.jindodata.gateway.http.request;

import com.aliyun.jindodata.gateway.common.JfsRequestOptions;

public class JfsMkdirsRequest extends JfsAbstractHttpRequest {
    private static final String REQUEST_TYPE = "mkdirs";
    private static final String PATH_KEY = "path";
    private static final String PERMISSION_KEY = "permission";
    private static final String CREATE_PARENT_KEY = "createParent";

    private String permission = "";
    private boolean createParent = false;

    public JfsMkdirsRequest() {
        super();
        setQueryParam(NS_DFS, "");
    }

    @Override
    public void prepareRequest(JfsRequestOptions options) {
        initRequestWithOptions(options);
        initRequestXml(REQUEST_TYPE);

        requestXml.addRequestParameter(PATH_KEY, encodePath(path));
        requestXml.addRequestParameter(PERMISSION_KEY, permission);
        requestXml.addRequestParameter(CREATE_PARENT_KEY, createParent);

        setBody(requestXml.getXmlString());
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

    public boolean isCreateParent() {
        return createParent;
    }

    public void setCreateParent(boolean createParent) {
        this.createParent = createParent;
    }
}
