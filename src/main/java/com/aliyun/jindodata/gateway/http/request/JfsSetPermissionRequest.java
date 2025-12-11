package com.aliyun.jindodata.gateway.http.request;

import com.aliyun.jindodata.gateway.common.JfsRequestOptions;

public class JfsSetPermissionRequest extends JfsAbstractHttpRequest {
    private static final String REQUEST_TYPE = "setPermission";
    private static final String PATH_KEY = "path";
    private static final String PERMISSION_KEY = "permission";

    private String permission = "";

    public JfsSetPermissionRequest() {
        super();
        setQueryParam(NS_DFS, "");
    }

    @Override
    public void prepareRequest(JfsRequestOptions options) {
        initRequestWithOptions(options);
        initRequestXml(REQUEST_TYPE);

        requestXml.addRequestParameter(PATH_KEY, encodePath(path));
        requestXml.addRequestParameter(PERMISSION_KEY, permission);

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

    public void setPermission(short permission) {
        this.permission = String.valueOf(permission);
    }
}
