package com.aliyun.jindodata.gateway.http.request;

import com.aliyun.jindodata.gateway.common.JfsRequestOptions;

public class JfsSetOwnerRequest extends JfsAbstractHttpRequest {
    private static final String REQUEST_TYPE = "setOwner";
    private static final String PATH_KEY = "path";
    private static final String USER_KEY = "user";
    private static final String GROUP_KEY = "group";

    private String user = "";
    private String group = "";

    public JfsSetOwnerRequest() {
        super();
        setQueryParam(NS_DFS, "");
    }

    @Override
    public void prepareRequest(JfsRequestOptions options) {
        initRequestWithOptions(options);
        initRequestXml(REQUEST_TYPE);

        requestXml.addRequestParameter(PATH_KEY, encodePath(path));
        requestXml.addRequestParameter(USER_KEY, user);
        requestXml.addRequestParameter(GROUP_KEY, group);

        setBody(requestXml.getXmlString());
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }
}
