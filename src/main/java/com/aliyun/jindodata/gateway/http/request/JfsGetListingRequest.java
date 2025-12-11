package com.aliyun.jindodata.gateway.http.request;

import com.aliyun.jindodata.gateway.common.JfsRequestOptions;

public class JfsGetListingRequest extends JfsAbstractHttpRequest {
    private static final String REQUEST_TYPE = "getListing";
    private static final String PATH_KEY = "path";
    private static final String MAX_KEYS_KEY = "maxkeys";
    private static final String MARKER_KEY = "marker";
    private static final String NEED_LOCATION_KEY = "needLocation";

    private int maxKeys = -1;
    private String marker = "";
    private boolean needLocation = false;

    public JfsGetListingRequest() {
        super();
        setQueryParam(NS_DFS, "");
    }

    @Override
    public void prepareRequest(JfsRequestOptions options) {
        initRequestWithOptions(options);
        initRequestXml(REQUEST_TYPE);

        requestXml.addRequestParameter(PATH_KEY, encodePath(path));
        requestXml.addRequestParameter(MAX_KEYS_KEY, maxKeys);
        requestXml.addRequestParameter(MARKER_KEY, encodePath(marker));
        requestXml.addRequestParameter(NEED_LOCATION_KEY, needLocation);

        setBody(requestXml.getXmlString());
    }

    public void setMaxKeys(int maxKeys) {
        this.maxKeys = maxKeys;
    }

    public void setMarker(String marker) {
        this.marker = marker;
    }

    public void setNeedLocation(boolean needLocation) {
        this.needLocation = needLocation;
    }
}
