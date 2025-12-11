package com.aliyun.jindodata.gateway.call;

import com.aliyun.jindodata.gateway.common.JfsRequestOptions;
import com.aliyun.jindodata.gateway.common.JfsStatus;
import com.aliyun.jindodata.gateway.http.JfsHttpClient;
import com.aliyun.jindodata.gateway.http.JfsHttpClientFactory;
import com.aliyun.jindodata.gateway.http.reponse.JfsAbstractHttpResponse;
import com.aliyun.jindodata.gateway.http.request.JfsAbstractHttpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class JfsBaseCall {
    private static final Logger LOG = LoggerFactory.getLogger(JfsBaseCall.class);

    private JfsHttpClient client;

    protected JfsAbstractHttpRequest request;
    protected JfsAbstractHttpResponse response;
    protected JfsRequestOptions requestOptions;

    protected void initRestCall(JfsRequestOptions requestOptions) {
        client = JfsHttpClientFactory.getInstance().getClient(requestOptions);
    }

    /**
     * transform dls to hdfs response
     */
    protected void processResponse() {}

    public JfsStatus execute(JfsRequestOptions requestOptions) {
        this.requestOptions = requestOptions;
        initRestCall(requestOptions);

        request.prepareRequest(requestOptions);

        client.sendRequest(request, response);
        if (!response.isOk()) {
            LOG.warn("[RequestId : {}] Failed to send request to bucket {}. {}",
                    response.getRequestId(), requestOptions.getBucket(), response.getStatus());
            return response.getStatus();
        }

        response.parseResponse();
        if (!response.isOk()) {
            LOG.warn("Failed to parse response. {}", response.getStatus());
            return response.getStatus();
        }
        processResponse();
        return JfsStatus.OK();
    }
}
