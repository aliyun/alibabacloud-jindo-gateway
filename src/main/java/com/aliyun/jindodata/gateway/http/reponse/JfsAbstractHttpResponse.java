package com.aliyun.jindodata.gateway.http.reponse;

import com.aliyun.jindodata.gateway.common.JfsResponseXml;
import com.aliyun.jindodata.gateway.common.JfsStatus;
import com.aliyun.jindodata.gateway.common.JfsUtil;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

public abstract class JfsAbstractHttpResponse {
    private static final Logger LOG = LoggerFactory.getLogger(JfsAbstractHttpResponse.class);

    protected Response okHttpResponse;

    protected String responseBody;

    protected JfsStatus status;

    protected JfsResponseXml responseXml;
    
    public JfsAbstractHttpResponse() {
        responseXml = new JfsResponseXml();
    }

    public void setOkHttpResponse(Response response) {
        this.okHttpResponse = response;
        // Initialize status
        this.status = JfsStatus.OK();
    }

    public int getStatusCode() {
        return okHttpResponse != null ? okHttpResponse.code() : 0;
    }

    public String getStatusMessage() {
        return okHttpResponse != null ? okHttpResponse.message() : "";
    }
    
    public String getResponseBody() {
        return responseBody;
    }
    
    public void setResponseBody(String responseBody) {
        this.responseBody = responseBody;
    }
    
    public JfsStatus getStatus() {
        return status;
    }
    
    public void setStatus(JfsStatus status) {
        this.status = status;
    }

    public String getHeader(String key) {
        return okHttpResponse != null ? okHttpResponse.header(key) : null;
    }

    public String getRequestId() {
        return getHeader("x-oss-request-id");
    }

    public String getServerTime() {
        return getHeader("x-oss-server-time");
    }

    public boolean isOk() {
        return (status != null && status.isOk());
    }

    public JfsStatus parseErrorXml(String xml) {
        if (xml == null || xml.isEmpty()) {
            return JfsStatus.OK();
        }

        try {
            JfsStatus status = responseXml.parseResponse(xml);
            if (!status.isOk()) {
                return status;
            }

            Element response = responseXml.getResponseNode();

            int errCode = 0;
            String errCodeStr = JfsResponseXml.getNodeString(response, "errCode", null, false);
            if (errCodeStr != null && !errCodeStr.isEmpty()) {
                try {
                    errCode = Integer.parseInt(errCodeStr);
                } catch (NumberFormatException e) {
                    LOG.warn("Failed to parse errCode: {}", errCodeStr);
                }
            }

            String errMsg = JfsResponseXml.getNodeString(response, "errMsg", null, false);

            String message = "";
            if (errMsg != null && !errMsg.isEmpty()) {
                try {
                    message = JfsUtil.decode(errMsg);
                } catch (IllegalArgumentException e) {
                    LOG.warn("Failed to decode error message: {}", errMsg);
                    message = errMsg;
                }

                String requestId = getRequestId();
                if (requestId != null && !requestId.isEmpty()) {
                    message += " [RequestId]: " + requestId + " ";
                }
            }

            if (errCode != 0 && !message.isEmpty()) {
                return new JfsStatus(errCode, message, false);
            }

        } catch (Exception e) {
            LOG.warn("Failed to parse error XML response", e);
            return JfsStatus.ioError("Failed to parse error XML response: " + e.getMessage());
        }

        return JfsStatus.OK();
    }


    public void parseResponse() {
        status = parseXml(responseBody);
    }

    abstract public JfsStatus parseXml(String xml);
}