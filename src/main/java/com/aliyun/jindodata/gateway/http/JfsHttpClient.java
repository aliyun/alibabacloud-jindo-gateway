package com.aliyun.jindodata.gateway.http;

import com.aliyun.jindodata.gateway.common.JfsRequestOptions;
import com.aliyun.jindodata.gateway.common.JfsStatus;
import com.aliyun.jindodata.gateway.http.reponse.JfsAbstractHttpResponse;
import com.aliyun.jindodata.gateway.http.request.JfsAbstractHttpRequest;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class JfsHttpClient {
    private static final Logger LOG = LoggerFactory.getLogger(JfsHttpClient.class);

    private final OkHttpClient client;
    private final AtomicLong errorTimeStamp = new AtomicLong(0);
    private final ReadWriteLock credentialLock = new ReentrantReadWriteLock();

    private String accessKey = "";
    private String accessKeySecret = "";
    private String accessToken = "";

    private final JfsRequestOptions requestOptions;

    public JfsHttpClient(JfsRequestOptions requestOptions) {
        this.requestOptions = requestOptions;

        this.client = new OkHttpClient.Builder()
                .connectTimeout(5, TimeUnit.SECONDS)
                .readTimeout(30, TimeUnit.SECONDS)
                .writeTimeout(30, TimeUnit.SECONDS)
                .build();
    }

    public void init() {
        LOG.info("Initializing JfsHttpClient");

        if (requestOptions.getCredential() != null) {
            credentialLock.readLock().lock();
            try {
                this.accessKey = requestOptions.getCredential().getAccessKey();
                this.accessKeySecret = requestOptions.getCredential().getAccessKeySecret();
                this.accessToken = requestOptions.getCredential().getAccessToken();

            } finally {
                credentialLock.readLock().unlock();
            }
        }
    }

    public void sendRequest(JfsAbstractHttpRequest request, JfsAbstractHttpResponse response) {
        if (request.getEndpoint() == null || request.getEndpoint().isEmpty()) {
            LOG.error("Failed to get OSS endpoint, please check your configuration.");
            String errorMsg = "Failed to get OSS endpoint, please check your configuration.";
            response.setStatus(JfsStatus.invalidArgument(errorMsg));
            return;
        }

        credentialLock.readLock().lock();
        try {
            if (accessKey == null || accessKey.isEmpty() || accessKeySecret == null || accessKeySecret.isEmpty()) {
                LOG.error("Failed to get OSS access key from configuration.");
                String errorMsg = "Failed to get OSS access key from configuration.";
                response.setStatus(JfsStatus.invalidArgument(errorMsg));
                return;
            }

            request.setAuth(accessKey, accessKeySecret, accessToken);
        } finally {
            credentialLock.readLock().unlock();
        }

        request.internalPrepareRequest();
        LOG.debug("Send request to {} method {} body {}",
                request.getUrl(), 
                request.getMethod(), 
                request.getBody() != null ? request.getBody() : "");

        try {
            Request okRequest = buildOkHttpRequest(request);

            Response okResponse = client.newCall(okRequest).execute();

            handleResponse(okResponse, response);
            
        } catch (IOException e) {
            LOG.error("Failed to send HTTP request to {}", request.getUrl(), e);
            response.setStatus(JfsStatus.ioError(e.getMessage()));
        }
    }

    private Request buildOkHttpRequest(JfsAbstractHttpRequest request) {
        Request okHttpRequest = request.buildRequest();
        LOG.debug("Build OkHttp request: {}", okHttpRequest);
        return okHttpRequest;
    }

    private void handleResponse(Response okResponse, JfsAbstractHttpResponse response) throws IOException {
        response.setOkHttpResponse(okResponse);

        String body = "";
        if (okResponse.body() != null) {
            body = okResponse.body().string();
            LOG.debug("Response body: {}", body);
            response.setResponseBody(body);
        }

        if (!body.isEmpty()) {
            JfsStatus status = response.parseErrorXml(body);
            if (!status.isOk()) {
                response.setStatus(status);
            }
        }
    }

    private long getCurrentTime() {
        return System.currentTimeMillis() / 1000;
    }

    public boolean ignoreError(JfsStatus status) {
        String errorMsg = status.toString();

        String[] ignoreKeysRoleAK = {"404 Not Found", "NoSuchKey", "Bad Request", "<errCode>30008</errCode>", "ObjectDoesNotMatchPrefixLink"};
        String[] ignoreKeysNonRoleAK = {"404 Not Found", "NoSuchKey", "Bad Request", "<errCode>30008</errCode>", "ObjectDoesNotMatchPrefixLink",
                "SignatureDoesNotMatch", "InvalidAccessKeyId", "403 Forbidden"};

        String[] ignoreKeys = /*requestOptions.getCredential().isToken() ?*/ ignoreKeysNonRoleAK /*: ignoreKeysRoleAK*/;

        for (String key : ignoreKeys) {
            if (errorMsg.contains(key)) {
                return true;
            }
        }

        return false;
    }
}