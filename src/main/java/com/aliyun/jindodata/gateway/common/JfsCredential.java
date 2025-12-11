package com.aliyun.jindodata.gateway.common;

public class JfsCredential {
    public JfsCredential(String accessKey, String accessKeySecret, String accessToken) {
        this.accessKey = accessKey;
        this.accessKeySecret = accessKeySecret;
        this.accessToken = accessToken;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getAccessKeySecret() {
        return accessKeySecret;
    }

    public void setAccessKeySecret(String accessKeySecret) {
        this.accessKeySecret = accessKeySecret;
    }

    public String getAccessToken() {
        return accessToken;
    }

    public void setAccessToken(String accessToken) {
        this.accessToken = accessToken;
    }

    public String accessKey;
    public String accessKeySecret;
    public String accessToken;
}
