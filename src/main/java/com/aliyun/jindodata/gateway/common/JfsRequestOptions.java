package com.aliyun.jindodata.gateway.common;

import org.apache.hadoop.conf.Configuration;

import static com.aliyun.jindodata.gateway.common.JfsConstant.*;

public class JfsRequestOptions {
    private JfsCredential credential;
    private String serviceCode = "oss";
    private int signerVersion = 1;
    private String region = "";
    private String bucket = "";
    private String endpoint = "";
    private String dataEndpoint = "";
    private String ossUser = "defaultUser";
    private String clientAddress = "localhost";
    private String requestUser = ""; // user name
    private boolean secondLevelDomain = false;
    private boolean enableHttps = false;
    private String clientName = "";
    private String host;
    private boolean enableCrc64 = true; // if enable oss client check crc64
    private int uploadThreads = 5;
    private int maxPendingUploadCount = 10;
    private String tmpDataDirsStr = "/tmp/jindo-gateway-tmp-data";
    private String[] tmpDataDirs = null;
    private long flushMergeThreshold = 0;
    private String backendLocation;
    private String nsName;

    public JfsRequestOptions() {
    }

    public JfsRequestOptions(JfsCredential credential) {
        this.credential = credential;
    }

    public void initWithConfiguration(Configuration conf) {
        // oss access config
        String accessKeyId = conf.get("fs.oss.accessKeyId");
        String accessKeySecret = conf.get("fs.oss.accessKeySecret");
        String securityToken = conf.get("fs.oss.securityToken");
        this.credential = new JfsCredential(accessKeyId, accessKeySecret, securityToken);
        setServiceCode(conf.get("fs.oss.serviceCode", "oss"));
        setSignerVersion(conf.getInt("fs.oss.signerVersion", 1));
        setEndpoint(conf.get("fs.oss.endpoint", ""));
        setRegion(conf.get("fs.oss.region", JfsUtil.guessRegion(endpoint)));
        setBucket(conf.get("fs.oss.bucket", ""));
        setOssUser(conf.get("fs.oss.user", "defaultUser"));
        setRequestUser(JfsUtil.getCurrentUserShort());
        setClientAddress(conf.get("fs.oss.client.address", "localhost"));
        setEnableHttps(conf.getBoolean("fs.oss.https.enable", false));
        setSecondLevelDomain(conf.getBoolean("fs.oss.second.level.domain.enable", false));
        this.dataEndpoint = conf.get("fs.oss.data.endpoint", guessDataEndpoint());
        initHost();
        this.backendLocation = OSS_DEFAULT_BACKEND_LOCATION;
        this.nsName = conf.get("fs.oss.dfs.namespace", bucket);

        // oss io config
        this.enableCrc64 = conf.getBoolean("fs.oss.checksum.crc64.enable", true);
        this.uploadThreads = conf.getInt("fs.oss.upload.thread.concurrency", JfsUtil.getDefaultConcurrency());
        this.maxPendingUploadCount = conf.getInt("fs.oss.upload.max.pending.tasks.per.stream", 10);
        this.tmpDataDirsStr = conf.get("fs.oss.tmp.data.dirs", TMP_DATA_DIRS_DEFAULT);
        this.tmpDataDirs = tmpDataDirsStr.split(",");
        if (tmpDataDirs.length == 0) {
            throw new IllegalArgumentException("Please set fs.oss.tmp.data.dirs properly!");
        }
        this.flushMergeThreshold = conf.getLong("fs.oss.flush.merge.threshold", FLUSH_MERGE_SIZE_DEFAULT);
    }

    private String guessDataEndpoint() {
        if (!endpoint.contains(".oss-dls.aliyuncs.com")) {
            return endpoint;
        }
        return "oss-" + region + "-internal.aliyuncs.com";
    }

    private void initHost() {
        if (secondLevelDomain) {
            host = "127.0.0.2";
        } else {
            if (bucket == null || bucket.isEmpty()) {
                host = endpoint;
            } else {
                host = bucket + "." + endpoint;
            }
        }
    }

    public static String generateCacheKey(JfsRequestOptions requestOptions) {
        String endpoint = requestOptions.getEndpoint() != null ? requestOptions.getEndpoint() : "";
        String accessKey = "";

        if (requestOptions.getCredential() != null && requestOptions.getCredential().getAccessKey() != null) {
            accessKey = requestOptions.getCredential().getAccessKey();
        }

        String serviceCode = requestOptions.getServiceCode() != null ? requestOptions.getServiceCode() : "";
        int signerVersion = requestOptions.getSignerVersion();
        boolean enableHttps = requestOptions.isEnableHttps();
        boolean secondLevelDomain = requestOptions.isSecondLevelDomain();

        // construct key：endpoint|accessKey|serviceCode|signerVersion|enableHttps|secondLevelDomain
        return String.format("%s|%s|%s|%d|%b|%b",
                endpoint, accessKey, serviceCode, signerVersion, enableHttps, secondLevelDomain);
    }

    public String getClientName() {
        return clientName;
    }

    public void setClientName(String clientName) {
        this.clientName = clientName;
    }

    public String getServiceCode() {
        return serviceCode;
    }

    public void setServiceCode(String serviceCode) {
        this.serviceCode = serviceCode;
    }

    public int getSignerVersion() {
        return signerVersion;
    }

    public void setSignerVersion(int signerVersion) {
        this.signerVersion = signerVersion;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getOssUser() {
        return ossUser;
    }

    public void setOssUser(String ossUser) {
        this.ossUser = ossUser;
    }

    public String getClientAddress() {
        return clientAddress;
    }

    public void setClientAddress(String clientAddress) {
        this.clientAddress = clientAddress;
    }

    public String getRequestUser() {
        return requestUser;
    }

    public void setRequestUser(String requestUser) {
        this.requestUser = requestUser;
    }

    public boolean isSecondLevelDomain() {
        return secondLevelDomain;
    }

    public void setSecondLevelDomain(boolean secondLevelDomain) {
        this.secondLevelDomain = secondLevelDomain;
    }

    public boolean isEnableHttps() {
        return enableHttps;
    }

    public void setEnableHttps(boolean enableHttps) {
        this.enableHttps = enableHttps;
    }

    public JfsCredential getCredential() {
        return credential;
    }
    
    public void setCredential(JfsCredential credential) {
        this.credential = credential;
    }

    public String getHost() {
        return host;
    }

    public String getDataEndpoint() {
        return dataEndpoint;
    }

    public boolean isEnableCrc64() {
        return enableCrc64;
    }

    public int getUploadThreads() {
        return uploadThreads;
    }

    public int getMaxPendingUploadCount() {
        return maxPendingUploadCount;
    }

    public String[] getTmpDataDirs() {
        return tmpDataDirs;
    }

    public long getFlushMergeThreshold() {
        return flushMergeThreshold;
    }

    public String getBackendLocation() {
        return backendLocation;
    }

    public void setBackendLocation(String backendLocation) {
        this.backendLocation = backendLocation;
    }

    public String getNsName() {
        return nsName;
    }
}