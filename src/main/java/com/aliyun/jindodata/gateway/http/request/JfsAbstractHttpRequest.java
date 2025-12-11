package com.aliyun.jindodata.gateway.http.request;

import com.aliyun.jindodata.gateway.common.JfsRequestOptions;
import com.aliyun.jindodata.gateway.common.JfsRequestXml;
import com.aliyun.jindodata.gateway.common.JfsUtil;
import com.aliyun.jindodata.gateway.common.JfsVersionUtil;
import com.aliyun.jindodata.gateway.hdfs.namenode.JindoNameNode;
import okhttp3.Request;
import okhttp3.RequestBody;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

public abstract class JfsAbstractHttpRequest {
    private static final Logger LOG = LoggerFactory.getLogger(JfsAbstractHttpRequest.class);

    protected Request.Builder requestBuilder;

    protected byte[] bodyBytes;

    // HTTP
    protected String endpoint;
    protected String url;
    protected String rootUrl;
    protected HttpMethod method = HttpMethod.POST;
    protected Map<String, String> headers = new HashMap<>();  // For signature calculation and quick access
    protected Map<String, String> queryParams = new HashMap<>();
    
    // OSS
    protected String resource;
    protected String bucket;
    protected String object;
    protected String path;
    protected String host;
    protected boolean useSecondLevelDomain = false;
    protected boolean enableHttps = true;

    protected String accessKey;
    protected String accessKeySecret;
    protected String accessToken;
    protected int signerVersion = 0; // 0, 4 = v4
    protected String region = "cn-shanghai";
    protected String service = "oss";

    protected String clientName;
    protected String requestUser;
    protected Map<String, String> userRequestHeaders;
    protected JfsRequestXml requestXml;

    public static final String OSS_DATE = "Date";
    public static final String OSS_DATE_V4 = "x-oss-date";
    public static final String OSS_CONTENT_MD5 = "Content-MD5";
    public static final String OSS_CONTENT_TYPE = "Content-Type";
    public static final String OSS_AUTHORIZATION = "Authorization";
    public static final String OSS_STS_SECURITY_TOKEN = "x-oss-security-token";
    public static final String OSS_CONTENT_SHA256 = "x-oss-content-sha256";
    public static final String DFS_REQUESTER = "x-oss-dfs-requester";
    public static final String DFS_SOURCE_ADDR = "x-oss-dfs-source-addr";
    public static final String REQUEST_NS_HEADER_KEY = "x-oss-dfs-ns";
    public static final String S3_UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD";

    public static final String NS_DFS = "dfs";

    private static final Set<String> V4_DEFAULT_SIGNED_HEADERS = new HashSet<>(Arrays.asList(
            "content-type", "content-md5"
    ));
    
    private static final Set<String> OSS_SUB_RESOURCES = new HashSet<>(Arrays.asList(
            "acl", "location", "bucketInfo", "stat", "referer", "cors", "website",
            "restore", "logging", "symlink", "qos", "uploadId", "uploads", "partNumber",
            "response-content-type", "response-content-language", "response-expires",
            "response-cache-control", "response-content-disposition", "response-content-encoding",
            "append", "position", "lifecycle", "delete", "copy", "live", "status", "comp",
            "vod", "startTime", "endTime", "x-oss-process", "security-token", "objectMeta",
            "versions", "versionId", "dfs", "dfssecurity", "dfsadmin"
    ));
    
    public enum HttpMethod {
        GET, POST, PUT, DELETE, HEAD
    }
    
    public JfsAbstractHttpRequest() {
        this.requestBuilder = new Request.Builder();

        requestXml = new JfsRequestXml();
    }

    protected void initRequestXml(String requestType) {
        requestXml.initRequest(requestType, requestUser, bucket, null, null);
    }

    protected void initRequestWithOptions(JfsRequestOptions options) {
        setService(options.getServiceCode());
        setSignerVersion(options.getSignerVersion());
        setRegion(options.getRegion());
        setBucket(options.getBucket());
        setHeader(DFS_REQUESTER, options.getOssUser());
        setHeader(DFS_SOURCE_ADDR, options.getClientAddress());
        setHeader(REQUEST_NS_HEADER_KEY, options.getNsName());
        try {
            UserGroupInformation tmpUGI = JindoNameNode.getRemoteUser();
            setRequestUser(tmpUGI.getShortUserName());
        } catch (IOException e) {
            LOG.warn("Failed to get remote user, set user: {}", options.getRequestUser());
            setRequestUser(options.getRequestUser());
        }
        if (clientName == null || clientName.isEmpty()) {
            setClientName(options.getClientName());
        }
        setEndpoint(options.getEndpoint());
        setUseSecondLevelDomain(options.isSecondLevelDomain());
        setEnableHttps(options.isEnableHttps());
    }
    
    /**
     * Build OkHttp Request object
     * @return OkHttp Request object
     */
    public Request buildRequest() {
        return requestBuilder.build();
    }
    
    // Getters and Setters
    public String getEndpoint() {
        return endpoint;
    }
    
    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }
    
    public String getUrl() {
        return url;
    }

    public String getRootUrl() {
        return rootUrl;
    }
    
    public void setRootUrl(String rootUrl) {
        this.rootUrl = rootUrl;
    }
    
    /**
     * Get request body (byte array)
     * @return request body
     */
    public byte[] getBodyBytes() {
        return bodyBytes;
    }
    
    /**
     * Get request body (string)
     * @return request body
     */
    public String getBody() {
        return bodyBytes != null ? new String(bodyBytes, StandardCharsets.UTF_8) : null;
    }
    
    /**
     * Set request body
     * @param body request body string
     */
    public void setBody(String body) {
        if (body != null) {
            this.bodyBytes = body.getBytes(StandardCharsets.UTF_8);
        } else {
            this.bodyBytes = null;
        }
    }
    
    public HttpMethod getMethod() {
        return method;
    }
    
    public void setMethod(HttpMethod method) {
        this.method = method;
    }
    
    /**
     * Get all request headers
     * @return request headers Map
     */
    public Map<String, String> getHeaders() {
        return headers;
    }
    
    /**
     * Set request header (update internal Map and Request.Builder simultaneously)
     * @param key header name
     * @param value header value
     */
    public void setHeader(String key, String value) {
        headers.put(key, value);
        requestBuilder.header(key, value);
    }
    
    /**
     * Get request header
     * @param key header name
     * @return header value
     */
    public String getHeader(String key) {
        return headers.get(key);
    }
    
    public void setQueryParam(String key, String value) {
        this.queryParams.put(key, value);
    }
    
    public String getQueryParam(String key) {
        return this.queryParams.get(key);
    }
    
    public Map<String, String> getQueryParams() {
        return queryParams;
    }
    
    public String getResource() {
        return resource;
    }
    
    public void setResource(String resource) {
        this.resource = resource;
    }
    
    public String getBucket() {
        return bucket;
    }
    
    public void setBucket(String bucket) {
        this.bucket = bucket;
    }
    
    public String getObject() {
        return object;
    }
    
    public void setObject(String object) {
        this.object = object;
    }
    
    public String getPath() {
        return path;
    }
    
    public void setPath(String path) {
        this.path = path;
    }
    
    public String getHost() {
        return host;
    }
    
    public void setHost(String host) {
        this.host = host;
    }
    
    public boolean isUseSecondLevelDomain() {
        return useSecondLevelDomain;
    }
    
    public void setUseSecondLevelDomain(boolean useSecondLevelDomain) {
        this.useSecondLevelDomain = useSecondLevelDomain;
    }
    
    public boolean isEnableHttps() {
        return enableHttps;
    }
    
    public void setEnableHttps(boolean enableHttps) {
        this.enableHttps = enableHttps;
    }
    
    public void setAuth(String accessKey, String accessKeySecret, String accessToken) {
        this.accessKey = accessKey;
        this.accessKeySecret = accessKeySecret;
        this.accessToken = accessToken;
    }
    
    public String getClientName() {
        return clientName;
    }
    
    public void setClientName(String clientName) {
        this.clientName = clientName;
    }
    
    public String getRequestUser() {
        return requestUser;
    }
    
    public void setRequestUser(String requestUser) {
        this.requestUser = requestUser;
    }
    
    public void setSignerVersion(int signerVersion) {
        this.signerVersion = signerVersion;
    }
    
    public void setRegion(String region) {
        this.region = region;
    }
    
    public void setService(String service) {
        this.service = service;
    }
    
    public void setUserRequestHeaders(Map<String, String> userRequestHeaders) {
        this.userRequestHeaders = userRequestHeaders;
    }

    protected String encodePath(String path) {
        return JfsUtil.urlEncode(path, true);
    }

    protected boolean isOssSubResource(String subResource) {
        return OSS_SUB_RESOURCES.contains(subResource);
    }

    protected String queryParamsToString(boolean isGenerateResource, char sep) {
        StringBuilder sb = new StringBuilder();

        if (isGenerateResource && queryParams.containsKey("append") && queryParams.containsKey("position")) {
            sb.append(sep);
            sb.append("append&position=").append(queryParams.get("position"));
            return sb.toString();
        }
        
        boolean first = true;
        for (Map.Entry<String, String> entry : queryParams.entrySet()) {
            if (isGenerateResource && !isOssSubResource(entry.getKey())) {
                continue;
            } else if (!isGenerateResource && isOssSubResource(entry.getKey())) {
                continue;
            }
            
            if (!first) {
                sb.append('&');
            } else {
                sb.append(sep);
                first = false;
            }
            
            sb.append(JfsUtil.urlEncode(entry.getKey()));
            if (entry.getValue() != null && !entry.getValue().isEmpty()) {
                sb.append('=').append(JfsUtil.urlEncode(entry.getValue()));
            }
        }
        
        return sb.toString();
    }

    protected String getCanonicalHeaderStr() {
        StringBuilder sb = new StringBuilder();

        Map<String, String> canonicalHeaders = new TreeMap<>();
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            if (entry.getKey().toLowerCase().startsWith("x-oss-")) {
                canonicalHeaders.put(entry.getKey().toLowerCase(), entry.getValue());
            }
        }

        for (Map.Entry<String, String> entry : canonicalHeaders.entrySet()) {
            sb.append(entry.getKey()).append(':').append(entry.getValue()).append('\n');
        }
        
        return sb.toString();
    }

    protected void setAuthorizeHeader(String queryStr) {
        StringBuilder signbuf = new StringBuilder();

        String dateStr = getGmtTime();
        setHeader(OSS_DATE, dateStr);

        signbuf.append(httpMethodToString(method)).append('\n');
        signbuf.append(headers.getOrDefault(OSS_CONTENT_MD5, "")).append('\n');
        signbuf.append(headers.getOrDefault(OSS_CONTENT_TYPE, "")).append('\n');
        signbuf.append(headers.getOrDefault(OSS_DATE, "")).append('\n');

        String canonicalHeader = getCanonicalHeaderStr();
        if (!canonicalHeader.isEmpty()) {
            signbuf.append(canonicalHeader);
        }

        if (resource == null || resource.isEmpty()) {
            resource = "/";
            if (bucket != null && !bucket.isEmpty()) {
                resource += bucket + "/";
            }
            if (object != null && !object.isEmpty()) {
                resource += object;
            }
        }
        signbuf.append(resource);
        
        if (queryStr != null && !queryStr.isEmpty()) {
            signbuf.append(queryStr);
        }

        String hmac = hmacSha1(signbuf.toString(), accessKeySecret);

        String authorization = "OSS " + accessKey + ":" + hmac;
        setHeader(OSS_AUTHORIZATION, authorization);
    }

    protected String hmacSha1(String data, String key) {
        try {
            Mac mac = Mac.getInstance("HmacSHA1");
            SecretKeySpec secretKey = new SecretKeySpec(key.getBytes(StandardCharsets.UTF_8), "HmacSHA1");
            mac.init(secretKey);
            byte[] bytes = mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(bytes);
        } catch (Exception e) {
            throw new RuntimeException("Failed to calculate HMAC-SHA1", e);
        }
    }

    protected byte[] hmacSha256(String data, byte[] key) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            SecretKeySpec secretKey = new SecretKeySpec(key, "HmacSHA256");
            mac.init(secretKey);
            return mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            throw new RuntimeException("Failed to calculate HMAC-SHA256", e);
        }
    }

    protected String sha256Hex(String data) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(data.getBytes(StandardCharsets.UTF_8));
            return bytesToHex(hash);
        } catch (Exception e) {
            throw new RuntimeException("Failed to calculate SHA-256", e);
        }
    }

    protected String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    protected String getGmtTime() {
        SimpleDateFormat sdf = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z", Locale.US);
        sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
        return sdf.format(new Date());
    }

    protected String getISO8601Time(Date date) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd'T'HHmmss'Z'");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        return sdf.format(date);
    }

    protected String getYYYYMMDDTime(Date date) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        return sdf.format(date);
    }

    protected String httpMethodToString(HttpMethod method) {
        switch (method) {
            case GET: return "GET";
            case POST: return "POST";
            case PUT: return "PUT";
            case DELETE: return "DELETE";
            case HEAD: return "HEAD";
            default: return "GET";
        }
    }

    public void internalPrepareRequest() {
        if (accessToken != null && !accessToken.isEmpty()) {
            setHeader(OSS_STS_SECURITY_TOKEN, accessToken);
        }

        if (getHeader(OSS_CONTENT_TYPE) == null) {
            setHeader(OSS_CONTENT_TYPE, "text/plain");
        }

        if (userRequestHeaders != null) {
            for (Map.Entry<String, String> entry : userRequestHeaders.entrySet()) {
                setHeader(entry.getKey(), entry.getValue());
            }
        }

        String resourceStr = queryParamsToString(true, '?');
        String queryParaStr;
        if (resourceStr.isEmpty()) {
            queryParaStr = queryParamsToString(false, '?');
        } else {
            queryParaStr = queryParamsToString(false, '&');
        }

        if (signerVersion == 0 || signerVersion == 4) {
            // V4
            Date now = new Date();
            String iso8601Time = getISO8601Time(now);
            setHeader(OSS_DATE_V4, iso8601Time);
            buildHashedPayloadOSSV4();
            String queryStr = queryParamsToStringV4();
            setAuthorizeHeaderOSSV4(queryStr, now, iso8601Time);
        } else {
            // V1
            setAuthorizeHeader(resourceStr);
        }

        if (url == null || url.isEmpty()) {
            String encodedObject = JfsUtil.urlEncode(object != null ? object : "");
            String scheme = enableHttps ? "https://" : "http://";
            
            if (useSecondLevelDomain) {
                rootUrl = scheme + endpoint;
                host = "127.0.0.2";
                if (bucket == null || bucket.isEmpty()) {
                    url = scheme + endpoint + "/" + encodedObject + resourceStr + queryParaStr;
                } else {
                    url = scheme + endpoint + "/" + bucket + "/" + encodedObject + resourceStr + queryParaStr;
                }
            } else {
                if (bucket == null || bucket.isEmpty()) {
                    host = endpoint;
                } else {
                    host = bucket + "." + endpoint;
                }
                rootUrl = scheme + host;
                url = rootUrl + "/" + encodedObject + resourceStr + queryParaStr;
            }
            requestBuilder.url(url);
        } else {
            throw new RuntimeException("URL should be empty before prepare.");
        }

        requestBuilder.header("Host", host);
        requestBuilder.header("user-agent", "EMR Jindo-Gateway/" + JfsVersionUtil.VERSION + " (okhttp3)");

        switch (method) {
            case GET:
                requestBuilder.get();
                break;
            case POST:
                if (bodyBytes != null) {
                    requestBuilder.post(RequestBody.create(bodyBytes, null));
                } else {
                    requestBuilder.post(RequestBody.create(new byte[0]));
                }
                break;
            case PUT:
                if (bodyBytes != null) {
                    requestBuilder.put(RequestBody.create(bodyBytes, null));
                } else {
                    requestBuilder.put(RequestBody.create(new byte[0]));
                }
                break;
            case DELETE:
                requestBuilder.delete();
                break;
            default:
                requestBuilder.get();
        }
    }

    protected String buildResourcePath() {
        StringBuilder sb = new StringBuilder();
        sb.append('/');
        if (bucket != null && !bucket.isEmpty()) {
            sb.append(bucket).append('/');
        }
        if (object != null && !object.isEmpty()) {
            sb.append(object);
        }
        return sb.toString();
    }

    protected void buildHashedPayloadOSSV4() {
        setHeader(OSS_CONTENT_SHA256, S3_UNSIGNED_PAYLOAD);
    }

    protected String queryParamsToStringV4() {
        if (queryParams.isEmpty()) {
            return "";
        }
        
        Map<String, String> sortedParams = new TreeMap<>();
        for (Map.Entry<String, String> entry : queryParams.entrySet()) {
            String key = JfsUtil.urlEncode(entry.getKey(), false);
            String val = entry.getValue() != null ? JfsUtil.urlEncode(entry.getValue(), true) : "";
            sortedParams.put(key, val);
        }
        
        return sortedParams.entrySet().stream()
                .map(e -> e.getValue().isEmpty() ? e.getKey() : e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining("&"));
    }

    protected String getCanonicalRequestOSSV4(String queryStr) {
        StringBuilder sb = new StringBuilder();

        sb.append(httpMethodToString(method)).append('\n');

        sb.append(buildResourcePath()).append('\n');

        sb.append(queryStr).append('\n');

        sb.append(getCanonicalHeaderOSSV4()).append('\n');
        sb.append('\n');

        sb.append(S3_UNSIGNED_PAYLOAD);
        
        return sb.toString();
    }

    protected String getCanonicalHeaderOSSV4() {
        Map<String, List<String>> canonicalHeaders = new TreeMap<>();
        
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            String headerName = entry.getKey().toLowerCase();
            if (V4_DEFAULT_SIGNED_HEADERS.contains(headerName) || headerName.startsWith("x-oss-")) {
                canonicalHeaders.computeIfAbsent(headerName, k -> new ArrayList<>()).add(entry.getValue().trim());
            }
        }
        
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, List<String>> entry : canonicalHeaders.entrySet()) {
            Collections.sort(entry.getValue());
            sb.append(entry.getKey()).append(':');
            sb.append(String.join(",", entry.getValue()));
            sb.append('\n');
        }
        
        return sb.toString();
    }

    protected String getStringToSignOSSV4(Date date, String canonicalRequest, String iso8601Time) {
        String scope = getScopeOSSV4(date);
        String hexRequest = sha256Hex(canonicalRequest);
        
        return "OSS4-HMAC-SHA256\n" +
                iso8601Time + "\n" +
                scope + "\n" +
                hexRequest;
    }

    protected String getScopeOSSV4(Date date) {
        String shortTime = getYYYYMMDDTime(date);
        String regionStr = region != null ? region : "cn-shanghai";
        return shortTime + "/" + regionStr + "/" + service + "/aliyun_v4_request";
    }

    protected byte[] getSigningKeyOSSV4(Date date) {
        String shortTime = getYYYYMMDDTime(date);
        String regionStr = region != null ? region : "cn-shanghai";
        
        byte[] dateKey = hmacSha256(shortTime, ("aliyun_v4" + accessKeySecret).getBytes(StandardCharsets.UTF_8));
        byte[] regionKey = hmacSha256(regionStr, dateKey);
        byte[] serviceKey = hmacSha256(service, regionKey);
        return hmacSha256("aliyun_v4_request", serviceKey);
    }

    protected String getCredentialOSSV4(Date date) {
        return accessKey + "/" + getScopeOSSV4(date);
    }

    protected String getSignatureOSSV4(byte[] signingKey, String stringToSign) {
        byte[] signature = hmacSha256(stringToSign, signingKey);
        return bytesToHex(signature);
    }

    protected void setAuthorizeHeaderOSSV4(String queryStr, Date date, String iso8601Time) {
        String canonicalRequest = getCanonicalRequestOSSV4(queryStr);
        LOG.debug("canonicalRequest: {}", canonicalRequest);

        String stringToSign = getStringToSignOSSV4(date, canonicalRequest, iso8601Time);
        LOG.debug("stringToSign: {}", stringToSign);

        byte[] signingKey = getSigningKeyOSSV4(date);

        String credential = getCredentialOSSV4(date);

        String signature = getSignatureOSSV4(signingKey, stringToSign);

        String authorization = "OSS4-HMAC-SHA256 " +
                "Credential=" + credential + "," +
                "Signature=" + signature;
        setHeader(OSS_AUTHORIZATION, authorization);
    }

    abstract public void prepareRequest(JfsRequestOptions options);
}