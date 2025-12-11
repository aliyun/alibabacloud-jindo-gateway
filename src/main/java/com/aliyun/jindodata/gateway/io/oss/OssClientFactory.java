package com.aliyun.jindodata.gateway.io.oss;

import com.aliyun.jindodata.gateway.common.JfsRequestOptions;
import com.aliyun.jindodata.gateway.common.JfsVersionUtil;
import com.aliyun.oss.ClientBuilderConfiguration;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.common.auth.CredentialsProvider;
import com.aliyun.oss.common.auth.DefaultCredentialProvider;
import com.aliyun.oss.common.comm.Protocol;
import com.aliyun.oss.common.comm.SignVersion;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.aliyun.jindodata.gateway.common.JfsRequestOptions.generateCacheKey;

public class OssClientFactory {
    private static final Logger LOG = LoggerFactory.getLogger(OssClientFactory.class);

    private static volatile OssClientFactory instance;
    private static final Object INSTANCE_LOCK = new Object();

    private final ConcurrentHashMap<String, OSS> clientCache;
    private final ReadWriteLock cacheLock;

    static {
        // modify oss http request builder
        installInterceptorOnStartup();
    }

    private OssClientFactory() {
        this.clientCache = new ConcurrentHashMap<>();
        this.cacheLock = new ReentrantReadWriteLock();
    }

    public static OssClientFactory getInstance() {
        if (instance == null) {
            synchronized (INSTANCE_LOCK) {
                if (instance == null) {
                    instance = new OssClientFactory();
                    LOG.info("OssClientFactory initialized");
                }
            }
        }
        return instance;
    }

    public OSS getClient(JfsRequestOptions requestOptions) {
        if (requestOptions == null) {
            throw new IllegalArgumentException("requestOptions cannot be null");
        }

        String cacheKey = generateCacheKey(requestOptions);

        cacheLock.readLock().lock();
        try {
            OSS cachedClient = clientCache.get(cacheKey);
            if (cachedClient != null) {
                LOG.debug("Found cached OSS client for key: {}", cacheKey);
                return cachedClient;
            }
        } finally {
            cacheLock.readLock().unlock();
        }

        cacheLock.writeLock().lock();
        try {
            OSS cachedClient = clientCache.get(cacheKey);
            if (cachedClient != null) {
                LOG.debug("Found cached OSS client for key: {}", cacheKey);
                return cachedClient;
            }

            OSS newClient = createOssClient(requestOptions);
            
            clientCache.put(cacheKey, newClient);
            LOG.info("Created new OSS client for key: {}", cacheKey);
            
            return newClient;
        } finally {
            cacheLock.writeLock().unlock();
        }
    }

    private OSS createOssClient(JfsRequestOptions requestOptions) {
        try {
            String endpoint = requestOptions.getDataEndpoint();
            String region = requestOptions.getRegion();

            ClientBuilderConfiguration config = new ClientBuilderConfiguration();

            if (requestOptions.getSignerVersion() == 0 ||
                    requestOptions.getSignerVersion() == 4) {
                config.setSignatureVersion(SignVersion.V4);
                LOG.debug("Using V4 signature");
            } else {
                config.setSignatureVersion(SignVersion.V1);
                LOG.debug("Using V1 signature");
            }

            config.setConnectionTimeout(5000);
            config.setSocketTimeout(30000);
            config.setIdleConnectionTime(30000);
            config.setMaxErrorRetry(5);
            if (requestOptions.isEnableHttps()) {
                config.setProtocol(Protocol.HTTPS);
            } else {
                config.setProtocol(Protocol.HTTP);
            }
            config.addDefaultHeader("user-agent", "EMR Jindo-Gateway" + JfsVersionUtil.VERSION + " (oss-java-sdk)");
            config.setCrcCheckEnabled(requestOptions.isEnableCrc64());

            CredentialsProvider credentialsProvider = getCredentialsProvider(requestOptions);

            OSSClientBuilder.OSSClientBuilderImpl builder = OSSClientBuilder.create()
                    .endpoint(endpoint)
                    .clientConfiguration(config)
                    .credentialsProvider(credentialsProvider);
            if (config.getSignatureVersion() == SignVersion.V4) {
                builder.region(region);
            }

            OSS ossClient = builder.build();
            LOG.info("OSS client created successfully for endpoint: {}", endpoint);
            
            return ossClient;
        } catch (Exception e) {
            LOG.error("Failed to create OSS client", e);
            throw new RuntimeException("Failed to create OSS client", e);
        }
    }

    private static @Nullable CredentialsProvider getCredentialsProvider(JfsRequestOptions requestOptions) {
        CredentialsProvider credentialsProvider = null;
        if (requestOptions.getCredential() != null) {
            String accessKeyId = requestOptions.getCredential().getAccessKey();
            String accessKeySecret = requestOptions.getCredential().getAccessKeySecret();
            String securityToken = requestOptions.getCredential().getAccessToken();

            if (securityToken != null && !securityToken.isEmpty()) {
                credentialsProvider = new DefaultCredentialProvider(accessKeyId, accessKeySecret, securityToken);
            } else {
                credentialsProvider = new DefaultCredentialProvider(accessKeyId, accessKeySecret);
            }
        }
        return credentialsProvider;
    }

    public void removeClient(JfsRequestOptions requestOptions) {
        if (requestOptions == null) {
            return;
        }

        String cacheKey = generateCacheKey(requestOptions);
        
        cacheLock.writeLock().lock();
        try {
            OSS removed = clientCache.remove(cacheKey);
            if (removed != null) {
                try {
                    removed.shutdown();
                    LOG.info("Removed and shutdown cached OSS client for key: {}", cacheKey);
                } catch (Exception e) {
                    LOG.warn("Error shutting down OSS client for key: {}", cacheKey, e);
                }
            }
        } finally {
            cacheLock.writeLock().unlock();
        }
    }

    public void clearAll() {
        cacheLock.writeLock().lock();
        try {
            int size = clientCache.size();

            clientCache.values().forEach(client -> {
                try {
                    client.shutdown();
                } catch (Exception e) {
                    LOG.warn("Error shutting down OSS client", e);
                }
            });
            
            clientCache.clear();
            LOG.info("Cleared {} cached OSS client instances", size);
        } finally {
            cacheLock.writeLock().unlock();
        }
    }

    public int getCacheSize() {
        cacheLock.readLock().lock();
        try {
            return clientCache.size();
        } finally {
            cacheLock.readLock().unlock();
        }
    }

    public void printCacheStats() {
        cacheLock.readLock().lock();
        try {
            LOG.info("OSS client cache statistics: size={}", clientCache.size());
            clientCache.forEach((key, client) -> {
                LOG.info("  - {}", key);
            });
        } finally {
            cacheLock.readLock().unlock();
        }
    }

    public static void installInterceptorOnStartup() {
        LOG.info("Installing OSS interceptor to allow custom Host header...");

        boolean success = OssHttpRequestInterceptor.install();

        if (success) {
            LOG.info("OSS interceptor installed successfully!");
            LOG.info("Host header filter has been removed from configureRequestHeaders method");
        } else {
            LOG.error("Failed to install OSS interceptor");
        }
    }
}
