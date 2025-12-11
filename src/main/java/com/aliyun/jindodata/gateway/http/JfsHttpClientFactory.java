package com.aliyun.jindodata.gateway.http;

import com.aliyun.jindodata.gateway.common.JfsRequestOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.aliyun.jindodata.gateway.common.JfsRequestOptions.generateCacheKey;

public class JfsHttpClientFactory {
    private static final Logger LOG = LoggerFactory.getLogger(JfsHttpClientFactory.class);

    private static volatile JfsHttpClientFactory instance;
    private static final Object INSTANCE_LOCK = new Object();

    private final ConcurrentHashMap<String, JfsHttpClient> clientCache;
    private final ReadWriteLock cacheLock;

    private JfsHttpClientFactory() {
        this.clientCache = new ConcurrentHashMap<>();
        this.cacheLock = new ReentrantReadWriteLock();
    }

    public static JfsHttpClientFactory getInstance() {
        if (instance == null) {
            synchronized (INSTANCE_LOCK) {
                if (instance == null) {
                    instance = new JfsHttpClientFactory();
                    LOG.info("JfsHttpClientFactory initialized");
                }
            }
        }
        return instance;
    }

    public JfsHttpClient getClient(JfsRequestOptions requestOptions) {
        if (requestOptions == null) {
            throw new IllegalArgumentException("requestOptions cannot be null");
        }

        String cacheKey = generateCacheKey(requestOptions);

        cacheLock.readLock().lock();
        try {
            JfsHttpClient cachedClient = clientCache.get(cacheKey);
            if (cachedClient != null) {
                LOG.debug("Found cached JfsHttpClient for key: {}", cacheKey);
                return cachedClient;
            }
        } finally {
            cacheLock.readLock().unlock();
        }

        cacheLock.writeLock().lock();
        try {
            JfsHttpClient cachedClient = clientCache.get(cacheKey);
            if (cachedClient != null) {
                LOG.debug("Found cached JfsHttpClient for key: {}", cacheKey);
                return cachedClient;
            }
            
            JfsHttpClient newClient = new JfsHttpClient(requestOptions);
            newClient.init();
            
            clientCache.put(cacheKey, newClient);
            LOG.info("Created new JfsHttpClient for key: {}", cacheKey);
            
            return newClient;
        } finally {
            cacheLock.writeLock().unlock();
        }
    }

    public void removeClient(JfsRequestOptions requestOptions) {
        if (requestOptions == null) {
            return;
        }

        String cacheKey = generateCacheKey(requestOptions);
        
        cacheLock.writeLock().lock();
        try {
            JfsHttpClient removed = clientCache.remove(cacheKey);
            if (removed != null) {
                LOG.info("Removed cached JfsHttpClient for key: {}", cacheKey);
            }
        } finally {
            cacheLock.writeLock().unlock();
        }
    }

    public void clearAll() {
        cacheLock.writeLock().lock();
        try {
            int size = clientCache.size();
            clientCache.clear();
            LOG.info("Cleared {} cached JfsHttpClient instances", size);
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
            LOG.info("JfsHttpClient cache statistics: size={}", clientCache.size());
            clientCache.forEach((key, client) -> {
                LOG.info("  - {}", key);
            });
        } finally {
            cacheLock.readLock().unlock();
        }
    }
}
