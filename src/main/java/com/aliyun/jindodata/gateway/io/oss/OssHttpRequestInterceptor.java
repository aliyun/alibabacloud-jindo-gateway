package com.aliyun.jindodata.gateway.io.oss;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.agent.ByteBuddyAgent;
import net.bytebuddy.dynamic.loading.ClassReloadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.AllArguments;
import net.bytebuddy.implementation.bind.annotation.Argument;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.matcher.ElementMatchers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Map;

public class OssHttpRequestInterceptor {
    private static final Logger LOG = LoggerFactory.getLogger(OssHttpRequestInterceptor.class);
    private static final String TARGET_CLASS = "com.aliyun.oss.common.comm.HttpRequestFactory";
    private static final String TARGET_METHOD = "configureRequestHeaders";
    
    private static volatile boolean installed = false;

    public static synchronized boolean install() {
        if (installed) {
            LOG.warn("OssHttpRequestInterceptor already installed");
            return true;
        }

        try {
            ByteBuddyAgent.install();

            Class<?> targetClass = Class.forName(TARGET_CLASS);

            new ByteBuddy()
                .redefine(targetClass)
                .method(ElementMatchers.named(TARGET_METHOD))
                .intercept(MethodDelegation.to(RequestHeadersInterceptor.class))
                .make()
                .load(targetClass.getClassLoader(), ClassReloadingStrategy.fromInstalledAgent());
            
            installed = true;
            LOG.info("Successfully installed OssHttpRequestInterceptor for {}.{} (Host header filter removed)", TARGET_CLASS, TARGET_METHOD);
            return true;
            
        } catch (ClassNotFoundException e) {
            LOG.error("Target class {} not found", TARGET_CLASS, e);
            return false;
        } catch (Exception e) {
            LOG.error("Failed to install OssHttpRequestInterceptor", e);
            return false;
        }
    }

    public static class RequestHeadersInterceptor {

        @RuntimeType
        public static void intercept(
                @Argument(0) Object request,
                @Argument(1) Object context,
                @Argument(2) Object httpRequest,
                @AllArguments Object[] allArgs) throws Exception {
            
            try {
                Method getHeadersMethod = request.getClass().getMethod("getHeaders");
                @SuppressWarnings("unchecked")
                Map<String, String> headers = (Map<String, String>) getHeadersMethod.invoke(request);

                Method addHeaderMethod = httpRequest.getClass().getMethod("addHeader", String.class, String.class);

                for (Map.Entry<String, String> entry : headers.entrySet()) {
                    String headerName = entry.getKey();
                    String headerValue = entry.getValue();

                    if (!headerName.equalsIgnoreCase("Content-Length")) {
                        addHeaderMethod.invoke(httpRequest, headerName, headerValue);
                        
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Added header: {} = {}", headerName, headerValue);
                        }
                    } else {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Filtered header: {} (Content-Length)", headerName);
                        }
                    }
                }
                
                LOG.debug("configureRequestHeaders completed, Host header is now allowed");
                
            } catch (Exception e) {
                LOG.error("Error in intercepted configureRequestHeaders method", e);
                throw e;
            }
        }
    }
}
