package com.aliyun.jindodata.gateway.common;

import java.io.InputStream;
import java.util.Properties;

public class JfsVersionUtil {
    public static final String VERSION;

    static {
        VERSION = getProjectVersion();
    }

    public static String getProjectVersion() {
        String groupId = "com.aliyun.jindodata";
        String artifactId = "jindo-gateway";

        String path = String.format("/META-INF/maven/%s/%s/pom.properties",
                groupId, artifactId);

        Properties props = new Properties();
        try (InputStream is = JfsUtil.class.getResourceAsStream(path)) {
            if (is != null) {
                props.load(is);
                return props.getProperty("version", "unknown");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "unknown";
    }
}
