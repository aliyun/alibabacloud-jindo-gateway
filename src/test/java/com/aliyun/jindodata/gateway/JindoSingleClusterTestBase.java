package com.aliyun.jindodata.gateway;

import com.aliyun.jindodata.gateway.common.JindoMiniCluster;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.io.IOException;
import java.net.InetSocketAddress;

public class JindoSingleClusterTestBase extends JindoTestBase{
    @BeforeAll
    public static void startSingleCluster() throws IOException {
        cluster = JindoMiniCluster.startSingleCluster(BASE_CONF);
        BASE_FS = getFs(BASE_CONF);
    }

    @AfterAll
    public static void stopSingleCluster() throws IOException {
        BASE_FS.close();
        cluster.stop();
    }

    protected int getNameNodePort() {
        return cluster.getNameNodePort();
    }

    public InetSocketAddress getNameNodeAddr() {
        return new InetSocketAddress("localhost",
                getNameNodePort());
    }
}
