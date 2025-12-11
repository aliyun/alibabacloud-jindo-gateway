package com.aliyun.jindodata.gateway;

import com.aliyun.jindodata.gateway.common.JindoMiniCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.io.IOException;

public class JindoMultiClusterTestBase extends JindoTestBase{
    @BeforeAll
    public static void startSingleCluster() throws IOException {
        cluster = JindoMiniCluster.startMultiCluster(BASE_CONF);
        BASE_FS = getFs(BASE_CONF);
    }

    @AfterAll
    public static void stopSingleCluster() throws IOException {
        BASE_FS.close();
        cluster.stop();
    }
}
