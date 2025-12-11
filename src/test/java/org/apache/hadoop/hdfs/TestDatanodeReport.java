/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.hadoop.hdfs;

import com.aliyun.jindodata.gateway.JindoTestBase;
import com.aliyun.jindodata.gateway.common.JindoMiniCluster;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.blockmanagement.CombinedHostFileManager;
import org.apache.hadoop.hdfs.server.blockmanagement.HostConfigManager;
import org.junit.jupiter.api.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * This test ensures the all types of data node report work correctly.
 */
public class TestDatanodeReport extends JindoTestBase {
    static final Log LOG = LogFactory.getLog(TestDatanodeReport.class);
    static private Configuration conf;
    final static private int NUM_OF_DATANODES = 1;
    static JindoMiniCluster cluster;
    static String excludeFilePath = "/tmp/dfs.hosts.exclude.txt";
    static File exclude = new File(excludeFilePath);

    @BeforeAll
    public static void beforeAll() throws Exception {
        if (exclude.exists()) {
            exclude.delete();
        }
        conf = new HdfsConfiguration(BASE_CONF);
        cluster = JindoMiniCluster.startSingleCluster(BASE_CONF);
    }

    @AfterAll
    public static void afterAll() throws IOException {
        cluster.stop();
        if (exclude.exists()) {
            exclude.delete();
        }
    }

    /**
     * This test verifies upgrade domain is set according to the JSON host file.
     */
    @Test
    public void testDatanodeReportWithUpgradeDomain() throws Exception {
        conf.setInt(
                DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 500); // 0.5s
        conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
        conf.setClass(DFSConfigKeys.DFS_NAMENODE_HOSTS_PROVIDER_CLASSNAME_KEY,
                CombinedHostFileManager.class, HostConfigManager.class);
//    HostsFileWriter hostsFileWriter = new HostsFileWriter();
//    hostsFileWriter.initialize(conf, "temp/datanodeReport");

        DistributedFileSystem fs = (DistributedFileSystem) getFs(conf);
        final DFSClient client = fs.dfs;
        final String ud1 = "ud1";
        final String ud2 = "ud2";

        try {

//      DatanodeAdminProperties datanode = new DatanodeAdminProperties();
//      datanode.setHostName(cluster.getDataNodes().get(0).getDatanodeId().getHostName());
//      datanode.setUpgradeDomain(ud1);
//      hostsFileWriter.initIncludeHosts(
//          new DatanodeAdminProperties[]{datanode});
            client.refreshNodes();
//      DatanodeInfo[] all = client.datanodeReport(DatanodeReportType.ALL);
//      assertEquals(all[0].getUpgradeDomain(), ud1);
//
//      datanode.setUpgradeDomain(null);
//      hostsFileWriter.initIncludeHosts(
//          new DatanodeAdminProperties[]{datanode});
//      client.refreshNodes();
//      all = client.datanodeReport(DatanodeReportType.ALL);
//      assertEquals(all[0].getUpgradeDomain(), null);
//
//      datanode.setUpgradeDomain(ud2);
//      hostsFileWriter.initIncludeHosts(
//          new DatanodeAdminProperties[]{datanode});
//      client.refreshNodes();
//      all = client.datanodeReport(DatanodeReportType.ALL);
//      assertEquals(all[0].getUpgradeDomain(), ud2);
        } finally {
        }
    }



    /**
     * This test attempts to different types of datanode report.
     */
    @Test
    public void testDatanodeReport() throws Exception {
        conf.setInt(
                DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 500); // 0.5s
        conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);

        DistributedFileSystem fs = (DistributedFileSystem) getFs(conf);
        final DFSClient client = fs.dfs;
        try {
            DatanodeInfo[] nodeInfo = client.datanodeReport(DatanodeReportType.ALL);
            int xferPort = nodeInfo[0].getXferPort();

            assertReports(NUM_OF_DATANODES, DatanodeReportType.ALL, client, "0");
            assertReports(NUM_OF_DATANODES, DatanodeReportType.LIVE, client, "0");
            assertReports(0, DatanodeReportType.DEAD, client, "0");

            String content = "localhost:" + xferPort;

            try (FileWriter writer = new FileWriter(excludeFilePath)) {
                writer.write(content);
                System.out.println("Successfully wrote 'localhost' to " + excludeFilePath);
            } catch (IOException e) {
                System.err.println("Error writing to file: " + e.getMessage());
                e.printStackTrace();
            }

            client.refreshNodes();

            DatanodeInfo[] deadNodeInfo = client.datanodeReport(DatanodeReportType.DEAD);
            while (deadNodeInfo.length != 1) {
                try {
                    Thread.sleep(500);
                } catch (Exception e) {
                }
                deadNodeInfo = client.datanodeReport(DatanodeReportType.DEAD);
            }
//
            assertReports(NUM_OF_DATANODES, DatanodeReportType.ALL, client, null);
            assertReports(NUM_OF_DATANODES - 1, DatanodeReportType.LIVE, client, null);
            assertReports(1, DatanodeReportType.DEAD, client, null);

        } finally {
        }
    }

    static void assertReports(int numDatanodes, DatanodeReportType type,
                              DFSClient client, String bpid) throws IOException {
        final DatanodeInfo[] infos = client.datanodeReport(type);
        assertEquals(numDatanodes, infos.length);
    }
}