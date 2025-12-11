package com.aliyun.jindodata.gateway;

import com.aliyun.jindodata.gateway.common.JindoMiniCluster;
import com.aliyun.jindodata.gateway.hdfs.datanode.JindoDataNode;
import com.aliyun.jindodata.gateway.hdfs.namenode.JindoNameNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.datanode.DataNodeLayoutVersion;
import org.apache.hadoop.hdfs.server.protocol.*;
import org.junit.jupiter.api.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

public class JindoNameNodeTest extends JindoTestBase{

    static ClientProtocol namenodeClient;
    static DatanodeProtocolClientSideTranslatorPB namenodeClient4DN;
    static JindoNameNode nn;
    static FileSystem fs;

    @BeforeAll
    public static void setUpTest() throws IOException {
        JindoMiniCluster.initSingleClusterConf(BASE_CONF);
        // start JindoNameNode
        nn = createNameNode();
        // start namenode proxy for client
        namenodeClient = createNameNodeClient(nn);
        // start namenode proxy for datanode
        namenodeClient4DN = createNameNodeClient4DN(nn);

        fs = FileSystem.get(new Configuration(BASE_CONF));
        fs.mkdirs(testRootPath);
    }

    @AfterAll
    public static void tearTest() throws IOException {
        fs.delete(testRootPath, true);

        stopClient(namenodeClient4DN);
        stopClient(namenodeClient);
        stopNameNode(nn);
    }

    @Test
    public void testClientName() throws IOException {
        String clientName = JindoNameNode.generateClientName();
        System.out.println("GetClientName result from server: " + clientName);
    }

    @Test
    public void testGetBlockLocations() throws IOException, InterruptedException {
        JindoDataNode dn = createDataNode();
        Thread.sleep(3000);
        Path path = makeTestPath("testGetBlockLocations");
        OutputStream out = fs.create(path);
        out.write("Hello testGetBlockLocations!".getBytes());
        out.close();

        if (fs instanceof DistributedFileSystem) {
            DistributedFileSystem dfs = (DistributedFileSystem) fs;
            LocatedBlocks result = dfs.getClient().getLocatedBlocks(path.toString(), 0);
            System.out.println("GetBlockLocations result from server: " + result.getFileLength());
        }

        dn.shutdown();
    }

    @Test
    public void testList() throws IOException {
        FileStatus[] files = fs.listStatus(testRootPath);
        System.out.println(Arrays.toString(files));
    }

    @Test
    public void testMkdirs() throws IOException {
        Path path = makeTestPath("testMkdirs");
        boolean res = fs.mkdirs(path);
        System.out.println(res);
    }

    @Test
    public void testCreate() throws IOException {
        Path path = makeTestPath("testCreate");
        OutputStream out = fs.create(path);
        out.close();
    }

    @Test
    public void testSetPermission() throws IOException {
        Path path = makeTestPath("testSetPermission");
        OutputStream out = fs.create(path);
        out.close();
        fs.setPermission(path, FsPermission.createImmutable((short) 0777));
    }

    @Test
    public void testSetOwner() throws IOException {
        Path path = makeTestPath("testSetOwner");
        FileSystem adminFs = getFileSystemAsAdmin();
        OutputStream out = adminFs.create(path);
        out.close();
        adminFs.setOwner(path, "fake_user", "fake_group");
    }

    @Test
    public void testRename() throws IOException {
        Path src = makeTestPath("testRename");
        Path dst = makeTestPath("testRename_rename");
        FileSystem fs = FileSystem.get(new Configuration(BASE_CONF));
        OutputStream out = fs.create(src);
        out.close();
        boolean res = fs.rename(src, dst);
        System.out.println(res);
        Assertions.assertTrue(fs.exists(dst));
        Assertions.assertFalse(fs.exists(src));
    }

    @Test
    public void testRename2() throws Exception {
        Path src = makeTestPath("testRename2");
        Path dst = makeTestPath("testRename2_rename");
        FileSystem fs = FileSystem.get(new Configuration(BASE_CONF));
        OutputStream out = fs.create(src);
        out.close();
        
        // reflect
        Method renameMethod = FileSystem.class.getDeclaredMethod("rename", Path.class, Path.class, org.apache.hadoop.fs.Options.Rename[].class);
        renameMethod.setAccessible(true);
        renameMethod.invoke(fs, src, dst, new org.apache.hadoop.fs.Options.Rename[]{org.apache.hadoop.fs.Options.Rename.OVERWRITE});

        OutputStream out2 = fs.create(src);
        out2.close();
        renameMethod.invoke(fs, src, dst, new org.apache.hadoop.fs.Options.Rename[]{org.apache.hadoop.fs.Options.Rename.OVERWRITE});
    }

    @Test
    public void testTruncate() throws IOException {
        Path path = makeTestPath("testTruncate");
        OutputStream out = fs.create(path);
        out.close();
        try {
            boolean res = fs.truncate(path, 22);
            fail("truncate should fail");
        } catch (Exception e) {
            System.out.println("Correct behaviour.");
        }
    }

    @Test
    public void testDelete() throws IOException {
        Path path = makeTestPath("testDelete");
        OutputStream out = fs.create(path);
        out.close();
        boolean res = fs.delete(path, false);
        System.out.println(res);
    }

    @Test
    public void testDeleteNotExist() throws IOException {
        Path path = makeTestPath("testDeleteNotExist");
        boolean res = fs.delete(path, false);
        System.out.println(res);
    }

    @Test
    public void testRenewLease() throws IOException {
        Path path = makeTestPath("testRenewLease");
        OutputStream out = fs.create(path);

        if (fs instanceof DistributedFileSystem) {
            DistributedFileSystem dfs = (DistributedFileSystem) fs;
            Boolean result = dfs.getClient().renewLease();
            System.out.println(result);
        }
        out.close();
    }

    @Test
    public void testRecoverLease() throws IOException {
        Path path = makeTestPath("testRecoverLease");
        OutputStream out = fs.create(path);

        if (fs instanceof DistributedFileSystem) {
            DistributedFileSystem dfs = (DistributedFileSystem) fs;
            Boolean result = dfs.recoverLease(path);
            System.out.println(result);
        }
        out.close();
    }

    @Test
    public void testGetFileInfo() throws IOException {
        Path path = makeTestPath("testGetFileInfo");
        try {
            FileStatus fileStatus = fs.getFileStatus(path);
            fail("getFileStatus should fail");
        } catch (FileNotFoundException e) {
            System.out.println("Expected File not found.");
        }
    }

    @Test
    public void testGetContentSummary() throws IOException {
        ContentSummary contentSummary = fs.getContentSummary(testRootPath);
        System.out.println(contentSummary);
    }

    @Test
    public void testGetFsStats() throws IOException {
        long[] stats = namenodeClient.getStats();
        System.out.println(Arrays.toString(stats));
    }

    @Test
    public void testFsync() throws IOException {
        Path path = makeTestPath("testFsync");
        OutputStream out = fs.create(path);
        out.flush();
        out.close();
    }

    @Test
    public void testVersionRequest() throws IOException {
        NamespaceInfo result = namenodeClient4DN.versionRequest();
        System.out.println("VersionRequest result from server: " + result);
    }

    @Test
    public void testRegisterDatanode() throws IOException {
        NamespaceInfo nsInfo = namenodeClient4DN.versionRequest();

        StorageInfo storageInfo = new StorageInfo(DataNodeLayoutVersion.CURRENT_LAYOUT_VERSION,
                nsInfo.getNamespaceID(), nsInfo.getClusterID(), nsInfo.getCTime(),
                HdfsServerConstants.NodeType.DATA_NODE);
        DatanodeID datanodeID = new DatanodeID("0.0.0.0",
                "127.0.0.1",
                "fake_datanode_uuid",
                50010, 50020,
                50030, 50040);
        DatanodeRegistration datanodeRegistration = new DatanodeRegistration(
                datanodeID, storageInfo, new ExportedBlockKeys(), "unknown");
        namenodeClient4DN.registerDatanode(datanodeRegistration);
        System.out.println("RegisterDatanode result from server: " + datanodeRegistration);
    }

    @Test
    public void testSendHeartbeat() throws IOException {
        NamespaceInfo nsInfo = namenodeClient4DN.versionRequest();

        StorageInfo storageInfo = new StorageInfo(DataNodeLayoutVersion.CURRENT_LAYOUT_VERSION,
                nsInfo.getNamespaceID(), nsInfo.getClusterID(), nsInfo.getCTime(),
                HdfsServerConstants.NodeType.DATA_NODE);
        DatanodeID datanodeID = new DatanodeID("0.0.0.0",
                "127.0.0.1",
                "fake_datanode_uuid",
                50010, 50020,
                50030, 50040);
        DatanodeRegistration datanodeRegistration = new DatanodeRegistration(
                datanodeID, storageInfo, new ExportedBlockKeys(), "unknown");

        HeartbeatResponse heartbeatResponse = namenodeClient4DN.sendHeartbeat(datanodeRegistration,
                new StorageReport[0], 0,
                0, 0, 0,
                0,  null, false);
        System.out.println("SendHeartbeat result from server: " + heartbeatResponse);
    }

    @Test
    public void testStaleDatanode() throws IOException, InterruptedException {
        NamespaceInfo nsInfo = namenodeClient4DN.versionRequest();

        StorageInfo storageInfo = new StorageInfo(DataNodeLayoutVersion.CURRENT_LAYOUT_VERSION,
                nsInfo.getNamespaceID(), nsInfo.getClusterID(), nsInfo.getCTime(),
                HdfsServerConstants.NodeType.DATA_NODE);
        DatanodeID datanodeID = new DatanodeID("0.0.0.0",
                "127.0.0.1",
                "fake_datanode_uuid",
                50010, 50020,
                50030, 50040);
        DatanodeRegistration datanodeRegistration = new DatanodeRegistration(
                datanodeID, storageInfo, new ExportedBlockKeys(), "unknown");
        namenodeClient4DN.registerDatanode(datanodeRegistration);
        System.out.println("RegisterDatanode result from server: " + datanodeRegistration);

        Thread.sleep(60000);
    }
}
