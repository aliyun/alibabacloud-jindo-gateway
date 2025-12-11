package com.aliyun.jindodata.gateway;

import com.aliyun.jindodata.gateway.common.JindoMiniCluster;
import com.aliyun.jindodata.gateway.hdfs.datanode.JindoDataNode;
import com.aliyun.jindodata.gateway.hdfs.namenode.JindoNameNode;
import com.google.common.base.Joiner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolTranslatorPB;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.*;

import java.io.*;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_IPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DfsAppendTestUtil.getFileSystemAs;

public class JindoTestBase {

    public static Configuration BASE_CONF;
    protected static String testDir;
    protected static Path testRootPath;
    protected static FileSystem BASE_FS;
    protected static JindoMiniCluster cluster;

    @BeforeAll
    public static void setUpAll() throws IOException {
        testDir = "/jindo-gateway-java-test/"+ UUID.randomUUID() + "/";
        testRootPath = new Path(testDir);
        BASE_CONF = new Configuration();

        BASE_FS = getFs(BASE_CONF);
    }

    @AfterAll
    public static void tearDownAll() throws IOException {
        BASE_FS.close();
    }

    @BeforeEach
    public void setUpBase() throws IOException, InterruptedException {
        int retry = 5;
        do {
            try {
                if (cluster != null && cluster.isRunning()) {
                    if (!BASE_FS.exists(testRootPath)) {
                        BASE_FS.mkdirs(testRootPath);
                    } else {
                        BASE_FS.delete(testRootPath, true);
                        BASE_FS.mkdirs(testRootPath);
                    }
                    BASE_FS.setPermission(testRootPath, FsPermission.createImmutable((short) 0777));
                }
            } catch (EOFException eof) {
                retry--;
                if (retry <= 0) throw eof;
                Thread.sleep(3000);
                continue;
            }
            retry = 0;
        } while(retry > 0);
    }

    @AfterEach
    public void tearDownBase() throws IOException {
        if (cluster != null && cluster.isRunning()) {
            FileSystem adminFs = getFileSystemAsAdmin();
            adminFs.delete(testRootPath, true);
            adminFs.close();
        }
    }

    protected static JindoNameNode createNameNode() throws IOException {
        Configuration nnConf = new Configuration(BASE_CONF);
        return JindoNameNode.createNameNode(new String[0], nnConf);
    }

    protected static ClientProtocol createNameNodeClient(JindoNameNode nameNode) throws IOException {
        Configuration configuration = new Configuration(BASE_CONF);
        RPC.setProtocolEngine(configuration, ClientNamenodeProtocolPB.class, ProtobufRpcEngine.class);
        ClientNamenodeProtocolPB proxy = RPC.getProtocolProxy(ClientNamenodeProtocolPB.class,
                RPC.getProtocolVersion(ClientNamenodeProtocolPB.class),
                nameNode.getRpcServer().getRpcAddr(), configuration).getProxy();
        return new ClientNamenodeProtocolTranslatorPB(proxy);
    }

    protected static DatanodeProtocolClientSideTranslatorPB
            createNameNodeClient4DN(JindoNameNode nameNode) throws IOException {
        Configuration configuration = new Configuration(BASE_CONF);
        return new DatanodeProtocolClientSideTranslatorPB(
                nameNode.getRpcServer().getRpcAddr(), configuration);
    }

    protected static void stopClient(Object client) {
        RPC.stopProxy(client);
    }

    protected static void stopNameNode(JindoNameNode nameNode) {
        nameNode.stop();
        nameNode.join();
    }

    protected static JindoDataNode createDataNode() throws IOException {
        Configuration dnConf = new HdfsConfiguration(BASE_CONF);
        dnConf.set(DFS_DATANODE_IPC_ADDRESS_KEY, "127.0.0.1:0");
        dnConf.set(DFS_DATANODE_ADDRESS_KEY, "127.0.0.1:0");
        return JindoDataNode.createDataNode(dnConf);
    }

    protected static void stopDataNode(JindoDataNode dataNode) {
        dataNode.shutdown();
    }

    /**
     * subPath should not start with '/'
     */
    protected static Path makeTestPath(String subPath) {
        if (subPath.startsWith("/")) {
            throw new RuntimeException("subPath should not start with '/'");
        }
        return new Path(testDir ,subPath);
    }

    /**
     * subPath should not start with '/'
     */
    protected static String makeTestPathStr(String subPath) {
        if (subPath.startsWith("/")) {
            throw new RuntimeException("subPath should not start with '/'");
        }
        return new Path(testDir ,subPath).toString();
    }

    protected static void createDummyPartFile(String filePath, int size) throws IOException {
        File file = new File(filePath);
        file.getParentFile().mkdirs();

        try (FileOutputStream fos = new FileOutputStream(file)) {
            byte[] data = new byte[size];
            // Fill test data
            for (int i = 0; i < size; i++) {
                data[i] = (byte) (i % 256);
            }
            fos.write(data);
        }
    }

    protected static FileSystem getDefaultFS() {
        return BASE_FS;
    }

    protected static FileSystem getFs(Configuration tmpConf) throws IOException {
        return FileSystem.get(tmpConf);
    }

    protected static FileSystem getFs() throws IOException {
        return FileSystem.get(BASE_CONF);
    }

    protected static DistributedFileSystem getDFS() {
        return (DistributedFileSystem) BASE_FS;
    }

    protected static DistributedFileSystem getDFS(Configuration configuration) throws IOException {
        return (DistributedFileSystem) getFs(configuration);
    }

    protected static DistributedFileSystem getNonCachedDFS(Configuration configuration) throws IOException {
        configuration.setBoolean("fs.hdfs.impl.disable.cache", true);
        return (DistributedFileSystem) getFs(configuration);
    }

    protected static DistributedFileSystem getNonCachedDFS() throws IOException {
        Configuration conf = new Configuration(BASE_CONF);
        return getNonCachedDFS(conf);
    }

    protected static FileSystem getNonCachedFs() throws IOException {
        Configuration conf = new Configuration(BASE_CONF);
        return getNonCachedFs(conf);
    }

    protected static FileSystem getNonCachedFs(Configuration tmpConf) throws IOException {
        tmpConf.setBoolean("fs.hdfs.impl.disable.cache", true);
        return FileSystem.get(tmpConf);
    }

    public static void createFile(FileSystem fs, Path fileName, long fileLen,
                                  short replFactor, long seed) throws IOException {
        if (!fs.mkdirs(fileName.getParent())) {
            throw new IOException("Mkdirs failed to create " +
                    fileName.getParent().toString());
        }
        try (FSDataOutputStream out = fs.create(fileName, replFactor)) {
            byte[] toWrite = new byte[1024];
            Random rb = new Random(seed);
            long bytesToWrite = fileLen;
            while (bytesToWrite>0) {
                rb.nextBytes(toWrite);
                int bytesToWriteNext = (1024<bytesToWrite)?1024:(int)bytesToWrite;

                out.write(toWrite, 0, bytesToWriteNext);
                bytesToWrite -= bytesToWriteNext;
            }
        }
    }

    public static void writeFile(FileSystem fs, Path p, String s)
            throws IOException {
        if (fs.exists(p)) {
            fs.delete(p, true);
        }
        try (InputStream is = new ByteArrayInputStream(s.getBytes());
             FSDataOutputStream os = fs.create(p)) {
            IOUtils.copyBytes(is, os, s.length());
        }
    }

    public static byte[] generateSequentialBytes(int start, int length) {
        byte[] result = new byte[length];

        for (int i = 0; i < length; i++) {
            result[i] = (byte) ((start + i) % 127);
        }

        return result;
    }

    public static void waitReplication(FileSystem fs, Path fileName, short replFactor)
            throws IOException, InterruptedException, TimeoutException {
        boolean correctReplFactor;
        final int ATTEMPTS = 40;
        int count = 0;

        do {
            correctReplFactor = true;
            BlockLocation[] locs = fs.getFileBlockLocations(
                    fs.getFileStatus(fileName), 0, Long.MAX_VALUE);
            count++;
            for (int j = 0; j < locs.length; j++) {
                String[] hostnames = locs[j].getNames();
                if (hostnames.length != replFactor) {
                    correctReplFactor = false;
                    System.out.println("Block " + j + " of file " + fileName
                            + " has replication factor " + hostnames.length
                            + " (desired " + replFactor + "); locations "
                            + Joiner.on(' ').join(hostnames));
                    Thread.sleep(1000);
                    break;
                }
            }
            if (correctReplFactor) {
                System.out.println("All blocks of file " + fileName
                        + " verified to have replication factor " + replFactor);
            }
        } while (!correctReplFactor && count < ATTEMPTS);

        if (count == ATTEMPTS) {
            throw new TimeoutException("Timed out waiting for " + fileName +
                    " to reach " + replFactor + " replicas");
        }
    }

    protected FileSystem getFileSystemAsAdmin() throws IOException {
        UserGroupInformation appenduser =
                UserGroupInformation.createUserForTesting("admin", new String[]{"supergroup"});
        return getFileSystemAs(appenduser, BASE_CONF);
    }
}
