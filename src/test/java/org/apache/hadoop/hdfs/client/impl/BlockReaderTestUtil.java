package org.apache.hadoop.hdfs.client.impl;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsTracer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.blockmanagement.CacheReplicationMonitor;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetCache;
import org.apache.hadoop.hdfs.server.namenode.CacheManager;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitCache;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitReplica;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.Token;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A helper class to get to BlockReader and DataNode for a block.
 */
public class BlockReaderTestUtil {
    /**
     * Returns true if we should run tests that generate large files (> 1GB)
     */
    static public boolean shouldTestLargeFiles() {
        String property = System.getProperty("hdfs.test.large.files");
        if (property == null) return false;
        if (property.isEmpty()) return true;
        return Boolean.parseBoolean(property);
    }

    private static Configuration conf = null;

    /**
     * Setup the cluster
     */
    public BlockReaderTestUtil(int replicationFactor) throws Exception {
        this(replicationFactor, new Configuration());
    }

    public BlockReaderTestUtil(int replicationFactor, Configuration config) throws Exception {
        this.conf = config;
        conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, replicationFactor);
    }

    /**
     * Shutdown cluster
     */
    public void shutdown() {
    }

    public Configuration getConf() {
        return conf;
    }

    /**
     * Create a file of the given size filled with random data.
     *
     * @return File data.
     */
    public byte[] writeFile(Path filepath, int sizeKB)
            throws IOException {
        FileSystem fs = FileSystem.get(conf);

        // Write a file with the specified amount of data
        DataOutputStream os = fs.create(filepath);
        byte data[] = new byte[1024 * sizeKB];
        new Random().nextBytes(data);
        os.write(data);
        os.close();
        return data;
    }

    /**
     * Get the list of Blocks for a file.
     */
    public List<LocatedBlock> getFileBlocks(Path filepath, int sizeKB)
            throws IOException {
        // Return the blocks we just wrote
        DFSClient dfsclient = getDFSClient();
        return dfsclient.getNamenode().getBlockLocations(
                filepath.toString(), 0, sizeKB * 1024).getLocatedBlocks();
    }

    /**
     * Get the DFSClient.
     */
    public DFSClient getDFSClient() throws IOException {
        String nnAddrStr = conf.get("fs.defaultFS");
        if (nnAddrStr == null) {
            throw new RuntimeException("fs.defaultFS not set");
        }
        URI nnUri = URI.create(nnAddrStr);
        InetSocketAddress nnAddr = new InetSocketAddress("localhost", nnUri.getPort());
        return new DFSClient(nnAddr, conf);
    }

    /**
     * Exercise the BlockReader and read length bytes.
     * <p>
     * It does not verify the bytes read.
     */
    public void readAndCheckEOS(BlockReader reader, int length, boolean expectEof)
            throws IOException {
        byte buf[] = new byte[1024];
        int nRead = 0;
        while (nRead < length) {
            DFSClient.LOG.info("So far read " + nRead + " - going to read more.");
            int n = reader.read(buf, 0, buf.length);
            assertTrue(n > 0);
            nRead += n;
        }

        if (expectEof) {
            DFSClient.LOG.info("Done reading, expect EOF for next read.");
            assertEquals(-1, reader.read(buf, 0, buf.length));
        }
    }

    /**
     * Get a BlockReader for the given block.
     */
    public BlockReader getBlockReader(LocatedBlock testBlock, int offset, int lenToRead)
            throws IOException {
        return getBlockReader(conf, testBlock, offset, lenToRead);
    }

    /**
     * Get a BlockReader for the given block.
     */
    public static BlockReader getBlockReader(Configuration conf, LocatedBlock testBlock, int offset, int lenToRead) throws IOException {
        InetSocketAddress targetAddr = null;
        ExtendedBlock block = testBlock.getBlock();
        DatanodeInfo[] nodes = testBlock.getLocations();
        targetAddr = NetUtils.createSocketAddr(nodes[0].getXferAddr());

        FileSystem dfs = FileSystem.get(conf);
        if (!(dfs instanceof DistributedFileSystem)) {
            throw new RuntimeException("FileSystem is not DistributedFileSystem");
        }
        final DistributedFileSystem fs = (DistributedFileSystem) dfs;
        return new BlockReaderFactory(fs.getClient().getConf()).
                setInetSocketAddress(targetAddr).
                setBlock(block).
                setFileName(targetAddr.toString() + ":" + block.getBlockId()).
                setBlockToken(testBlock.getBlockToken()).
                setStartOffset(offset).
                setLength(lenToRead).
                setVerifyChecksum(true).
                setClientName("BlockReaderTestUtil").
                setDatanodeInfo(nodes[0]).
                setClientCacheContext(ClientContext.getFromConf(fs.getConf())).
                setCachingStrategy(CachingStrategy.newDefaultStrategy()).
                setConfiguration(fs.getConf()).
                setAllowShortCircuitLocalReads(true).
                setTracer(FsTracer.get(fs.getConf())).
                setRemotePeerFactory(new RemotePeerFactory() {
                    @Override
                    public Peer newConnectedPeer(InetSocketAddress addr,
                                                 Token<BlockTokenIdentifier> blockToken, DatanodeID datanodeId)
                            throws IOException {
                        Peer peer = null;
                        Socket sock = NetUtils.
                                getDefaultSocketFactory(fs.getConf()).createSocket();
                        try {
                            sock.connect(addr, HdfsConstants.READ_TIMEOUT);
                            sock.setSoTimeout(HdfsConstants.READ_TIMEOUT);
                            peer = DFSUtilClient.peerFromSocket(sock);
                        } finally {
                            if (peer == null) {
                                IOUtils.closeQuietly(sock);
                            }
                        }
                        return peer;
                    }
                }).
                build();
    }

    public static void enableHdfsCachingTracing() {
        LogManager.getLogger(CacheReplicationMonitor.class.getName()).setLevel(
                Level.TRACE);
        LogManager.getLogger(CacheManager.class.getName()).setLevel(
                Level.TRACE);
        LogManager.getLogger(FsDatasetCache.class.getName()).setLevel(
                Level.TRACE);
    }

    public static void enableBlockReaderFactoryTracing() {
        LogManager.getLogger(BlockReaderFactory.class.getName()).setLevel(
                Level.TRACE);
        LogManager.getLogger(ShortCircuitCache.class.getName()).setLevel(
                Level.TRACE);
        LogManager.getLogger(ShortCircuitReplica.class.getName()).setLevel(
                Level.TRACE);
        LogManager.getLogger(BlockReaderLocal.class.getName()).setLevel(
                Level.TRACE);
    }
}
