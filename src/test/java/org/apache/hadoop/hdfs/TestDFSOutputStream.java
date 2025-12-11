package org.apache.hadoop.hdfs;

import com.aliyun.jindodata.gateway.JindoSingleClusterTestBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FsTracer;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketReceiver;
import org.apache.htrace.core.SpanId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;

import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedList;

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY;
import static org.mockito.Mockito.*;

public class TestDFSOutputStream extends JindoSingleClusterTestBase {

    @Test
    public void testCloseTwice() throws IOException {
        DistributedFileSystem fs = getDFS();
        FSDataOutputStream os = fs.create(makeTestPath("test"));
        DFSOutputStream dos = (DFSOutputStream) Whitebox.getInternalState(os,
                "wrappedStream");
        DataStreamer streamer = (DataStreamer) Whitebox
                .getInternalState(dos, "streamer");
        DataStreamer.LastExceptionInStreamer ex = (DataStreamer.LastExceptionInStreamer) Whitebox
                .getInternalState(streamer, "lastException");
        Throwable thrown = (Throwable) Whitebox.getInternalState(ex, "thrown");
        Assertions.assertNull(thrown);

        dos.close();

        IOException dummy = new IOException("dummy");
        ex.set(dummy);
        try {
            dos.close();
        } catch (IOException e) {
            Assertions.assertEquals(e, dummy);
        }
        thrown = (Throwable) Whitebox.getInternalState(ex, "thrown");
        Assertions.assertNull(thrown);
        dos.close();
    }

    @Test
    public void testComputePacketChunkSize() throws Exception {
        DistributedFileSystem fs = getDFS();
        FSDataOutputStream os = fs.create(makeTestPath("test"));
        DFSOutputStream dos = (DFSOutputStream) Whitebox.getInternalState(os,
                "wrappedStream");

        final int packetSize = 64*1024;
        final int bytesPerChecksum = 512;

        Method method = dos.getClass().getDeclaredMethod("computePacketChunkSize",
                int.class, int.class);
        method.setAccessible(true);
        method.invoke(dos, packetSize, bytesPerChecksum);

        Field field = dos.getClass().getDeclaredField("packetSize");
        field.setAccessible(true);

        Assertions.assertTrue((Integer) field.get(dos) + 33 < packetSize);
        // If PKT_MAX_HEADER_LEN is 257, actual packet size come to over 64KB
        // without a fix on HDFS-7308.
        Assertions.assertTrue((Integer) field.get(dos) + 257 < packetSize);

        os.close();
    }

    @Test
    public void testPreventOverflow() throws IOException, NoSuchFieldException,
            SecurityException, IllegalAccessException, IllegalArgumentException,
            InvocationTargetException, NoSuchMethodException {

        final int defaultWritePacketSize = DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT;
        int configuredWritePacketSize = defaultWritePacketSize;
        int finalWritePacketSize = defaultWritePacketSize;

        /* test default WritePacketSize, e.g. 64*1024 */
        runAdjustChunkBoundary(configuredWritePacketSize, finalWritePacketSize);

        /* test large WritePacketSize, e.g. 1G */
        configuredWritePacketSize = 1000 * 1024 * 1024;
        finalWritePacketSize = PacketReceiver.MAX_PACKET_SIZE;
        runAdjustChunkBoundary(configuredWritePacketSize, finalWritePacketSize);
    }
    private void runAdjustChunkBoundary(
            final int configuredWritePacketSize,
            final int finalWritePacketSize) throws IOException, NoSuchFieldException,
            SecurityException, IllegalAccessException, IllegalArgumentException,
            InvocationTargetException, NoSuchMethodException {

        final boolean appendChunk = false;
        final long blockSize = 3221225500L;
        final long bytesCurBlock = 1073741824L;
        final int bytesPerChecksum = 512;
        final int checksumSize = 4;
        final int chunkSize = bytesPerChecksum + checksumSize;
        final int packateMaxHeaderLength = 33;

        try {
            final Configuration dfsConf = new Configuration();
            dfsConf.setInt(DFS_CLIENT_WRITE_PACKET_SIZE_KEY,
                    configuredWritePacketSize);

            final FSDataOutputStream os = getNonCachedDFS(dfsConf)
                    .create(makeTestPath("testPreventOverflow"));
            final DFSOutputStream dos = (DFSOutputStream) Whitebox
                    .getInternalState(os, "wrappedStream");

            /* set appendChunk */
            final Method setAppendChunkMethod = dos.getClass()
                    .getDeclaredMethod("setAppendChunk", boolean.class);
            setAppendChunkMethod.setAccessible(true);
            setAppendChunkMethod.invoke(dos, appendChunk);

            /* set bytesCurBlock */
            final Method setBytesCurBlockMethod = dos.getClass()
                    .getDeclaredMethod("setBytesCurBlock", long.class);
            setBytesCurBlockMethod.setAccessible(true);
            setBytesCurBlockMethod.invoke(dos, bytesCurBlock);

            /* set blockSize */
            final Field blockSizeField = dos.getClass().getDeclaredField("blockSize");
            blockSizeField.setAccessible(true);
            blockSizeField.setLong(dos, blockSize);

            /* call adjustChunkBoundary */
            final Method method = dos.getClass()
                    .getDeclaredMethod("adjustChunkBoundary");
            method.setAccessible(true);
            method.invoke(dos);

            /* get and verify writePacketSize */
            final Field writePacketSizeField = dos.getClass()
                    .getDeclaredField("writePacketSize");
            writePacketSizeField.setAccessible(true);
            Assertions.assertEquals(writePacketSizeField.getInt(dos), finalWritePacketSize);

            /* get and verify chunksPerPacket */
            final Field chunksPerPacketField = dos.getClass()
                    .getDeclaredField("chunksPerPacket");
            chunksPerPacketField.setAccessible(true);
            Assertions.assertEquals(chunksPerPacketField.getInt(dos), (finalWritePacketSize - packateMaxHeaderLength) / chunkSize);

            /* get and verify packetSize */
            final Field packetSizeField = dos.getClass()
                    .getDeclaredField("packetSize");
            packetSizeField.setAccessible(true);
            Assertions.assertEquals(packetSizeField.getInt(dos), chunksPerPacketField.getInt(dos) * chunkSize);
        } finally {
        }
    }

    @Test
    public void testCongestionBackoff() throws IOException {
        DfsClientConf dfsClientConf = mock(DfsClientConf.class);
        DFSClient client = mock(DFSClient.class);
        when(client.getConf()).thenReturn(dfsClientConf);
        when(client.getTracer()).thenReturn(FsTracer.get(new Configuration()));
        client.clientRunning = true;
        DataStreamer stream = new DataStreamer(
                mock(HdfsFileStatus.class),
                mock(ExtendedBlock.class),
                client,
                "foo", null, null, null, null, null, null);

        DataOutputStream blockStream = mock(DataOutputStream.class);
        doThrow(new IOException()).when(blockStream).flush();
        Whitebox.setInternalState(stream, "blockStream", blockStream);
        Whitebox.setInternalState(stream, "stage",
                BlockConstructionStage.PIPELINE_CLOSE);
        @SuppressWarnings("unchecked")
        LinkedList<DFSPacket> dataQueue = (LinkedList<DFSPacket>)
                Whitebox.getInternalState(stream, "dataQueue");
        @SuppressWarnings("unchecked")
        ArrayList<DatanodeInfo> congestedNodes = (ArrayList<DatanodeInfo>)
                Whitebox.getInternalState(stream, "congestedNodes");
        congestedNodes.add(mock(DatanodeInfo.class));
        DFSPacket packet = mock(DFSPacket.class);
        when(packet.getTraceParents()).thenReturn(new SpanId[] {});
        dataQueue.add(packet);
        stream.run();
        Assertions.assertTrue(congestedNodes.isEmpty());
    }

    @Test
    public void testEndLeaseCall() throws Exception {
        Configuration conf = new Configuration();
        DFSClient client = new DFSClient(getNameNodeAddr(), conf);
        DFSClient spyClient = Mockito.spy(client);
        DFSOutputStream dfsOutputStream = spyClient.create(makeTestPathStr("file2"),
                FsPermission.getFileDefault(),
                EnumSet.of(CreateFlag.CREATE), (short) 3, 1024 * 1024, null , 1024, null);
        DFSOutputStream spyDFSOutputStream = Mockito.spy(dfsOutputStream);
        spyDFSOutputStream.closeThreads(anyBoolean());
        verify(spyClient, times(1)).endFileLease(anyLong());
    }
}
