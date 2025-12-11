///**
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package com.aliyun.jindodata.gateway.hdfs.datanode;
//
//import com.aliyun.jindodata.gateway.hdfs.blockIO.JindoBlockCrcInputStream;
//import com.aliyun.jindodata.gateway.io.JfsCloudBlock;
//import org.apache.hadoop.classification.InterfaceAudience;
//import org.apache.hadoop.hdfs.DFSUtilClient;
//import org.apache.hadoop.hdfs.protocol.*;
//import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtoUtil;
//import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
//import org.apache.hadoop.hdfs.protocol.datatransfer.Op;
//import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
//import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos;
//import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
//import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
//import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
//import org.apache.hadoop.hdfs.server.datanode.DataNode;
//import org.apache.hadoop.hdfs.server.datanode.erasurecode.StripedBlockChecksumCompositeCrcReconstructor;
//import org.apache.hadoop.hdfs.server.datanode.erasurecode.StripedBlockChecksumMd5CrcReconstructor;
//import org.apache.hadoop.hdfs.server.datanode.erasurecode.StripedBlockChecksumReconstructor;
//import org.apache.hadoop.hdfs.server.datanode.erasurecode.StripedReconstructionInfo;
//import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;
//import org.apache.hadoop.hdfs.util.StripedBlockUtil;
//import org.apache.hadoop.io.DataOutputBuffer;
//import org.apache.hadoop.io.IOUtils;
//import org.apache.hadoop.io.MD5Hash;
//import org.apache.hadoop.security.token.Token;
//import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
//import org.apache.hadoop.util.CrcComposer;
//import org.apache.hadoop.util.CrcUtil;
//import org.apache.hadoop.util.DataChecksum;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.*;
//import java.security.MessageDigest;
//import java.util.HashMap;
//import java.util.Map;
//
//import static com.aliyun.jindodata.gateway.common.JfsConstant.BYTES_PER_CHECKSUM_DEFAULT;
//
///**
// * Utilities for Block checksum computing, for both replicated and striped
// * blocks.
// */
//@InterfaceAudience.Private
//final class BlockChecksumHelper {
//
//  static final Logger LOG = LoggerFactory.getLogger(BlockChecksumHelper.class);
//
//  private BlockChecksumHelper() {
//  }
//
//  /**
//   * The abstract block checksum computer.
//   */
//  static abstract class AbstractBlockChecksumComputer {
//    private final JindoDataNode datanode;
//    private final BlockChecksumOptions blockChecksumOptions;
//
//    private byte[] outBytes;
//    private long crcPerBlock = -1;
//    private int checksumSize = -1;
//
//    AbstractBlockChecksumComputer(
//            JindoDataNode datanode,
//        BlockChecksumOptions blockChecksumOptions) throws IOException {
//      this.datanode = datanode;
//      this.blockChecksumOptions = blockChecksumOptions;
//    }
//
//    abstract void compute() throws IOException;
//
//    Sender createSender(IOStreamPair pair) {
//      DataOutputStream out = (DataOutputStream) pair.out;
//      return new Sender(out);
//    }
//
//    JindoDataNode getDatanode() {
//      return datanode;
//    }
//
//    BlockChecksumOptions getBlockChecksumOptions() {
//      return blockChecksumOptions;
//    }
//
//    void setOutBytes(byte[] bytes) {
//      this.outBytes = bytes;
//    }
//
//    byte[] getOutBytes() {
//      return outBytes;
//    }
//
//    int getBytesPerCRC() {
//      return BYTES_PER_CHECKSUM_DEFAULT;
//    }
//
//    public void setCrcPerBlock(long crcPerBlock) {
//      this.crcPerBlock = crcPerBlock;
//    }
//
//    public void setChecksumSize(int checksumSize) {
//      this.checksumSize = checksumSize;
//    }
//
//    DataChecksum.Type getCrcType() {
//      return DataChecksum.Type.CRC32C;
//    }
//
//    long getCrcPerBlock() {
//      return crcPerBlock;
//    }
//
//    int getChecksumSize() {
//      return checksumSize;
//    }
//  }
//
//  /**
//   * The abstract base block checksum computer, mainly for replicated blocks.
//   */
//  static abstract class BlockChecksumComputer
//      extends AbstractBlockChecksumComputer {
//    private final ExtendedBlock block;
//    // client side now can specify a range of the block for checksum
//    private final long requestLength;
//    protected byte[] crcBuf;
//
//    BlockChecksumComputer(JindoDataNode datanode,
//                          ExtendedBlock block,
//                          BlockChecksumOptions blockChecksumOptions)
//        throws IOException {
//      super(datanode, blockChecksumOptions);
//      this.block = block;
//      this.requestLength = block.getNumBytes();
//      Preconditions.checkArgument(requestLength >= 0);
//    }
//
//    Sender createSender(IOStreamPair pair) {
//      DataOutputStream out = (DataOutputStream) pair.out;
//      return new Sender(out);
//    }
//
//
//    ExtendedBlock getBlock() {
//      return block;
//    }
//
//    long getRequestLength() {
//      return requestLength;
//    }
//
//    /**
//     * Perform the block checksum computing.
//     *
//     * @throws IOException
//     */
//    abstract void compute() throws IOException;
//
//    /**
//     * Read block metadata header.
//     *
//     * @throws IOException
//     */
//    void readHeader() throws IOException {
//      long crcPerBlock = requestLength / getBytesPerCRC();
//      setCrcPerBlock(crcPerBlock <= 0 ? 1 : crcPerBlock);
//    }
//
//    void readCrc() throws IOException {
//      try(JindoBlockCrcInputStream crcIn = new JindoBlockCrcInputStream(block,
//              getDatanode().getRequestOptions())) {
//        crcBuf = crcIn.getCrcBytes(requestLength);
//      }
//    }
//  }
//
//  /**
//   * Replicated block checksum computer.
//   */
//  static class ReplicatedBlockChecksumComputer extends BlockChecksumComputer {
//    JfsCloudBlock cloudBlock;
//    ReplicatedBlockChecksumComputer(JindoDataNode datanode,
//                                    ExtendedBlock block,
//                                    BlockChecksumOptions blockChecksumOptions)
//        throws IOException {
//      super(datanode, block, blockChecksumOptions);
//      cloudBlock = new JfsCloudBlock(block, datanode.getRequestOptions());
//    }
//
//    @Override
//    void compute() throws IOException {
//      try {
//        readHeader();
//        readCrc();
//        BlockChecksumType type =
//            getBlockChecksumOptions().getBlockChecksumType();
//        switch (type) {
//        case MD5CRC:
//          computeMd5Crc();
//          break;
//        case COMPOSITE_CRC:
//          computeCompositeCrc(getBlockChecksumOptions().getStripeLength());
//          break;
//        default:
//          throw new IOException(String.format(
//              "Unrecognized BlockChecksumType: %s", type));
//        }
//      } finally {
//        crcBuf = null;
//      }
//    }
//
//    private void computeMd5Crc() throws IOException {
//      MD5Hash md5out;
//
//      md5out = checksumWholeBlock();
//
//      setOutBytes(md5out.getDigest());
//
//      LOG.debug("block={}, bytesPerCRC={}, crcPerBlock={}, md5out={}",
//          getBlock(), getBytesPerCRC(), getCrcPerBlock(), md5out);
//    }
//
//    private MD5Hash checksumWholeBlock() throws IOException {
//        return MD5Hash.digest(crcBuf);
//    }
//
//    private void computeCompositeCrc(long stripeLength) throws IOException {
//      long checksumDataLength = getRequestLength();
//      if (stripeLength <= 0 || stripeLength > checksumDataLength) {
//        stripeLength = checksumDataLength;
//      }
//
//      CrcComposer crcComposer = CrcComposer.newStripedCrcComposer(
//          getCrcType(), getBytesPerCRC(), stripeLength);
//
//      crcComposer.update(crcBuf, 0, crcBuf.length, getBytesPerCRC());
//      byte[] composedCrcs = crcComposer.digest();
//      setOutBytes(composedCrcs);
//      if (LOG.isDebugEnabled()) {
//        LOG.debug(
//            "block={}, getBytesPerCRC={}, crcPerBlock={}, compositeCrc={}",
//            getBlock(), getBytesPerCRC(), getCrcPerBlock(),
//            CrcUtil.toMultiCrcString(composedCrcs));
//      }
//    }
//  }
//}
