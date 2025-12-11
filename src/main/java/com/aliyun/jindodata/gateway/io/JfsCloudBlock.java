package com.aliyun.jindodata.gateway.io;

import com.aliyun.jindodata.gateway.common.JfsConstant;
import com.aliyun.jindodata.gateway.common.JfsRequestOptions;
import com.aliyun.jindodata.gateway.common.JfsStatus;
import com.aliyun.jindodata.gateway.common.JfsUtil;
import com.aliyun.jindodata.gateway.io.oss.OssFileStatus;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class JfsCloudBlock {
    private static final Logger LOG = LoggerFactory.getLogger(JfsCloudBlock.class);
    private final String blockId;
    private final String backendLocation;
    private final String generationStamp;
    final long totalSize;
    private long realSize = 0;
    private final JfsOssBackend ossBackend;
    private JfsCloudBlockType type;
    private String ossPath;

    private boolean crcFileInited;
    private boolean isInitGetCrcFileStatus;
    private OssFileStatus crcFile;
    private List<JfsBlockSlice> blockSlices;

    public enum JfsCloudBlockType {
        JfsCloudBlockType_Normal,
        JfsCloudBlockType_Composed
    }

    public JfsCloudBlockType getType() {
        return type;
    }

    public JfsCloudBlock(Block block, JfsRequestOptions options) {
        this.blockId = Long.toString(block.getBlockId());
        this.generationStamp = Long.toString(block.getGenerationStamp());
        this.totalSize = block.getNumBytes();
        this.backendLocation = options.getBackendLocation();
        this.ossBackend = new JfsOssBackend(options);
    }

    public JfsCloudBlock(ExtendedBlock block, JfsRequestOptions options) {
        this.blockId = Long.toString(block.getBlockId());
        this.generationStamp = Long.toString(block.getGenerationStamp());
        this.totalSize = block.getNumBytes();
        this.backendLocation = options.getBackendLocation();
        this.ossBackend = new JfsOssBackend(options);
    }

    public JfsStatus init() {
        String fileName = JfsUtil.concatPath(backendLocation, blockId);
        ossPath = fileName;
        OssFileStatus ossFileStatus = new OssFileStatus();
        JfsStatus jfsStatus = ossBackend.getStatus(fileName, ossFileStatus);
        if (!jfsStatus.isOk()) {
            if (jfsStatus.getCode() == JfsStatus.FILE_NOT_FOUND_ERROR) {
                // Empty block
                type = JfsCloudBlockType.JfsCloudBlockType_Normal;
                realSize = 0;
                return JfsStatus.OK();
            } else {
                return JfsStatus.ioError("Failed to get oss path for block" + fileName + ". " + jfsStatus.getMessage());
            }
        }

        if (ossFileStatus.isFile()) {
            LOG.debug("Normal block {} size {} isInitGetCrcFileStatus {}",
                    fileName, ossFileStatus.getSize(), isInitGetCrcFileStatus);
            type = JfsCloudBlockType.JfsCloudBlockType_Normal;
            realSize = ossFileStatus.getSize();
            if (isInitGetCrcFileStatus) {
                StringBuilder crcSuffixSB = new StringBuilder();
                crcSuffixSB.append("_").append(generationStamp).append(".meta");
                String crcPath = fileName + crcSuffixSB.toString();
                OssFileStatus crcFileStatus = new OssFileStatus();
                jfsStatus = ossBackend.getStatus(crcPath, crcFileStatus);
                if (jfsStatus.isOk()) {
                    LOG.debug("Crc file {} size {}", crcPath, crcFileStatus.getSize());
                    crcFile = crcFileStatus;
                    crcFileInited = true;
                } else {
                    LOG.info("Crc file {} not found", crcPath);
                }
            }
        } else {
            LOG.debug("Composed block {} isInitGetCrcFileStatus {}", fileName, isInitGetCrcFileStatus);
            type = JfsCloudBlockType.JfsCloudBlockType_Composed;
            List<OssFileStatus> sliceInfos = new ArrayList<>();
            JfsStatus status = ossBackend.list(fileName, sliceInfos, false);
            if (!status.isOk()) {
                return JfsStatus.ioError("Failed to list oss path for block" + fileName + ". " + status.getMessage());
            }
            LOG.debug("Composed block {} slice size {}", fileName, sliceInfos.size());
            blockSlices = new ArrayList<>(sliceInfos.size());
            HashMap<String, OssFileStatus> crcInfos = new HashMap<>();
            for (OssFileStatus sliceInfo : sliceInfos) {
                LOG.debug("Process file {}", sliceInfo.getPath());
                String slicePath = sliceInfo.getPath();
                String[] components = slicePath.split("/");
                String sliceName = components[components.length - 1];
                if (sliceName.contains(".meta")) {
                    crcInfos.put(sliceName, sliceInfo);
                    continue;
                }
                int sliceIndex;
                try {
                    sliceIndex = Integer.parseInt(sliceName);
                } catch (NumberFormatException e) {
                    LOG.warn("Failed to parse slice index for slice: {}", sliceName);
                    return JfsStatus.ioError("Failed to parse slice index for slice: " + sliceName);
                }
                LOG.debug("Slice index {}", sliceIndex);
                if (sliceIndex >= blockSlices.size()) {
                    blockSlices.addAll(blockSlices.size(), Collections.nCopies(sliceIndex - blockSlices.size() + 1, null));
                }
                JfsBlockSlice blockSlice = new JfsBlockSlice(sliceIndex, sliceInfo.getPath(), sliceInfo.getSize());
                blockSlices.set(sliceIndex, blockSlice);
                realSize += sliceInfo.getSize();
            }
            LOG.debug("Composed block slice number {}", blockSlices.size());
            for (JfsBlockSlice blockSlice : blockSlices) {
                String key = blockSlice.index + "_" + generationStamp + ".meta";
                OssFileStatus crcInfo = crcInfos.get(key);
                if (crcInfo != null) {
                    blockSlice.setCrcFileInfo(crcInfo);
                } else {
                    LOG.info("Failed to find crc file {}", key);
                }
            }
        }
        return JfsStatus.OK();
    }

    /**
     * @param offset start offset in block.(0 - Long.MAX_VALUE)
     * @param length read length.(1 - Integer.MAX_VALUE)
     */
    public JfsStatus read(byte[] buf, long offset, int length) {
        return read(buf, offset, length, 0);
    }

    public JfsStatus read(byte[] buf, long offset, int length, int offsetInBuf) {
        if (type == JfsCloudBlockType.JfsCloudBlockType_Normal) {
            return readNormalBlock(buf, offset, length, offsetInBuf);
        } else if (type == JfsCloudBlockType.JfsCloudBlockType_Composed) {
            return readComposedBlock(buf, offset, length, offsetInBuf);
        } else {
            return JfsStatus.invalidArgument("Invalid block type");
        }
    }

    public JfsStatus readNormalBlock(byte[] buf, long offset, int length, int offsetInBuf) {
        if (offset + length > totalSize) {
            StringBuilder msg = new StringBuilder();
            msg.append("Invalid range to read normal cloud block ")
                    .append(blockId)
                    .append(", total size ")
                    .append(totalSize)
                    .append(", offset ")
                    .append(offset)
                    .append(", length ")
                    .append(length);
            LOG.warn(msg.toString());
            return JfsStatus.invalidArgument(msg.toString());
        }
        return readFile(ossPath, buf, offset, length, offsetInBuf);
    }

    public JfsStatus readComposedBlock(byte[] buf, long offset, int length, int offsetInBuf) {
        if (offset + length > totalSize) {
            StringBuilder msg = new StringBuilder();
            msg.append("Invalid range to read composed cloud block ")
                    .append(blockId)
                    .append(", total size ")
                    .append(totalSize)
                    .append(", offset ")
                    .append(offset)
                    .append(", length ")
                    .append(length);
            LOG.warn(msg.toString());
            return JfsStatus.invalidArgument(msg.toString());
        }

        int blockIndex = 0;
        int totalRead = 0;
        for (long startOffsett = 0; blockIndex < blockSlices.size() && length > 0; blockIndex++) {
            JfsBlockSlice blockSlice = blockSlices.get(blockIndex);
            if (startOffsett + blockSlice.size <= offset) {
                // Skip to next slice
            } else {
                long offsetInSlice = 0;
                int lenInSlice = 0;
                if (startOffsett <= offset) {
                    offsetInSlice = offset - startOffsett;
                    lenInSlice = (int) Math.min(blockSlice.size - offsetInSlice, length);
                } else {
                    lenInSlice = (int) Math.min(blockSlice.size, length);
                }

                JfsStatus status = readFile(blockSlice.ossPath, buf, offsetInSlice, lenInSlice, totalRead + offsetInBuf);
                if (!status.isOk()) {
                    return status;
                }

                totalRead += lenInSlice;
                length -= lenInSlice;
            }
            startOffsett += blockSlice.size;
        }
        return JfsStatus.OK();
    }

    public JfsStatus readChecksumData(byte[] buf, long chunkIndex, long chunkCount) {
        if (type == JfsCloudBlockType.JfsCloudBlockType_Composed && blockSlices.size() > 1) {
            return JfsStatus.notSupported("Cannot get raw checksum data for composed block with " 
                    + blockSlices.size() + " slices");
        }

        OssFileStatus crcFileToRead = null;
        if (type == JfsCloudBlockType.JfsCloudBlockType_Composed) {
            crcFileToRead = blockSlices.get(0).getCrcFileName();
        } else {
            initCrcFile();
            crcFileToRead = crcFile;
        }
        
        if (crcFileToRead == null) {
            LOG.warn("Failed to find crc file for {}", ossPath);
            return JfsStatus.ioError("Failed to find crc file");
        }

        long offset = chunkIndex * JfsConstant.CHECKSUM_BYTES_PER_CHECKSUM_SIZE + JfsConstant.CHECKSUM_HEADER_SIZE;
        long length = chunkCount * JfsConstant.CHECKSUM_BYTES_PER_CHECKSUM_SIZE;

        if (offset + length > crcFileToRead.getSize()) {
            return JfsStatus.invalidArgument("Request chunkIndex " + chunkIndex + " chunkCount " 
                    + chunkCount + " exceeds total crc file size " + crcFileToRead.getSize());
        }

        return readFile(crcFileToRead.getPath(), buf, offset, (int) length);
    }

    public JfsStatus readFile(String ossFilePath, byte[] buf, long offset, int length) {
        return readFile(ossFilePath, buf, offset, length, 0);
    }

    /**
     * @offset offset in oss file
     * @return result is crc file data
     */
    public JfsStatus readFile(String ossFilePath, byte[] buf, long offset, int length, int offsetInBuf) {
        JfsOssFileInputStream in = ossBackend.open(ossFilePath);
        try {
            JfsStatus status = in.readFully(buf, offset, length, offsetInBuf);
            if (!status.isOk()) {
                LOG.warn("Failed to read backend file {}, error: {}", ossFilePath, status.getMessage());
                return status;
            }
            return status;
        } catch (IOException e) {
            LOG.warn("Failed to read backend file {}", ossFilePath, e);
            return JfsStatus.fromException(e);
        }
    }

    private void initCrcFile() {
        if (crcFileInited) {
            return;
        }
        
        String crcSuffix = "_" + generationStamp + ".meta";
        String crcPath = ossPath + crcSuffix;
        
        OssFileStatus crcFileStatus = new OssFileStatus();
        JfsStatus ret = ossBackend.getStatus(crcPath, crcFileStatus);
        if (ret.isOk()) {
            crcFile = crcFileStatus;
            LOG.info("Normal block {}, crc file {}", ossPath, crcFile.getPath());
        } else {
            LOG.info("Failed to find crc file for {}", ossPath);
        }
        
        crcFileInited = true;
    }

    public long getRealSize() {
        return realSize;
    }
}
