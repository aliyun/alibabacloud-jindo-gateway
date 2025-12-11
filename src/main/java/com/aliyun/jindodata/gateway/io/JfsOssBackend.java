package com.aliyun.jindodata.gateway.io;

import com.aliyun.jindodata.gateway.common.JfsRequestOptions;
import com.aliyun.jindodata.gateway.common.JfsStatus;
import com.aliyun.jindodata.gateway.common.JfsUtil;
import com.aliyun.jindodata.gateway.io.oss.OssClientFactory;
import com.aliyun.jindodata.gateway.io.oss.OssFileStatus;
import com.aliyun.oss.OSS;
import com.aliyun.oss.model.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.List;

import static com.aliyun.jindodata.gateway.common.JfsConstant.FILE_TYPE_DIRECTORY;
import static com.aliyun.jindodata.gateway.common.JfsConstant.FILE_TYPE_FILE;

public class JfsOssBackend {
    private static final Logger LOG = LoggerFactory.getLogger(JfsOssBackend.class);
    private JfsRequestOptions options;
    private OSS ossClient;

    public JfsOssBackend(JfsRequestOptions options) {
        this.options = options;
        this.ossClient = OssClientFactory.getInstance().getClient(options);
    }

    public JfsStatus getStatus(String path, OssFileStatus status) {
        if (path == null || path.isEmpty() || path.equals("/")) {
            status.setPath(path);
            status.setFileType(FILE_TYPE_DIRECTORY);
            status.setSize(0);
            return JfsStatus.OK();
        }

        if (!path.endsWith("/")) {
            JfsStatus jfsStatus = JfsStatus.OK();
            // object is file
            SimplifiedObjectMeta objectMeta = null;
            try {
                objectMeta = ossClient.getSimplifiedObjectMeta(options.getBucket(), path);
            } catch (RuntimeException e) {
                jfsStatus = JfsStatus.fromException(e);
            }
            if (jfsStatus.isOk()) {
                assert objectMeta != null;
                status.setPath(path);
                status.setSize(objectMeta.getSize());
                status.setFileType(FILE_TYPE_FILE);
                return jfsStatus;
            } else if (jfsStatus.getCode() != JfsStatus.FILE_NOT_FOUND_ERROR) {
                return jfsStatus;
            }
        }

        // object is directory marker
        String tmpPath = JfsUtil.maybeAddTrailingSlash(path);
        {
            JfsStatus jfsStatus = JfsStatus.OK();
            SimplifiedObjectMeta objectMeta = null;
            try {
                objectMeta = ossClient.getSimplifiedObjectMeta(options.getBucket(), tmpPath);
            } catch (RuntimeException e) {
                jfsStatus = JfsStatus.fromException(e);
            }
            if (jfsStatus.isOk()) {
                status.setPath(path);
                status.setSize(0);
                status.setFileType(FILE_TYPE_DIRECTORY);
                return jfsStatus;
            } else if (jfsStatus.getCode() != JfsStatus.FILE_NOT_FOUND_ERROR) {
                return jfsStatus;
            }
        }

        // object is directory and no marker
        {
            JfsStatus jfsStatus = JfsStatus.OK();
            ObjectListing objectListing = null;
            try {
                objectListing = ossClient.listObjects(new ListObjectsRequest(options.getBucket())
                        .withMaxKeys(1).withPrefix(tmpPath));
            } catch (RuntimeException e) {
                jfsStatus = JfsStatus.fromException(e);
            }

            if (jfsStatus.isOk()) {
                assert objectListing != null;
                if (!objectListing.getObjectSummaries().isEmpty()) {
                    status.setPath(path);
                    status.setSize(0);
                    status.setFileType(FILE_TYPE_DIRECTORY);
                    return jfsStatus;
                }
            } else if (jfsStatus.getCode() != JfsStatus.FILE_NOT_FOUND_ERROR) {
                return jfsStatus;
            }
        }

        // file not found
        return JfsStatus.fileNotFound("File not found: " + path + " in bucket: " + options.getBucket());
    }

    public JfsStatus list(String path, List<OssFileStatus> statusList, boolean recursive) {
        {
            OssFileStatus ossFileStatus = new OssFileStatus();
            JfsStatus jfsStatus = getStatus(path, ossFileStatus);
            if (!jfsStatus.isOk()) {
                return jfsStatus;
            }
            if (ossFileStatus.isFile()) {
                statusList.add(ossFileStatus);
                return jfsStatus;
            }
        }


        String tmpPath = JfsUtil.maybeAddTrailingSlash(path);
        {
            JfsStatus jfsStatus = JfsStatus.OK();
            String marker = "";
            boolean isTruncated;
            String delimiter = "";
            if (!recursive) {
                delimiter = "/";
            }
            do {
                ObjectListing objectListing = null;
                try{
                    objectListing = ossClient.listObjects(new ListObjectsRequest(options.getBucket())
                            .withPrefix(tmpPath).withMaxKeys(1000).withMarker(marker).withDelimiter(delimiter));
                } catch (RuntimeException e) {
                    jfsStatus = JfsStatus.fromException(e);
                }

                if (!jfsStatus.isOk()) {
                    LOG.debug("Failed to list objects with prefix {} in bucket: {}", tmpPath, options.getBucket());
                    return jfsStatus;
                }

                assert objectListing != null;
                marker = objectListing.getNextMarker();
                isTruncated = objectListing.isTruncated();
                if (!objectListing.getObjectSummaries().isEmpty()) {
                    for (OSSObjectSummary objectMeta : objectListing.getObjectSummaries()) {
                        if (objectMeta.getKey().equals(tmpPath)) {
                            LOG.info("Skip oss key itself {}", tmpPath);
                            continue;
                        }
                        OssFileStatus singleOssFileStatus = getOssFileStatus(objectMeta);
                        statusList.add(singleOssFileStatus);
                    }
                }
                if (!objectListing.getCommonPrefixes().isEmpty()) {
                    for (String commonPrefix : objectListing.getCommonPrefixes()) {
                        OssFileStatus singleOssFileStatus = new OssFileStatus();
                        singleOssFileStatus.setPath(commonPrefix);
                        singleOssFileStatus.setFileType(FILE_TYPE_DIRECTORY);
                        singleOssFileStatus.setSize(0);
                        statusList.add(singleOssFileStatus);
                    }
                }
            } while(isTruncated);
        }

        if (statusList.isEmpty()) {
            return JfsStatus.fileNotFound("File not found: " + path + " in bucket: " + options.getBucket());
        } else {
            return JfsStatus.OK();
        }
    }

    public JfsOssFileInputStream open(String path) {
        return new JfsOssFileInputStream(path, options, ossClient);
    }

    private static @NotNull OssFileStatus getOssFileStatus(OSSObjectSummary objectMeta) {
        OssFileStatus singleOssFileStatus = new OssFileStatus();
        singleOssFileStatus.setPath(objectMeta.getKey());
        singleOssFileStatus.setSize(objectMeta.getSize());
        if (objectMeta.getSize() == 0 && objectMeta.getKey().endsWith("/")) {
            singleOssFileStatus.setFileType(FILE_TYPE_DIRECTORY);
        } else {
            singleOssFileStatus.setFileType(FILE_TYPE_FILE);
        }
        return singleOssFileStatus;
    }

    public JfsStatus put(String path, byte[] buf) {
        JfsStatus jfsStatus = JfsStatus.OK();
        try {
            PutObjectRequest putObjectRequest = new PutObjectRequest(options.getBucket(), path,
                    new ByteArrayInputStream(buf));
            PutObjectResult result = ossClient.putObject(putObjectRequest);
        } catch (RuntimeException e) {
            jfsStatus = JfsStatus.fromException(e);
        }

        return jfsStatus;
    }

    public JfsStatus put(String path, String localFilePath) {
        JfsStatus jfsStatus = JfsStatus.OK();
        try {
            PutObjectRequest putObjectRequest = new PutObjectRequest(options.getBucket(), path,
                    new FileInputStream(localFilePath));
            PutObjectResult result = ossClient.putObject(putObjectRequest);
        } catch (RuntimeException e) {
            jfsStatus = JfsStatus.fromException(e);
        } catch (FileNotFoundException e) {
            LOG.warn("Local file not found: {} for putObject", localFilePath);
            jfsStatus = JfsStatus.fromException(e);
        }

        return jfsStatus;
    }

    /**
     * @return JfsStatus.result is uploadId
     */
    public JfsStatus initUpload(String path) {
        JfsStatus jfsStatus = JfsStatus.OK();

        try {
            InitiateMultipartUploadRequest request = new InitiateMultipartUploadRequest(options.getBucket(), path);
            InitiateMultipartUploadResult result = ossClient.initiateMultipartUpload(request);
            jfsStatus.setResult(result.getUploadId());
        } catch (RuntimeException e) {
            jfsStatus = JfsStatus.fromException(e);
        }
        return jfsStatus;
    }

    /**
     * @return JfsStatus.result is etag
     */
    public JfsStatus uploadPart(String path, String uploadId, int partNumber, long partSize, String localFilePath) {
        JfsStatus jfsStatus = JfsStatus.OK();
        try {
            UploadPartRequest request = new UploadPartRequest();
            request.setBucketName(options.getBucket());
            request.setKey(path);
            request.setUploadId(uploadId);
            request.setPartNumber(partNumber);
            request.setInputStream(new FileInputStream(localFilePath));
            request.setPartSize(partSize);
            UploadPartResult result = ossClient.uploadPart(request);
            jfsStatus.setResult(result.getETag());
        } catch (RuntimeException e) {
            jfsStatus = JfsStatus.fromException(e);
        } catch (FileNotFoundException e) {
            LOG.warn("Local file not found: {} for uploadPart", localFilePath);
            jfsStatus = JfsStatus.fromException(e);
        }
        return jfsStatus;
    }

    /**
     * @return JfsStatus.result is etag
     */
    public JfsStatus uploadPart(String path, String uploadId, int partNumber, long partSize, byte[] data) {
        JfsStatus jfsStatus = JfsStatus.OK();
        try {
            UploadPartRequest request = new UploadPartRequest();
            request.setBucketName(options.getBucket());
            request.setKey(path);
            request.setUploadId(uploadId);
            request.setPartNumber(partNumber);
            request.setInputStream(new ByteArrayInputStream(data));
            request.setPartSize(partSize);
            UploadPartResult result = ossClient.uploadPart(request);
            jfsStatus.setResult(result.getETag());
        } catch (RuntimeException e) {
            jfsStatus = JfsStatus.fromException(e);
        }
        return jfsStatus;
    }

    public JfsStatus completeUpload(String path, String uploadId, List<PartETag> partETags) {
        JfsStatus jfsStatus = JfsStatus.OK();
        try {
            CompleteMultipartUploadRequest request = new CompleteMultipartUploadRequest(options.getBucket(), path, uploadId, partETags);
            CompleteMultipartUploadResult result = ossClient.completeMultipartUpload(request);
        } catch (RuntimeException e) {
            jfsStatus = JfsStatus.fromException(e);
        }
        return jfsStatus;
    }

    public JfsStatus abortUpload(String path, String uploadId) {
        JfsStatus jfsStatus = JfsStatus.OK();
        try {
            AbortMultipartUploadRequest request = new AbortMultipartUploadRequest(options.getBucket(), path, uploadId);
            ossClient.abortMultipartUpload(request);
        } catch (RuntimeException e) {
            jfsStatus = JfsStatus.fromException(e);
        }
        return jfsStatus;
    }
}
