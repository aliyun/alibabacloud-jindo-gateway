package com.aliyun.jindodata.gateway.io;

import com.aliyun.jindodata.gateway.common.JfsRequestOptions;
import com.aliyun.jindodata.gateway.common.JfsStatus;
import com.aliyun.oss.OSS;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.OSSObject;

import java.io.IOException;
import java.io.InputStream;

public class JfsOssFileInputStream {

    private final String path;
    private final JfsRequestOptions options;
    private final OSS ossClient;

    public JfsOssFileInputStream(String path, JfsRequestOptions options, OSS ossClient) {
        this.path = path;
        this.options = options;
        this.ossClient = ossClient;
    }

    public JfsStatus readFully(byte[] buffer, long offset, int length) throws IOException {
        return readFully(buffer, offset, length, 0);
    }

    public JfsStatus readFully(byte[] buffer, long offset, int length, int offsetInBuffer) throws IOException {
        InputStream in = null;
        try {
            GetObjectRequest getObjectRequest = new GetObjectRequest(options.getBucket(), path);
            getObjectRequest.setRange(offset, offset + length - 1);
            OSSObject ossObject = ossClient.getObject(getObjectRequest);

            in = ossObject.getObjectContent();
            int totalRead = 0;
            for (int n = 0; n != -1 && totalRead < length; ) {
                n = in.read(buffer, totalRead + offsetInBuffer, length - totalRead);
                if (n > 0) {
                    totalRead += n;
                }
            }
            ossObject.close();
            if (totalRead < length) {
                return JfsStatus.eof("EOF: expected to read " + length
                        + "bytes, but got " + totalRead + ". offset: " + offset);
            }
            return JfsStatus.OK();
        } catch (Exception e) {
            return JfsStatus.fromException(e);
        } finally {
            if (in != null) {
                in.close();
            }
        }
    }
}
