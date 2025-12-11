package com.aliyun.jindodata.gateway.common;

import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.server.blockmanagement.UnresolvedTopologyException;
import org.apache.hadoop.hdfs.server.namenode.JournalManager;
import org.apache.hadoop.hdfs.server.namenode.LeaseExpiredException;
import org.apache.hadoop.hdfs.server.namenode.NotReplicatedYetException;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.ipc.RpcNoSuchMethodException;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.sasl.SaslException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.io.FileNotFoundException;

public class JfsUtil {
    private static final Logger LOG = LoggerFactory.getLogger(JfsUtil.class.getName());

    public static String guessRegion(String endpoint) {
        if (endpoint == null || endpoint.isEmpty()) {
            return null;
        }

        // check .oss-dls.aliyuncs.com suffix
        {
            int pos = endpoint.indexOf(".oss-dls.aliyuncs.com");
            if (pos != -1) {
                String region = endpoint.substring(0, pos);
                int dotPos = region.lastIndexOf(".");
                if (dotPos != -1) {
                    region = region.substring(dotPos + 1);
                }
                return region;
            }
        }

        // check .dls.aliyuncs.com suffix
        {
            int pos = endpoint.indexOf(".dls.aliyuncs.com");
            if (pos != -1) {
                String region = endpoint.substring(0, pos);
                int dotPos = region.lastIndexOf(".");
                if (dotPos != -1) {
                    region = region.substring(dotPos + 1);
                }
                return region;
            }
        }

        return "";
    }

    public static String getCurrentUser() {
        try {
            UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
            if (ugi != null) {
                String shortName = ugi.getShortUserName();
                if (shortName != null && !shortName.isEmpty()) {
                    LOG.debug("Got current user from UserGroupInformation: {}", shortName);
                    return shortName;
                }
            }
        } catch (IOException e) {
            LOG.debug("Failed to get user from UserGroupInformation: {}", e.getMessage());
        }

        // user.name
        String userName = System.getProperty("user.name");
        if (userName != null && !userName.isEmpty()) {
            LOG.debug("Got current user from system property: {}", userName);
            return userName;
        }

        LOG.warn("Could not determine current user");
        return "unknown";
    }

    public static String getCurrentUserShort() {
        String user = getCurrentUser();
        if (user == null || user.isEmpty()) {
            return "unknown";
        }

        int atPos = user.indexOf("@");
        if (atPos != -1) {
            return user.substring(0, atPos);
        }

        int slashPos = user.lastIndexOf("\\");
        if (slashPos != -1) {
            return user.substring(slashPos + 1);
        }

        return user;
    }

    /**
     * encode path's separator / to %2F
     * @param path
     * @return
     */
    public static String urlEncode(String path) {
        return urlEncode(path, true);
    }

    public static String urlEncode(String value, boolean encodeSeparator) {
        if (value == null || value.isEmpty()) {
            return "";
        }
        try {
            String encoded = URLEncoder.encode(value, "UTF-8")
                    .replace("+", "%20")
                    .replace("*", "%2A")
                    .replace("%7E", "~");
            if (!encodeSeparator) {
                encoded = encoded.replace("%2F", "/");
            }
            return encoded;
        } catch (UnsupportedEncodingException e) {
            return value;
        }
    }

    public static String decode(String input) {
        if (input == null) {
            return null;
        }

        // 0-9: 0-9, A-F: 10-15, a-f: 10-15, others: -1
        final byte[] hexTable = new byte[256];
        for (int i = 0; i < 256; i++) {
            hexTable[i] = -1;
        }
        // 0-9
        for (int i = 0; i < 10; i++) {
            hexTable['0' + i] = (byte) i;
        }
        // A-F
        for (int i = 0; i < 6; i++) {
            hexTable['A' + i] = (byte) (10 + i);
        }
        // a-f
        for (int i = 0; i < 6; i++) {
            hexTable['a' + i] = (byte) (10 + i);
        }

        StringBuilder output = new StringBuilder();
        int i = 0;
        while (i < input.length()) {
            char c = input.charAt(i);
            if (c == '%') {
                if (i + 2 >= input.length()) {
                    return null;
                }

                char c1 = input.charAt(i + 1);
                char c2 = input.charAt(i + 2);

                byte v1 = hexTable[c1 & 0xFF];
                byte v2 = hexTable[c2 & 0xFF];

                if (v1 < 0 || v2 < 0) {
                    return null;
                }

                byte decodedByte = (byte) ((v1 << 4) | v2);
                output.append((char) (decodedByte & 0xFF));
                i += 3;
            } else if (c == '+') {
                output.append(' ');
                i++;
            } else {
                output.append(c);
                i++;
            }
        }

        return output.toString();
    }

    public static HdfsFileStatus convert2HdfsFileStatus(JfsFileStatus jfsFileStatus) {
        return new HdfsFileStatus(
                jfsFileStatus.getFileSize(),
                jfsFileStatus.isDir(),
                jfsFileStatus.getBlockReplication(),
                jfsFileStatus.getBlockSize(),
                jfsFileStatus.getMtime(),
                jfsFileStatus.getAtime(),
                new FsPermission(jfsFileStatus.getPermission().toShort()),
                jfsFileStatus.getOwner(),
                jfsFileStatus.getOwnerGroup(),
                jfsFileStatus.getSymlink() == null ? null : jfsFileStatus.getSymlink().getBytes(),
                jfsFileStatus.getName().getBytes(StandardCharsets.UTF_8),
                jfsFileStatus.getFileIdAsLong(),
                jfsFileStatus.getChildrenNum(),
                null,
                (byte) jfsFileStatus.getStoragePolicy()
        );
    }

    public static HdfsLocatedFileStatus convert2HdfsLocatedFileStatus(JfsFileStatus jfsFileStatus,
                                                               LocatedBlocks locatedBlocks) {
        return new HdfsLocatedFileStatus(
                jfsFileStatus.getFileSize(),
                jfsFileStatus.isDir(),
                jfsFileStatus.getBlockReplication(),
                jfsFileStatus.getBlockSize(),
                jfsFileStatus.getMtime(),
                jfsFileStatus.getAtime(),
                new FsPermission(jfsFileStatus.getPermission().toShort()),
                jfsFileStatus.getOwner(),
                jfsFileStatus.getOwnerGroup(),
                jfsFileStatus.getSymlink() == null ? null : jfsFileStatus.getSymlink().getBytes(),
                jfsFileStatus.getName().getBytes(StandardCharsets.UTF_8),
                jfsFileStatus.getFileIdAsLong(),
                locatedBlocks,
                jfsFileStatus.getChildrenNum(),
                null,
                (byte) 0
        );
    }

    public static String makeErrorRespone(String host, int port, JfsStatus status) {
        return "[" + host + ":" + port + "] " + "[E" + status.getCode() + "] " + status.getMessage();
    }

    public static void throwException(JfsStatus status) throws IOException {
        if (!status.isOk()) {
            String errorMsg = status.toString();
            switch (status.getCode()) {
                case JfsStatus.OK:
                    return;
                case JfsStatus.FILE_NOT_FOUND_ERROR:
                    throw new FileNotFoundException(errorMsg);
                case JfsStatus.CORRUPTION_ERROR:
                    throw new JournalManager.CorruptionException(errorMsg);
                case JfsStatus.NOT_SUPPORTED_ERROR:
                case JfsStatus.UNSUPPORTED_OPERATION:
                    throw new UnsupportedOperationException(errorMsg);
                case JfsStatus.INVALID_ARGUMENT_ERROR:
                    throw new IllegalArgumentException(errorMsg);
                case JfsStatus.IO_ERROR:
                    throw new IOException(errorMsg);
                case JfsStatus.INVALID_PATH:
                    throw new InvalidPathException(errorMsg);
                case JfsStatus.NO_PERMISSION:
                case JfsStatus.ACCESS_CONTROL_ERROR:
                    throw new AccessControlException(errorMsg);
                case JfsStatus.FILE_ALREADY_EXISTS:
                    throw new FileAlreadyExistsException(errorMsg);
                case JfsStatus.NOT_REPLICATED_YET:
                    throw new NotReplicatedYetException(errorMsg);
                case JfsStatus.STANDBY:
                    throw new StandbyException(errorMsg);
                case JfsStatus.LEASE_EXPIRED:
                    throw new LeaseExpiredException(errorMsg);
                case JfsStatus.ALREADY_BEING_CREATED:
                    throw new AlreadyBeingCreatedException(errorMsg);
                case JfsStatus.RECOVERY_IN_PROGRESS:
                    throw new RecoveryInProgressException(errorMsg);
                case JfsStatus.RETRIABLE_ERROR:
                    throw new RetriableException(errorMsg);
                case JfsStatus.UNRESOLVED_PATH:
                    throw new UnresolvedPathException(errorMsg);
                case JfsStatus.PARENT_NOT_DIRECTORY:
                    throw new ParentNotDirectoryException(errorMsg);
                case JfsStatus.ACL_ERROR:
                    throw new AclException(errorMsg);
                case JfsStatus.INVALID_TOPOLOGY:
                    throw new NetworkTopology.InvalidTopologyException(errorMsg);
                case JfsStatus.UNRESOLVED_TOPOLOGY:
                    throw new UnresolvedTopologyException(errorMsg);
                case JfsStatus.SNAPSHOT_EXCEPTION:
                    throw new SnapshotException(errorMsg);
                case JfsStatus.SNAPSHOT_ACCESS_CONTROL_ERROR:
                    throw new SnapshotAccessControlException(errorMsg);
                case JfsStatus.QUOTA_EXCEEDED_EXCEPTION:
                    throw new NSQuotaExceededException(errorMsg);
                case JfsStatus.INVALID_BLOCK_TOKEN:
                    throw new SecretManager.InvalidToken(errorMsg);
                case JfsStatus.UNRESOLVED_LINK:
                    throw new UnresolvedLinkException(errorMsg);
                case JfsStatus.RPC_NO_SUCH_METHOD:
                    throw new RpcNoSuchMethodException(errorMsg);
                case JfsStatus.SASL_ERROR:
                    throw new SaslException(errorMsg);
                default:
                    throw new IOException(errorMsg);
            }
        }
    }

    public static void setFieldValue(Object obj, String fieldName, Object value)
            throws NoSuchFieldException, IllegalAccessException {
        if (obj == null) {
            throw new IllegalArgumentException("Object cannot be null");
        }
        if (fieldName == null || fieldName.isEmpty()) {
            throw new IllegalArgumentException("Field name cannot be null or empty");
        }
        
        Class<?> clazz = obj.getClass();
        Field field = null;

        while (clazz != null) {
            try {
                field = clazz.getDeclaredField(fieldName);
                break;
            } catch (NoSuchFieldException e) {
                clazz = clazz.getSuperclass();
            }
        }
        
        if (field == null) {
            throw new NoSuchFieldException("Field '" + fieldName + "' not found in class hierarchy");
        }

        field.setAccessible(true);
        field.set(obj, value);
    }

    public static Object getFieldValue(Object obj, String fieldName) 
            throws NoSuchFieldException, IllegalAccessException {
        if (obj == null) {
            throw new IllegalArgumentException("Object cannot be null");
        }
        if (fieldName == null || fieldName.isEmpty()) {
            throw new IllegalArgumentException("Field name cannot be null or empty");
        }
        
        Class<?> clazz = obj.getClass();
        Field field = null;

        while (clazz != null) {
            try {
                field = clazz.getDeclaredField(fieldName);
                break;
            } catch (NoSuchFieldException e) {
                clazz = clazz.getSuperclass();
            }
        }
        
        if (field == null) {
            throw new NoSuchFieldException("Field '" + fieldName + "' not found in class hierarchy");
        }

        field.setAccessible(true);
        return field.get(obj);
    }

    public static String maybeAddTrailingSlash(String path) {
        if (!path.endsWith("/")) {
            return path + "/";
        }
        return path;
    }

    public static String concatPath(String path1, String path2) {
        StringBuilder sb = new StringBuilder();
        if (path1.endsWith("/")) {
            sb.append(path1, 0, path1.length() - 1);
        } else {
            sb.append(path1);
        }
        if (path2.startsWith("/")) {
            sb.append(path2);
        } else {
            sb.append("/").append(path2);
        }
        return sb.toString();
    }

    public static int getDefaultConcurrency() {
        int cpuCores = Runtime.getRuntime().availableProcessors();
        return Math.max(cpuCores, 16);
    }
}
