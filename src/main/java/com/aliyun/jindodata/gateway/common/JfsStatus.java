package com.aliyun.jindodata.gateway.common;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSSException;

public class JfsStatus {
    public static final int OK = 0;
    public static final int FILE_NOT_FOUND_ERROR = 30001;
    public static final int CORRUPTION_ERROR = 30002;
    public static final int NOT_SUPPORTED_ERROR = 30003;
    public static final int INVALID_ARGUMENT_ERROR = 30004;
    public static final int IO_ERROR = 30005;
    public static final int NOT_REPLICATED_YET = 30006;
    public static final int INVALID_PATH = 30007;
    public static final int STANDBY = 30008;
    public static final int NO_PERMISSION = 30009;
    public static final int NOT_ENOUGH_REPLICAS = 30010;
    public static final int NOT_REGISTERED_NODE = 30011;
    public static final int FAILED = 30012;
    public static final int LEASE_EXPIRED = 30013;
    public static final int UNSUPPORTED_OPERATION = 30014;
    public static final int ALREADY_BEING_CREATED = 30015;
    public static final int RECOVERY_IN_PROGRESS = 30016;
    public static final int RETRIABLE_ERROR = 30017;
    public static final int ACCESS_CONTROL_ERROR = 30018;
    public static final int UNRESOLVED_PATH = 30019;
    public static final int PARENT_NOT_DIRECTORY = 30020;
    public static final int TRAVERSE_ACCESS_CONTROL_ERROR = 30021;
    public static final int ACL_ERROR = 30022;
    public static final int INVALID_TOPOLOGY = 30023;
    public static final int UNRESOLVED_TOPOLOGY = 30024;
    public static final int DISALLOWED_DATANODE = 30025;
    public static final int FILE_ALREADY_EXISTS = 30026;
    public static final int SAFE_MODE_ERROR = 30027;
    public static final int PATH_IS_NOT_EMPTY_DIRECTORY = 30028;
    public static final int SNAPSHOT_EXCEPTION = 30029;
    public static final int QUOTA_EXCEEDED_EXCEPTION = 30030;
    public static final int ASSERTION_ERROR = 30031;
    public static final int ILLEGAL_STATE = 30032;
    public static final int FEATURE_NOT_FOUND = 30033;
    public static final int SNAPSHOT_ACCESS_CONTROL_ERROR = 30034;
    public static final int PATH_COMPONENT_TOO_LONG = 30035;
    public static final int MAX_DIRECTORY_ITEMS_EXCEEDED = 30036;
    public static final int INTERRUPT = 30037;
    public static final int EOF_ERROR = 30038;
    public static final int CLOSED_CHANNEL = 30039;
    public static final int UNREGISTERED_NODE = 30040;
    public static final int INCORRECT_VERSION = 30041;
    public static final int ASYNCHRONOUS_CLOSE = 30042;
    public static final int DISK_ERROR = 30043;
    public static final int INVALID_CHECKSUM_SIZE_EXCEPTION = 30044;
    public static final int CHECKSUM_EXCEPTION = 30045;
    public static final int INCONSISTENT_FS_STATE = 30046;
    public static final int NUMBER_FORMAT_EXCEPTION = 30047;
    public static final int REPLICA_NOT_FOUND = 30048;
    public static final int IOE_TO_SOCKET_EXCEPTION = 30049;
    public static final int REMOTE_ERROR = 30050;
    public static final int BP_SERVICE_ACTOR_ACTION_EXCEPTION = 30051;
    public static final int MUST_STOP_EXISTING_WRITER = 30052;
    public static final int REPLICA_ALREADY_EXISTS = 30053;
    public static final int DISK_OUT_OF_SPACE = 30054;
    public static final int UNEXPECTED_REPLICA_STATE = 30055;
    public static final int INVALID_BLOCK_TOKEN = 30056;
    public static final int UNRESOLVED_LINK = 30057;
    public static final int RPC_NO_SUCH_METHOD = 30058;
    public static final int SASL_ERROR = 30059;
    
    private int code;
    private String message;
    private Object result;

    public void setOk(boolean ok) {
        this.ok = ok;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public void setCode(int code) {
        this.code = code;
    }

    private boolean ok;
    
    public JfsStatus(int code, String message, boolean ok) {
        this.code = code;
        this.message = message;
        this.ok = ok;
    }
    
    public static JfsStatus OK() {
        return new JfsStatus(OK, "OK", true);
    }
    
    public static JfsStatus invalidArgument(String message) {
        return new JfsStatus(INVALID_ARGUMENT_ERROR, message, false);
    }
    
    public static JfsStatus ioError(String message) {
        return new JfsStatus(IO_ERROR, message, false);
    }

    public static JfsStatus notSupported(String message) {
        return new JfsStatus(NOT_SUPPORTED_ERROR, message, false);
    }

    public static JfsStatus corruption(String message) {
        return new JfsStatus(CORRUPTION_ERROR, message, false);
    }

    public static JfsStatus invalidPath(String message) {
        return new JfsStatus(INVALID_PATH, message, false);
    }

    public static JfsStatus fileNotFound(String message) {
        return new JfsStatus(FILE_NOT_FOUND_ERROR, message, false);
    }

    public static JfsStatus eof(String message) {
        return new JfsStatus(EOF_ERROR, message, false);
    }

    public int getCode() {
        return code;
    }
    
    public String getMessage() {
        return message;
    }
    
    public boolean isOk() {
        return ok;
    }
    
    @Override
    public String toString() {
        return "[E" + code + "] " + message;
    }

    public static JfsStatus fromException(Exception e) {
        if (e instanceof OSSException) {
            return fromOssException((OSSException) e);
        } else if (e instanceof ClientException) {
            return JfsStatus.ioError(e.getMessage());
        }

        return JfsStatus.ioError(e.getMessage());
    }

    public static JfsStatus fromOssException(OSSException oe) {
        switch (oe.getErrorCode()) {
            case "NoSuchKey":
                return JfsStatus.fileNotFound(formatOssExceptionMsg(oe));
            default:
                return JfsStatus.ioError(formatOssExceptionMsg(oe));
        }
    }

    public static String formatOssExceptionMsg(OSSException oe) {
        return oe.getErrorMessage()
                + " [Error Code]: " + oe.getErrorCode()
                + " [Request ID]: " + oe.getRequestId();
    }

    public void setResult(Object result) {
        this.result = result;
    }

    public Object getResult() {
        return result;
    }
}