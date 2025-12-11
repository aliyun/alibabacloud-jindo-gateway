package com.aliyun.jindodata.gateway.common;

public class JfsFilePermission {
    private static final int READ = 4;    // r--
    private static final int WRITE = 2;   // -w-
    private static final int EXECUTE = 1; // --x

    public static final short DEFAULT_DIR_PERMISSION = 0755;   // drwxr-xr-x
    public static final short DEFAULT_FILE_PERMISSION = 0666;  // -rw-rw-rw-

    private int userAction = 0;      // User action (0-7)
    private int groupAction = 0;     // Group action (0-7)
    private int otherAction = 0;     // Other action (0-7)
    private boolean stickyBit = false; // Sticky bit

    public JfsFilePermission() {
    }

    public JfsFilePermission(short mode) {
        fromShort(mode);
    }

    public JfsFilePermission(int userAction, int groupAction, int otherAction) {
        this(userAction, groupAction, otherAction, false);
    }

    public JfsFilePermission(int userAction, int groupAction, int otherAction, boolean stickyBit) {
        set(userAction, groupAction, otherAction, stickyBit);
    }

    public int getUserAction() {
        return userAction;
    }

    public int getGroupAction() {
        return groupAction;
    }

    public int getOtherAction() {
        return otherAction;
    }

    public boolean getStickyBit() {
        return stickyBit;
    }

    public void fromShort(short mode) {
        this.userAction = (mode >> 6) & 0x7;
        this.groupAction = (mode >> 3) & 0x7;
        this.otherAction = mode & 0x7;
        this.stickyBit = (mode & 01000) != 0;
    }

    public void set(int userAction, int groupAction, int otherAction, boolean stickyBit) {
        this.userAction = userAction & 0x7;
        this.groupAction = groupAction & 0x7;
        this.otherAction = otherAction & 0x7;
        this.stickyBit = stickyBit;
    }

    public void set(int userAction, int groupAction, int otherAction) {
        set(userAction, groupAction, otherAction, false);
    }

    public void setStickyBit(boolean stickyBit) {
        this.stickyBit = stickyBit;
    }

    public short toShort() {
        short result = 0;
        result |= (userAction & 0x7) << 6;
        result |= (groupAction & 0x7) << 3;
        result |= (otherAction & 0x7);
        if (stickyBit) {
            result |= 01000;
        }
        return result;
    }

    public short toExtendedShort() {
        return toShort();
    }

    public String getSymbol() {
        return getSymbolFromAction(userAction) +
               getSymbolFromAction(groupAction) +
               getSymbolFromAction(otherAction);
    }

    public String getSymbolFromAction(int action) {
        StringBuilder sb = new StringBuilder(3);
        sb.append((action & READ) != 0 ? 'r' : '-');
        sb.append((action & WRITE) != 0 ? 'w' : '-');
        sb.append((action & EXECUTE) != 0 ? 'x' : '-');
        return sb.toString();
    }

    public String getOctalString() {
        short mode = toShort();
        return String.format("%04o", mode);
    }

    public static JfsFilePermission getDefault() {
        return new JfsFilePermission(DEFAULT_DIR_PERMISSION);
    }

    public static JfsFilePermission getFileDefault() {
        return new JfsFilePermission(DEFAULT_FILE_PERMISSION);
    }

    @Override
    public String toString() {
        return "JfsFilePermission{" +
                "octal='" + getOctalString() + '\'' +
                ", symbol='" + getSymbol() + '\'' +
                ", stickyBit=" + stickyBit +
                '}';
    }
}
