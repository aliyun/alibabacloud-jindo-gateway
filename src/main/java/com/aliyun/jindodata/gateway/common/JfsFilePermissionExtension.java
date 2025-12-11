package com.aliyun.jindodata.gateway.common;

public class JfsFilePermissionExtension extends JfsFilePermission {
    public static final short ACL_BIT = 1 << 12;  // 0x1000 (4096)

    public static final short ENCRYPTED_BIT = 1 << 13;  // 0x2000 (8192)

    private boolean aclBit;
    private boolean encryptedBit;

    public JfsFilePermissionExtension() {
        super();
        this.aclBit = false;
        this.encryptedBit = false;
    }

    public JfsFilePermissionExtension(JfsFilePermission permission, boolean hasAcl, boolean isEncrypted) {
        super(permission.toShort());
        this.aclBit = hasAcl;
        this.encryptedBit = isEncrypted;
    }

    public JfsFilePermissionExtension(short extendedPermission) {
        super((short)(extendedPermission & ~(ACL_BIT | ENCRYPTED_BIT)));
        this.aclBit = (extendedPermission & ACL_BIT) != 0;
        this.encryptedBit = (extendedPermission & ENCRYPTED_BIT) != 0;
    }

    public JfsFilePermissionExtension(JfsFilePermissionExtension other) {
        super(other.toShort());
        this.aclBit = other.aclBit;
        this.encryptedBit = other.encryptedBit;
    }

    public boolean getAclBit() {
        return aclBit;
    }

    public boolean getEncryptedBit() {
        return encryptedBit;
    }

    public void setAclBit(boolean aclBit) {
        this.aclBit = aclBit;
    }

    public void setEncryptedBit(boolean encryptedBit) {
        this.encryptedBit = encryptedBit;
    }

    public short toExtendedShort() {
        short basePermission = super.toShort();
        short extended = (short) (basePermission | (aclBit ? ACL_BIT : 0) | (encryptedBit ? ENCRYPTED_BIT : 0));
        return extended;
    }

    public void fromExtendedShort(short extendedPermission) {
        // Extract base permission part (remove extension flag bits)
        short basePermission = (short) (extendedPermission & ~(ACL_BIT | ENCRYPTED_BIT));
        super.fromShort(basePermission);
        
        // Extract extension flag bits
        this.aclBit = (extendedPermission & ACL_BIT) != 0;
        this.encryptedBit = (extendedPermission & ENCRYPTED_BIT) != 0;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        JfsFilePermissionExtension other = (JfsFilePermissionExtension) obj;
        return super.equals(obj) && 
               this.aclBit == other.aclBit && 
               this.encryptedBit == other.encryptedBit;
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(
                super.toShort(),
                aclBit,
                encryptedBit
        );
    }

    @Override
    public String toString() {
        return "JfsFilePermissionExtension{" +
                "permission=" + super.toString() +
                ", aclBit=" + aclBit +
                ", encryptedBit=" + encryptedBit +
                ", extendedShort=" + String.format("0x%04X", toExtendedShort()) +
                '}';
    }

    public String toReadableString() {
        return "JfsFilePermissionExtension{" +
                "permission='" + super.getSymbol() + '\'' +
                ", octal='" + super.getOctalString() + '\'' +
                ", aclBit=" + aclBit +
                ", encryptedBit=" + encryptedBit +
                ", extendedShort=" + String.format("0x%04X", toExtendedShort()) +
                '}';
    }
}
