package com.aliyun.jindodata.gateway.common;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;


import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static com.aliyun.jindodata.gateway.common.JfsConstant.*;

public class JfsResponseXml {
    private static final String RESPONSE_NODE_KEY = "response";
    private static final Logger LOG = LoggerFactory.getLogger(JfsResponseXml.class);
    private Element responseNode;

    public JfsResponseXml() {
    }

    public JfsStatus parseResponse(String response) throws ParserConfigurationException, IOException, SAXException {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document document = builder.parse(new ByteArrayInputStream(response.getBytes(StandardCharsets.UTF_8)));

        NodeList nodeList = document.getElementsByTagName(RESPONSE_NODE_KEY);


        if (nodeList.getLength() > 0) {
            Node node = nodeList.item(0);
            responseNode = (Element) node;
        } else {
            return JfsStatus.corruption("Missing response key");
        }

        return JfsStatus.OK();
    }

    public Element getResponseNode() {
        return responseNode;
    }

    public static Element getNode(Element node, String key) {
        NodeList nodeList = node.getElementsByTagName(key);
        if (nodeList.getLength() == 0) {
            return null;
        }
        return (Element) nodeList.item(0);
    }

    public static String getNodeString(Element parent, String key, String defaultValue,
                                boolean isRequired) throws IOException {
        String res = getElementText(parent, key);
        if (null != res) {
            return res;
        }
        if (isRequired) {
            LOG.warn("Missing required String XML element: {}", key);
            throw new IOException("Missing required XML element: " + key);
        }
        return defaultValue;
    }

    public static int getNodeInt(Element parent, String key, int defaultValue,
                          boolean isRequired) throws IOException {
        String res = getElementText(parent, key);
        if (null != res && !res.isEmpty()) {
            try {
                return Integer.parseInt(res);
            } catch (NumberFormatException e) {
                LOG.warn("Failed to parse int value for key: {}, value: {}", key, res);
                throw new IOException("Invalid integer value for element: " + key + ", value: " + res);
            }
        }
        if (isRequired) {
            LOG.warn("Missing required int XML element: {}", key);
            throw new IOException("Missing required XML element: " + key);
        }
        return defaultValue;
    }

    public static boolean getNodeBool(Element parent, String key, boolean defaultValue,
                               boolean isRequired) throws IOException {
        String res = getElementText(parent, key);
        if (null != res && !res.isEmpty()) {
            String value = res.trim().toLowerCase();
            if ("true".equals(value)) {
                return true;
            } else if ("false".equals(value)) {
                return false;
            } else {
                LOG.warn("Invalid boolean value for key: " + key + ", value: " + res);
                throw new IOException("Invalid boolean value for element: " + key + ", value: " + res);
            }
        }
        if (isRequired) {
            LOG.warn("Missing required bool XML element: {}", key);
            throw new IOException("Missing required XML element: " + key);
        }
        return defaultValue;
    }

    public static short getNodeShort(Element parent, String key, short defaultValue,
                              boolean isRequired) throws IOException {
        String res = getElementText(parent, key);
        if (null != res && !res.isEmpty()) {
            try {
                return Short.parseShort(res);
            } catch (NumberFormatException e) {
                LOG.warn("Failed to parse short value for key: {}, value: {}", key, res);
                throw new IOException("Invalid short value for element: " + key + ", value: " + res);
            }
        }
        if (isRequired) {
            LOG.warn("Missing required short XML element: {}", key);
            throw new IOException("Missing required XML element: " + key);
        }
        return defaultValue;
    }

    public static long getNodeLong(Element parent, String key, long defaultValue,
                            boolean isRequired) throws IOException {
        String res = getElementText(parent, key);
        if (null != res && !res.isEmpty()) {
            try {
                return Long.parseLong(res);
            } catch (NumberFormatException e) {
                LOG.warn("Failed to parse long value for key: {}, value: {}", key, res);
                throw new IOException("Invalid long value for element: " + key + ", value: " + res);
            }
        }
        if (isRequired) {
            LOG.warn("Missing required long XML element: {}", key);
            throw new IOException("Missing required XML element: " + key);
        }
        return defaultValue;
    }

    public long getNodeUint32(Element parent, String key, long defaultValue,
                              boolean isRequired) throws IOException {
        String res = getElementText(parent, key);
        if (null != res && !res.isEmpty()) {
            try {
                long value = Long.parseLong(res);
                // check：0 ~ 2^32-1
                if (value < 0 || value > 0xFFFFFFFFL) {
                    throw new NumberFormatException("Value out of uint32 range: " + value);
                }
                return value;
            } catch (NumberFormatException e) {
                LOG.warn("Failed to parse uint32 value for key: {}, value: {}", key, res);
                throw new IOException("Invalid uint32 value for element: " + key + ", value: " + res);
            }
        }
        if (isRequired) {
            LOG.warn("Missing required uint32 XML element: {}", key);
            throw new IOException("Missing required XML element: " + key);
        }
        return defaultValue;
    }

    public long getNodeUint64(Element parent, String key, long defaultValue,
                              boolean isRequired) throws IOException {
        String res = getElementText(parent, key);
        if (null != res && !res.isEmpty()) {
            try {
                return Long.parseUnsignedLong(res);
            } catch (NumberFormatException e) {
                LOG.warn("Failed to parse uint64 value for key: {}, value: {}", key, res);
                throw new IOException("Invalid uint64 value for element: " + key + ", value: " + res);
            }
        }
        if (isRequired) {
            LOG.warn("Missing required XML element: {}", key);
            throw new IOException("Missing required XML element: " + key);
        }
        return defaultValue;
    }

    private static String getElementText(Element parent, String tagName) {
        if (null == parent) {
            throw new IllegalArgumentException("parent element is null");
        }
        NodeList nodeList = parent.getElementsByTagName(tagName);
        if (nodeList.getLength() > 0) {
            Node node = nodeList.item(0);
            if (node.getFirstChild() != null) {
                return node.getFirstChild().getNodeValue();
            } else {
                return "";
            }
        }
        return null;
    }

    public JfsStatus getAccessPolicies(List<String> accessPolicies) {
        Element accessPoliciesNode = getNode(responseNode, "accessPolicies");
        if (null == accessPoliciesNode) {
            LOG.warn("Failed to get accessPolicies node");
            return JfsStatus.corruption("Missing accessPolicies");
        }

        NodeList nodeList = accessPoliciesNode.getElementsByTagName("policy");
        for (int i=0; i<nodeList.getLength(); i++) {
            Element policyNode = (Element) nodeList.item(i);
            try {
                String policyName = getNodeString(policyNode, "policyStr", "", true);
                accessPolicies.add(policyName);
            } catch (IOException e) {
                LOG.warn("Failed to parse error XML response", e);
                return JfsStatus.ioError("Failed to parse error XML response: " + e.getMessage());
            }
        }
        return JfsStatus.OK();
    }

    public static String getPath(Element parent, String key, String defaultValue,
                          boolean isRequired) throws IOException {
        String value = getNodeString(parent, key, defaultValue, isRequired);

        if (value == null || value.isEmpty()) {
            return "";
        }

        String path =  JfsUtil.decode(value);
        if (path == null) {
            LOG.warn("Failed to decode path: {}", value);
            throw new IOException("Failed to decode path: " + value);
        }
        return path;
    }

    public static JfsFileStatus getJfsFileStatus(Element statusNode) throws IOException {
        JfsFileStatus jfsFileStatus = new JfsFileStatus();
        jfsFileStatus.setFileId(getNodeString(statusNode, "fileId", "-1", true));
        jfsFileStatus.setFileSize(getNodeLong(statusNode, "length", -1, true));
        jfsFileStatus.setName(getPath(statusNode, "path", null, true));

        short permissionValue = getNodeShort(statusNode, "permission", (short) -1, true);
        JfsFilePermission permission = new JfsFilePermission(permissionValue);
        boolean hasAcl = getNodeBool(statusNode, "hasAcl", false, false);
        JfsFilePermissionExtension permissionExtension = new JfsFilePermissionExtension(permission, hasAcl, false);
        jfsFileStatus.setPermission(permissionExtension);

        jfsFileStatus.setOwner(getNodeString(statusNode, "owner", null, true));
        jfsFileStatus.setOwnerGroup(getNodeString(statusNode, "ownerGroup", null, true));
        jfsFileStatus.setMtime(getNodeLong(statusNode, "mtime", -1, true));
        jfsFileStatus.setAtime(getNodeLong(statusNode, "atime", -1, true));
        jfsFileStatus.setSymlink(getNodeString(statusNode, "symlink", null, false));

        boolean isDir = getNodeBool(statusNode, "isDir", false, false);
        jfsFileStatus.setFileType(isDir ? FILE_TYPE_DIRECTORY : (jfsFileStatus.getSymlink() != null ? FILE_TYPE_SYMLINK : FILE_TYPE_FILE));

        jfsFileStatus.setStoragePolicy(getNodeInt(statusNode, "storagePolicy", 0, true));
        jfsFileStatus.setFsState(getNodeInt(statusNode, "fsState", 0, false));
        jfsFileStatus.setStorageState(getNodeInt(statusNode, "storageState", 0, false));
        jfsFileStatus.setChildrenNum(getNodeInt(statusNode, "childNum", 0, false));
        jfsFileStatus.setBlockSize(getNodeLong(statusNode, "blockSize", 0, false));
        jfsFileStatus.setDeltaGeneration(getNodeLong(statusNode, "deltaGeneration", 0, false));
        jfsFileStatus.setBackendLocation(getNodeString(statusNode, "backendLocation", ".dlsdata", false));
        jfsFileStatus.setBlockReplication(getNodeShort(statusNode, "replication", (short) 0, false));

        return jfsFileStatus;
    }

    public JfsFileStatus getJfsFileStatus() throws IOException {
        Element statusNode = getNode(responseNode, "status");
        if (null == statusNode) {
            LOG.warn("Failed to get fileStatus node");
            throw new IOException("Missing status");
        }
        return getJfsFileStatus(statusNode);
    }

    public void getJfsFileStatuses(List<JfsFileStatus> jfsFileStatuses) throws IOException {
        Element fileStatusesNode = getNode(responseNode, "fileStatuses");
        if (null == fileStatusesNode) {
            LOG.warn("Failed to get fileStatuses node");
            throw new IOException("Missing fileStatuses");
        }

        NodeList nodeList = fileStatusesNode.getElementsByTagName("status");
        for (int i=0; i<nodeList.getLength(); i++) {
            Element statusNode = (Element) nodeList.item(i);
            try {
                jfsFileStatuses.add(getJfsFileStatus(statusNode));
            } catch (IOException e) {
                LOG.warn("Failed to parse file status response", e);
                throw new IOException("Failed to parse file status response: " + e.getMessage());
            }
        }
    }

    public static LocatedBlock getLocatedBlock(Element blockNode) throws IOException {
        Element extendedBlockNode = getNode(blockNode, "extendedBlock");
        ExtendedBlock extendedBlock = null;
        if (null != extendedBlockNode) {
            extendedBlock = getExtendedBlock(extendedBlockNode);
        } else {
            extendedBlock = new ExtendedBlock();
        }

        Token<BlockTokenIdentifier> blockToken = null;
        Element blockTokenNode = getNode(blockNode, "blockToken");
        if (null != blockTokenNode) {
            blockToken = getBlockToken(blockTokenNode);
        }

        long offset = getNodeLong(blockNode, "offset", -1, true);
        boolean isCorrupt = getNodeBool(blockNode, "corrupt", false, true);

        // real locs will be filled in JindoNameNodeRpcServer
        DatanodeInfoWithStorage[] locs = new DatanodeInfoWithStorage[0];
        String[] storageIds = new String[0];
        StorageType[] storageTypes = new StorageType[0];
        DatanodeInfo[] cacheLocs = new DatanodeInfo[0];

        LocatedBlock locatedBlock = new LocatedBlock(extendedBlock, locs, storageIds, storageTypes, offset, isCorrupt, cacheLocs);
        if (null != blockToken) {
            locatedBlock.setBlockToken(blockToken);
        }
        return locatedBlock;
    }

    public LocatedBlocks getLocatedBlocks() throws IOException {
        long fileLength = getNodeLong(responseNode, "fileLength", -1, true);
        boolean underConstruction = getNodeBool(responseNode, "underConstruction", false, true);
        boolean isLastBlockComplete = getNodeBool(responseNode, "isLastBlockComplete", false, true);
        long fileId = getNodeLong(responseNode, "fileId", -1, true);

        LocatedBlock lastLocatedBlock = null;
        Element lastBlockNode = getNode(responseNode, "lastBlock");
        if (lastBlockNode != null) {
            LOG.debug("Last block exists" );
            lastLocatedBlock = getLocatedBlock(lastBlockNode);
        }

        Element blocksNode = getNode(responseNode, "blocks");
        if (blocksNode == null) {
            LOG.warn("Failed to get blocks node");
            throw new IOException("Missing blocks");
        }

        List<LocatedBlock> locatedBlockList = new ArrayList<>();
        NodeList nodeList = blocksNode.getElementsByTagName("block");
        for (int i=0; i<nodeList.getLength(); i++) {
            Element blockNode = (Element) nodeList.item(i);
            locatedBlockList.add(getLocatedBlock(blockNode));
        }

        return new LocatedBlocks(fileLength, underConstruction, locatedBlockList, lastLocatedBlock, isLastBlockComplete, null);
    }

    public static ExtendedBlock getExtendedBlock(Element extendedBlockNode) throws IOException {
        String poolId = getNodeString(extendedBlockNode, "poolId", null, true);
        long blockId = getNodeLong(extendedBlockNode, "blockId", -1, true);
        long generationStamp = getNodeLong(extendedBlockNode, "generationStamp", -1, true);
        long numBytes = getNodeLong(extendedBlockNode, "numBytes", -1, true);
        return new ExtendedBlock(poolId, blockId, numBytes, generationStamp);
    }

    public static Token<BlockTokenIdentifier> getBlockToken(Element blockTokenNode) throws IOException {
        String identifier = getNodeString(blockTokenNode, "identifier", null, true);
        String password = getNodeString(blockTokenNode, "password", null, true);
        String kind = getNodeString(blockTokenNode, "kind", null, true);
        String service = getNodeString(blockTokenNode, "service", null, true);
        return new Token<>(identifier.getBytes(StandardCharsets.UTF_8),
                password.getBytes(StandardCharsets.UTF_8), new Text(kind), new Text(service));
    }
}
