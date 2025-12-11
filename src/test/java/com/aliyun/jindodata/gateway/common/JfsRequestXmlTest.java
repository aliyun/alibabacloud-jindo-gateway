package com.aliyun.jindodata.gateway.common;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Element;

import static org.junit.jupiter.api.Assertions.*;

/**
 * JfsRequestXml Test
 */
public class JfsRequestXmlTest {

    private JfsRequestXml requestXml;

    @BeforeEach
    public void setUp() {
        requestXml = new JfsRequestXml();
    }

    @Test
    public void testInitRequestNewVersion() {
        int result = requestXml.initRequest(
                "GetFileStatus",
                "admin",
                "test-instance",
                "caller-context",
                "caller-signature"
        );

        assertEquals(0, result);
        assertNotNull(requestXml.getParametersNode());

        String xmlString = requestXml.getXmlString();
        assertNotNull(xmlString);
        assertTrue(xmlString.contains("GetFileStatus"));
        assertTrue(xmlString.contains("admin"));
        assertTrue(xmlString.contains("test-instance"));
    }

    @Test
    public void testInitRequestWithoutOptionalFields() {
        int result = requestXml.initRequest(
                "GetFileStatus",
                null,
                "test-instance",
                null,
                null
        );

        assertEquals(0, result);

        String xmlString = requestXml.getXmlString();
        assertNotNull(xmlString);
        assertTrue(xmlString.contains("GetFileStatus"));
        assertTrue(xmlString.contains("test-instance"));
        assertFalse(xmlString.contains("<user>"));
    }

    @Test
    public void testAddRequestNodeString() {
        requestXml.initRequest("Test", null, "test",  null, null);
        Element paramsNode = requestXml.getParametersNode();

        int result = requestXml.addRequestNode(paramsNode, "path", "/user/test", false);

        assertEquals(0, result);
        String xmlString = requestXml.getXmlString();
        assertTrue(xmlString.contains("<path>/user/test</path>"));
    }

    @Test
    public void testAddRequestNodeStringWithEmpty() {
        requestXml.initRequest("Test", null, "test",  null, null);
        Element paramsNode = requestXml.getParametersNode();

        // Empty values are not allowed
        int result = requestXml.addRequestNode(paramsNode, "path", "", false);

        assertEquals(1, result); // Return 1 indicates skipped
    }

     @Test
    public void testAddRequestNodeStringAllowEmptyDisabled() {
        requestXml.initRequest("Test", null, "test", null, null);
        Element paramsNode = requestXml.getParametersNode();

        int result = requestXml.addRequestNode(paramsNode, "path", "", true);

        assertEquals(0, result);
        String xmlString = requestXml.getXmlString();
        assertTrue(xmlString.contains("<path/>"));
    }

    @Test
    public void testAddRequestNodeByte() {
        requestXml.initRequest("Test", null, "test", null, null);
        Element paramsNode = requestXml.getParametersNode();

        int result = requestXml.addRequestNode(paramsNode, "permission", (byte) 127);

        assertEquals(0, result);
        String xmlString = requestXml.getXmlString();
        assertTrue(xmlString.contains("<permission>127</permission>"));
    }

    @Test
    public void testAddRequestNodeInt() {
        requestXml.initRequest("Test", null, "test", null, null);
        Element paramsNode = requestXml.getParametersNode();

        int result = requestXml.addRequestNode(paramsNode, "port", 8080);

        assertEquals(0, result);
        String xmlString = requestXml.getXmlString();
        assertTrue(xmlString.contains("<port>8080</port>"));
    }

    @Test
    public void testAddRequestNodeLong() {
        requestXml.initRequest("Test", null, "test", null, null);
        Element paramsNode = requestXml.getParametersNode();

        int result = requestXml.addRequestNode(paramsNode, "fileSize", 1234567890123L);

        assertEquals(0, result);
        String xmlString = requestXml.getXmlString();
        assertTrue(xmlString.contains("<fileSize>1234567890123</fileSize>"));
    }

    @Test
    public void testAddRequestNodeBoolean() {
        requestXml.initRequest("Test", null, "test",  null, null);
        Element paramsNode = requestXml.getParametersNode();

        requestXml.addRequestNode(paramsNode, "recursive", true);
        requestXml.addRequestNode(paramsNode, "overwrite", false);

        String xmlString = requestXml.getXmlString();
        assertTrue(xmlString.contains("<recursive>true</recursive>"));
        assertTrue(xmlString.contains("<overwrite>false</overwrite>"));
    }

    @Test
    public void testAddRequestParameter() {
        requestXml.initRequest("Test", null, "test", null, null);

        int result1 = requestXml.addRequestParameter("path", "/user/test");
        int result2 = requestXml.addRequestParameter("recursive", 1);
        int result3 = requestXml.addRequestParameter("overwrite", true);

        assertEquals(0, result1);
        assertEquals(0, result2);
        assertEquals(0, result3);

        String xmlString = requestXml.getXmlString();
        assertTrue(xmlString.contains("<path>/user/test</path>"));
        assertTrue(xmlString.contains("<recursive>1</recursive>"));
        assertTrue(xmlString.contains("<overwrite>true</overwrite>"));
    }

    @Test
    public void testAddRequestParameterWithoutInitialize() {
        int result = requestXml.addRequestParameter("path", "/user/test");

        assertEquals(-1, result);
    }

    @Test
    public void testUrlEncodingPath() {
        String path = "/user/test path";
        String encoded = JfsRequestXml.urlEncode(path, true);

        assertFalse(encoded.contains("/"));
        assertTrue(encoded.contains("%2F"));
        assertTrue(encoded.contains("test%20path"));
    }

    @Test
    public void testUrlEncodingPathNoEncodingSlash() {
        String path = "/user/test path";
        String encoded = JfsRequestXml.urlEncode(path, false);

        assertTrue(encoded.contains("/"));
        assertTrue(encoded.contains("test%20path"));
    }

    @Test
    public void testUrlEncodingSpecialCharacters() {
        String path = "test@#$%^&*()";
        String encoded = JfsRequestXml.urlEncode(path, true);

        assertTrue(encoded.contains("%40")); // @
        assertTrue(encoded.contains("%23")); // #
        assertTrue(encoded.contains("%24")); // $
    }

    @Test
    public void testUrlEncodingEmptyString() {
        String path = "";
        String encoded = JfsRequestXml.urlEncode(path, true);

        assertEquals("", encoded);
    }

    @Test
    public void testUrlEncodingNull() {
        String encoded = JfsRequestXml.urlEncode(null, true);

        assertNull(encoded);
    }

    @Test
    public void testEncodePath() {
        String path = "/user/test";
        String encoded = JfsRequestXml.encodePath(path);

        assertTrue(encoded.contains("%2F"));
    }

    @Test
    public void testUrlEncodingUTF8Characters() {
        String path = "/用户/test";
        String encoded = JfsRequestXml.urlEncode(path, false);

        assertTrue(encoded.contains("%"));
        assertTrue(encoded.contains("/test"));
    }

    @Test
    public void testGetXmlStringNewVersion() throws Exception {
        requestXml.initRequest(
                "GetFileStatus",
                "admin",
                "test-instance",
                null,
                null
        );
        requestXml.addRequestParameter("path", "/user/test");

        String xmlString = requestXml.getXmlString();

        assertNotNull(xmlString);
        assertTrue(xmlString.contains("<?xml"));
        assertTrue(xmlString.contains("<request>"));
        assertTrue(xmlString.contains("<clientVersion>"));
        assertTrue(xmlString.contains("<instanceName>test-instance</instanceName>"));
        assertTrue(xmlString.contains("<requestType>GetFileStatus</requestType>"));
        assertTrue(xmlString.contains("<user>admin</user>"));
        assertTrue(xmlString.contains("<parameters>"));
        assertTrue(xmlString.contains("<path>/user/test</path>"));
        assertTrue(xmlString.contains("</parameters>"));
        assertTrue(xmlString.contains("</request>"));
    }

    @Test
    public void testComplexXmlBuilding() {
        requestXml.initRequest(
                "CreateFile",
                "admin",
                "test-instance",
                null,
                null
        );

        Element paramsNode = requestXml.getParametersNode();

        requestXml.addRequestParameter("path", "/user/test/file.txt");
        requestXml.addRequestParameter("overwrite", true);
        requestXml.addRequestParameter("blockSize", 134217728L);
        requestXml.addRequestParameter("replication", 3);
        requestXml.addRequestParameter("bufferSize", 4096);

        String xmlString = requestXml.getXmlString();

        assertNotNull(xmlString);
        assertTrue(xmlString.contains("<path>/user/test/file.txt</path>"));
        assertTrue(xmlString.contains("<overwrite>true</overwrite>"));
        assertTrue(xmlString.contains("<blockSize>134217728</blockSize>"));
        assertTrue(xmlString.contains("<replication>3</replication>"));
        assertTrue(xmlString.contains("<bufferSize>4096</bufferSize>"));
    }

    @Test
    public void testCallerContextEncoding() {
        String callerContext = "test context with spaces";
        String callerSignature = "test signature";

        int result = requestXml.initRequest(
                "Test",
                null,
                "test",
                callerContext,
                callerSignature
        );

        assertEquals(0, result);

        String xmlString = requestXml.getXmlString();
        assertNotNull(xmlString);
        assertTrue(xmlString.contains("<callerContext>"));
        assertTrue(xmlString.contains("<context>"));
        assertTrue(xmlString.contains("<signature>"));
        // space should be encoded to %20
        assertTrue(xmlString.contains("%20"));
    }

    @Test
    public void testAddRequestNodeWithNullParent() {
        int result = requestXml.addRequestNode(null, "key", (byte) 1);

        assertEquals(-1, result);
    }

    @Test
    public void testAddRequestParameterWithLargeNumber() {
        requestXml.initRequest("Test", null, "test",  null, null);

        long largeNumber = Long.MAX_VALUE;
        int result = requestXml.addRequestParameter("largeValue", largeNumber);

        assertEquals(0, result);
        String xmlString = requestXml.getXmlString();
        assertTrue(xmlString.contains("<largeValue>" + Long.MAX_VALUE + "</largeValue>"));
    }

    @Test
    public void testAddRequestParameterWithNegativeNumber() {
        requestXml.initRequest("Test", null, "test", null, null);

        int result = requestXml.addRequestParameter("negativeValue", -12345);

        assertEquals(0, result);
        String xmlString = requestXml.getXmlString();
        assertTrue(xmlString.contains("<negativeValue>-12345</negativeValue>"));
    }
}
