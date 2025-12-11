package com.aliyun.jindodata.gateway.common;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Element;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * JfsResponseXml Test
 */
public class JfsResponseXmlTest {

    private JfsResponseXml responseXml;

    @BeforeEach
    public void setUp() {
        responseXml = new JfsResponseXml();
    }

    // ==================== getNodeString test ====================

    @Test
    public void testGetNodeString_WithValidValue() throws Exception {
        String xml = "<response><name>John</name></response>";
        responseXml.parseResponse(xml);
        Element response = responseXml.getResponseNode();

        String result = responseXml.getNodeString(response, "name", "default", false);

        assertEquals("John", result);
    }

    @Test
    public void testGetNodeString_WithDefaultValue() throws Exception {
        String xml = "<response><age>25</age></response>";
        responseXml.parseResponse(xml);
        Element response = responseXml.getResponseNode();

        String result = responseXml.getNodeString(response, "name", "Unknown", false);

        assertEquals("Unknown", result);
    }

    @Test
    public void testGetNodeString_RequiredButMissing() throws Exception {
        String xml = "<response><age>25</age></response>";
        responseXml.parseResponse(xml);
        Element response = responseXml.getResponseNode();

        assertThrows(IOException.class, () -> {
            responseXml.getNodeString(response, "name", null, true);
        });
    }

    @Test
    public void testGetNodeString_WithEmptyElement() throws Exception {
        String xml = "<response><name></name></response>";
        responseXml.parseResponse(xml);
        Element response = responseXml.getResponseNode();

        String result = responseXml.getNodeString(response, "name", "default", false);

        assertEquals("", result);
    }

    @Test
    public void testGetNodeString_WithEmptyElement2() throws Exception {
        String xml = "<response><name/></response>";
        responseXml.parseResponse(xml);
        Element response = responseXml.getResponseNode();

        String result = responseXml.getNodeString(response, "name", "default", false);

        assertEquals("", result);
    }

    // ==================== getNodeInt test ====================

    @Test
    public void testGetNodeInt_WithValidValue() throws Exception {
        String xml = "<response><count>42</count></response>";
        responseXml.parseResponse(xml);
        Element response = responseXml.getResponseNode();

        int result = responseXml.getNodeInt(response, "count", 0, false);

        assertEquals(42, result);
    }

    @Test
    public void testGetNodeInt_WithNegativeValue() throws Exception {
        String xml = "<response><count>-100</count></response>";
        responseXml.parseResponse(xml);
        Element response = responseXml.getResponseNode();

        int result = responseXml.getNodeInt(response, "count", 0, false);

        assertEquals(-100, result);
    }

    @Test
    public void testGetNodeInt_WithDefaultValue() throws Exception {
        String xml = "<response><name>John</name></response>";
        responseXml.parseResponse(xml);
        Element response = responseXml.getResponseNode();

        int result = responseXml.getNodeInt(response, "count", 99, false);

        assertEquals(99, result);
    }

    @Test
    public void testGetNodeInt_WithInvalidValue() throws Exception {
        String xml = "<response><count>not_a_number</count></response>";
        responseXml.parseResponse(xml);
        Element response = responseXml.getResponseNode();

        assertThrows(IOException.class, () -> {
            responseXml.getNodeInt(response, "count", 0, false);
        });
    }

    @Test
    public void testGetNodeInt_RequiredButMissing() throws Exception {
        String xml = "<response><name>John</name></response>";
        responseXml.parseResponse(xml);
        Element response = responseXml.getResponseNode();

        assertThrows(IOException.class, () -> {
            responseXml.getNodeInt(response, "count", 0, true);
        });
    }

    // ==================== getNodeBool test ====================

    @Test
    public void testGetNodeBool_WithTrueString() throws Exception {
        String xml = "<response><enabled>true</enabled></response>";
        responseXml.parseResponse(xml);
        Element response = responseXml.getResponseNode();

        boolean result = responseXml.getNodeBool(response, "enabled", false, false);

        assertTrue(result);
    }

    @Test
    public void testGetNodeBool_WithFalseString() throws Exception {
        String xml = "<response><enabled>false</enabled></response>";
        responseXml.parseResponse(xml);
        Element response = responseXml.getResponseNode();

        boolean result = responseXml.getNodeBool(response, "enabled", true, false);

        assertFalse(result);
    }

    @Test
    public void testGetNodeBool_WithInvalidValue() throws Exception {
        String xml = "<response><enabled>maybe</enabled></response>";
        responseXml.parseResponse(xml);
        Element response = responseXml.getResponseNode();

        assertThrows(IOException.class, () -> {
            responseXml.getNodeBool(response, "enabled", false, false);
        });
    }

    @Test
    public void testGetNodeBool_WithDefaultValue() throws Exception {
        String xml = "<response><name>John</name></response>";
        responseXml.parseResponse(xml);
        Element response = responseXml.getResponseNode();

        boolean result = responseXml.getNodeBool(response, "enabled", true, false);

        assertTrue(result);
    }

    // ==================== getNodeShort test ====================

    @Test
    public void testGetNodeShort_WithValidValue() throws Exception {
        String xml = "<response><port>8080</port></response>";
        responseXml.parseResponse(xml);
        Element response = responseXml.getResponseNode();

        short result = responseXml.getNodeShort(response, "port", (short) 0, false);

        assertEquals(8080, result);
    }

    @Test
    public void testGetNodeShort_WithMaxValue() throws Exception {
        String xml = "<response><value>32767</value></response>";
        responseXml.parseResponse(xml);
        Element response = responseXml.getResponseNode();

        short result = responseXml.getNodeShort(response, "value", (short) 0, false);

        assertEquals(32767, result);
    }

    @Test
    public void testGetNodeShort_WithMinValue() throws Exception {
        String xml = "<response><value>-32768</value></response>";
        responseXml.parseResponse(xml);
        Element response = responseXml.getResponseNode();

        short result = responseXml.getNodeShort(response, "value", (short) 0, false);

        assertEquals(-32768, result);
    }

    @Test
    public void testGetNodeShort_WithInvalidValue() throws Exception {
        String xml = "<response><port>not_a_number</port></response>";
        responseXml.parseResponse(xml);
        Element response = responseXml.getResponseNode();

        assertThrows(IOException.class, () -> {
            responseXml.getNodeShort(response, "port", (short) 0, false);
        });
    }

    @Test
    public void testGetNodeShort_WithDefaultValue() throws Exception {
        String xml = "<response><name>John</name></response>";
        responseXml.parseResponse(xml);
        Element response = responseXml.getResponseNode();

        short result = responseXml.getNodeShort(response, "port", (short) 9999, false);

        assertEquals(9999, result);
    }

    // ==================== getNodeLong test ====================

    @Test
    public void testGetNodeLong_WithValidValue() throws Exception {
        String xml = "<response><fileSize>1234567890123</fileSize></response>";
        responseXml.parseResponse(xml);
        Element response = responseXml.getResponseNode();

        long result = responseXml.getNodeLong(response, "fileSize", 0L, false);

        assertEquals(1234567890123L, result);
    }

    @Test
    public void testGetNodeLong_WithMaxValue() throws Exception {
        String xml = "<response><value>9223372036854775807</value></response>";
        responseXml.parseResponse(xml);
        Element response = responseXml.getResponseNode();

        long result = responseXml.getNodeLong(response, "value", 0L, false);

        assertEquals(Long.MAX_VALUE, result);
    }

    @Test
    public void testGetNodeLong_WithMinValue() throws Exception {
        String xml = "<response><value>-9223372036854775808</value></response>";
        responseXml.parseResponse(xml);
        Element response = responseXml.getResponseNode();

        long result = responseXml.getNodeLong(response, "value", 0L, false);

        assertEquals(Long.MIN_VALUE, result);
    }

    @Test
    public void testGetNodeLong_WithInvalidValue() throws Exception {
        String xml = "<response><fileSize>not_a_number</fileSize></response>";
        responseXml.parseResponse(xml);
        Element response = responseXml.getResponseNode();

        assertThrows(IOException.class, () -> {
            responseXml.getNodeLong(response, "fileSize", 0L, false);
        });
    }

    @Test
    public void testGetNodeLong_WithDefaultValue() throws Exception {
        String xml = "<response><name>John</name></response>";
        responseXml.parseResponse(xml);
        Element response = responseXml.getResponseNode();

        long result = responseXml.getNodeLong(response, "fileSize", 999999L, false);

        assertEquals(999999L, result);
    }

    // ==================== getNodeUint32 test ====================

    @Test
    public void testGetNodeUint32_WithValidValue() throws Exception {
        String xml = "<response><size>2147483648</size></response>";
        responseXml.parseResponse(xml);
        Element response = responseXml.getResponseNode();

        long result = responseXml.getNodeUint32(response, "size", 0L, false);

        assertEquals(2147483648L, result);
    }

    @Test
    public void testGetNodeUint32_WithMaxValue() throws Exception {
        String xml = "<response><value>4294967295</value></response>";
        responseXml.parseResponse(xml);
        Element response = responseXml.getResponseNode();

        long result = responseXml.getNodeUint32(response, "value", 0L, false);

        assertEquals(4294967295L, result);
    }

    @Test
    public void testGetNodeUint32_WithZero() throws Exception {
        String xml = "<response><value>0</value></response>";
        responseXml.parseResponse(xml);
        Element response = responseXml.getResponseNode();

        long result = responseXml.getNodeUint32(response, "value", 1L, false);

        assertEquals(0L, result);
    }

    @Test
    public void testGetNodeUint32_WithNegativeValue() throws Exception {
        String xml = "<response><value>-1</value></response>";
        responseXml.parseResponse(xml);
        Element response = responseXml.getResponseNode();

        assertThrows(IOException.class, () -> {
            responseXml.getNodeUint32(response, "value", 0L, false);
        });
    }

    @Test
    public void testGetNodeUint32_WithOverflowValue() throws Exception {
        String xml = "<response><value>4294967296</value></response>";
        responseXml.parseResponse(xml);
        Element response = responseXml.getResponseNode();

        assertThrows(IOException.class, () -> {
            responseXml.getNodeUint32(response, "value", 0L, false);
        });
    }

    @Test
    public void testGetNodeUint32_WithInvalidValue() throws Exception {
        String xml = "<response><size>not_a_number</size></response>";
        responseXml.parseResponse(xml);
        Element response = responseXml.getResponseNode();

        assertThrows(IOException.class, () -> {
            responseXml.getNodeUint32(response, "size", 0L, false);
        });
    }

    @Test
    public void testGetNodeUint32_WithDefaultValue() throws Exception {
        String xml = "<response><name>John</name></response>";
        responseXml.parseResponse(xml);
        Element response = responseXml.getResponseNode();

        long result = responseXml.getNodeUint32(response, "size", 99999L, false);

        assertEquals(99999L, result);
    }

    // ==================== getNodeUint64 test ====================

    @Test
    public void testGetNodeUint64_WithValidValue() throws Exception {
        String xml = "<response><bigValue>9223372036854775807</bigValue></response>";
        responseXml.parseResponse(xml);
        Element response = responseXml.getResponseNode();

        long result = responseXml.getNodeUint64(response, "bigValue", 0L, false);

        assertEquals(9223372036854775807L, result);
    }

    @Test
    public void testGetNodeUint64_WithZero() throws Exception {
        String xml = "<response><value>0</value></response>";
        responseXml.parseResponse(xml);
        Element response = responseXml.getResponseNode();

        long result = responseXml.getNodeUint64(response, "value", 1L, false);

        assertEquals(0L, result);
    }

    @Test
    public void testGetNodeUint64_WithLargeValue() throws Exception {
        // 2^63 (greater than Long.MAX_VALUE，but in uint64 range)
        String xml = "<response><value>9223372036854775808</value></response>";
        responseXml.parseResponse(xml);
        Element response = responseXml.getResponseNode();

        long result = responseXml.getNodeUint64(response, "value", 0L, false);

        assertEquals(-9223372036854775808L, result); // valid in unsigned long
    }

    @Test
    public void testGetNodeUint64_WithNegativeValue() throws Exception {
        String xml = "<response><value>-1</value></response>";
        responseXml.parseResponse(xml);
        Element response = responseXml.getResponseNode();

        assertThrows(IOException.class, () -> {
            responseXml.getNodeUint64(response, "value", 0L, false);
        });
    }

    @Test
    public void testGetNodeUint64_WithInvalidValue() throws Exception {
        String xml = "<response><bigValue>not_a_number</bigValue></response>";
        responseXml.parseResponse(xml);
        Element response = responseXml.getResponseNode();

        assertThrows(IOException.class, () -> {
            responseXml.getNodeUint64(response, "bigValue", 0L, false);
        });
    }

    @Test
    public void testGetNodeUint64_WithDefaultValue() throws Exception {
        String xml = "<response><name>John</name></response>";
        responseXml.parseResponse(xml);
        Element response = responseXml.getResponseNode();

        long result = responseXml.getNodeUint64(response, "bigValue", 999999L, false);

        assertEquals(999999L, result);
    }

    // ==================== getNode test ====================

    @Test
    public void testGetNode_WithValidElement() throws Exception {
        String xml = "<response><user><name>Alice</name></user></response>";
        responseXml.parseResponse(xml);
        Element response = responseXml.getResponseNode();

        Element userNode = responseXml.getNode(response, "user");

        assertNotNull(userNode);
        assertEquals("user", userNode.getTagName());
    }

    @Test
    public void testGetNode_WithNonExistentElement() throws Exception {
        String xml = "<response><user><name>Alice</name></user></response>";
        responseXml.parseResponse(xml);
        Element response = responseXml.getResponseNode();

        Element notFoundNode = responseXml.getNode(response, "admin");

        assertNull(notFoundNode);
    }

    @Test
    public void testGetNode_WithNestedElement() throws Exception {
        String xml = "<response><user><name>Alice</name><age>30</age></user></response>";
        responseXml.parseResponse(xml);
        Element response = responseXml.getResponseNode();

        Element userNode = responseXml.getNode(response, "user");
        assertNotNull(userNode);

        // Get name sub-node from user node
        Element nameNode = responseXml.getNode(userNode, "name");
        assertNotNull(nameNode);
        assertEquals("name", nameNode.getTagName());
    }

    // ==================== parseResponse test ====================

    @Test
    public void testParseResponse_WithValidXml() throws Exception {
        String xml = "<response><status>success</status></response>";

        JfsStatus status = responseXml.parseResponse(xml);

        assertTrue(status.isOk());
        assertNotNull(responseXml.getResponseNode());
    }

    @Test
    public void testParseResponse_WithoutResponseNode() throws Exception {
        String xml = "<data><status>success</status></data>";

        JfsStatus status = responseXml.parseResponse(xml);

        assertFalse(status.isOk());
    }

    @Test
    public void testParseResponse_WithInvalidXml() {
        String invalidXml = "This is not XML";

        assertThrows(Exception.class, () -> {
            responseXml.parseResponse(invalidXml);
        });
    }

    @Test
    public void testComplexResponseParsing() throws Exception {
        String xml = "<response>" +
                "<errCode>0</errCode>" +
                "<message>Success</message>" +
                "<success>true</success>" +
                "<count>42</count>" +
                "<port>8080</port>" +
                "<fileSize>1234567890123</fileSize>" +
                "<dataSize>3000000000</dataSize>" +
                "</response>";

        responseXml.parseResponse(xml);
        Element response = responseXml.getResponseNode();

        // Verify that various types of values can be parsed correctly
        assertEquals("Success", responseXml.getNodeString(response, "message", "", false));
        assertTrue(responseXml.getNodeBool(response, "success", false, false));
        assertEquals(42, responseXml.getNodeInt(response, "count", 0, false));
        assertEquals(8080, responseXml.getNodeShort(response, "port", (short) 0, false));
        assertEquals(1234567890123L, responseXml.getNodeLong(response, "fileSize", 0L, false));
        assertEquals(3000000000L, responseXml.getNodeUint32(response, "dataSize", 0L, false));
    }
}
