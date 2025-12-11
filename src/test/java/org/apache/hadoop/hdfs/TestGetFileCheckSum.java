package org.apache.hadoop.hdfs;

import com.aliyun.jindodata.gateway.JindoMultiClusterTestBase;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestGetFileCheckSum extends JindoMultiClusterTestBase {
    private DistributedFileSystem dfs;
    private static final int BLOCKSIZE = 1024;

    @BeforeEach
    public void before() throws IOException {
        dfs = getDFS();
    }

    public void testGetFileChecksum(final Path foo, final int appendLength)
            throws Exception {
        final int appendRounds = 16;
        FileChecksum[] fc = new FileChecksum[appendRounds + 1];
        TestUtil.createFile(dfs, foo, appendLength, (short)1, 0L);
        fc[0] = dfs.getFileChecksum(foo);
        for (int i = 0; i < appendRounds; i++) {
            TestUtil.appendFile(dfs, foo, appendLength);
            fc[i + 1] = dfs.getFileChecksum(foo);
        }

        for (int i = 0; i < appendRounds + 1; i++) {
            FileChecksum checksum = dfs.getFileChecksum(foo, appendLength * (i+1));
            assertTrue(checksum.equals(fc[i]));
        }
    }

    @Test
    public void testGetFileChecksumForBlocksUnderConstruction() throws IOException {
        FSDataOutputStream file = dfs.create(makeTestPath("testFile"));
        try {
            file.write("Performance Testing".getBytes());
            dfs.getFileChecksum(makeTestPath("testFile"));
            fail("getFileChecksum should fail for files "
                    + "with blocks under construction");
        } catch (IOException ie) {
            assertTrue(ie.getMessage().contains(
                    "Fail to get checksum, since file "+makeTestPathStr("testFile")+" "
                            + "is under construction."));
        } finally {
            file.close();
        }
    }

    @Test
    public void testGetFileChecksum() throws Exception {
        testGetFileChecksum(makeTestPath("foo"), BLOCKSIZE / 4);
        testGetFileChecksum(makeTestPath("bar"), BLOCKSIZE / 4 - 1);
    }

    byte[] fillBuffer(int size, int offset) {
        int todo = size;
        byte[] buffer = new byte[size];

        char c;
        int i = 0;
        while (todo-- > 0) {
            c = (char)(offset++ % 10);
            c = (char)(c < 9 ? c + '0' : '\n');
            buffer[i++] = (byte) c;
        }
        return buffer;
    }

    void createEmptyFile(Path path) throws IOException {
        FSDataOutputStream stm = DfsAppendTestUtil.createFile(BASE_FS, path, 1);
        stm.close();
    }

    void createSimpleFile(Path path, int size) throws IOException {
        byte[] buffer = fillBuffer(size, 0);
        FSDataOutputStream stm = DfsAppendTestUtil.createFile(BASE_FS, path, 1);
        stm.write(buffer);
        stm.close();
    }

    void createSimpleFile(Path path, int size, int blocksize) throws IOException {
        byte[] buffer = fillBuffer(size, 0);
        FSDataOutputStream stm = DfsAppendTestUtil.createFile(BASE_FS, path, 1, blocksize);
        stm.write(buffer);
        stm.close();
    }

    void compareChecksum(Path path, String checksum) throws IOException {
        FileChecksum fc =  BASE_FS.getFileChecksum(path);
        assertTrue(fc.toString().equals(checksum));
    }

    @Test
    public void testMD5FileChecksum() throws Exception {
        {
            Path path = makeTestPath("TestSimpleFile.txt");
            createSimpleFile(path, 100);
            compareChecksum(path, "MD5-of-0MD5-of-512CRC32C:e96fb488535b368809f0c408062e9b61");
        }

        {
            int size = 7 * 1024 * 1024 + 9;
            Path path1 = makeTestPath("TestMidiumFile1.txt");
            Path path2 = makeTestPath("TestMidiumFile2.txt");
            Path path3 = makeTestPath("TestMidiumFile3.txt");
            Path path4 = makeTestPath("TestMidiumFile4.txt");

            createSimpleFile(path1, size, 128 * 1024 * 1024);
            compareChecksum(path1, "MD5-of-0MD5-of-512CRC32C:71e647f82aeea6b864f8737d17c19499");

//            createSimpleFile(path2, size, 256 * 1024);
//            compareChecksum(path2, "MD5-of-512MD5-of-512CRC32C:b9df8a51e06f33b0e5ddd9637fc31736");

            createSimpleFile(path3, size, 3 * 1024 * 1024);
            compareChecksum(path3, "MD5-of-6144MD5-of-512CRC32C:ded60f6fc12fcb2381cc09870a9046bb");

//            createSimpleFile(path4, size, 703 * 1024);
//            compareChecksum(path4, "MD5-of-1406MD5-of-512CRC32C:ba4f1f76e1a72dc0c4c230b62af2ff11");
        }
        {
            Path path = makeTestPath("TestEmptyFile.txt");
            createEmptyFile(path);
            compareChecksum(path, "MD5-of-0MD5-of-0CRC32:70bc8f4b72a86921468bf8e8441dce51");
        }
        {
            Path path = makeTestPath("TestNonCompleteFile.txt");
            FSDataOutputStream stm = DfsAppendTestUtil.createFile(BASE_FS, path, 1, 1048576);
            stm.write("hello".getBytes());
            try{
                FileChecksum fc =  BASE_FS.getFileChecksum(path);
            } catch (IOException e) {
                System.out.println("Got ioe, good!");
            }

            stm.close();
            compareChecksum(path, "MD5-of-0MD5-of-512CRC32C:2ebac5fafc444475c9720587791d7412");
        }
    }
}