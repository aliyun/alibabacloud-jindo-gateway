package org.apache.hadoop.hdfs;

import com.aliyun.jindodata.gateway.JindoMultiClusterTestBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

public class TestListFilesInFileContext extends JindoMultiClusterTestBase {
    static final long seed = 0xDEADBEEFL;

    final private static Configuration conf = new Configuration();
    private static FileContext fc;
    final private static Path TEST_DIR = makeTestPath("main_");
    final private static int  FILE_LEN = 10;
    final private static Path FILE1    = new Path(TEST_DIR, "file1");
    final private static Path DIR1     = new Path(TEST_DIR, "dir1");
    final private static Path FILE2    = new Path(DIR1, "file2");
    final private static Path FILE3    = new Path(DIR1, "file3");

    @BeforeEach
    public void before() throws IOException {
        fc = FileContext.getFileContext(BASE_CONF);
    }

    private static void writeFile(FileContext fc, Path name, int fileSize)
            throws IOException {
        // Create and write a file that contains three blocks of data
        FSDataOutputStream stm = fc.create(name, EnumSet.of(CreateFlag.CREATE),
                Options.CreateOpts.createParent());
        byte[] buffer = new byte[fileSize];
        Random rand = new Random(seed);
        rand.nextBytes(buffer);
        stm.write(buffer);
        stm.close();
    }

    @Test
    public void testFile() throws IOException {
        fc.mkdir(TEST_DIR, FsPermission.getDefault(), true);
        writeFile(fc, FILE1, FILE_LEN);

        RemoteIterator<LocatedFileStatus> itor = fc.util().listFiles(
                FILE1, true);
        LocatedFileStatus stat = itor.next();
        assertFalse(itor.hasNext());
        assertTrue(stat.isFile());
        assertEquals(FILE_LEN, stat.getLen());
        assertEquals(fc.makeQualified(FILE1), stat.getPath());
        assertEquals(1, stat.getBlockLocations().length);

        itor = fc.util().listFiles(FILE1, false);
        stat = itor.next();
        assertFalse(itor.hasNext());
        assertTrue(stat.isFile());
        assertEquals(FILE_LEN, stat.getLen());
        assertEquals(fc.makeQualified(FILE1), stat.getPath());
        assertEquals(1, stat.getBlockLocations().length);
    }

    @Test
    public void testDirectory() throws IOException {
        fc.mkdir(DIR1, FsPermission.getDefault(), true);

        // test empty directory
        RemoteIterator<LocatedFileStatus> itor = fc.util().listFiles(
                DIR1, true);
        assertFalse(itor.hasNext());
        itor = fc.util().listFiles(DIR1, false);
        assertFalse(itor.hasNext());

        // testing directory with 1 file
        writeFile(fc, FILE2, FILE_LEN);

        itor = fc.util().listFiles(DIR1, true);
        LocatedFileStatus stat = itor.next();
        assertFalse(itor.hasNext());
        assertTrue(stat.isFile());
        assertEquals(FILE_LEN, stat.getLen());
        assertEquals(fc.makeQualified(FILE2), stat.getPath());
        assertEquals(1, stat.getBlockLocations().length);

        itor = fc.util().listFiles(DIR1, false);
        stat = itor.next();
        assertFalse(itor.hasNext());
        assertTrue(stat.isFile());
        assertEquals(FILE_LEN, stat.getLen());
        assertEquals(fc.makeQualified(FILE2), stat.getPath());
        assertEquals(1, stat.getBlockLocations().length);

        // test more complicated directory
        writeFile(fc, FILE1, FILE_LEN);
        writeFile(fc, FILE3, FILE_LEN);

        itor = fc.util().listFiles(TEST_DIR, true);
        stat = itor.next();
        assertTrue(stat.isFile());
        assertEquals(fc.makeQualified(FILE2), stat.getPath());
        stat = itor.next();
        assertTrue(stat.isFile());
        assertEquals(fc.makeQualified(FILE3), stat.getPath());
        stat = itor.next();
        assertTrue(stat.isFile());
        assertEquals(fc.makeQualified(FILE1), stat.getPath());
        assertFalse(itor.hasNext());

        itor = fc.util().listFiles(TEST_DIR, false);
        stat = itor.next();
        assertTrue(stat.isFile());
        assertEquals(fc.makeQualified(FILE1), stat.getPath());
        assertFalse(itor.hasNext());
    }
}