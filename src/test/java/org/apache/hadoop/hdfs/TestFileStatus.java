package org.apache.hadoop.hdfs;

import com.aliyun.jindodata.gateway.JindoSingleClusterTestBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.ipc.RemoteException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

public class TestFileStatus extends JindoSingleClusterTestBase {

    static final long seed = 0xDEADBEEFL;
    static final int blockSize = 1024 * 1024;
    static final int fileSize = 16384;

    private static FileSystem fs;
    private static DFSClient dfsClient;
    private static Path file1;
    private static Configuration conf;
    private static FileContext fc;

    private static void writeFile(FileSystem fileSys, Path name, int repl,
                                  int fileSize, int blockSize) throws IOException {
        // Create and write a file that contains three blocks of data
        FSDataOutputStream stm = fileSys.create(name, true,
                DFSUtilClient.getIoFileBufferSize(conf), (short)repl, (long)blockSize);
        byte[] buffer = new byte[fileSize];
        Random rand = new Random(seed);
        rand.nextBytes(buffer);
        stm.write(buffer);
        stm.close();
    }

    @BeforeEach
    public void before() throws IOException {
        conf = new HdfsConfiguration(BASE_CONF);
        fs = getDefaultFS();
        dfsClient = new DFSClient(DFSUtilClient.getNNAddress(BASE_CONF), BASE_CONF);
        file1 = makeTestPath("filestatus.dat");
        fc = FileContext.getFileContext(fs.getUri(), conf);
        writeFile(fs, file1, 1, fileSize, blockSize);
    }

    @Test
    public void testGetFileInfo() throws IOException {
        // Check that / exists
        Path path = testRootPath;
        assertTrue(fs.getFileStatus(path).isDirectory(), "/ should be a directory");

        // Make sure getFileInfo returns null for files which do not exist
        HdfsFileStatus fileInfo = dfsClient.getFileInfo(makeTestPathStr("noSuchFile"));
        assertEquals(null, fileInfo, "Non-existant file should result in null");

        Path path1 = makeTestPath("name1");
        Path path2 = makeTestPath("name1/name2");
        assertTrue(fs.mkdirs(path1));
        FSDataOutputStream out = fs.create(path2, false);
        out.close();
        // konna : getChildrenNum not support in oss-hdfs
//        fileInfo = dfsClient.getFileInfo(path1.toString());
//        assertEquals(1, fileInfo.getChildrenNum());
//        fileInfo = dfsClient.getFileInfo(path2.toString());
//        assertEquals(0, fileInfo.getChildrenNum());

        // Test getFileInfo throws the right exception given a non-absolute path.
        try {
            dfsClient.getFileInfo("non-absolute1");
            fail("getFileInfo for a non-absolute path did not throw IOException");
        } catch (RemoteException re) {
            assertTrue(re.toString().contains("Absolute path required"),
                    "Wrong exception for invalid file name: "+re);
        }
    }

    private void checkFile(FileSystem fileSys, Path name, int repl)
            throws IOException, InterruptedException, TimeoutException {
        waitReplication(fileSys, name, (short) repl);
    }

    @Test
    public void testGetFileStatusOnFile() throws Exception {
        checkFile(fs, file1, 1);
        // test getFileStatus on a file
        FileStatus status = fs.getFileStatus(file1);
        assertFalse(status.isDirectory(), file1 + " should be a file");
        assertEquals(blockSize, status.getBlockSize());
        assertEquals(1, status.getReplication());
        assertEquals(fileSize, status.getLen());
        assertEquals(file1.makeQualified(fs.getUri(),
                        fs.getWorkingDirectory()).toString(),
                status.getPath().toString());
    }

    @Test
    public void testListStatusOnFile() throws IOException {
        FileStatus[] stats = fs.listStatus(file1);
        assertEquals(1, stats.length);
        FileStatus status = stats[0];
        assertFalse(status.isDirectory(), file1 + " should be a file");
        assertEquals(blockSize, status.getBlockSize());
        assertEquals(1, status.getReplication());
        assertEquals(fileSize, status.getLen());
        assertEquals(file1.makeQualified(fs.getUri(),
                        fs.getWorkingDirectory()).toString(),
                status.getPath().toString());

        RemoteIterator<FileStatus> itor = fc.listStatus(file1);
        status = itor.next();
        assertEquals(stats[0], status);
        assertFalse(status.isDirectory(), file1 + " should be a file");
    }

    @Test
    public void testGetFileStatusOnNonExistantFileDir() throws IOException {
        Path dir = makeTestPath("test/mkdirs");
        try {
            fs.listStatus(dir);
            fail("listStatus of non-existent path should fail");
        } catch (FileNotFoundException fe) {
            System.out.println("sls " + fe);
//            assertEquals("File " + dir + " does not exist.",fe.getMessage());
            assertTrue(fe.getMessage().contains("does not exist"));
        }

        try {
            fc.listStatus(dir);
            fail("listStatus of non-existent path should fail");
        } catch (FileNotFoundException fe) {
//            assertEquals("File " + dir + " does not exist.", fe.getMessage());
            assertTrue(fe.getMessage().contains("does not exist"));
        }
        try {
            fs.getFileStatus(dir);
            fail("getFileStatus of non-existent path should fail");
        } catch (FileNotFoundException fe) {
            assertTrue(fe.getMessage().contains("does not exist"),
                    "Exception doesn't indicate non-existant path");
        }
    }

    @Test
    public void testListStatus() throws Exception {
        Path dir = makeTestPath("test/mkdirs2");
        Path subDir1 = makeTestPath("test/mkdirs2/sub1");
        Path subDir2 = makeTestPath("test/mkdirs2/sub2");
        Path subDir3 = makeTestPath("test/mkdirs2/sub3");
        assertTrue(fs.mkdirs(dir), "mkdir failed");
        assertTrue(fs.mkdirs(subDir1), "mkdir failed");
        assertTrue(fs.mkdirs(subDir2), "mkdir failed");
        assertTrue(fs.mkdirs(subDir3), "mkdir failed");

        FileStatus[] stats = fs.listStatus(dir);
        assertEquals(3, stats.length);
    }

    @Test
    public void testGetFileStatusOnDir() throws Exception {
        // Create the directory
        Path dir = makeTestPath("test/mkdirs");
        assertTrue(fs.mkdirs(dir), "mkdir failed");
        assertTrue(fs.exists(dir), "mkdir failed");

        // test getFileStatus on an empty directory
        FileStatus status = fs.getFileStatus(dir);
        assertTrue(status.isDirectory(), dir + " should be a directory");
        assertEquals(0, status.getLen(), dir + " should be zero size ");
        assertEquals(dir.makeQualified(fs.getUri(),
                        fs.getWorkingDirectory()).toString(),
                status.getPath().toString());

        // test listStatus on an empty directory
        FileStatus[] stats = fs.listStatus(dir);
        assertEquals(0, stats.length, dir + " should be empty");
        assertEquals(0, fs.getContentSummary(dir).getLength(),
                dir + " should be zero size ");
//        assertEquals(dir + " should be zero size using hftp",
//                0, hftpfs.getContentSummary(dir).getLength());

        RemoteIterator<FileStatus> itor = fc.listStatus(dir);
        assertFalse(itor.hasNext(), dir + " should be empty");

        itor = fs.listStatusIterator(dir);
        assertFalse(itor.hasNext(), dir + " should be empty");

        // create another file that is smaller than a block.
        Path file2 = new Path(dir, "filestatus2.dat");
        writeFile(fs, file2, 1, blockSize/4, blockSize);
        checkFile(fs, file2, 1);

        // verify file attributes
        status = fs.getFileStatus(file2);
        assertEquals(blockSize, status.getBlockSize());
        assertEquals(1, status.getReplication());
        file2 = fs.makeQualified(file2);
        assertEquals(file2.toString(), status.getPath().toString());

        // Create another file in the same directory
        Path file3 = new Path(dir, "filestatus3.dat");
        writeFile(fs, file3, 1, blockSize/4, blockSize);
        checkFile(fs, file3, 1);
        file3 = fs.makeQualified(file3);

        // Verify that the size of the directory increased by the size
        // of the two files
        final int expected = blockSize/2;
        assertEquals(expected, fs.getContentSummary(dir).getLength(),
                dir + " size should be " + expected);
//        assertEquals(dir + " size should be " + expected + " using hftp",
//                expected, hftpfs.getContentSummary(dir).getLength());

        // Test listStatus on a non-empty directory
        stats = fs.listStatus(dir);
        assertEquals(2, stats.length, dir + " should have two entries");
        assertEquals(file2.toString(), stats[0].getPath().toString());
        assertEquals(file3.toString(), stats[1].getPath().toString());

        itor = fc.listStatus(dir);
        assertEquals(file2.toString(), itor.next().getPath().toString());
        assertEquals(file3.toString(), itor.next().getPath().toString());
        assertFalse(itor.hasNext(), "Unexpected addtional file");

        itor = fs.listStatusIterator(dir);
        assertEquals(file2.toString(), itor.next().getPath().toString());
        assertEquals(file3.toString(), itor.next().getPath().toString());
        assertFalse(itor.hasNext(), "Unexpected addtional file");


        // Test iterative listing. Now dir has 2 entries, create one more.
        Path dir3 = fs.makeQualified(new Path(dir, "dir3"));
        fs.mkdirs(dir3);
        dir3 = fs.makeQualified(dir3);
        stats = fs.listStatus(dir);
        assertEquals(3, stats.length, dir + " should have three entries");
        assertEquals(dir3.toString(), stats[0].getPath().toString());
        assertEquals(file2.toString(), stats[1].getPath().toString());
        assertEquals(file3.toString(), stats[2].getPath().toString());

        itor = fc.listStatus(dir);
        assertEquals(dir3.toString(), itor.next().getPath().toString());
        assertEquals(file2.toString(), itor.next().getPath().toString());
        assertEquals(file3.toString(), itor.next().getPath().toString());
        assertFalse(itor.hasNext(), "Unexpected addtional file");

        itor = fs.listStatusIterator(dir);
        assertEquals(dir3.toString(), itor.next().getPath().toString());
        assertEquals(file2.toString(), itor.next().getPath().toString());
        assertEquals(file3.toString(), itor.next().getPath().toString());
        assertFalse(itor.hasNext(), "Unexpected addtional file");

        // Now dir has 3 entries, create two more
        Path dir4 = fs.makeQualified(new Path(dir, "dir4"));
        fs.mkdirs(dir4);
        dir4 = fs.makeQualified(dir4);
        Path dir5 = fs.makeQualified(new Path(dir, "dir5"));
        fs.mkdirs(dir5);
        dir5 = fs.makeQualified(dir5);
        stats = fs.listStatus(dir);
        assertEquals(5, stats.length, dir + " should have five entries");
        assertEquals(dir3.toString(), stats[0].getPath().toString());
        assertEquals(dir4.toString(), stats[1].getPath().toString());
        assertEquals(dir5.toString(), stats[2].getPath().toString());
        assertEquals(file2.toString(), stats[3].getPath().toString());
        assertEquals(file3.toString(), stats[4].getPath().toString());

        itor = fc.listStatus(dir);
        assertEquals(dir3.toString(), itor.next().getPath().toString());
        assertEquals(dir4.toString(), itor.next().getPath().toString());
        assertEquals(dir5.toString(), itor.next().getPath().toString());
        assertEquals(file2.toString(), itor.next().getPath().toString());
        assertEquals(file3.toString(), itor.next().getPath().toString());
        assertFalse(itor.hasNext());

        itor = fs.listStatusIterator(dir);
        assertEquals(dir3.toString(), itor.next().getPath().toString());
        assertEquals(dir4.toString(), itor.next().getPath().toString());
        assertEquals(dir5.toString(), itor.next().getPath().toString());
        assertEquals(file2.toString(), itor.next().getPath().toString());
        assertEquals(file3.toString(), itor.next().getPath().toString());
        assertFalse(itor.hasNext());

//        { //test permission error on hftp
//            fs.setPermission(dir, new FsPermission((short)0));
//            try {
//                final String username = UserGroupInformation.getCurrentUser().getShortUserName() + "1";
//                final HftpFileSystem hftp2 = cluster.getHftpFileSystemAs(username, conf, 0, "somegroup");
//                hftp2.getContentSummary(dir);
//                fail();
//            } catch(IOException ioe) {
//                FileSystem.LOG.info("GOOD: getting an exception", ioe);
//            }
//        }
        fs.delete(dir, true);
    }
}