package org.apache.hadoop.hdfs;

import com.aliyun.jindodata.gateway.JindoMultiClusterTestBase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.EnumSet;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_SYNCONCLOSE_KEY;
import static org.apache.hadoop.hdfs.DfsAppendTestUtil.getFileSystemAs;
import static org.junit.jupiter.api.Assertions.*;

public class TestFileCreation2 extends JindoMultiClusterTestBase {
    static final String DIR = "/" + TestFileCreation2.class.getSimpleName() + "/";
    private static final String RPC_DETAILED_METRICS =
            "RpcDetailedActivityForPort";

    static final long seed = 0xDEADBEEFL;
    static final int blockSize = 1024 * 1024;
    static final int numBlocks = 2;
    static final int fileSize = numBlocks * blockSize + 1;
    boolean simulatedStorage = false;

    private static final String[] NON_CANONICAL_PATHS =
            new String[] { testDir + "//foo", testDir + "///foo2", testDir + "//dir//file", testDir + "////test2/file",
                    testDir + "/dir/./file2", testDir + "/dir/../file3" };
    private static final Log LOG = LogFactory.getLog(TestFileCreation2.class);

    // creates a file but does not close it
    public static FSDataOutputStream createFile(FileSystem fileSys, Path name,
                                                int repl) throws IOException {
        System.out
                .println("createFile: Created " + name + " with " + repl + " replica.");
        FSDataOutputStream stm = fileSys.create(name, true,
                fileSys.getConf()
                        .getInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, 4096),
                (short) repl, blockSize);
        return stm;
    }

    public static HdfsDataOutputStream create(DistributedFileSystem dfs,
                                              Path name, int repl) throws IOException {
        return (HdfsDataOutputStream) createFile(dfs, name, repl);
    }

    //
    // writes to file but does not close it
    //
    static void writeFile(FSDataOutputStream stm) throws IOException {
        writeFile(stm, fileSize);
    }

    //
    // writes specified bytes to file.
    //
    public static void writeFile(FSDataOutputStream stm, int size)
            throws IOException {
        byte[] buffer = DfsAppendTestUtil.randomBytes(seed, size);
        stm.write(buffer, 0, size);
    }

    @Test
    public void testDeleteOnExit() throws IOException {
        Configuration conf = new HdfsConfiguration(BASE_CONF);
        conf.setBoolean("fs.hdfs.impl.disable.cache", true);
        FileSystem fs = getFs(conf);
        FileSystem localfs = FileSystem.getLocal(conf);

        try {

            // Creates files in HDFS and local file system.
            //
            Path file1 = makeTestPath("filestatus.dat");
            Path file2 = makeTestPath("filestatus2.dat");
            Path file3 = new Path("filestatus3.dat");
            FSDataOutputStream stm1 = createFile(fs, file1, 1);
            FSDataOutputStream stm2 = createFile(fs, file2, 1);
            FSDataOutputStream stm3 = createFile(localfs, file3, 1);
            System.out.println("DeleteOnExit: Created files.");

            // write to files and close. Purposely, do not close file2.
            writeFile(stm1);
            writeFile(stm3);
            stm1.close();
            stm2.close();
            stm3.close();

            // set delete on exit flag on files.
            fs.deleteOnExit(file1);
            fs.deleteOnExit(file2);
            localfs.deleteOnExit(file3);

            // close the file system. This should make the above files
            // disappear.
            fs.close();
            localfs.close();
            fs = null;
            localfs = null;

            // reopen file system and verify that file does not exist.
            fs = getFs(conf);
            localfs = FileSystem.getLocal(conf);

            assertFalse(fs.exists(file1), file1 + " still exists inspite of deletOnExit set.");
            assertFalse(fs.exists(file2), file2 + " still exists inspite of deletOnExit set.");
            assertFalse(localfs.exists(file3), file3 + " still exists inspite of deletOnExit set.");
            System.out.println("DeleteOnExit successful.");

        } finally {
            IOUtils.closeStream(fs);
            IOUtils.closeStream(localfs);
        }
    }

    @Test
    public void testOverwriteOpenForWrite() throws Exception {
        Configuration conf = new HdfsConfiguration(BASE_CONF);
        conf.setBoolean("fs.hdfs.impl.disable.cache", true);

        FileSystem fs = getFs(conf);

        UserGroupInformation otherUgi = UserGroupInformation
                .createUserForTesting("testuser", new String[] { "testgroup" });
        FileSystem fs2 = getFileSystemAs(otherUgi, conf);

        try {
            Path p = makeTestPath("testfile");
            FSDataOutputStream stm1 = fs.create(p);
            fs.setPermission(p, new FsPermission((short) 0777));    // konna: simulate disable permission
            stm1.write(1);

            // assertCounter("CreateNumOps", 1L, getMetrics(metricsName));

            // Create file again without overwrite
            try {
                fs2.create(p, false);
                fail("Did not throw!");
            } catch (IOException abce) {
                GenericTestUtils.assertExceptionContains("Failed to CREATE_FILE", abce);
            }
//      assertCounter("AlreadyBeingCreatedExceptionNumOps", 1L,
//          getMetrics(metricsName));
            FSDataOutputStream stm2 = fs2.create(p, true);
            stm2.write(2);
            stm2.close();

            try {
                stm1.close();
                fail("Should have exception closing stm1 since it was deleted");
            } catch (IOException ioe) {
                GenericTestUtils.assertExceptionContains("File does not exist", ioe);
            }

        } finally {
        }
    }

    @Test
    public void testDFSClientDeath() throws IOException, InterruptedException {
        Configuration conf = new HdfsConfiguration(BASE_CONF);
        conf.setBoolean("fs.hdfs.impl.disable.cache", true);
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 0);  // OK?
        System.out.println("Testing adbornal client death.");

        FileSystem fs = getFs(conf);
        DistributedFileSystem dfs = (DistributedFileSystem) fs;
        DFSClient dfsclient = dfs.getClient();
        try {

            // create a new file in home directory. Do not close it.
            //
            Path file1 = makeTestPath("clienttest.dat");
            FSDataOutputStream stm = createFile(fs, file1, 1);
            System.out.println("Created file clienttest.dat");

            // write to file
            writeFile(stm);

            // close the dfsclient before closing the output stream.
            // This should close all existing file.
            dfsclient.close();

            // reopen file system and verify that file exists.
            assertTrue(DfsAppendTestUtil.createHdfsWithDifferentUsername(conf).exists(file1),
                    file1 + " does not exist.");
        } finally {
        }
    }

    @Test
    public void testFileCreationNonRecursive() throws IOException {
        Configuration conf = new HdfsConfiguration(BASE_CONF);
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 0);  // OK?

        FileSystem fs = getDefaultFS();

        try {
            testFileCreationNonRecursive(fs);
        } finally {
        }
    }

    public static void testFileCreationNonRecursive(FileSystem fs)
            throws IOException {
        final Path path =
                makeTestPath(Time.now() + "-testFileCreationNonRecursive");
        IOException expectedException = null;
        final String nonExistDir = makeTestPathStr("non-exist-") + Time.now();

        fs.delete(new Path(nonExistDir), true);
        EnumSet<CreateFlag> createFlag = EnumSet.of(CreateFlag.CREATE);
        // Create a new file in root dir, should succeed
        assertNull(createNonRecursive(fs, path, 1, createFlag));

        // Create a file when parent dir exists as file, should fail
        expectedException =
                createNonRecursive(fs, new Path(path, "Create"), 1, createFlag);
        assertTrue(
                expectedException != null
                        && expectedException instanceof ParentNotDirectoryException,
                "Create a file when parent directory exists as a file"
                        + " should throw ParentNotDirectoryException ");
        fs.delete(path, true);
        // Create a file in a non-exist directory, should fail
        final Path path2 = new Path(nonExistDir + "/testCreateNonRecursive");
        expectedException = createNonRecursive(fs, path2, 1, createFlag);
        assertTrue(
                expectedException != null
                        && expectedException instanceof FileNotFoundException,
                "Create a file in a non-exist dir using"
                        + " createNonRecursive() should throw FileNotFoundException ");

        EnumSet<CreateFlag> overwriteFlag =
                EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE);
        // Overwrite a file in root dir, should succeed
        assertNull(createNonRecursive(fs, path, 1, overwriteFlag));

        // Overwrite a file when parent dir exists as file, should fail
        expectedException =
                createNonRecursive(fs, new Path(path, "Overwrite"), 1, overwriteFlag);

        assertTrue(
                expectedException != null
                        && expectedException instanceof ParentNotDirectoryException,
                "Overwrite a file when parent directory exists as a file"
                        + " should throw ParentNotDirectoryException ");
        fs.delete(path, true);

        // Overwrite a file in a non-exist directory, should fail
        final Path path3 = new Path(nonExistDir + "/testOverwriteNonRecursive");
        expectedException = createNonRecursive(fs, path3, 1, overwriteFlag);

        assertTrue(
                expectedException != null
                        && expectedException instanceof FileNotFoundException,
                "Overwrite a file in a non-exist dir using"
                        + " createNonRecursive() should throw FileNotFoundException ");
    }

    static IOException createNonRecursive(FileSystem fs, Path name, int repl,
                                          EnumSet<CreateFlag> flag) throws IOException {
        try {
            System.out.println("createNonRecursive: Attempting to create " + name
                    + " with " + repl + " replica.");
            int bufferSize = fs.getConf()
                    .getInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, 4096);
            FSDataOutputStream stm =
                    fs.createNonRecursive(name, FsPermission.getDefault(), flag,
                            bufferSize, (short) repl, blockSize, null);
            stm.close();
        } catch (IOException e) {
            System.out.println("sls e: " + e);
            return e;
        }
        return null;
    }

    @Test
    public void testConcurrentFileCreation() throws IOException {
        Configuration conf = new HdfsConfiguration(BASE_CONF);
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 0);  // OK?

        try {
            FileSystem fs = getDefaultFS();

            Path[] p = { makeTestPath("foo"), makeTestPath("bar") };

            // write 2 files at the same time
            FSDataOutputStream[] out = { fs.create(p[0]), fs.create(p[1]) };
            int i = 0;
            for (; i < 100; i++) {
                out[0].write(i);
                out[1].write(i);
            }
            out[0].close();
            for (; i < 200; i++) {
                out[1].write(i);
            }
            out[1].close();

            // verify
            FSDataInputStream[] in = { fs.open(p[0]), fs.open(p[1]) };
            for (i = 0; i < 100; i++) {
                assertEquals(i, in[0].read());
            }
            for (i = 0; i < 200; i++) {
                assertEquals(i, in[1].read());
            }
        } finally {
        }
    }

    @Test
    public void testFileCreationSyncOnClose() throws IOException {
        Configuration conf = new HdfsConfiguration(BASE_CONF);
        conf.setBoolean(DFS_DATANODE_SYNCONCLOSE_KEY, true);
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 0);  // OK?

        try {
            FileSystem fs = getDefaultFS();

            Path[] p = { makeTestPath("foo"), makeTestPath("bar") };

            // write 2 files at the same time
            FSDataOutputStream[] out = { fs.create(p[0]), fs.create(p[1]) };
            int i = 0;
            for (; i < 100; i++) {
                out[0].write(i);
                out[1].write(i);
            }
            out[0].close();
            for (; i < 200; i++) {
                out[1].write(i);
            }
            out[1].close();

            // verify
            FSDataInputStream[] in = { fs.open(p[0]), fs.open(p[1]) };
            for (i = 0; i < 100; i++) {
                assertEquals(i, in[0].read());
            }
            for (i = 0; i < 200; i++) {
                assertEquals(i, in[1].read());
            }
        } finally {
        }
    }

    @Test
    public void testFsClose() throws Exception {
        System.out.println("test file system close start");
        final int DATANODE_NUM = 3;

        Configuration conf = new HdfsConfiguration(BASE_CONF);
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 0);  // OK?
        conf.setBoolean("fs.hdfs.impl.disable.cache", true);

        DistributedFileSystem dfs = null;
        try {
            dfs = getDFS(conf);

            // create a new file.
            final String f = DIR + "foofs";
            final Path fpath = new Path(f);
            FSDataOutputStream out =
                    TestFileCreation2.createFile(dfs, fpath, DATANODE_NUM);
            out.write("something".getBytes());

            // close file system without closing file
            dfs.close();
        } finally {
            System.out.println("testFsClose successful");
        }
    }

    private static enum CreationMethod {
        DIRECT_NN_RPC, PATH_FROM_URI, PATH_FROM_STRING
    };

    @Test
    public void testCreateNonCanonicalPathAndRestartRpc() throws Exception {
        doCreateTest(CreationMethod.DIRECT_NN_RPC);
    }

    @Test
    public void testCreateNonCanonicalPathAndRestartFromString()
            throws Exception {
        doCreateTest(CreationMethod.PATH_FROM_STRING);
    }

    @Test
    public void testCreateNonCanonicalPathAndRestartFromUri() throws Exception {
        doCreateTest(CreationMethod.PATH_FROM_URI);
    }

    private void doCreateTest(CreationMethod method) throws Exception {
        Configuration conf = new HdfsConfiguration(BASE_CONF);
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 0);  // OK?
        try {
            FileSystem fs = getFs(conf);

            for (String pathStr : NON_CANONICAL_PATHS) {
                System.out.println("Creating " + pathStr + " by " + method);
                switch (method) {
                    case DIRECT_NN_RPC:
                        try {
                            getDFS().getClient()
                                    .getNamenode().create(pathStr, new FsPermission((short)0755), "client",
                                            new EnumSetWritable<CreateFlag>(EnumSet.of(CreateFlag.CREATE)),
                                            true, (short) 1, 128 * 1024 * 1024L, CryptoProtocolVersion.supported());
                            // konna : OSS-HDFS will normalize them
//                            fail("Should have thrown exception when creating '" + pathStr + "'"
//                                    + " by " + method);
                        } catch (RemoteException ipe) {
                            // When we create by direct NN RPC, the NN just rejects the
                            // non-canonical paths, rather than trying to normalize them.
                            // So, we expect all of them to fail.
                        }
                        break;

                    case PATH_FROM_URI:
                    case PATH_FROM_STRING:
                        // Unlike the above direct-to-NN case, we expect these to succeed,
                        // since the Path constructor should normalize the path.
                        Path p;
                        if (method == CreationMethod.PATH_FROM_URI) {
                            p = new Path(new URI(fs.getUri() + pathStr));
                        } else {
                            p = new Path(fs.getUri() + pathStr);
                        }
                        FSDataOutputStream stm = fs.create(p);
                        IOUtils.closeStream(stm);
                        break;
                    default:
                        throw new AssertionError("bad method: " + method);
                }
            }

        } finally {
        }
    }

    @Test
    public void testFileIdMismatch() throws IOException {
        Configuration conf = new HdfsConfiguration(BASE_CONF);
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 0);  // OK?
        DistributedFileSystem dfs = null;
        try {
            dfs = getDFS();
            DFSClient client = dfs.getClient();

            final Path f = new Path("/testFileIdMismatch.txt");
            createFile(dfs, f, 3);
            long someOtherFileId = -1;
            try {
                getDFS().getClient()
                        .getNamenode().complete(f.toString(), client.getClientName(),
                                null, someOtherFileId);
                fail();
            } catch (RemoteException e) {
                FileSystem.LOG.info("Caught Expected FileNotFoundException: ", e);
            }
        } finally {
        }
    }
}