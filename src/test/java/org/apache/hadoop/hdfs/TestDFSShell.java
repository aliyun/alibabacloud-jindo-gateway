package org.apache.hadoop.hdfs;

import com.aliyun.jindodata.gateway.JindoSingleClusterTestBase;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.google.common.base.Supplier;
import java.io.*;
import java.security.Permission;
import java.security.PrivilegedExceptionAction;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPOutputStream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.StringContains.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

public class TestDFSShell extends JindoSingleClusterTestBase {
    private static final Log LOG     = LogFactory.getLog(TestDFSShell.class);
    private static final AtomicInteger counter = new AtomicInteger();
    private final        int           SUCCESS = 0;
    private final        int           ERROR   = 1;

    static final String TEST_ROOT_DIR = TestUtil.getTestDirName(TestDFSShell.class);

    private static final String RAW_A1           = "raw.a1";
    private static final String TRUSTED_A1       = "trusted.a1";
    private static final String USER_A1          = "user.a1";
    private static final byte[] RAW_A1_VALUE     = new byte[]{0x32, 0x32, 0x32};
    private static final byte[] TRUSTED_A1_VALUE = new byte[]{0x31, 0x31, 0x31};
    private static final byte[] USER_A1_VALUE    = new byte[]{0x31, 0x32, 0x33};

    static Path writeFile(FileSystem fs, Path f) throws IOException {
        DataOutputStream out = fs.create(f);
        out.writeBytes("dhruba: " + f);
        out.close();
        assertTrue(fs.exists(f));
        return f;
    }

    static Path mkdir(FileSystem fs, Path p) throws IOException {
        assertTrue(fs.mkdirs(p));
        assertTrue(fs.exists(p));
        assertTrue(fs.getFileStatus(p).isDirectory());
        return p;
    }

    static File createLocalFile(File f) throws IOException {
        assertFalse(f.exists());
        PrintWriter out = new PrintWriter(f);
        out.print("createLocalFile: " + f.getAbsolutePath());
        out.flush();
        out.close();
        assertTrue(f.exists());
        assertTrue(f.isFile());
        return f;
    }

    static void show(String s) {
        System.out.println(Thread.currentThread().getStackTrace()[2] + " " + s);
    }

    @Test
    public void testZeroSizeFile() throws IOException {
        FileSystem fs = getDefaultFS();
        assertInstanceOf(DistributedFileSystem.class, fs, "Not a HDFS: " + fs.getUri());
        final DistributedFileSystem dfs = (DistributedFileSystem)fs;

        try {
            //create a zero size file
            final File f1 = new File(TEST_ROOT_DIR, "f1");
            assertFalse(f1.exists());
            assertTrue(f1.createNewFile());
            assertTrue(f1.exists());
            assertTrue(f1.isFile());
            assertEquals(0L, f1.length());

            //copy to remote
            final Path root = mkdir(dfs, makeTestPath("test/zeroSizeFile"));
            final Path remotef = new Path(root, "dst");
            show("copy local " + f1 + " to remote " + remotef);
            dfs.copyFromLocalFile(false, false, new Path(f1.getPath()), remotef);

            //getBlockSize() should not throw exception
            show("Block size = " + dfs.getFileStatus(remotef).getBlockSize());

            //copy back
            final File f2 = new File(TEST_ROOT_DIR, "f2");
            assertFalse(f2.exists());
            dfs.copyToLocalFile(remotef, new Path(f2.getPath()));
            assertTrue(f2.exists());
            assertTrue(f2.isFile());
            assertEquals(0L, f2.length());

            f1.delete();
            f2.delete();
        } finally {
        }
    }

    @Test
    public void testRecursiveRm() throws IOException {
        Configuration conf = new HdfsConfiguration(BASE_CONF);
        FileSystem fs = getDefaultFS();
        assertInstanceOf(DistributedFileSystem.class, fs, "Not a HDFS: " + fs.getUri());
        try {
            fs.mkdirs(new Path(makeTestPath("parent"), "child"));
            try {
                fs.delete(makeTestPath("parent"), false);
                fail("Should have thrown an exception"); // should never reach here.
            } catch(IOException e) {
                //should have thrown an exception
            }
            try {
                fs.delete(makeTestPath("parent"), true);
            } catch(IOException e) {
                fail("Should not have thrown an exception");
            }
        } finally {
        }
    }

    @Test
    public void testDu() throws IOException {
        Configuration conf = new HdfsConfiguration(BASE_CONF);
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 0);  // OK?
        DistributedFileSystem fs = getDFS();
        PrintStream psBackup = System.out;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream psOut = new PrintStream(out);
        System.setOut(psOut);
        FsShell shell = new FsShell();
        shell.setConf(conf);

        try {

            Path myPath = makeTestPath("test/dir");
            assertTrue(fs.mkdirs(myPath));
            assertTrue(fs.exists(myPath));
            Path myFile = makeTestPath("test/dir/file");
            writeFile(fs, myFile);
            assertTrue(fs.exists(myFile));
            Path myFile2 = makeTestPath("test/dir/file2");
            writeFile(fs, myFile2);
            assertTrue(fs.exists(myFile2));
            Long myFileLength = fs.getFileStatus(myFile).getLen();
            Long myFile2Length = fs.getFileStatus(myFile2).getLen();

            String[] args = new String[2];
            args[0] = "-du";
            args[1] = makeTestPathStr("test/dir");
            int val = -1;
            try {
                val = shell.run(args);
            } catch (Exception e) {
                System.err.println("Exception raised from DFSShell.run " +
                        e.getLocalizedMessage());
            }
            assertEquals(0, val);
            String returnString = out.toString();
            out.reset();
            // Check if size matches as expected
            assertThat(returnString, containsString(myFileLength.toString()));
            assertThat(returnString, containsString(myFile2Length.toString()));

      /*
      // Check that -du -s reports the state of the snapshot
      String snapshotName = "ss1";
      Path snapshotPath = new Path(myPath, ".snapshot/" + snapshotName);
      fs.allowSnapshot(myPath);
      assertThat(fs.createSnapshot(myPath, snapshotName), is(snapshotPath));
      assertThat(fs.delete(myFile, false), is(true));
      assertThat(fs.exists(myFile), is(false));

      args = new String[3];
      args[0] = "-du";
      args[1] = "-s";
      args[2] = snapshotPath.toString();
      val = -1;
      try {
        val = shell.run(args);
      } catch (Exception e) {
        System.err.println("Exception raised from DFSShell.run " +
            e.getLocalizedMessage());
      }
      assertThat(val, is(0));
      returnString = out.toString();
      out.reset();
      Long combinedLength = myFileLength + myFile2Length;
      assertThat(returnString, containsString(combinedLength.toString()));
      */
        } finally {
            System.setOut(psBackup);
        }
    }

    @Test
    public void testPut() throws IOException {
        Configuration conf = new HdfsConfiguration(BASE_CONF);
        FileSystem fs = getDefaultFS();
        assertTrue(fs instanceof DistributedFileSystem, "Not a HDFS: "+fs.getUri());
        final DistributedFileSystem dfs = (DistributedFileSystem)fs;

        try {
            // remove left over crc files:
            new File(TEST_ROOT_DIR, ".f1.crc").delete();
            new File(TEST_ROOT_DIR, ".f2.crc").delete();
            final File f1 = createLocalFile(new File(TEST_ROOT_DIR, "f1"));
            final File f2 = createLocalFile(new File(TEST_ROOT_DIR, "f2"));

            final Path root = mkdir(dfs, makeTestPath("test/put"));
            final Path dst = new Path(root, "dst");

            show("begin");

            final Thread copy2ndFileThread = new Thread() {
                @Override
                public void run() {
                    try {
                        show("copy local " + f2 + " to remote " + dst);
                        dfs.copyFromLocalFile(false, false, new Path(f2.getPath()), dst);
                    } catch (IOException ioe) {
                        show("good " + StringUtils.stringifyException(ioe));
                        return;
                    }
                    //should not be here, must got IOException
                    fail("Should have thrown an exception");
                }
            };

            //use SecurityManager to pause the copying of f1 and begin copying f2
            SecurityManager sm = System.getSecurityManager();
            System.out.println("SecurityManager = " + sm);
            System.setSecurityManager(new SecurityManager() {
                private boolean firstTime = true;

                @Override
                public void checkPermission(Permission perm) {
                    if (firstTime) {
                        Thread t = Thread.currentThread();
                        if (!t.toString().contains("DataNode")) {
                            String s = "" + Arrays.asList(t.getStackTrace());
                            if (s.contains("FileUtil.copyContent")) {
                                //pause at FileUtil.copyContent

                                firstTime = false;
                                copy2ndFileThread.start();
                                try {Thread.sleep(5000);} catch (InterruptedException e) {}
                            }
                        }
                    }
                }
            });
            show("copy local " + f1 + " to remote " + dst);
            dfs.copyFromLocalFile(false, false, new Path(f1.getPath()), dst);
            show("done");

            try {copy2ndFileThread.join();} catch (InterruptedException e) { }
            System.setSecurityManager(sm);

            // copy multiple files to destination directory
            final Path destmultiple = mkdir(dfs, makeTestPath("test/putmultiple"));
            Path[] srcs = new Path[2];
            srcs[0] = new Path(f1.getPath());
            srcs[1] = new Path(f2.getPath());
            dfs.copyFromLocalFile(false, false, srcs, destmultiple);
            srcs[0] = new Path(destmultiple,"f1");
            srcs[1] = new Path(destmultiple,"f2");
            assertTrue(dfs.exists(srcs[0]));
            assertTrue(dfs.exists(srcs[1]));

            // move multiple files to destination directory
            final Path destmultiple2 = mkdir(dfs, makeTestPath("test/movemultiple"));
            srcs[0] = new Path(f1.getPath());
            srcs[1] = new Path(f2.getPath());
            dfs.moveFromLocalFile(srcs, destmultiple2);
            assertFalse(f1.exists());
            assertFalse(f2.exists());
            srcs[0] = new Path(destmultiple2, "f1");
            srcs[1] = new Path(destmultiple2, "f2");
            assertTrue(dfs.exists(srcs[0]));
            assertTrue(dfs.exists(srcs[1]));

            f1.delete();
            f2.delete();
        } finally {
        }
    }

    @Test
    public void testErrOutPut() throws Exception {
        Configuration conf = new HdfsConfiguration(BASE_CONF);
        PrintStream bak = null;
        try {
            conf.setInt(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 0);  // OK?
            FileSystem srcFs = getDefaultFS();
            String nonexistentfile = makeTestPathStr("nonexistentfile");
            Path root = makeTestPath("nonexistentfile");
            bak = System.err;
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            PrintStream tmp = new PrintStream(out);
            System.setErr(tmp);
            String[] argv = new String[2];
            argv[0] = "-cat";
            argv[1] = root.toUri().getPath();
            int ret = ToolRunner.run(new FsShell(), argv);
            assertEquals(1, ret, " -cat returned 1 ");
            String returned = out.toString();
            assertFalse(returned.contains("Exception"), "cat does not print exceptions ");
            out.reset();
            argv[0] = "-rm";
            argv[1] = root.toString();
            FsShell shell = new FsShell();
            shell.setConf(conf);
            ret = ToolRunner.run(shell, argv);
            assertEquals(1, ret, " -rm returned 1 ");
            returned = out.toString();
            out.reset();
            assertTrue(returned.contains("No such file or directory"), "rm prints reasonable error ");
            argv[0] = "-rmr";
            argv[1] = root.toString();
            ret = ToolRunner.run(shell, argv);
            assertEquals(1, ret, " -rmr returned 1");
            returned = out.toString();
            assertTrue(returned.contains("No such file or directory"), "rmr prints reasonable error ");
            out.reset();
            argv[0] = "-du";
            argv[1] = nonexistentfile;
            ret = ToolRunner.run(shell, argv);
            returned = out.toString();
            assertTrue(returned.contains("No such file or directory"), " -du prints reasonable error ");
            out.reset();
            argv[0] = "-dus";
            argv[1] = nonexistentfile;
            ret = ToolRunner.run(shell, argv);
            returned = out.toString();
            assertTrue(returned.contains("No such file or directory"), " -dus prints reasonable error");
            out.reset();
            argv[0] = "-ls";
            argv[1] = nonexistentfile;
            ret = ToolRunner.run(shell, argv);
            returned = out.toString();
            assertFalse(returned.contains("Found 0"), " -ls does not return Found 0 items");
            out.reset();
            argv[0] = "-ls";
            argv[1] = nonexistentfile;
            ret = ToolRunner.run(shell, argv);
            assertEquals(1, ret, " -lsr should fail ");
            out.reset();
            srcFs.mkdirs(makeTestPath("testdir"));
            argv[0] = "-ls";
            argv[1] = makeTestPathStr("testdir");
            ret = ToolRunner.run(shell, argv);
            returned = out.toString();
            assertFalse(returned.contains("Found 0"), " -ls does not print out anything ");
            out.reset();
            argv[0] = "-ls";
            argv[1] = makeTestPathStr("user/nonxistant/*");
            ret = ToolRunner.run(shell, argv);
            assertEquals(1, ret, " -ls on nonexistent glob returns 1");
            out.reset();
            argv[0] = "-mkdir";
            argv[1] = makeTestPathStr("testdir");
            ret = ToolRunner.run(shell, argv);
            returned = out.toString();
            assertEquals(1, ret, " -mkdir returned 1 ");
            assertTrue(returned.contains("File exists"), " -mkdir returned File exists");
            Path testFile = makeTestPath("testfile");
            OutputStream outtmp = srcFs.create(testFile);
            outtmp.write(testFile.toString().getBytes());
            outtmp.close();
            out.reset();
            argv[0] = "-mkdir";
            argv[1] = makeTestPathStr("testfile");
            ret = ToolRunner.run(shell, argv);
            returned = out.toString();
            assertEquals(1, ret, " -mkdir returned 1");
            assertTrue(returned.contains("not a directory"), " -mkdir returned this is a file ");
            out.reset();
            argv = new String[3];
            argv[0] = "-mv";
            argv[1] = makeTestPathStr("testfile");
            argv[2] = "file";
            ret = ToolRunner.run(shell, argv);
            returned = out.toString();
            assertEquals(1, ret, "mv failed to rename");
            System.out.println(returned);
            out.reset();
            argv = new String[3];
            argv[0] = "-mv";
            argv[1] = makeTestPathStr("testfile");
            argv[2] = makeTestPathStr("testfiletest");
            ret = ToolRunner.run(shell, argv);
            returned = out.toString();
            assertFalse(returned.contains("Renamed"), "no output from rename");
            out.reset();
            argv[0] = "-mv";
            argv[1] = makeTestPathStr("testfile");
            argv[2] = makeTestPathStr("testfiletmp");
            ret = ToolRunner.run(shell, argv);
            returned = out.toString();
            assertTrue(returned.contains("No such file or"), " unix like output");
            out.reset();
//            argv = new String[1];
//            argv[0] = "-du";
//            srcFs.mkdirs(srcFs.getHomeDirectory());
//            ret = ToolRunner.run(shell, argv);
//            returned = out.toString();
//            assertEquals(0, ret, " no error ");
//            assertTrue("empty path specified",
//                    (returned.lastIndexOf("empty string") == -1));
//            out.reset();
            argv = new String[3];
            argv[0] = "-test";
            argv[1] = "-d";
            argv[2] = makeTestPathStr("no/such/dir");
            ret = ToolRunner.run(shell, argv);
            returned = out.toString();
            assertEquals(1, ret, " -test -d wrong result ");
            assertTrue(returned.isEmpty());
        } finally {
            if (bak != null) {
                System.setErr(bak);
            }
        }
    }

    @Test
    public void testMoveWithTargetPortEmpty() throws Exception {
        Configuration conf = new HdfsConfiguration(BASE_CONF);
        try {
            conf.setInt(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 0);  // OK?
            FileSystem srcFs = getDefaultFS();
            FsShell shell = new FsShell(BASE_CONF);
            shell.setConf(conf);
            String[] argv = new String[2];
            argv[0] = "-mkdir";
            argv[1] = makeTestPathStr("testfile");
            ToolRunner.run(shell, argv);
            argv = new String[3];
            argv[0] = "-mv";
            argv[1] = srcFs.getUri() +  makeTestPathStr("testfile");
            argv[2] = "hdfs://" + srcFs.getUri().getHost() +  makeTestPathStr("testfile2");
            boolean res = srcFs.exists(testRootPath);
            int ret = ToolRunner.run(shell, argv);
            assertEquals(0, ret, "mv should have succeeded");
        } finally {
        }
    }

    @Test
    public void testTail() throws Exception {
        final int blockSize = 1024;
        final int fileLen = 5 * blockSize;
        final Configuration conf = new Configuration();
        conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 0);  // OK?
        {
            final DistributedFileSystem dfs = getDFS();

            // create a text file with multiple KB bytes (and multiple blocks)
            final Path testFile = new Path("testTail", "file1");
            final String text = RandomStringUtils.randomAscii(fileLen);
            try (OutputStream pout = dfs.create(testFile)) {
                pout.write(text.getBytes());
            }
            final ByteArrayOutputStream out = new ByteArrayOutputStream();
            System.setOut(new PrintStream(out));
            final String[] argv = new String[]{"-tail", testFile.toString()};
            final int ret = ToolRunner.run(new FsShell(conf), argv);

            assertEquals(0, ret, Arrays.toString(argv) + " returned " + ret);
            assertEquals(1024, out.size(), "-tail returned " + out.size() + " bytes data, expected 1KB");
            // tailed out last 1KB of the file content
            assertArrayEquals(text.substring(fileLen - 1024).getBytes(), out.toByteArray(), "Tail output doesn't match input");
            out.reset();
        }
    }

    @Test
    public void testTailWithFresh() throws Exception {
        final int blockSize = 1024;
        final Configuration conf = new Configuration();
        conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 0);  // OK?
        {
            final DistributedFileSystem dfs = getDFS();
            final Path testFile = new Path(makeTestPathStr("testTailWithFresh"), "file1");
            OutputStream pout = dfs.create(testFile);

            final ByteArrayOutputStream out = new ByteArrayOutputStream();
            System.setOut(new PrintStream(out));
            final Thread tailer = new Thread() {
                @Override
                public void run() {
                    final String[] argv = new String[]{"-tail", "-f",
                            testFile.toString()};
                    try {
                        ToolRunner.run(new FsShell(conf), argv);
                    } catch (Exception e) {
                        LOG.error("Client that tails the test file fails", e);
                    } finally {
                        out.reset();
                    }
                }
            };
            tailer.start();
            // wait till the tailer is sleeping
            GenericTestUtils.waitFor(new Supplier<Boolean>() {
                @Override
                public Boolean get() {
                    return tailer.getState() == Thread.State.TIMED_WAITING;
                }
            }, 100, 10000);

            final String text = RandomStringUtils.randomAscii(blockSize / 2);

            pout.write(text.getBytes());
            pout.close();
            // The tailer should eventually show the file contents
            GenericTestUtils.waitFor(new Supplier<Boolean>() {
                @Override
                public Boolean get() {
                    return Arrays.equals(text.getBytes(), out.toByteArray());
                }
            }, 100, 10000);
        }
    }

    @Test
    public void testText() throws Exception {
        Configuration conf = new HdfsConfiguration(BASE_CONF);
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 0);  // OK?
        try {
            final FileSystem dfs = getDefaultFS();
            textTest(makeTestPath("texttest").makeQualified(dfs.getUri(),
                    dfs.getWorkingDirectory()), conf);

            conf.set("fs.defaultFS", dfs.getUri().toString());
            final FileSystem lfs = FileSystem.getLocal(conf);
            textTest(new Path(TEST_ROOT_DIR, "texttest").makeQualified(lfs.getUri(),
                    lfs.getWorkingDirectory()), conf);
        } finally {

        }
    }

    private void textTest(Path root, Configuration conf) throws Exception {
        PrintStream bak = null;
        try {
            final FileSystem fs = root.getFileSystem(conf);
            fs.mkdirs(root);

            // Test the gzip type of files. Magic detection.
            OutputStream zout = new GZIPOutputStream(
                    fs.create(new Path(root, "file.gz")));
            Random r = new Random();
            bak = System.out;
            ByteArrayOutputStream file = new ByteArrayOutputStream();
            for (int i = 0; i < 1024; ++i) {
                char c = Character.forDigit(r.nextInt(26) + 10, 36);
                file.write(c);
                zout.write(c);
            }
            zout.close();

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            System.setOut(new PrintStream(out));

            String[] argv = new String[2];
            argv[0] = "-text";
            argv[1] = new Path(root, "file.gz").toString();
            int ret = ToolRunner.run(new FsShell(conf), argv);
            assertEquals(0, ret, "'-text " + argv[1] + " returned " + ret);
            assertTrue(Arrays.equals(file.toByteArray(), out.toByteArray()), "Output doesn't match input");

            // Create a sequence file with a gz extension, to test proper
            // container detection. Magic detection.
            SequenceFile.Writer writer = SequenceFile.createWriter(
                    conf,
                    SequenceFile.Writer.file(new Path(root, "file.gz")),
                    SequenceFile.Writer.keyClass(Text.class),
                    SequenceFile.Writer.valueClass(Text.class));
            writer.append(new Text("Foo"), new Text("Bar"));
            writer.close();
            out = new ByteArrayOutputStream();
            System.setOut(new PrintStream(out));
            argv = new String[2];
            argv[0] = "-text";
            argv[1] = new Path(root, "file.gz").toString();
            ret = ToolRunner.run(new FsShell(conf), argv);
            assertEquals(0, ret, "'-text " + argv[1] + " returned " + ret);
            assertTrue(Arrays.equals("Foo\tBar\n".getBytes(), out.toByteArray()), "Output doesn't match input");
            out.reset();

            // Test deflate. Extension-based detection.
            OutputStream dout = new DeflaterOutputStream(
                    fs.create(new Path(root, "file.deflate")));
            byte[] outbytes = "foo".getBytes();
            dout.write(outbytes);
            dout.close();
            out = new ByteArrayOutputStream();
            System.setOut(new PrintStream(out));
            argv = new String[2];
            argv[0] = "-text";
            argv[1] = new Path(root, "file.deflate").toString();
            ret = ToolRunner.run(new FsShell(conf), argv);
            assertEquals(0, ret, "'-text " + argv[1] + " returned " + ret);
            assertTrue(Arrays.equals(outbytes, out.toByteArray()), "Output doesn't match input");
            out.reset();

            // Test a simple codec. Extension based detection. We use
            // Bzip2 cause its non-native.
            CompressionCodec codec = ReflectionUtils.newInstance(BZip2Codec.class, conf);
            String extension = codec.getDefaultExtension();
            Path p = new Path(root, "file." + extension);
            OutputStream fout = new DataOutputStream(codec.createOutputStream(
                    fs.create(p, true)));
            byte[] writebytes = "foo".getBytes();
            fout.write(writebytes);
            fout.close();
            out = new ByteArrayOutputStream();
            System.setOut(new PrintStream(out));
            argv = new String[2];
            argv[0] = "-text";
            argv[1] = new Path(root, p).toString();
            ret = ToolRunner.run(new FsShell(conf), argv);
            assertEquals(0, ret, "'-text " + argv[1] + " returned " + ret);
            assertTrue(Arrays.equals(writebytes, out.toByteArray()), "Output doesn't match input");
            out.reset();
        } finally {
            if (null != bak) {
                System.setOut(bak);
            }
        }
    }

    @Test
    public void testCopyToLocal() throws IOException {
        Configuration conf = new HdfsConfiguration(BASE_CONF);
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 0);  // OK?

        FileSystem fs = getDefaultFS();
        assertTrue(fs instanceof DistributedFileSystem, "Not a HDFS: "+fs.getUri());
        DistributedFileSystem dfs = (DistributedFileSystem)fs;
        FsShell shell = new FsShell();
        shell.setConf(conf);

        try {
            String root = createTree(dfs, "copyToLocal");

            // Verify copying the tree
            {
                try {
                    assertEquals(0,
                            runCmd(shell, "-copyToLocal", root + "*", TEST_ROOT_DIR));
                } catch (Exception e) {
                    System.err.println("Exception raised from DFSShell.run " +
                            e.getLocalizedMessage());
                }

                File localroot = new File(TEST_ROOT_DIR, "copyToLocal");
                File localroot2 = new File(TEST_ROOT_DIR, "copyToLocal2");

                File f1 = new File(localroot, "f1");
                System.out.println(f1.getPath());
                assertTrue(f1.isFile(), "Copying failed.");

                File f2 = new File(localroot, "f2");
                assertTrue(f2.isFile(), "Copying failed.");

                File sub = new File(localroot, "sub");
                assertTrue(sub.isDirectory(), "Copying failed.");

                File f3 = new File(sub, "f3");
                assertTrue(f3.isFile(), "Copying failed.");

                File f4 = new File(sub, "f4");
                assertTrue(f4.isFile(), "Copying failed.");

                File f5 = new File(localroot2, "f1");
                assertTrue(f5.isFile(), "Copying failed.");

                f1.delete();
                f2.delete();
                f3.delete();
                f4.delete();
                f5.delete();
                sub.delete();
            }
            // Verify copying non existing sources do not create zero byte
            // destination files
            {
                String[] args = {"-copyToLocal", "nosuchfile", TEST_ROOT_DIR};
                try {
                    assertEquals(1, shell.run(args));
                } catch (Exception e) {
                    System.err.println("Exception raised from DFSShell.run " +
                            e.getLocalizedMessage());
                }
                File f6 = new File(TEST_ROOT_DIR, "nosuchfile");
                assertFalse(f6.exists());
            }
        } finally {
        }
    }

    static String createTree(FileSystem fs, String name) throws IOException {
        // create a tree
        //   ROOT
        //   |- f1
        //   |- f2
        //   + sub
        //      |- f3
        //      |- f4
        //   ROOT2
        //   |- f1
        String path = makeTestPathStr(name);
        Path root = mkdir(fs, new Path(path));
        Path sub = mkdir(fs, new Path(root, "sub"));
        Path root2 = mkdir(fs, new Path(path + "2"));

        writeFile(fs, new Path(root, "f1"));
        writeFile(fs, new Path(root, "f2"));
        writeFile(fs, new Path(sub, "f3"));
        writeFile(fs, new Path(sub, "f4"));
        writeFile(fs, new Path(root2, "f1"));
        mkdir(fs, new Path(root2, "sub"));
        return path;
    }

    private static int runCmd(FsShell shell, String... args) throws IOException {
        StringBuilder cmdline = new StringBuilder("RUN:");
        for (String arg : args) cmdline.append(" " + arg);
        LOG.info(cmdline.toString());
        try {
            int exitCode;
            exitCode = shell.run(args);
            LOG.info("RUN: "+args[0]+" exit=" + exitCode);
            return exitCode;
        } catch (IOException e) {
            LOG.error("RUN: "+args[0]+" IOException="+e.getMessage());
            throw e;
        } catch (RuntimeException e) {
            LOG.error("RUN: "+args[0]+" RuntimeException="+e.getMessage());
            throw e;
        } catch (Exception e) {
            LOG.error("RUN: "+args[0]+" Exception="+e.getMessage());
            throw new IOException(StringUtils.stringifyException(e));
        }
    }

    private static void runCount(String path, long dirs, long files, FsShell shell
    ) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(bytes);
        PrintStream oldOut = System.out;
        System.setOut(out);
        Scanner in = null;
        String results = null;
        try {
            runCmd(shell, "-count", path);
            results = bytes.toString();
            in = new Scanner(results);
            assertEquals(dirs, in.nextLong());
            assertEquals(files, in.nextLong());
        } finally {
            System.setOut(oldOut);
            if (in!=null) in.close();
            IOUtils.closeStream(out);
            System.out.println("results:\n" + results);
        }
    }

    @Test
    public void testCount() throws Exception {
        Configuration conf = new HdfsConfiguration(BASE_CONF);
        DistributedFileSystem dfs = getDFS();
        FsShell shell = new FsShell();
        shell.setConf(conf);

        try {
            String root = createTree(dfs, "count");

            // Verify the counts
            runCount(root, 2, 4, shell);
            runCount(root + "2", 2, 1, shell);
            runCount(root + "2/f1", 0, 1, shell);
            runCount(root + "2/sub", 1, 0, shell);

            final FileSystem localfs = FileSystem.getLocal(conf);
            Path localpath = new Path(TEST_ROOT_DIR, "testcount");
            localpath = localpath.makeQualified(localfs.getUri(),
                    localfs.getWorkingDirectory());
            localfs.mkdirs(localpath);

            final String localstr = localpath.toString();
            System.out.println("localstr=" + localstr);
            runCount(localstr, 1, 0, shell);
            assertEquals(0, runCmd(shell, "-count", root, localstr));
        } finally {
        }
    }

    @Test
    public void testTotalSizeOfAllFiles() throws Exception {
        Configuration conf = new HdfsConfiguration(BASE_CONF);

        try {
            conf.setInt(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 0);  // OK?
            FileSystem fs = getDefaultFS();
            FileSystem adminFs = getFileSystemAsAdmin();
            long tmp = adminFs.getUsed();
            // create file under root
            FSDataOutputStream File1 = fs.create(makeTestPath("File1"));
            File1.write("hi".getBytes());
            File1.close();
            // create file under sub-folder
            FSDataOutputStream File2 = fs.create(makeTestPath("Folder1/File2"));
            File2.write("hi".getBytes());
            File2.close();
            // getUsed() should return total length of all the files in Filesystem
            assertEquals(4 + tmp, adminFs.getUsed());
        } finally {
        }
    }

    private void confirmPermissionChange(String toApply, String expected,
                                         FileSystem fs, FsShell shell, Path dir2) throws IOException {
        LOG.info("Confirming permission change of " + toApply + " to " + expected);
        runCmd(shell, "-chmod", toApply, dir2.toString());

        String result = fs.getFileStatus(dir2).getPermission().toString();

        LOG.info("Permission change result: " + result);
        assertEquals(expected, result);
    }

    void testChmod(Configuration conf, FileSystem fs, String chmodDir)
            throws IOException {
        FsShell shell = new FsShell();
        shell.setConf(conf);

        try {
            //first make dir
            Path dir = new Path(chmodDir);
            fs.delete(dir, true);
            fs.mkdirs(dir);

            confirmPermissionChange(/* Setting */ "u+rwx,g=rw,o-rwx",
                    /* Should give */ "rwxrw----", fs, shell, dir);

            //create an empty file
            Path file = new Path(chmodDir, "file");
            TestDFSShell.writeFile(fs, file);

            //test octal mode
            confirmPermissionChange("644", "rw-r--r--", fs, shell, file);

            //test recursive
            runCmd(shell, "-chmod", "-R", "a+rwX", chmodDir);
            assertEquals("rwxrwxrwx",
                    fs.getFileStatus(dir).getPermission().toString());
            assertEquals("rw-rw-rw-",
                    fs.getFileStatus(file).getPermission().toString());

            // Skip "sticky bit" tests on Windows.
            //
            if (!Path.WINDOWS) {
                // test sticky bit on directories
                Path dir2 = new Path(dir, "stickybit");
                fs.mkdirs(dir2);
                LOG.info("Testing sticky bit on: " + dir2);
                LOG.info("Sticky bit directory initial mode: " +
                        fs.getFileStatus(dir2).getPermission());

                confirmPermissionChange("u=rwx,g=rx,o=rx", "rwxr-xr-x", fs, shell, dir2);

                confirmPermissionChange("+t", "rwxr-xr-t", fs, shell, dir2);

                confirmPermissionChange("-t", "rwxr-xr-x", fs, shell, dir2);

                confirmPermissionChange("=t", "--------T", fs, shell, dir2);

                confirmPermissionChange("0000", "---------", fs, shell, dir2);

                confirmPermissionChange("1666", "rw-rw-rwT", fs, shell, dir2);

                confirmPermissionChange("777", "rwxrwxrwt", fs, shell, dir2);

                fs.delete(dir2, true);
            } else {
                LOG.info("Skipped sticky bit tests on Windows");
            }

            fs.delete(dir, true);

        } finally {
            try {
                fs.close();
                shell.close();
            } catch (IOException ignored) {}
        }
    }

    private void confirmOwner(String owner, String group,
                              FileSystem fs, Path... paths) throws IOException {
        for(Path path : paths) {
            if (owner != null) {
                assertEquals(owner, fs.getFileStatus(path).getOwner());
            }
            if (group != null) {
                assertEquals(group, fs.getFileStatus(path).getGroup());
            }
        }
    }

    @Test
    @Disabled("konna : shell can not chmod local file?")
    public void testFilePermissions() throws IOException {
        Configuration conf = new HdfsConfiguration(BASE_CONF);

        //test chmod on local fs
        FileSystem fs = FileSystem.getLocal(conf);
        testChmod(conf, fs,
                (new File(TEST_ROOT_DIR, "chmodTest")).getAbsolutePath());

        conf.set(DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY, "true");

        //test chmod on DFS
        fs = getDefaultFS();
        testChmod(conf, fs, "/tmp/chmodTest");

        // test chown and chgrp on DFS:

        FsShell shell = new FsShell();
        shell.setConf(conf);

        /* For dfs, I am the super user and I can change owner of any file to
         * anything. "-R" option is already tested by chmod test above.
         */

        String file = "/tmp/chownTest";
        Path path = new Path(file);
        Path parent = new Path("/tmp");
        Path root = new Path("/");
        TestDFSShell.writeFile(fs, path);

        runCmd(shell, "-chgrp", "-R", "herbivores", "/*", "unknownFile*");
        confirmOwner(null, "herbivores", fs, parent, path);

        runCmd(shell, "-chgrp", "mammals", file);
        confirmOwner(null, "mammals", fs, path);

        runCmd(shell, "-chown", "-R", ":reptiles", "/");
        confirmOwner(null, "reptiles", fs, root, parent, path);

        runCmd(shell, "-chown", "python:", "/nonExistentFile", file);
        confirmOwner("python", "reptiles", fs, path);

        runCmd(shell, "-chown", "-R", "hadoop:toys", "unknownFile", "/");
        confirmOwner("hadoop", "toys", fs, root, parent, path);

        // Test different characters in names

        runCmd(shell, "-chown", "hdfs.user", file);
        confirmOwner("hdfs.user", null, fs, path);

        runCmd(shell, "-chown", "_Hdfs.User-10:_hadoop.users--", file);
        confirmOwner("_Hdfs.User-10", "_hadoop.users--", fs, path);

        runCmd(shell, "-chown", "hdfs/hadoop-core@apache.org:asf-projects", file);
        confirmOwner("hdfs/hadoop-core@apache.org", "asf-projects", fs, path);

        runCmd(shell, "-chgrp", "hadoop-core@apache.org/100", file);
        confirmOwner(null, "hadoop-core@apache.org/100", fs, path);
    }

    @Test
    public void testDFSShell() throws Exception {
        final Configuration conf = new HdfsConfiguration(BASE_CONF);
        /* This tests some properties of ChecksumFileSystem as well.
         * Make sure that we create ChecksumDFS */
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 0);  // OK?
        FileSystem fs = getDefaultFS();
        assertTrue(fs instanceof DistributedFileSystem, "Not a HDFS: "+fs.getUri());
        DistributedFileSystem fileSys = (DistributedFileSystem)fs;
        FsShell shell = new FsShell();
        shell.setConf(conf);

        try {
            // First create a new directory with mkdirs
            Path myPath = makeTestPath("test/mkdirs");
            assertTrue(fileSys.mkdirs(myPath));
            assertTrue(fileSys.exists(myPath));
            assertTrue(fileSys.mkdirs(myPath));

            // Second, create a file in that directory.
            Path myFile = makeTestPath("test/mkdirs/myFile");
            writeFile(fileSys, myFile);
            assertTrue(fileSys.exists(myFile));
            Path myFile2 = makeTestPath("test/mkdirs/myFile2");
            writeFile(fileSys, myFile2);
            assertTrue(fileSys.exists(myFile2));

            // Verify that rm with a pattern
            {
                String[] args = new String[3];
                args[0] = "-rm";
                args[1] = "-skipTrash";
                args[2] = makeTestPathStr("test/mkdirs/myFile*");
                int val = -1;
                try {
                    val = shell.run(args);
                } catch (Exception e) {
                    System.err.println("Exception raised from DFSShell.run " +
                            e.getLocalizedMessage());
                }
                assertEquals(0, val);
                assertFalse(fileSys.exists(myFile));
                assertFalse(fileSys.exists(myFile2));

                //re-create the files for other tests
                writeFile(fileSys, myFile);
                assertTrue(fileSys.exists(myFile));
                writeFile(fileSys, myFile2);
                assertTrue(fileSys.exists(myFile2));
            }

            // Verify that we can read the file
            {
                String[] args = new String[3];
                args[0] = "-cat";
                args[1] = makeTestPathStr("test/mkdirs/myFile");
                args[2] = makeTestPathStr("test/mkdirs/myFile2");
                int val = -1;
                try {
                    val = shell.run(args);
                } catch (Exception e) {
                    System.err.println("Exception raised from DFSShell.run: " +
                            StringUtils.stringifyException(e));
                }
                assertEquals(0, val);
            }
            fileSys.delete(myFile2, true);

            // Verify that we get an error while trying to read an nonexistent file
            {
                String[] args = new String[2];
                args[0] = "-cat";
                args[1] = makeTestPathStr("test/mkdirs/myFile1");
                int val = -1;
                try {
                    val = shell.run(args);
                } catch (Exception e) {
                    System.err.println("Exception raised from DFSShell.run " +
                            e.getLocalizedMessage());
                }
                assertNotEquals(0, val);
            }

            // Verify that we get an error while trying to delete an nonexistent file
            {
                String[] args = new String[2];
                args[0] = "-rm";
                args[1] = makeTestPathStr("test/mkdirs/myFile1");
                int val = -1;
                try {
                    val = shell.run(args);
                } catch (Exception e) {
                    System.err.println("Exception raised from DFSShell.run " +
                            e.getLocalizedMessage());
                }
                assertNotEquals(0, val);
            }

            // Verify that we succeed in removing the file we created
            {
                String[] args = new String[3];
                args[0] = "-rm";
                args[1] = "-skipTrash";
                args[2] = makeTestPathStr("test/mkdirs/myFile");
                int val = -1;
                try {
                    val = shell.run(args);
                } catch (Exception e) {
                    System.err.println("Exception raised from DFSShell.run " +
                            e.getLocalizedMessage());
                }
                assertEquals(0, val);
            }

            // Verify touch/test
            {
                String[] args;
                int val;

                args = new String[3];
                args[0] = "-test";
                args[1] = "-e";
                args[2] = makeTestPathStr("test/mkdirs/noFileHere");
                val = -1;
                try {
                    val = shell.run(args);
                } catch (Exception e) {
                    System.err.println("Exception raised from DFSShell.run " +
                            e.getLocalizedMessage());
                }
                assertEquals(1, val);

                args[1] = "-z";
                val = -1;
                try {
                    val = shell.run(args);
                } catch (Exception e) {
                    System.err.println("Exception raised from DFSShell.run " +
                            e.getLocalizedMessage());
                }
                assertEquals(1, val);

                args = new String[2];
                args[0] = "-touchz";
                args[1] = makeTestPathStr("test/mkdirs/isFileHere");
                val = -1;
                try {
                    val = shell.run(args);
                } catch (Exception e) {
                    System.err.println("Exception raised from DFSShell.run " +
                            e.getLocalizedMessage());
                }
                assertEquals(0, val);

                args = new String[2];
                args[0] = "-touchz";
                args[1] = makeTestPathStr("test/mkdirs/thisDirNotExists/isFileHere");
                val = -1;
                try {
                    val = shell.run(args);
                } catch (Exception e) {
                    System.err.println("Exception raised from DFSShell.run " +
                            e.getLocalizedMessage());
                }
                assertEquals(1, val);


                args = new String[3];
                args[0] = "-test";
                args[1] = "-e";
                args[2] = makeTestPathStr("test/mkdirs/isFileHere");
                val = -1;
                try {
                    val = shell.run(args);
                } catch (Exception e) {
                    System.err.println("Exception raised from DFSShell.run " +
                            e.getLocalizedMessage());
                }
                assertEquals(0, val);

                args[1] = "-d";
                val = -1;
                try {
                    val = shell.run(args);
                } catch (Exception e) {
                    System.err.println("Exception raised from DFSShell.run " +
                            e.getLocalizedMessage());
                }
                assertEquals(1, val);

                args[1] = "-z";
                val = -1;
                try {
                    val = shell.run(args);
                } catch (Exception e) {
                    System.err.println("Exception raised from DFSShell.run " +
                            e.getLocalizedMessage());
                }
                assertEquals(0, val);
            }

            // Verify that cp from a directory to a subdirectory fails
            {
                String[] args = new String[2];
                args[0] = "-mkdir";
                args[1] = makeTestPathStr("test/dir1");
                int val = -1;
                try {
                    val = shell.run(args);
                } catch (Exception e) {
                    System.err.println("Exception raised from DFSShell.run " +
                            e.getLocalizedMessage());
                }
                assertEquals(0, val);

//                // this should fail
                String[] args1 = new String[3];
                args1[0] = "-cp";
                args1[1] = makeTestPathStr("test/dir1");
                args1[2] = makeTestPathStr("test/dir1/dir2");
                val = 0;
                try {
                    val = shell.run(args1);
                } catch (Exception e) {
                    System.err.println("Exception raised from DFSShell.run " +
                            e.getLocalizedMessage());
                }
                assertEquals(1, val);

                // this should succeed
                args1[0] = "-cp";
                args1[1] = makeTestPathStr("test/dir1");
                args1[2] = makeTestPathStr("test/dir1foo");
                val = -1;
                try {
                    val = shell.run(args1);
                } catch (Exception e) {
                    System.err.println("Exception raised from DFSShell.run " +
                            e.getLocalizedMessage());
                }
                assertEquals(0, val);

                // this should fail
                args1[0] = "-cp";
                args1[1] = testDir;
                args1[2] = makeTestPathStr("test");
                val = 0;
                try {
                    val = shell.run(args1);
                } catch (Exception e) {
                    System.err.println("Exception raised from DFSShell.run " +
                            e.getLocalizedMessage());
                }
                assertEquals(1, val);
            }

            // Verify -test -f negative case (missing file)
            {
                String[] args = new String[3];
                args[0] = "-test";
                args[1] = "-f";
                args[2] = makeTestPathStr("test/mkdirs/noFileHere");
                int val = -1;
                try {
                    val = shell.run(args);
                } catch (Exception e) {
                    System.err.println("Exception raised from DFSShell.run " +
                            e.getLocalizedMessage());
                }
                assertEquals(1, val);
            }

            // Verify -test -f negative case (directory rather than file)
            {
                String[] args = new String[3];
                args[0] = "-test";
                args[1] = "-f";
                args[2] = makeTestPathStr("test/mkdirs");
                int val = -1;
                try {
                    val = shell.run(args);
                } catch (Exception e) {
                    System.err.println("Exception raised from DFSShell.run " +
                            e.getLocalizedMessage());
                }
                assertEquals(1, val);
            }

            // Verify -test -f positive case
            {
                writeFile(fileSys, myFile);
                assertTrue(fileSys.exists(myFile));

                String[] args = new String[3];
                args[0] = "-test";
                args[1] = "-f";
                args[2] = myFile.toString();
                int val = -1;
                try {
                    val = shell.run(args);
                } catch (Exception e) {
                    System.err.println("Exception raised from DFSShell.run " +
                            e.getLocalizedMessage());
                }
                assertEquals(0, val);
            }

            // Verify -test -s negative case (missing file)
            {
                String[] args = new String[3];
                args[0] = "-test";
                args[1] = "-s";
                args[2] = makeTestPathStr("test/mkdirs/noFileHere");
                int val = -1;
                try {
                    val = shell.run(args);
                } catch (Exception e) {
                    System.err.println("Exception raised from DFSShell.run " +
                            e.getLocalizedMessage());
                }
                assertEquals(1, val);
            }

            // Verify -test -s negative case (zero length file)
            {
                String[] args = new String[3];
                args[0] = "-test";
                args[1] = "-s";
                args[2] = makeTestPathStr("test/mkdirs/isFileHere");
                int val = -1;
                try {
                    val = shell.run(args);
                } catch (Exception e) {
                    System.err.println("Exception raised from DFSShell.run " +
                            e.getLocalizedMessage());
                }
                assertEquals(1, val);
            }

            // Verify -test -s positive case (nonzero length file)
            {
                String[] args = new String[3];
                args[0] = "-test";
                args[1] = "-s";
                args[2] = myFile.toString();
                int val = -1;
                try {
                    val = shell.run(args);
                } catch (Exception e) {
                    System.err.println("Exception raised from DFSShell.run " +
                            e.getLocalizedMessage());
                }
                assertEquals(0, val);
            }

            // konna : TODO: checkAccess() not implemented.
//            // Verify -test -w/-r
//            {
//                Path permDir = makeTestPath("test/permDir");
//                Path permFile = makeTestPath("test/permDir/permFile");
//                mkdir(fs, permDir);
//                writeFile(fs, permFile);
//
//                // Verify -test -w positive case (dir exists and can write)
//                final String[] wargs = new String[3];
//                wargs[0] = "-test";
//                wargs[1] = "-w";
//                wargs[2] = permDir.toString();
//                int val = -1;
//                try {
//                    val = shell.run(wargs);
//                } catch (Exception e) {
//                    System.err.println("Exception raised from DFSShell.run " +
//                            e.getLocalizedMessage());
//                }
//                assertEquals(0, val);
//
//                // Verify -test -r positive case (file exists and can read)
//                final String[] rargs = new String[3];
//                rargs[0] = "-test";
//                rargs[1] = "-r";
//                rargs[2] = permFile.toString();
//                try {
//                    val = shell.run(rargs);
//                } catch (Exception e) {
//                    System.err.println("Exception raised from DFSShell.run " +
//                            e.getLocalizedMessage());
//                }
//                assertEquals(0, val);
//
//                // Verify -test -r negative case (file exists but cannot read)
//                runCmd(shell, "-chmod", "600", permFile.toString());
//
//                UserGroupInformation smokeUser =
//                        UserGroupInformation.createUserForTesting("smokeUser",
//                                new String[] {"hadoop"});
//                smokeUser.doAs(new PrivilegedExceptionAction<String>() {
//                    @Override
//                    public String run() throws Exception {
//                        FsShell shell = new FsShell(conf);
//                        int exitCode = shell.run(rargs);
//                        assertEquals(1, exitCode);
//                        return null;
//                    }
//                });
//
//                // Verify -test -w negative case (dir exists but cannot write)
//                runCmd(shell, "-chown", "-R", "not_allowed", permDir.toString());
//                runCmd(shell, "-chmod", "-R", "700", permDir.toString());
//
//                smokeUser.doAs(new PrivilegedExceptionAction<String>() {
//                    @Override
//                    public String run() throws Exception {
//                        FsShell shell = new FsShell(conf);
//                        int exitCode = shell.run(wargs);
//                        assertEquals(1, exitCode);
//                        return null;
//                    }
//                });
//
//                // cleanup
//                fs.delete(permDir, true);
//            }
        } finally {
        }
    }

    @Test
    public void testRemoteException() throws Exception {
        UserGroupInformation tmpUGI =
                UserGroupInformation.createUserForTesting("tmpname", new String[] {"mygroup"});
        PrintStream bak = null;
        try {
            final Configuration conf = new HdfsConfiguration(BASE_CONF);
            FileSystem fs = getDefaultFS();
            Path p = makeTestPath("foo");
            fs.mkdirs(p);
            fs.setPermission(p, new FsPermission((short)0700));
            bak = System.err;

            tmpUGI.doAs(new PrivilegedExceptionAction<Object>() {
                @Override
                public Object run() throws Exception {
                    FsShell fshell = new FsShell(conf);
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    PrintStream tmp = new PrintStream(out);
                    System.setErr(tmp);
                    String[] args = new String[2];
                    args[0] = "-ls";
                    args[1] = makeTestPathStr("foo");
                    int ret = ToolRunner.run(fshell, args);
                    assertEquals(1, ret, "returned should be 1");
                    String str = out.toString();
                    assertTrue(str.contains("Permission denied"), "permission denied printed");
                    out.reset();
                    return null;
                }
            });
        } finally {
            if (bak != null) {
                System.setErr(bak);
            }
        }
    }

    private static void doFsStat(Configuration conf, String format, Path... files)
            throws Exception {
        if (files == null || files.length == 0) {
            final String[] argv = (format == null ? new String[] {"-stat"} :
                    new String[] {"-stat", format});
            assertEquals(-1, ToolRunner.run(new FsShell(conf), argv), "Should have failed with missing arguments");
        } else {
            List<String> argv = new LinkedList<>();
            argv.add("-stat");
            if (format != null) {
                argv.add(format);
            }
            for (Path f : files) {
                argv.add(f.toString());
            }

            int ret = ToolRunner.run(new FsShell(conf), argv.toArray(new String[0]));
            assertEquals(0, ret, argv + " returned non-zero status " + ret);
        }
    }

    @Test
    public void testStat() throws Exception {
        final int blockSize = 1024;
        final Configuration conf = new HdfsConfiguration(BASE_CONF);
        {

            final DistributedFileSystem dfs = getDFS();

            final SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            fmt.setTimeZone(TimeZone.getTimeZone("UTC"));
            final Path testDir1 = new Path(makeTestPathStr("testStat"), "dir1");
            dfs.mkdirs(testDir1);
            final Path testFile2 = new Path(testDir1, "file2");
            createFile(dfs, testFile2, 2 * blockSize, (short) 3, 0);
            final FileStatus status1 = dfs.getFileStatus(testDir1);
            final String mtime1 = fmt.format(new Date(status1.getModificationTime()));
            final FileStatus status2 = dfs.getFileStatus(testFile2);
            final String mtime2 = fmt.format(new Date(status2.getModificationTime()));

            final ByteArrayOutputStream out = new ByteArrayOutputStream();
            System.setOut(new PrintStream(out));

            doFsStat(conf, null);

            out.reset();
            doFsStat(conf, null, testDir1);
            assertEquals(out.toString(), String.format("%s%n", mtime1), "Unexpected -stat output: " + out);

            out.reset();
            doFsStat(conf, null, testDir1, testFile2);
            assertEquals(out.toString(), String.format("%s%n%s%n", mtime1, mtime2), "Unexpected -stat output: " + out);

            doFsStat(conf, "%F %u:%g %b %y %n");

            out.reset();
            doFsStat(conf, "%F %u:%g %b %y %n", testDir1);
            assertTrue(out.toString().contains(mtime1));
            assertTrue(out.toString().contains("directory"));
            assertTrue(out.toString().contains(status1.getGroup()));

            out.reset();
            doFsStat(conf, "%F %u:%g %b %y %n", testDir1, testFile2);
            assertTrue(out.toString().contains(mtime1));
            assertTrue(out.toString().contains("regular file"));
            assertTrue(out.toString().contains(mtime2));
        }
    }

    @Test
    public void testLsr() throws Exception {
        final Configuration conf = new HdfsConfiguration(BASE_CONF);
        DistributedFileSystem dfs = getDFS();

        try {
            final String root = createTree(dfs, "lsr");
            dfs.mkdirs(new Path(root, "zzz"));

            runLsr(new FsShell(conf), root, 0);

            final Path sub = new Path(root, "sub");
            dfs.setPermission(sub, new FsPermission((short)0));

            final UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
            final String tmpusername = ugi.getShortUserName() + "1";
            UserGroupInformation tmpUGI = UserGroupInformation.createUserForTesting(
                    tmpusername, new String[] {tmpusername});
            String results = tmpUGI.doAs(new PrivilegedExceptionAction<String>() {
                @Override
                public String run() throws Exception {
                    return runLsr(new FsShell(conf), root, 1);
                }
            });
            assertTrue(results.contains("zzz"));
        } finally {
        }
    }

    private static String runLsr(final FsShell shell, String root, int returnvalue) throws Exception {
        System.out.println("root=" + root + ", returnvalue=" + returnvalue);
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        final PrintStream out = new PrintStream(bytes);
        final PrintStream oldOut = System.out;
        final PrintStream oldErr = System.err;
        System.setOut(out);
        System.setErr(out);
        final String results;
        try {
            assertEquals(returnvalue, shell.run(new String[]{"-lsr", root}));
            results = bytes.toString();
        } finally {
            System.setOut(oldOut);
            System.setErr(oldErr);
            IOUtils.closeStream(out);
        }
        System.out.println("results:\n" + results);
        return results;
    }

    static File createLocalFileWithRandomData(int fileLength, File f)
            throws IOException {
        assertTrue(!f.exists());
        f.createNewFile();
        FileOutputStream out = new FileOutputStream(f.toString());
        byte[] buffer = new byte[fileLength];
        out.write(buffer);
        out.flush();
        out.close();
        return f;
    }

    @Test
    public void testAppendToFile() throws Exception {
        final int inputFileLength = 1024 * 1024;
        File testRoot = new File(TEST_ROOT_DIR, "testAppendtoFileDir");
        testRoot.mkdirs();

        File file1 = new File(testRoot, "file1");
        File file2 = new File(testRoot, "file2");
        createLocalFileWithRandomData(inputFileLength, file1);
        createLocalFileWithRandomData(inputFileLength, file2);

        Configuration conf = new HdfsConfiguration(BASE_CONF);

        try {
            FileSystem dfs = getDefaultFS();
            assertTrue(dfs instanceof DistributedFileSystem, "Not a HDFS: " + dfs.getUri());

            // Run appendToFile once, make sure that the target file is
            // created and is of the right size.
            Path remoteFile = makeTestPath("remoteFile");
            FsShell shell = new FsShell();
            shell.setConf(conf);
            String[] argv = new String[] {
                    "-appendToFile", file1.toString(), file2.toString(), remoteFile.toString() };
            int res = ToolRunner.run(shell, argv);
            assertThat(res, is(0));
            assertThat(dfs.getFileStatus(remoteFile).getLen(), is((long) inputFileLength * 2));

            // Run the command once again and make sure that the target file
            // size has been doubled.
            res = ToolRunner.run(shell, argv);
            assertThat(res, is(0));
            assertThat(dfs.getFileStatus(remoteFile).getLen(), is((long) inputFileLength * 4));
        } finally {
        }
    }
}