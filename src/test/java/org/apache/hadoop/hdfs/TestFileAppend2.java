package org.apache.hadoop.hdfs;

import com.aliyun.jindodata.gateway.JindoMultiClusterTestBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

import static org.apache.hadoop.hdfs.DfsAppendTestUtil.getFileSystemAs;
import static org.junit.jupiter.api.Assertions.*;

public class TestFileAppend2 extends JindoMultiClusterTestBase {
    private byte[] fileContents = null;

    final int numberOfFiles = 50;
    final int numThreads = 10;
    final int numAppendsPerThread = 20;

    Workload[] workload = null;
    final ArrayList<Path> testFiles    = new ArrayList<Path>();
    volatile static boolean         globalStatus = true;

    @Test
    public void testSimpleAppend() throws IOException {
        final Configuration conf = new HdfsConfiguration(BASE_CONF);
        conf.setInt(DFSConfigKeys.DFS_DATANODE_HANDLER_COUNT_KEY, 50);
        fileContents = DfsAppendTestUtil.initBuffer(DfsAppendTestUtil.FILE_SIZE);
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 0);  // OK?
        FileSystem fs = getNonCachedFs();
        try {
            { // test appending to a file.

                // create a new file.
                Path file1 = makeTestPath("simpleAppend.dat");
                FSDataOutputStream stm = DfsAppendTestUtil.createFile(fs, file1, 1);
                System.out.println("Created file simpleAppend.dat");

                // write to file
                int mid = 186;   // io.bytes.per.checksum bytes
                System.out.println("Writing " + mid + " bytes to file " + file1);
                stm.write(fileContents, 0, mid);
                stm.close();
                System.out.println("Wrote and Closed first part of file.");

                // write to file
                int mid2 = 607;   // io.bytes.per.checksum bytes
                System.out.println("Writing " + mid + " bytes to file " + file1);
                stm = fs.append(file1);
                stm.write(fileContents, mid, mid2 - mid);
                stm.close();
                System.out.println("Wrote and Closed second part of file.");

                // write the remainder of the file
                stm = fs.append(file1);

                // ensure getPos is set to reflect existing size of the file
                assertTrue(stm.getPos() > 0);

                System.out.println("Writing " + (DfsAppendTestUtil.FILE_SIZE - mid2) +
                        " bytes to file " + file1);
                stm.write(fileContents, mid2, DfsAppendTestUtil.FILE_SIZE - mid2);
                System.out.println("Written second part of file");
                stm.close();
                System.out.println("Wrote and Closed second part of file.");

                // verify that entire file is good
                DfsAppendTestUtil.checkFullFile(fs, file1, DfsAppendTestUtil.FILE_SIZE,
                        fileContents, "Read 2");
            }

            { // test appending to an non-existing file.
                FSDataOutputStream out = null;
                try {
                    out = fs.append(makeTestPath("non-existing.dat"));
                    fail("Expected to have FileNotFoundException");
                } catch (java.io.FileNotFoundException fnfe) {
                    System.out.println("Good: got " + fnfe);
                    fnfe.printStackTrace(System.out);
                } finally {
                    IOUtils.closeStream(out);
                }
            }

            { // test append permission.
                fs.close();

                // login as a different user
                final UserGroupInformation superuser =
                        UserGroupInformation.getCurrentUser();
                String username = "testappenduser";
                String group = "testappendgroup";
                assertFalse(superuser.getShortUserName().equals(username));
                assertFalse(Arrays.asList(superuser.getGroupNames()).contains(group));
                UserGroupInformation appenduser =
                        UserGroupInformation.createUserForTesting(username, new String[]{group});

                fs = getFileSystemAs(appenduser, conf);

                // create a file
                Path dir = makeTestPath(getClass().getSimpleName());
                Path foo = new Path(dir, "foo.dat");
                FSDataOutputStream out = null;
                int offset = 0;
                try {
                    out = fs.create(foo);
                    int len = 10 + DfsAppendTestUtil.nextInt(100);
                    out.write(fileContents, offset, len);
                    offset += len;
                } finally {
                    IOUtils.closeStream(out);
                }

                // change dir and foo to minimal permissions.
                fs.setPermission(dir, new FsPermission((short) 0100));
                fs.setPermission(foo, new FsPermission((short) 0200));

                // try append, should success
                out = null;
                try {
                    out = fs.append(foo);
                    int len = 10 + DfsAppendTestUtil.nextInt(100);
                    out.write(fileContents, offset, len);
                    offset += len;
                } finally {
                    IOUtils.closeStream(out);
                }

                //perm check is disabled in JindoFS by default

                // change dir and foo to all but no write on foo.
                fs.setPermission(foo, new FsPermission((short) 0577));
                fs.setPermission(dir, new FsPermission((short) 0777));

                // try append, should fail
                out = null;
                try {
                    out = fs.append(foo);
                    fail("Expected to have AccessControlException");
                } catch (AccessControlException ace) {
                    System.out.println("Good: got " + ace);
                    ace.printStackTrace(System.out);
                } finally {
                    IOUtils.closeStream(out);
                }

            }
        } catch (IOException e) {
            System.out.println("Exception :" + e);
            throw e;
        } catch (Throwable e) {
            System.out.println("Throwable :" + e);
            e.printStackTrace();
            throw new IOException("Throwable : " + e);
        } finally {
            fs.close();
        }
    }

    @Test
    public void testSimpleAppend2() throws Exception {
        final Configuration conf = new HdfsConfiguration(BASE_CONF);
        conf.setInt(DFSConfigKeys.DFS_DATANODE_HANDLER_COUNT_KEY, 50);
        fileContents = DfsAppendTestUtil.initBuffer(DfsAppendTestUtil.FILE_SIZE);
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 0);  // OK?
        DistributedFileSystem fs = getNonCachedDFS();
        try {
            { // test appending to a file.
                // create a new file.
                Path file1 = makeTestPath("simpleAppend.dat");
                FSDataOutputStream stm = DfsAppendTestUtil.createFile(fs, file1, 1);
                System.out.println("Created file simpleAppend.dat");

                // write to file
                int mid = 186;   // io.bytes.per.checksum bytes
                System.out.println("Writing " + mid + " bytes to file " + file1);
                stm.write(fileContents, 0, mid);
                stm.close();
                System.out.println("Wrote and Closed first part of file.");

                // write to file
                int mid2 = 607;   // io.bytes.per.checksum bytes
                System.out.println("Writing " + mid + " bytes to file " + file1);
                stm = fs.append(file1,
                        EnumSet.of(CreateFlag.APPEND, CreateFlag.NEW_BLOCK), 4096, null);
                stm.write(fileContents, mid, mid2-mid);
                stm.close();
                System.out.println("Wrote and Closed second part of file.");

                // write the remainder of the file
                stm = fs.append(file1,
                        EnumSet.of(CreateFlag.APPEND, CreateFlag.NEW_BLOCK), 4096, null);
                // ensure getPos is set to reflect existing size of the file
                assertTrue(stm.getPos() > 0);
                System.out.println("Writing " + (DfsAppendTestUtil.FILE_SIZE - mid2) +
                        " bytes to file " + file1);
                stm.write(fileContents, mid2, DfsAppendTestUtil.FILE_SIZE - mid2);
                System.out.println("Written second part of file");
                stm.close();
                System.out.println("Wrote and Closed second part of file.");

                // verify that entire file is good
                DfsAppendTestUtil.checkFullFile(fs, file1, DfsAppendTestUtil.FILE_SIZE,
                        fileContents, "Read 2");
                // also make sure there three different blocks for the file
                List<LocatedBlock> blocks = fs.getClient().getLocatedBlocks(
                        file1.toString(), 0L).getLocatedBlocks();
                assertEquals(12, blocks.size()); // the block size is 1024
                assertEquals(mid, blocks.get(0).getBlockSize());
                assertEquals(mid2 - mid, blocks.get(1).getBlockSize());
                for (int i = 2; i < 11; i++) {
                    assertEquals(DfsAppendTestUtil.BLOCK_SIZE, blocks.get(i).getBlockSize());
                }
                assertEquals((DfsAppendTestUtil.FILE_SIZE - mid2)
                        % DfsAppendTestUtil.BLOCK_SIZE, blocks.get(11).getBlockSize());
            }

            { // test appending to an non-existing file.
                FSDataOutputStream out = null;
                try {
                    out = fs.append(makeTestPath("non-existing.dat"),
                            EnumSet.of(CreateFlag.APPEND, CreateFlag.NEW_BLOCK), 4096, null);
                    fail("Expected to have FileNotFoundException");
                } catch(java.io.FileNotFoundException fnfe) {
                    System.out.println("Good: got " + fnfe);
                    fnfe.printStackTrace(System.out);
                } finally {
                    IOUtils.closeStream(out);
                }
            }

            { // test append permission.
                fs.close();

                // login as a different user
                final UserGroupInformation superuser =
                        UserGroupInformation.getCurrentUser();
                String username = "testappenduser";
                String group = "testappendgroup";
                assertFalse(superuser.getShortUserName().equals(username));
                assertFalse(Arrays.asList(superuser.getGroupNames()).contains(group));
                UserGroupInformation appenduser = UserGroupInformation
                        .createUserForTesting(username, new String[] { group });

                fs = (DistributedFileSystem) getFileSystemAs(appenduser,
                        conf);

                // create a file
                Path dir = makeTestPath(getClass().getSimpleName());
                Path foo = new Path(dir, "foo.dat");
                FSDataOutputStream out = null;
                int offset = 0;
                try {
                    out = fs.create(foo);
                    int len = 10 + DfsAppendTestUtil.nextInt(100);
                    out.write(fileContents, offset, len);
                    offset += len;
                } finally {
                    IOUtils.closeStream(out);
                }

                // change dir and foo to minimal permissions.
                fs.setPermission(dir, new FsPermission((short)0100));
                fs.setPermission(foo, new FsPermission((short)0200));

                // try append, should success
                out = null;
                try {
                    out = fs.append(foo,
                            EnumSet.of(CreateFlag.APPEND, CreateFlag.NEW_BLOCK), 4096, null);
                    int len = 10 + DfsAppendTestUtil.nextInt(100);
                    out.write(fileContents, offset, len);
                    offset += len;
                } finally {
                    IOUtils.closeStream(out);
                }

                // change dir and foo to all but no write on foo.
                fs.setPermission(foo, new FsPermission((short)0577));
                fs.setPermission(dir, new FsPermission((short)0777));

                // try append, should fail
                out = null;
                try {
                    out = fs.append(foo,
                            EnumSet.of(CreateFlag.APPEND, CreateFlag.NEW_BLOCK), 4096, null);
                    fail("Expected to have AccessControlException");
                } catch(AccessControlException ace) {
                    System.out.println("Good: got " + ace);
                    ace.printStackTrace(System.out);
                } finally {
                    IOUtils.closeStream(out);
                }
            }
        } finally {
            fs.close();
        }
    }

    @Test
    public void testLongTimeWrite() throws Exception {
        final Configuration conf = new HdfsConfiguration(BASE_CONF);
        conf.setInt(DFSConfigKeys.DFS_DATANODE_HANDLER_COUNT_KEY, 50);
        int fileSize = 3 * 1024 * 1024 + 77;
        fileContents = DfsAppendTestUtil.initBuffer(fileSize);
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 0);  // OK?
        DistributedFileSystem fs = getDFS();
        try {
            { // test appending to a file.
                // create a new file.
                Path file1 = makeTestPath("simpleAppend.dat");
                FSDataOutputStream stm = DfsAppendTestUtil.createFile(fs, file1, 1);
                System.out.println("Created file simpleAppend.dat");

                // write to file
                int mid = 2 * 1024 * 1024;   // io.bytes.per.checksum bytes
                System.out.println("Writing " + mid + " bytes to file " + file1);
                stm.write(fileContents, 0, mid);
                System.out.println("Wrote and Closed first part of file.");

                // write to file
                int mid2 = 321992 + mid;   // io.bytes.per.checksum bytes
                System.out.println("Writing " + mid + " bytes to file " + file1);
                stm.write(fileContents, mid, mid2-mid);
                System.out.println("Wrote and Closed second part of file.");
                Thread.sleep(70 * 1000);

                // write the remainder of the file
                // ensure getPos is set to reflect existing size of the file
                assertTrue(stm.getPos() > 0);
                System.out.println("Writing " + (fileSize - mid2) +
                        " bytes to file " + file1);
                stm.write(fileContents, mid2, fileSize - mid2);
                System.out.println("Written second part of file");
                stm.close();
                System.out.println("Wrote and Closed second part of file.");

                // verify that entire file is good
                DfsAppendTestUtil.checkFullFile(fs, file1, fileSize,
                        fileContents, "Read 2");
                // also make sure there three different blocks for the file
                List<LocatedBlock> blocks = fs.getClient().getLocatedBlocks(
                        file1.toString(), 0L).getLocatedBlocks();
                assertEquals(4, blocks.size()); // the block size is 1024
                assertEquals(DfsAppendTestUtil.BLOCK_SIZE, blocks.get(0).getBlockSize());
                assertEquals(DfsAppendTestUtil.BLOCK_SIZE, blocks.get(1).getBlockSize());
                assertEquals(DfsAppendTestUtil.BLOCK_SIZE, blocks.get(2).getBlockSize());
                assertEquals((fileSize)
                        % DfsAppendTestUtil.BLOCK_SIZE, blocks.get(3).getBlockSize());
            }

            { // test appending to an non-existing file.
                FSDataOutputStream out = null;
                try {
                    out = fs.append(makeTestPath("non-existing.dat"),
                            EnumSet.of(CreateFlag.APPEND, CreateFlag.NEW_BLOCK), 4096, null);
                    fail("Expected to have FileNotFoundException");
                } catch(java.io.FileNotFoundException fnfe) {
                    System.out.println("Good: got " + fnfe);
                    fnfe.printStackTrace(System.out);
                } finally {
                    IOUtils.closeStream(out);
                }
            }
        } finally {
        }
    }

    class Workload extends Thread {
        private final int            id;
        private final boolean        appendToNewBlock;

        Workload(int threadIndex, boolean append2) {
            id = threadIndex;
            this.appendToNewBlock = append2;
        }

        // create a bunch of files. Write to them and then verify.
        @Override
        public void run() {
            System.out.println("Workload " + id + " starting... ");
            for (int i = 0; i < numAppendsPerThread; i++) {

                // pick a file at random and remove it from pool
                Path testfile;
                synchronized (testFiles) {
                    if (testFiles.size() == 0) {
                        System.out.println("Completed write to almost all files.");
                        return;
                    }
                    int index = DfsAppendTestUtil.nextInt(testFiles.size());
                    testfile = testFiles.remove(index);
                }

                long len = 0;
                int sizeToAppend = 0;
                try {
                    DistributedFileSystem fs = getDFS();

                    // add a random number of bytes to file
                    len = fs.getFileStatus(testfile).getLen();

                    // if file is already full, then pick another file
                    if (len >= DfsAppendTestUtil.FILE_SIZE) {
                        System.out.println("File " + testfile + " is full.");
                        continue;
                    }

                    // do small size appends so that we can trigger multiple
                    // appends to the same file.
                    //
                    int left = (int)(DfsAppendTestUtil.FILE_SIZE - len)/3;
                    if (left <= 0) {
                        left = 1;
                    }
                    sizeToAppend = DfsAppendTestUtil.nextInt(left);

                    System.out.println("Workload thread " + id +
                            " appending " + sizeToAppend + " bytes " +
                            " to file " + testfile +
                            " of size " + len);
                    FSDataOutputStream stm = appendToNewBlock ? fs.append(testfile,
                            EnumSet.of(CreateFlag.APPEND, CreateFlag.NEW_BLOCK), 4096, null)
                            : fs.append(testfile);
                    stm.write(fileContents, (int)len, sizeToAppend);
                    stm.close();

                    // wait for the file size to be reflected in the namenode metadata
                    while (fs.getFileStatus(testfile).getLen() != (len + sizeToAppend)) {
                        try {
                            System.out.println("Workload thread " + id +
                                    " file " + testfile  +
                                    " size " + fs.getFileStatus(testfile).getLen() +
                                    " expected size " + (len + sizeToAppend) +
                                    " waiting for namenode metadata update.");
                            Thread.sleep(5000);
                        } catch (InterruptedException e) {}
                    }

                    assertEquals(len + sizeToAppend, fs.getFileStatus(testfile).getLen(),
                            "File " + testfile + " size is " +
                                    fs.getFileStatus(testfile).getLen() +
                                    " but expected " + (len + sizeToAppend));

                    DfsAppendTestUtil.checkFullFile(fs, testfile, (int) (len + sizeToAppend),
                            fileContents, "Read 2");
                } catch (Throwable e) {
                    System.out.println("sls" + e);
                    globalStatus = false;
                    if (e.toString() != null) {
                        System.out.println("Workload exception " + id +
                                " testfile " + testfile +
                                " " + e);
                        e.printStackTrace();
                    }
                    fail("Workload exception " + id + " testfile " + testfile +
                                    " expected size " + (len + sizeToAppend));
                }

                // Add testfile back to the pool of files.
                synchronized (testFiles) {
                    testFiles.add(testfile);
                }
            }
        }
    }

    private void testComplexAppend(boolean appendToNewBlock) throws IOException {
        fileContents = DfsAppendTestUtil.initBuffer(DfsAppendTestUtil.FILE_SIZE);
        Configuration conf = new HdfsConfiguration(BASE_CONF);
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 2000);
        conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 2);
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_PENDING_TIMEOUT_SEC_KEY, 2);
        conf.setInt(HdfsClientConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY, 30000);
        conf.setInt(HdfsClientConfigKeys.DFS_DATANODE_SOCKET_WRITE_TIMEOUT_KEY, 30000);
        conf.setInt(DFSConfigKeys.DFS_DATANODE_HANDLER_COUNT_KEY, 50);
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 0);  // OK?

        FileSystem fs = getDefaultFS();

        try {
            // create a bunch of test files with random replication factors.
            // Insert them into a linked list.
            //
            for (int i = 0; i < numberOfFiles; i++) {
                final int replication = 1;
                Path testFile = makeTestPath(i + ".dat");
                FSDataOutputStream stm =
                        DfsAppendTestUtil.createFile(fs, testFile, replication);
                stm.close();
                testFiles.add(testFile);
            }

            // Create threads and make them run workload concurrently.
            workload = new Workload[numThreads];
            for (int i = 0; i < numThreads; i++) {
                workload[i] = new Workload(i, appendToNewBlock);
                workload[i].start();
            }

            // wait for all transactions to get over
            for (int i = 0; i < numThreads; i++) {
                try {
                    System.out.println("Waiting for thread " + i + " to complete...");
                    workload[i].join();
                    System.out.println("Waiting for thread " + i + " complete.");
                } catch (InterruptedException e) {
                    i--;      // retry
                }
            }
        } finally {
        }

        // If any of the worker thread failed in their job, indicate that
        // this test failed.
        //
        assertTrue(globalStatus, "testComplexAppend Worker encountered exceptions.");
    }

    @Test
    public void testComplexAppend() throws IOException {
        testComplexAppend(false);
    }

    @Test
    public void testComplexAppend2() throws IOException {
        testComplexAppend(true);
    }

    @Test
    public void testAppendLessThanChecksumChunk() throws Exception {
        final byte[] buf = new byte[1024];
        DistributedFileSystem fs = getDFS();
        try {
            final int len1 = 200;
            final int len2 = 300;
            final Path p = makeTestPath("foo");

            FSDataOutputStream out = fs.create(p);
            out.write(buf, 0, len1);
            out.close();

            out = fs.append(p);
            out.write(buf, 0, len2);
            // flush but leave open
            out.hflush();

            // read data to verify the replica's content and checksum are correct
            FSDataInputStream in = fs.open(p);
            final int length = in.read(0, buf, 0, len1 + len2);
            assertTrue(length > 0);
            in.close();
            out.close();
        } finally {
        }
    }
}