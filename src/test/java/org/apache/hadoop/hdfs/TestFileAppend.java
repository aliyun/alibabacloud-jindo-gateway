package org.apache.hadoop.hdfs;

import com.aliyun.jindodata.gateway.JindoMultiClusterTestBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.EnumSet;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestFileAppend extends JindoMultiClusterTestBase {
    private static final long RANDOM_TEST_RUNTIME = 10000;
    private static byte[] fileContents = null;

    private void checkFile(DistributedFileSystem fileSys, Path name, int repl)
            throws IOException {
        boolean done = false;

        // wait till all full blocks are confirmed by the datanodes.
        while (!done) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                ;
            }
            done = true;
            BlockLocation[] locations = fileSys.getFileBlockLocations(
                    fileSys.getFileStatus(name), 0, DfsAppendTestUtil.FILE_SIZE);
            if (locations.length < DfsAppendTestUtil.NUM_BLOCKS) {
                System.out.println("Number of blocks found " + locations.length);
                done = false;
                continue;
            }
            for (int idx = 0; idx < DfsAppendTestUtil.NUM_BLOCKS; idx++) {
                if (locations[idx].getHosts().length < repl) {
                    System.out.println("Block index " + idx + " not yet replciated.");
                    done = false;
                    break;
                }
            }
        }
        byte[] expected =
                new byte[DfsAppendTestUtil.NUM_BLOCKS * DfsAppendTestUtil.BLOCK_SIZE];
        System.arraycopy(fileContents, 0, expected, 0, expected.length);
        // do a sanity check. Read the file
        // do not check file status since the file is not yet closed.
        DfsAppendTestUtil.checkFullFile(fileSys, name,
                DfsAppendTestUtil.NUM_BLOCKS * DfsAppendTestUtil.BLOCK_SIZE, expected,
                "Read 1", false);
    }

    private void checkSmallFile(DistributedFileSystem fileSys, Path name, int repl)
            throws IOException {
        boolean done = false;

        // wait till all full blocks are confirmed by the datanodes.
        while (!done) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                ;
            }
            done = true;
            BlockLocation[] locations = fileSys.getFileBlockLocations(
                    fileSys.getFileStatus(name), 0, DfsAppendTestUtil.SMALL_FILE_SIZE);
            if (locations.length < DfsAppendTestUtil.SMALL_NUM_BLOCKS) {
                System.out.println("Number of blocks found " + locations.length);
                done = false;
                continue;
            }
            for (int idx = 0; idx < DfsAppendTestUtil.SMALL_NUM_BLOCKS; idx++) {
                if (locations[idx].getHosts().length < repl) {
                    System.out.println("Block index " + idx + " not yet replciated.");
                    done = false;
                    break;
                }
            }
        }
        byte[] expected =
                new byte[DfsAppendTestUtil.SMALL_NUM_BLOCKS * DfsAppendTestUtil.SMALL_BLOCK_SIZE];
        System.arraycopy(fileContents, 0, expected, 0, expected.length);
        // do a sanity check. Read the file
        // do not check file status since the file is not yet closed.
        DfsAppendTestUtil.checkFullFile(fileSys, name,
                DfsAppendTestUtil.SMALL_NUM_BLOCKS * DfsAppendTestUtil.SMALL_BLOCK_SIZE, expected,
                "Read 1", false);
    }

    @Test
    public void testSmallFlush() throws IOException {
        Configuration conf = new HdfsConfiguration(BASE_CONF);
        fileContents = DfsAppendTestUtil.initBuffer(DfsAppendTestUtil.SMALL_FILE_SIZE);
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 0);  // OK?
        DistributedFileSystem fs = getDFS();
        try {

            // create a new file.
            Path file1 = makeTestPath("simpleFlush.dat");
            FSDataOutputStream stm = DfsAppendTestUtil.createFile(fs, file1, 1);
            System.out.println("Created file simpleFlush.dat");

            // write to file
            int mid = DfsAppendTestUtil.SMALL_FILE_SIZE / 2;
            stm.write(fileContents, 0, mid);
            stm.hsync();
            System.out.println("Wrote and Flushed first part of file.");

            // write the remainder of the file
            stm.write(fileContents, mid, DfsAppendTestUtil.SMALL_FILE_SIZE - mid);
            System.out.println("Written second part of file");
            stm.hsync();
            stm.hsync();
            System.out.println("Wrote and Flushed second part of file.");

            // verify that full blocks are sane
            checkSmallFile(fs, file1, 1);

            stm.close();
            System.out.println("Closed file.");

            // verify that entire file is good
            DfsAppendTestUtil.checkFullFile(fs, file1, DfsAppendTestUtil.SMALL_FILE_SIZE,
                    fileContents, "Read 2");

        } catch (IOException e) {
            System.out.println("Exception :" + e);
            throw e;
        } catch (Throwable e) {
            System.out.println("Throwable :" + e);
            e.printStackTrace();
            throw new IOException("Throwable : " + e);
        } finally {
        }
    }
    
    @Test
    public void testSimpleFlush() throws IOException {
        Configuration conf = new HdfsConfiguration(BASE_CONF);
        fileContents = DfsAppendTestUtil.initBuffer(DfsAppendTestUtil.FILE_SIZE);
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 0);  // OK?
        DistributedFileSystem fs = getDFS();
        try {

            // create a new file.
            Path file1 = makeTestPath("simpleFlush.dat");
            FSDataOutputStream stm = DfsAppendTestUtil.createFile(fs, file1, 1);
            System.out.println("Created file simpleFlush.dat");

            // write to file
            int mid = DfsAppendTestUtil.FILE_SIZE / 2;
            stm.write(fileContents, 0, mid);
            stm.hflush();
            System.out.println("Wrote and Flushed first part of file.");

            // write the remainder of the file
            stm.write(fileContents, mid, DfsAppendTestUtil.FILE_SIZE - mid);
            System.out.println("Written second part of file");
            stm.hflush();
            stm.hflush();
            System.out.println("Wrote and Flushed second part of file.");

            // verify that full blocks are sane
            checkFile(fs, file1, 1);

            stm.close();
            System.out.println("Closed file.");

            // verify that entire file is good
            DfsAppendTestUtil.checkFullFile(fs, file1, DfsAppendTestUtil.FILE_SIZE,
                    fileContents, "Read 2");

        } catch (IOException e) {
            System.out.println("Exception :" + e);
            throw e;
        } catch (Throwable e) {
            System.out.println("Throwable :" + e);
            e.printStackTrace();
            throw new IOException("Throwable : " + e);
        } finally {
        }
    }

    @Test
    public void testComplexFlush() throws IOException {
        Configuration conf = new HdfsConfiguration(BASE_CONF);
        fileContents = DfsAppendTestUtil.initBuffer(DfsAppendTestUtil.FILE_SIZE);
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 0);  // OK?
        DistributedFileSystem fs = getDFS();
        try {

            // create a new file.
            Path file1 = makeTestPath("complexFlush.dat");
            FSDataOutputStream stm = DfsAppendTestUtil.createFile(fs, file1, 1);
            System.out.println("Created file complexFlush.dat");

            int start = 0;
            for (start = 0; (start + 29) < DfsAppendTestUtil.FILE_SIZE;) {
                stm.write(fileContents, start, 29);
                stm.hflush();
                start += 29;
            }
            stm.write(fileContents, start, DfsAppendTestUtil.FILE_SIZE - start);
            // need to make sure we completely write out all full blocks before
            // the checkFile() call (see FSOutputSummer#flush)
            stm.flush();
            // verify that full blocks are sane
            checkFile(fs, file1, 1);
            stm.close();

            // verify that entire file is good
            DfsAppendTestUtil.checkFullFile(fs, file1, DfsAppendTestUtil.FILE_SIZE,
                    fileContents, "Read 2");
        } catch (IOException e) {
            System.out.println("Exception :" + e);
            throw e;
        } catch (Throwable e) {
            System.out.println("Throwable :" + e);
            e.printStackTrace();
            throw new IOException("Throwable : " + e);
        } finally {
        }
    }

    @Test
    public void testFileNotFound() throws IOException {
        Configuration conf = new HdfsConfiguration(BASE_CONF);
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 0);  // OK?
        FileSystem fs = getDefaultFS();
        boolean fileNotFound = false;
        try {
            Path file1 = makeTestPath("nonexistingfile.dat");
            fs.append(file1);
        } catch (FileNotFoundException e) {
            fileNotFound = true;
        }
        assertTrue(fileNotFound);
    }

    @Test
    public void testAppendTwice() throws Exception {
        Configuration conf = new HdfsConfiguration(BASE_CONF);
        String username = UserGroupInformation.getCurrentUser().getShortUserName()+"_XXX";
        conf.set("hadoop.user.group.static.mapping.overrides", username + "=supergroup");
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 0);  // OK?
        final FileSystem fs1 = getDefaultFS();
        final FileSystem fs2 = DfsAppendTestUtil.createHdfsWithDifferentUsername(conf);
        try {

            final Path p = makeTestPath("testAppendTwice/foo");
            final int len = 1 << 20;
            final byte[] fileContents = DfsAppendTestUtil.initBuffer(len);

            {
                // create a new file with a full block.
                FSDataOutputStream out = fs2.create(p, true, 4096, (short) 1, len);
                out.write(fileContents, 0, len);
                out.close();
                fs2.setPermission(p, new FsPermission("0777")); // konna: simulate disable permission
            }

            // 1st append does not add any data so that the last block remains full
            // and the last block in INodeFileUnderConstruction is a BlockInfo
            // but does not have a BlockUnderConstructionFeature.
            fs2.append(p);

            // 2nd append should get AlreadyBeingCreatedException
            fs1.append(p);
            fail("Expected AlreadyBeingCreatedException");
        } catch (RemoteException re) {
            DfsAppendTestUtil.LOG.info("Got an exception:", re);
            assertEquals(AlreadyBeingCreatedException.class.getName(),
                    re.getClassName());
        } finally {
            fs2.close();
        }
    }

    @Test
    public void testAppend2Twice() throws Exception {
        Configuration conf = new HdfsConfiguration(BASE_CONF);
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 0);
        String username = UserGroupInformation.getCurrentUser().getShortUserName()+"_XXX";
        conf.set("hadoop.user.group.static.mapping.overrides", username + "=supergroup");
        final DistributedFileSystem fs1 = getDFS();
        final FileSystem fs2 = DfsAppendTestUtil.createHdfsWithDifferentUsername(conf);

        try {
            final Path p = makeTestPath("testAppendTwice/foo");
            final int len = 1 << 20;
            final byte[] fileContents = DfsAppendTestUtil.initBuffer(len);

            {
                // create a new file with a full block.
                FSDataOutputStream out = fs2.create(p, true, 4096, (short) 1, len);
                out.write(fileContents, 0, len);
                out.close();
                fs2.setPermission(p, new FsPermission("0777")); // konna: simulate disable permission
            }

            // 1st append does not add any data so that the last block remains full
            // and the last block in INodeFileUnderConstruction is a BlockInfo
            // but does not have a BlockUnderConstructionFeature.
            ((DistributedFileSystem) fs2).append(p,
                    EnumSet.of(CreateFlag.APPEND, CreateFlag.NEW_BLOCK), 4096, null);

            // 2nd append should get AlreadyBeingCreatedException
            fs1.append(p);
            fail("Expected AlreadyBeingCreatedException");
        } catch (RemoteException re) {
            DfsAppendTestUtil.LOG.info("Got an exception:", re);
            assertEquals(AlreadyBeingCreatedException.class.getName(),
                    re.getClassName());
        } finally {
            fs2.close();
        }
    }

    @Test
    public void testMultipleAppends() throws Exception {
        final long startTime = Time.monotonicNow();
        final Configuration conf = new HdfsConfiguration(BASE_CONF);
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_FILE_CLOSE_NUM_COMMITTED_ALLOWED_KEY,
                1);
        conf.setBoolean(
                HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.ENABLE_KEY,
                false);
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 0);  // OK?
        final DistributedFileSystem fs = getDFS();
        try {
            final Path p = makeTestPath("testMultipleAppend/foo");
            final int blockSize = 1 << 20;
            final byte[] data = DfsAppendTestUtil.initBuffer(blockSize);

            // create an empty file.
            fs.create(p, true, 4096, (short) 3, blockSize).close();

            int fileLen = 0;
            for (int i = 0; i < 10
                    || Time.monotonicNow() - startTime < RANDOM_TEST_RUNTIME; i++) {
                int appendLen = ThreadLocalRandom.current().nextInt(100) + 1;
                if (fileLen + appendLen > data.length) {
                    break;
                }

                DfsAppendTestUtil.LOG
                        .info(i + ") fileLen=" + fileLen + ", appendLen=" + appendLen);
                final FSDataOutputStream out = fs.append(p);
                out.write(data, fileLen, appendLen);
                out.close();
                fileLen += appendLen;
            }

            assertEquals(fileLen, fs.getFileStatus(p).getLen());
            final byte[] actual = new byte[fileLen];
            final FSDataInputStream in = fs.open(p);
            in.readFully(actual);
            in.close();
            for (int i = 0; i < fileLen; i++) {
                assertEquals(data[i], actual[i]);
            }
        } finally {
        }
    }
}