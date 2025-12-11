package org.apache.hadoop.hdfs;

import com.aliyun.jindodata.gateway.JindoMultiClusterTestBase;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

public class TestDFSMkdirs extends JindoMultiClusterTestBase {

    private static final String[] NON_CANONICAL_PATHS = new String[] {
//      "//test1",
//            "/test2/..",
//      "/test2//bar",
//            "/test2/../test4",
//            "/test5/."
    };

    @Test
    public void testDFSMkdirs() throws IOException {

        FileSystem fileSys = getDFS();
        try {
            // First create a new directory with mkdirs
            Path myPath = makeTestPath("test/mkdirs");
            assertTrue(fileSys.mkdirs(myPath));
            assertTrue(fileSys.exists(myPath));
            assertTrue(fileSys.mkdirs(myPath));

            // Second, create a file in that directory.
            Path myFile = makeTestPath("test/mkdirs/myFile");
            writeFile(fileSys, myFile, "hello world");

            // Third, use mkdir to create a subdirectory off of that file,
            // and check that it fails.
            Path myIllegalPath = makeTestPath("test/mkdirs/myFile/subdir");
            Boolean exist = true;
            try {
                fileSys.mkdirs(myIllegalPath);
            } catch (IOException e) {
                exist = false;
            }
            assertFalse(exist);
            try {
                assertFalse(fileSys.exists(myIllegalPath));
            }catch (AccessControlException ae) {
                assertTrue(ae.getMessage().contains("Parent not directory"));
            }
            fileSys.delete(myFile, true);

        } finally {
        }
    }

    @Test
    public void testMkdir() throws IOException {
        DistributedFileSystem dfs = getDFS();
        try {
            // Create a dir in root dir, should succeed
            assertTrue(dfs.mkdir(makeTestPath("mkdir-" + Time.now()),
                    FsPermission.getDefault()));
            // Create a dir when parent dir exists as a file, should fail
            IOException expectedException = null;
            String filePath = "mkdir-file-" + Time.now();
            writeFile(dfs, makeTestPath(filePath), "hello world");
            try {
                dfs.mkdir(makeTestPath(filePath + "/mkdir"), FsPermission.getDefault());
            } catch (IOException e) {
                expectedException = e;
            }
            assertTrue(expectedException != null
                            && expectedException instanceof ParentNotDirectoryException,
                    "Create a directory when parent dir exists as file using"
                            + " mkdir() should throw ParentNotDirectoryException ");
            // Create a dir in a non-exist directory, should fail
            expectedException = null;
            try {
                dfs.mkdir(makeTestPath("non-exist/mkdir-" + Time.now()),
                        FsPermission.getDefault());
            } catch (IOException e) {
                expectedException = e;
            }
            assertTrue(expectedException != null
                            && expectedException instanceof FileNotFoundException,
                    "Create a directory in a non-exist parent dir using"
                            + " mkdir() should throw FileNotFoundException ");
        } finally {
        }
    }

    @Test
    public void testMkdirRpcNonCanonicalPath() throws IOException {
        DistributedFileSystem dfs = getDFS();
        try {

            for (String pathStr : NON_CANONICAL_PATHS) {
                try {
                    // original test case use namenode to get results. Thus it can catch InvalidPathException.
                    // We change to use client. We will catch RemoteException.
                    dfs.getClient().mkdirs(pathStr, new FsPermission((short)0755), true);
                    fail("Did not fail when called with a non-canonicalized path: "
                            + pathStr);
                } catch (RemoteException re){
                }
            }
        } finally {
        }
    }

    @Test
    public void testMkdirsUtf8() throws IOException {
        DistributedFileSystem dfs = getDFS();
        FileSystem fs = getFs();
        // 1
        Path path = makeTestPath("测试（¥）");
        assertTrue(fs.mkdirs(path));

        FileStatus status = fs.getFileStatus(path);
        assertTrue(status.getPath().toString().contains(path.toString()));
        // 2
        path = makeTestPath(" path with blank");
        assertTrue(fs.mkdirs(path));

        status = fs.getFileStatus(path);
        assertTrue(status.getPath().toString().contains(path.toString()));
        // 3
        path = makeTestPath(" path with blank2 ");
        assertTrue(fs.mkdirs(path));
        Path subDir = new Path(path, "subDir");
        assertTrue(fs.mkdirs(subDir));

        status = fs.getFileStatus(path);
        assertTrue(status.getPath().toString().contains(path.toString()));
        FileStatus[] statuses =  fs.listStatus(path);
        assertEquals(1, statuses.length);

        // 4
        StringBuilder strBuilder = new StringBuilder(6);
        strBuilder.append((char) 0xc4);
        strBuilder.append((char) 0xb0);
        strBuilder.append((char) '-');
        strBuilder.append((char) 0xc3);
        strBuilder.append((char) 0x87);
        strBuilder.append((char) 0x2);
        path = makeTestPath(strBuilder.toString());
        assertTrue(fs.mkdirs(path));

        status = fs.getFileStatus(path);
        assertTrue(status.getPath().toString().contains(path.toString()));
        //5
        strBuilder.setCharAt(0, (char) 0xc4);
        strBuilder.setCharAt(1, (char) 0xb0);
        strBuilder.setCharAt(2, '-');
        strBuilder.setCharAt(3, (char) 0xc3);
        strBuilder.setCharAt(4, (char) 0x87);
        strBuilder.setCharAt(5, (char) -2);
        path = makeTestPath(strBuilder.toString());
        try{
            fs.mkdirs(path);
        } catch (RemoteException e) {
            System.out.println("Got RemoteException, good!");
            assertTrue(e.getMessage().contains("Path is not in UTF-8 format"));
        }
    }

    public static String getRandomString(int length){
        String str="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        Random random=new Random();
        StringBuffer sb=new StringBuffer();
        for(int i=0;i<length;i++){
            int number=random.nextInt(62);
            sb.append(str.charAt(number));
        }
        return sb.toString();
    }

    @Test
    public void testLongPath() throws IOException {
        Path path = testRootPath;
        int totalSize = 4096;
        int nameSize = 255;
        for (int i = 0; i < totalSize / nameSize; i++) {
            path = new Path(path, getRandomString(nameSize));
        }

        FileSystem fs = getFs();
        assertTrue(fs.mkdirs(path));
    }
}