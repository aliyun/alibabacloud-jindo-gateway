package org.apache.hadoop.hdfs;

import com.aliyun.jindodata.gateway.JindoMultiClusterTestBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.DataOutputStream;
import java.io.IOException;

public class TestDFSRename extends JindoMultiClusterTestBase {
    final Path dir = makeTestPath("test/rename/");

    void list(FileSystem fs, String name) throws IOException {
        FileSystem.LOG.info("\n\n" + name);
        for (FileStatus s : fs.listStatus(dir)) {
            FileSystem.LOG.info("" + s.getPath());
        }
    }

    static void createFile(FileSystem fs, Path f) throws IOException {
        DataOutputStream a_out = fs.create(f);
        a_out.writeBytes("something");
        a_out.close();
    }

    @Test
    public void testRename() throws Exception {
        Configuration conf = new HdfsConfiguration();
        try {
            FileSystem fs = getDefaultFS();
            Assertions.assertTrue(fs.mkdirs(dir));

            { // test lease
                Path a = new Path(dir, "a");
                Path aa = new Path(dir, "aa");
                Path b = new Path(dir, "b");

                createFile(fs, a);

                DataOutputStream aa_out = fs.create(aa);
                aa_out.writeBytes("something");

                list(fs, "rename0");
                fs.rename(a, b);
                list(fs, "rename1");
                aa_out.writeBytes(" more");
                aa_out.close();
                list(fs, "rename2");
//                 should not have any lease
//                assertEquals(0, countLease(cluster));
            }

            { // test non-existent destination
                Path dstPath = makeTestPath("c/d");
                Assertions.assertFalse(fs.exists(dstPath));
                Assertions.assertFalse(fs.rename(dir, dstPath));
            }

            { // dst cannot be a file or directory under src
                // test rename /a/b/foo to /a/b/c
                Path src = makeTestPath("a/b");
                Path dst = makeTestPath("a/b/c");

                createFile(fs, new Path(src, "foo"));

                // dst cannot be a file under src
                Assertions.assertFalse(fs.rename(src, dst));

                // dst cannot be a directory under src
                Assertions.assertFalse(fs.rename(src.getParent(), dst.getParent()));
            }

            { // dst can start with src, if it is not a directory or file under src
                // test rename /test /testfile
                Path src = makeTestPath("testPrefix");
                Path dst = makeTestPath("testPrefixfile");

                createFile(fs, src);
                Assertions.assertTrue(fs.rename(src, dst));
            }

            { // dst should not be same as src test rename /a/b/c to /a/b/c
                Path src = makeTestPath("a/b/c");
                createFile(fs, src);
                Assertions.assertTrue(fs.rename(src, src));
                Assertions.assertFalse(fs.rename(makeTestPath("a/b"), makeTestPath("a/b/")));
                Assertions.assertTrue(fs.rename(src, makeTestPath("a/b/c/")));
            }
            fs.delete(dir, true);
        } finally {
        }
    }

    @Test
    public void testRenameWithOverwrite() throws Exception {
        final short replFactor = 2;
        final long blockSize = 512;
        DistributedFileSystem dfs = getDFS();
        try {

            long fileLen = blockSize * 3;
            String src = makeTestPathStr("foo/src");
            String dst = makeTestPathStr("foo/dst");
            Path srcPath = new Path(src);
            Path dstPath = new Path(dst);

            createFile(dfs, srcPath, fileLen, replFactor, 1);
            createFile(dfs, dstPath, fileLen, replFactor, 1);

            LocatedBlocks lbs = getDFS().getClient().getNamenode()
                    .getBlockLocations(dst, 0, fileLen);
//            assertTrue(cluster.getBlock(
//                    lbs.getLocatedBlocks().get(0).getBlock().getBlockId()) != null);
            dfs.rename(srcPath, dstPath, Options.Rename.OVERWRITE);
//            HdfsProtos.ExtendedBlockProto oldDestBlock = cluster.getBlock(lbs.getLocatedBlocks().get(0).getBlock().getBlockId());
//            assertTrue(oldDestBlock == null || !oldDestBlock.isInitialized());

            Assertions.assertFalse(dfs.exists(srcPath));
            Assertions.assertTrue(dfs.exists(dstPath));
        } finally {
        }
    }

    @Test
    public void testRenameNonExist() throws Exception {
        Path path1 = makeTestPath("NonExist1");
        Path path2 = makeTestPath("NonExist2");

        FileSystem fs = getDefaultFS();
        Assertions.assertFalse(fs.rename(path1, path2));
    }

}
