package org.apache.hadoop.hdfs;

import com.aliyun.jindodata.gateway.JindoMultiClusterTestBase;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.apache.hadoop.hdfs.DfsAppendTestUtil.checkFullFile;
import static org.apache.hadoop.hdfs.DfsAppendTestUtil.randomBytes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestTruncate extends JindoMultiClusterTestBase {
    int BLOCK_SIZE = 512;

    int[] makeTruncateCandidates(int fileLength) {
        List<Integer> newLength = new ArrayList<Integer>();
        // Add 0 ~ 3.
        int newLengthNext = 0;
        for (; newLengthNext <= 3 && newLengthNext < fileLength; ++newLengthNext) {
            newLength.add(newLengthNext);
        }
        // Add [BLOCK_SIZE/2, fileLength-2) with step size BLOCK_SIZE/2.
        for (int i = BLOCK_SIZE; i < fileLength - 2; i += BLOCK_SIZE) {
            if ((i - BLOCK_SIZE / 2) >= newLengthNext) {
                newLengthNext = (i - BLOCK_SIZE / 2) + 1;
                newLength.add(newLengthNext - 1);
            }
            if (i >= newLengthNext) {
                newLengthNext = i + 1;
                newLength.add(newLengthNext - 1);
            }
        }
        // Add fileLength-2 ~ fileLength.
        if (fileLength - 2 >= newLengthNext) {
            newLengthNext = fileLength - 2;
        }
        for (; newLengthNext <= fileLength; ++newLengthNext) {
            newLength.add(newLengthNext);
        }

        // Convert newLength to candidates.
//        List<Integer> candidates = new ArrayList<Integer>();
        int[] candidates = new int[newLength.size()];
        for (int i = 0; i < newLength.size(); ++i) {
            candidates[(newLength.size() - i - 1)] = fileLength - newLength.get(i);
        }
        return candidates;
    }

    void writeContents(byte[] contents, int fileLength, Path p) throws IOException {
        FSDataOutputStream stm = BASE_FS.create(p);
        stm.write(contents, 0, fileLength);
        stm.close();
    }

    void checkBlockRecovery(String p) throws IOException, InterruptedException {
        DistributedFileSystem fs = getDFS();
        DFSClient client = fs.getClient();
        boolean success = false;
        for (int i=0; i<100; ++i) {
            LocatedBlocks locatedBlocks = DFSClient.callGetBlockLocations(client.getNamenode(), p, 0, Long.MAX_VALUE);
            LocatedBlock last = locatedBlocks.getLastLocatedBlock();
            if (!locatedBlocks.isUnderConstruction() && (last ==null || locatedBlocks.isLastBlockComplete())) {
                success = true;
            }
            Thread.sleep(100);
        }
        assertTrue(success);
//        fs.getFileBlockLocations(p);
    }

    @Test
    public void fileTruncateBasicTestCase() throws IOException, InterruptedException {
        int startingFileSize = 3 * BLOCK_SIZE;
        byte[] contents = randomBytes(-1, startingFileSize);
        for (int fileLength = startingFileSize; fileLength > 0; fileLength -= BLOCK_SIZE - 1) {
            int[] candidates = makeTruncateCandidates(fileLength);
            // Really truncate
            for (int toTruncate : candidates) {
            String p = makeTestPathStr("testBasicTruncate" + fileLength);
            Path path = makeTestPath("testBasicTruncate" + fileLength);
            writeContents(contents, fileLength, path);

                int newLength = fileLength - toTruncate;
//                boolean expectedReady = (toTruncate == 0) || ((newLength % BLOCK_SIZE) == 0);
                boolean isReady = BASE_FS.truncate(path, newLength);

                assertEquals(true, isReady);
                // validate the file content
                if (!isReady) {
                    checkBlockRecovery(p);
                }
                checkFullFile(BASE_FS, path, newLength, contents);
            }
        }
    }

    @Test
    public void fileTruncateMultipleTestCase() throws IOException, InterruptedException {
        int startingFileSize = 10 * BLOCK_SIZE;
        byte[] contents = randomBytes(-1, startingFileSize);
        Path path = makeTestPath("testBasicTruncate");
        writeContents(contents, startingFileSize, path);

        for(int i=startingFileSize; i > 0;) {
            int newLength = nextInt(i);
            boolean isReady = BASE_FS.truncate(path, newLength);
            assertEquals(true, isReady);
            checkFullFile(BASE_FS, path, newLength, contents);
            isReady = BASE_FS.truncate(path, newLength);
            assertEquals(true, isReady);
            i = newLength;
        }
    }

    int nextInt(int limit) {
        Random rb = new Random(2025);
        return rb.nextInt(limit);
    }
}
