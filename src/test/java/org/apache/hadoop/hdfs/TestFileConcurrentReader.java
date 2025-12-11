package org.apache.hadoop.hdfs;

import com.aliyun.jindodata.gateway.JindoMultiClusterTestBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

public class TestFileConcurrentReader extends JindoMultiClusterTestBase {
    private static final Logger LOG =
            Logger.getLogger(TestFileConcurrentReader.class);

    private enum SyncType {
        SYNC,
        APPEND,
    }

    private static final int DEFAULT_WRITE_SIZE = 1024 + 1;
    private static final int SMALL_WRITE_SIZE = 61;
    static final int blockSize = 8192;
    private FileSystem fileSystem;

    private void writeFileAndSync(FSDataOutputStream stm, int size)
            throws IOException {
        byte[] buffer = generateSequentialBytes(0, size);
        stm.write(buffer, 0, size);
        // konna : TODO: hflush not support
//        stm.hflush();
        stm.hsync();
    }

    private void checkCanRead(FileSystem fileSys, Path path, int numBytes)
            throws IOException {
        waitForBlocks(fileSys, path);
        assertBytesAvailable(fileSys, path, numBytes);
    }

    // make sure bytes are available and match expected
    private void assertBytesAvailable(
            FileSystem fileSystem,
            Path path,
            int numBytes
    ) throws IOException {
        byte[] buffer = new byte[numBytes];
        FSDataInputStream inputStream = fileSystem.open(path);
        IOUtils.readFully(inputStream, buffer, 0, numBytes);
        inputStream.close();

        assertTrue(
                validateSequentialBytes(buffer, 0, numBytes),
                "unable to validate bytes"
        );
    }

    private void waitForBlocks(FileSystem fileSys, Path name)
            throws IOException {
        // wait until we have at least one block in the file to read.
        boolean done = false;

        while (!done) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
            done = true;
            BlockLocation[] locations = fileSys.getFileBlockLocations(
                    fileSys.getFileStatus(name), 0, blockSize);
            if (locations.length < 1) {
                done = false;
                continue;
            }
        }
    }

    private boolean validateSequentialBytes(byte[] buf, int startPos, int len) {
        for (int i = 0; i < len; i++) {
            int expected = (i + startPos) % 127;

            if (buf[i] % 127 != expected) {
                LOG.error(String.format("at position [%d], got [%d] and expected [%d]",
                        startPos, buf[i], expected));

                return false;
            }
        }

        return true;
    }

    private long tailFile(Path file, long startPos) throws IOException {
        long numRead = 0;
        FSDataInputStream inputStream = fileSystem.open(file);
        inputStream.seek(startPos);

        int len = 4 * 1024;
        byte[] buf = new byte[len];
        int read;
        while ((read = inputStream.read(buf)) > -1) {
            LOG.info(String.format("read %d bytes", read));

            if (!validateSequentialBytes(buf, (int) (startPos + numRead), read)) {
                LOG.error(String.format("invalid bytes: [%s]\n", Arrays.toString(buf)));
                throw new ChecksumException(
                        String.format("unable to validate bytes"),
                        startPos
                );
            }

            numRead += read;
        }

        inputStream.close();
        return numRead + startPos - 1;
    }

    @BeforeEach
    public void before() throws IOException {
        fileSystem = getDefaultFS();
    }

    @Test
    public void testUnfinishedBlockRead()
            throws IOException {
        // create a new file in the root, write data, do no close
        Path file1 = makeTestPath("unfinished-block");
        FSDataOutputStream stm = TestFileCreation2.createFile(fileSystem, file1, 1);

        // write partial block and sync
        int partialBlockSize = blockSize / 2;
        writeFileAndSync(stm, partialBlockSize);

        // Make sure a client can read it before it is closed
        checkCanRead(fileSystem, file1, partialBlockSize);

        stm.close();
    }

    @Test
    public void testUnfinishedBlockPacketBufferOverrun() throws IOException {
        // check that / exists
        Path path = testRootPath;
        System.out.println("Path : \"" + path.toString() + "\"");

        // create a new file in the root, write data, do no close
        Path file1 = makeTestPath("unfinished-block");
        final FSDataOutputStream stm =
                TestFileCreation2.createFile(fileSystem, file1, 1);

        // write partial block and sync
        final int bytesPerChecksum = 512;
        final int partialBlockSize = bytesPerChecksum - 1;

        writeFileAndSync(stm, partialBlockSize);

        // Make sure a client can read it before it is closed
        checkCanRead(fileSystem, file1, partialBlockSize);

        stm.close();
    }

    @Test
    public void testImmediateReadOfNewFile()
            throws IOException {
        final int blockSize = 64 * 1024;
        final int writeSize = 10 * blockSize;

        final int requiredSuccessfulOpens = 100;
        final Path file = makeTestPath("file1");
        final AtomicBoolean openerDone = new AtomicBoolean(false);
        final AtomicReference<String> errorMessage = new AtomicReference<String>();
        final FSDataOutputStream out = fileSystem.create(file);

        final Thread writer = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while (!openerDone.get()) {
                        out.write(generateSequentialBytes(0, writeSize));
                        out.hflush();
                    }
                } catch (IOException e) {
                    LOG.warn("error in writer", e);
                } finally {
                    try {
                        out.close();
                    } catch (IOException e) {
                        LOG.error("unable to close file");
                    }
                }
            }
        });

        Thread opener = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    for (int i = 0; i < requiredSuccessfulOpens; i++) {
                        fileSystem.open(file).close();
                    }
                    openerDone.set(true);
                } catch (IOException e) {
                    openerDone.set(true);
                    errorMessage.set(String.format(
                            "got exception : %s",
                            StringUtils.stringifyException(e)
                    ));
                } catch (Exception e) {
                    openerDone.set(true);
                    errorMessage.set(String.format(
                            "got exception : %s",
                            StringUtils.stringifyException(e)
                    ));
                    writer.interrupt();
                    fail("here");
                }
            }
        });

        writer.start();
        opener.start();

        try {
            writer.join();
            opener.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        assertNull(errorMessage.get(), errorMessage.get());
    }

    private void runTestUnfinishedBlockCRCError(
            final boolean transferToAllowed, SyncType syncType, int writeSize
    ) throws IOException {
        runTestUnfinishedBlockCRCError(
                transferToAllowed, syncType, writeSize, new Configuration()
        );
    }

    private void runTestUnfinishedBlockCRCError(
            final boolean transferToAllowed,
            final SyncType syncType,
            final int writeSize,
            Configuration conf
    ) throws IOException {

        final Path file = makeTestPath("block-being-written-to");
        final int numWrites = 2000;
        final AtomicBoolean writerDone = new AtomicBoolean(false);
        final AtomicBoolean writerStarted = new AtomicBoolean(false);
        final AtomicBoolean error = new AtomicBoolean(false);

        final Thread writer = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    FSDataOutputStream outputStream = fileSystem.create(file);
                    if (syncType == SyncType.APPEND) {
                        outputStream.close();
                        outputStream = fileSystem.append(file);
                    }
                    try {
                        for (int i = 0; !error.get() && i < numWrites; i++) {
                            final byte[] writeBuf =
                                    generateSequentialBytes(i * writeSize, writeSize);
                            outputStream.write(writeBuf);
                            if (syncType == SyncType.SYNC) {
                                // TODO:hflush not support
//                                outputStream.hflush();
                                outputStream.hsync();
                            }
                            writerStarted.set(true);
                        }
                    } catch (IOException e) {
                        error.set(true);
                        LOG.error("error writing to file", e);
                    } finally {
                        outputStream.close();
                    }
                    writerDone.set(true);
                } catch (Exception e) {
                    LOG.error("error in writer", e);

                    throw new RuntimeException(e);
                }
            }
        });
        Thread tailer = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    long startPos = 0;
                    while (!writerDone.get() && !error.get()) {
                        if (writerStarted.get()) {
                            try {
                                startPos = tailFile(file, startPos);
                            } catch (IOException e) {
                                LOG.error(String.format("error tailing file %s", file), e);

                                throw new RuntimeException(e);
                            }
                        }
                    }
                } catch (RuntimeException e) {
                    if (e.getCause() instanceof ChecksumException) {
                        error.set(true);
                    }

                    writer.interrupt();
                    LOG.error("error in tailer", e);
                    throw e;
                }
            }
        });

        writer.start();
        tailer.start();

        try {
            writer.join();
            tailer.join();

            assertFalse(
                    error.get(),
                    "error occurred, see log above"
            );
        } catch (InterruptedException e) {
            LOG.info("interrupted waiting for writer or tailer to complete");

            Thread.currentThread().interrupt();
        }
    }

    @Test
    public void testUnfinishedBlockCRCErrorTransferTo() throws IOException {
        runTestUnfinishedBlockCRCError(true, SyncType.SYNC, DEFAULT_WRITE_SIZE);
    }

    // konna : read file may be timeout cause of the gateway timeout
    @Test
    public void testUnfinishedBlockCRCErrorTransferToVerySmallWrite()
            throws IOException {
        runTestUnfinishedBlockCRCError(true, SyncType.SYNC, SMALL_WRITE_SIZE);
    }
}