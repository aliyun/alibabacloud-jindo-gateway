package org.apache.hadoop.hdfs;

import com.aliyun.jindodata.gateway.JindoMultiClusterTestBase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.EnumSet;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestWriteRead extends JindoMultiClusterTestBase {
    // Junit test settings.
    private static final int WR_NTIMES = 350;
    private static final int WR_CHUNK_SIZE = 10000;

    private static final int    BUFFER_SIZE = 8192 * 100;
    private static final String ROOT_DIR    = makeTestPathStr("tmp/");
    private static final long   blockSize   = 1024*100;

    // command-line options. Different defaults for unit test vs real cluster
    String filenameOption  = ROOT_DIR + "fileX1";
    int    chunkSizeOption = 10000;
    int    loopOption      = 10;

    private Configuration conf; // = new HdfsConfiguration();
    private FileSystem mfs; // = cluster.getFileSystem();
    private FileContext mfc; // = FileContext.getFileContext();

    // configuration
    private boolean useFCOption = false; // use either FileSystem or FileContext
    private boolean verboseOption = true;
    private boolean positionReadOption = false;
    private boolean truncateOption = false;
    private final boolean abortTestOnFailure = true;

    static private Log LOG = LogFactory.getLog(TestWriteRead.class);

    @BeforeEach
    public void before() throws IOException {
        mfs = getDefaultFS();
        mfc = FileContext.getFileContext(BASE_CONF);

        Path rootdir = new Path(ROOT_DIR);
        mfs.mkdirs(rootdir);
    }

    private int testWriteAndRead(String fname, int loopN, int chunkSize, long readBeginPosition)
            throws IOException {

        int countOfFailures = 0;
        long byteVisibleToRead = 0;
        FSDataOutputStream out = null;

        byte[] outBuffer = new byte[BUFFER_SIZE];
        byte[] inBuffer = new byte[BUFFER_SIZE];

        for (int i = 0; i < BUFFER_SIZE; i++) {
            outBuffer[i] = (byte) (i & 0x00ff);
        }

        try {
            Path path = getFullyQualifiedPath(fname);
            long fileLengthBeforeOpen = 0;

            if (ifExists(path)) {
                if (truncateOption) {
                    out = useFCOption ? mfc.create(path, EnumSet.of(CreateFlag.OVERWRITE)):
                            mfs.create(path, truncateOption);
                    LOG.info("File already exists. File open with Truncate mode: "+ path);
                } else {
                    out = useFCOption ? mfc.create(path, EnumSet.of(CreateFlag.APPEND))
                            : mfs.append(path);
                    fileLengthBeforeOpen = getFileLengthFromNN(path);
                    LOG.info("File already exists of size " + fileLengthBeforeOpen
                            + " File open for Append mode: " + path);
                }
            } else {
                out = useFCOption ? mfc.create(path, EnumSet.of(CreateFlag.CREATE))
                        : mfs.create(path);
            }

            long totalByteWritten = fileLengthBeforeOpen;
            long totalByteVisible = fileLengthBeforeOpen;
            long totalByteWrittenButNotVisible = 0;

            boolean toFlush;
            for (int i = 0; i < loopN; i++) {
                toFlush = (i % 2) == 0;

                writeData(out, outBuffer, chunkSize);

                totalByteWritten += chunkSize;

                if (toFlush) {
                    // konna : TODO: hflush not support
//                    out.hflush();
                    out.hsync();
                    totalByteVisible += chunkSize + totalByteWrittenButNotVisible;
                    totalByteWrittenButNotVisible = 0;
                } else {
                    totalByteWrittenButNotVisible += chunkSize;
                }

                if (verboseOption) {
                    LOG.info("TestReadWrite - Written " + chunkSize
                            + ". Total written = " + totalByteWritten
                            + ". TotalByteVisible = " + totalByteVisible + " to file "
                            + fname);
                }
                byteVisibleToRead = readData(fname, inBuffer, totalByteVisible, readBeginPosition);

                String readmsg = "Written=" + totalByteWritten + " ; Expected Visible="
                        + totalByteVisible + " ; Got Visible=" + byteVisibleToRead
                        + " of file " + fname;

                if (byteVisibleToRead >= totalByteVisible
                        && byteVisibleToRead <= totalByteWritten) {
                    readmsg = "pass: reader sees expected number of visible byte. "
                            + readmsg + " [pass]";
                } else {
                    countOfFailures++;
                    readmsg = "fail: reader see different number of visible byte. "
                            + readmsg + " [fail]";
                    if (abortTestOnFailure) {
                        throw new IOException(readmsg);
                    }
                }
                LOG.info(readmsg);
            }

            // test the automatic flush after close
            writeData(out, outBuffer, chunkSize);
            totalByteWritten += chunkSize;
            totalByteVisible += chunkSize + totalByteWrittenButNotVisible;
            totalByteWrittenButNotVisible += 0;

            out.close();

            byteVisibleToRead = readData(fname, inBuffer, totalByteVisible, readBeginPosition);

            String readmsg2 = "Written=" + totalByteWritten + " ; Expected Visible="
                    + totalByteVisible + " ; Got Visible=" + byteVisibleToRead
                    + " of file " + fname;
            String readmsg;

            if (byteVisibleToRead >= totalByteVisible
                    && byteVisibleToRead <= totalByteWritten) {
                readmsg = "pass: reader sees expected number of visible byte on close. "
                        + readmsg2 + " [pass]";
            } else {
                countOfFailures++;
                readmsg = "fail: reader sees different number of visible byte on close. "
                        + readmsg2 + " [fail]";
                LOG.info(readmsg);
                if (abortTestOnFailure)
                    throw new IOException(readmsg);
            }

            // now check if NN got the same length
            long lenFromFc = getFileLengthFromNN(path);
            if (lenFromFc != byteVisibleToRead){
                readmsg = "fail: reader sees different number of visible byte from NN "
                        + readmsg2 + " [fail]";
                throw new IOException(readmsg);
            }
        } catch (IOException e) {
            throw new IOException(
                    "##### Caught Exception in testAppendWriteAndRead. Close file. "
                            + "Total Byte Read so far = " + byteVisibleToRead, e);
        } finally {
            if (out != null)
                out.close();
        }
        return -countOfFailures;
    }

    private long getFileLengthFromNN(Path path) throws IOException {
        FileStatus fileStatus = useFCOption ? mfc.getFileStatus(path) :
                mfs.getFileStatus(path);
        return fileStatus.getLen();
    }

    private boolean ifExists(Path path) throws IOException {
        return useFCOption ? mfc.util().exists(path) : mfs.exists(path);
    }

    private FSDataInputStream openInputStream(Path path) throws IOException {
        FSDataInputStream in = useFCOption ? mfc.open(path) : mfs.open(path);
        return in;
    }

    private Path getFullyQualifiedPath(String pathString) {
        return useFCOption ? mfc.makeQualified(new Path(ROOT_DIR, pathString))
                : mfs.makeQualified(new Path(ROOT_DIR, pathString));
    }

    private long readData(String fname, byte[] buffer, long byteExpected, long beginPosition)
            throws IOException {
        long totalByteRead = 0;
        Path path = getFullyQualifiedPath(fname);

        FSDataInputStream in = null;
        try {
            in = openInputStream(path);

            long visibleLenFromReadStream = ((HdfsDataInputStream)in).getVisibleLength();

            if (visibleLenFromReadStream < byteExpected)
            {
                throw new IOException(visibleLenFromReadStream
                        + " = visibleLenFromReadStream < bytesExpected= "
                        + byteExpected);
            }

            totalByteRead = readUntilEnd(in, buffer, buffer.length, fname,
                    beginPosition, visibleLenFromReadStream, positionReadOption);
            in.close();

            // reading more data than visibleLeng is OK, but not less
            if (totalByteRead + beginPosition < byteExpected ){
                throw new IOException("readData mismatch in byte read: expected="
                        + byteExpected + " ; got " +  (totalByteRead + beginPosition));
            }
            return totalByteRead + beginPosition;

        } catch (IOException e) {
            throw new IOException("##### Caught Exception in readData. "
                    + "Total Byte Read so far = " + totalByteRead + " beginPosition = "
                    + beginPosition, e);
        } finally {
            if (in != null)
                in.close();
        }
    }

    private long readUntilEnd(FSDataInputStream in, byte[] buffer, long size,
                              String fname, long pos, long visibleLen, boolean positionReadOption)
            throws IOException {

        if (pos >= visibleLen || visibleLen <= 0)
            return 0;

        int chunkNumber = 0;
        long totalByteRead = 0;
        long currentPosition = pos;
        int byteRead = 0;
        long byteLeftToRead = visibleLen - pos;
        int byteToReadThisRound = 0;

        if (!positionReadOption) {
            in.seek(pos);
            currentPosition = in.getPos();
        }
        if (verboseOption)
            LOG.info("reader begin: position: " + pos + " ; currentOffset = "
                    + currentPosition + " ; bufferSize =" + buffer.length
                    + " ; Filename = " + fname);
        try {
            while (byteLeftToRead > 0 && currentPosition < visibleLen) {
                byteToReadThisRound = (int) (byteLeftToRead >= buffer.length
                        ? buffer.length : byteLeftToRead);
                if (positionReadOption) {
                    byteRead = in.read(currentPosition, buffer, 0, byteToReadThisRound);
                } else {
                    byteRead = in.read(buffer, 0, byteToReadThisRound);
                }
                if (byteRead <= 0)
                    break;
                chunkNumber++;
                totalByteRead += byteRead;
                currentPosition += byteRead;
                byteLeftToRead -= byteRead;

                if (verboseOption) {
                    LOG.info("reader: Number of byte read: " + byteRead
                            + " ; totalByteRead = " + totalByteRead + " ; currentPosition="
                            + currentPosition + " ; chunkNumber =" + chunkNumber
                            + "; File name = " + fname);
                }
            }
        } catch (IOException e) {
            throw new IOException(
                    "#### Exception caught in readUntilEnd: reader  currentOffset = "
                            + currentPosition + " ; totalByteRead =" + totalByteRead
                            + " ; latest byteRead = " + byteRead + "; visibleLen= "
                            + visibleLen + " ; bufferLen = " + buffer.length
                            + " ; Filename = " + fname, e);
        }

        if (verboseOption)
            LOG.info("reader end:   position: " + pos + " ; currentOffset = "
                    + currentPosition + " ; totalByteRead =" + totalByteRead
                    + " ; Filename = " + fname);

        return totalByteRead;
    }

    private void writeData(FSDataOutputStream out, byte[] buffer, int length)
            throws IOException {

        int totalByteWritten = 0;
        int remainToWrite = length;

        while (remainToWrite > 0) {
            int toWriteThisRound = remainToWrite > buffer.length ? buffer.length
                    : remainToWrite;
            out.write(buffer, 0, toWriteThisRound);
            totalByteWritten += toWriteThisRound;
            remainToWrite -= toWriteThisRound;
        }
        if (totalByteWritten != length) {
            throw new IOException("WriteData: failure in write. Attempt to write "
                    + length + " ; written=" + totalByteWritten);
        }
    }

    @Test
    public void testWriteReadSeq() throws IOException {
        useFCOption = false;
        positionReadOption = false;
        String fname = filenameOption;
        long rdBeginPos = 0;
        // need to run long enough to fail: takes 25 to 35 seec on Mac
        int stat = testWriteAndRead(fname, WR_NTIMES, WR_CHUNK_SIZE, rdBeginPos);
        LOG.info("Summary status from test1: status= " + stat);
        assertEquals(0, stat);
    }

    @Test
    public void testWriteReadPos() throws IOException {
        String fname = filenameOption;
        positionReadOption = true;   // position read
        long rdBeginPos = 0;
        int stat = testWriteAndRead(fname, WR_NTIMES, WR_CHUNK_SIZE, rdBeginPos);
        assertEquals(0, stat);
    }

    @Test
    public void testReadPosCurrentBlock() throws IOException {
        String fname = filenameOption;
        positionReadOption = true;   // position read
        int wrChunkSize = (int)(blockSize) + (int)(blockSize/2);
        long rdBeginPos = blockSize+1;
        int numTimes=5;
        int stat = testWriteAndRead(fname, numTimes, wrChunkSize, rdBeginPos);
        assertEquals(0, stat);
    }
}