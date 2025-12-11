package org.apache.hadoop.hdfs;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.security.ShellBasedUnixGroupsMapping;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.*;

public class TestUtil {

    public static File getTestDir(Class<?> caller, boolean create) {
        File dir = new File(System.getProperty("test.build.data", "target/test/data") + "/" + RandomStringUtils.randomAlphanumeric(10), caller.getSimpleName());
        if (create) {
            dir.mkdirs();
        }

        return dir;
    }

    public static String getTestDirName(Class<?> caller) {
        return getTestDirName(caller, true);
    }

    public static String getTestDirName(Class<?> caller, boolean create) {
        return getTestDir(caller, create).getAbsolutePath();
    }

    public static void createFile(FileSystem fs, Path fileName, long fileLen,
                                  short replFactor, long seed) throws IOException {
        if (!fs.mkdirs(fileName.getParent())) {
            throw new IOException("Mkdirs failed to create " +
                    fileName.getParent().toString());
        }
        try (FSDataOutputStream out = fs.create(fileName, replFactor)) {
            byte[] toWrite = new byte[1024];
            Random rb = new Random(seed);
            long bytesToWrite = fileLen;
            while (bytesToWrite>0) {
                rb.nextBytes(toWrite);
                int bytesToWriteNext = (1024<bytesToWrite)?1024:(int)bytesToWrite;

                out.write(toWrite, 0, bytesToWriteNext);
                bytesToWrite -= bytesToWriteNext;
            }
        }
    }

    public static void appendFile(FileSystem fs, Path p, int length)
            throws IOException {
        assert fs.exists(p);
        assert length >= 0;
        byte[] toAppend = new byte[length];
        Random random = new Random();
        random.nextBytes(toAppend);
        try (FSDataOutputStream out = fs.append(p)) {
            out.write(toAppend);
        }
    }

    public static class ShortCircuitTestContext implements Closeable {
        private final String                   testName;
        private boolean closed = false;
        private final boolean formerTcpReadsDisabled;
        private File dir;

        public ShortCircuitTestContext(String testName) {
            this.testName = testName;
            String tmp = System.getProperty("java.io.tmpdir", "/tmp");
            this.dir = new File(tmp, "socks." + System.currentTimeMillis() + "." + (new Random()).nextInt());
            this.dir.mkdirs();
            FileUtil.setWritable(this.dir, true);
            DomainSocket.disableBindPathValidation();
            formerTcpReadsDisabled = DFSInputStream.tcpReadsDisabledForTesting;
            System.out.println(DomainSocket.getLoadingFailureReason());
//            Assume.assumeTrue(DomainSocket.getLoadingFailureReason() == null);
        }

        public Configuration newConfiguration() {
            Configuration conf = new Configuration();
            conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY, true);
            conf.set(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY,
                    new File(dir,
                            testName + "._PORT.sock").getAbsolutePath());
            return conf;
        }
        public Configuration newConfiguration(Configuration base) {
            Configuration conf = new Configuration(base);
            conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY, true);
            conf.set(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY,
                    new File(dir,
                            testName + "._PORT.sock").getAbsolutePath());
            return conf;
        }


        public String getTestName() {
            return testName;
        }

        public void close() throws IOException {
            if (closed) return;
            closed = true;
            DFSInputStream.tcpReadsDisabledForTesting = formerTcpReadsDisabled;
            if (this.dir != null) {
                FileUtils.deleteDirectory(this.dir);
                this.dir = null;
            }
        }
    }

    /**
     * mock class to get group mapping for fake users
     *
     */
    static class MockUnixGroupsMapping extends ShellBasedUnixGroupsMapping {
        static               Map<String, String []> fakeUser2GroupsMap;
        private static final List<String> defaultGroups;
        static {
            defaultGroups = new ArrayList<String>(1);
            defaultGroups.add("supergroup");
            fakeUser2GroupsMap = new HashMap<String, String[]>();
        }

        @Override
        public List<String> getGroups(String user) throws IOException {
            boolean found = false;

            // check to see if this is one of fake users
            List<String> l = new ArrayList<String>();
            for(String u : fakeUser2GroupsMap.keySet()) {
                if(user.equals(u)) {
                    found = true;
                    for(String gr : fakeUser2GroupsMap.get(u)) {
                        l.add(gr);
                    }
                }
            }

            // default
            if(!found) {
                l =  super.getGroups(user);
                if(l.size() == 0) {
                    System.out.println("failed to get real group for " + user +
                            "; using default");
                    return defaultGroups;
                }
            }
            return l;
        }
    }

    /**
     * update the configuration with fake class for mapping user to groups
     * @param conf
     * @param map - user to groups mapping
     */
    static public void updateConfWithFakeGroupMapping
    (Configuration conf, Map<String, String []> map) {
        if(map!=null) {
            MockUnixGroupsMapping.fakeUser2GroupsMap = map;
        }

        // fake mapping user to groups
        conf.setClass(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
                MockUnixGroupsMapping.class,
                ShellBasedUnixGroupsMapping.class);

    }

    /**
     * Close current file system and create a new instance as given
     * {@link UserGroupInformation}.
     */
    public static FileSystem login(final FileSystem fs,
                                   final Configuration conf, final UserGroupInformation ugi)
            throws IOException, InterruptedException {
        if (fs != null) {
            fs.close();
        }
        return DfsAppendTestUtil.getFileSystemAs(ugi, conf);
    }
}
