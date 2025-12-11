package org.apache.hadoop.hdfs.ipc;

import com.aliyun.jindodata.gateway.JindoSingleClusterTestBase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;
import static org.apache.hadoop.security.SaslRpcServer.AuthMethod.SIMPLE;
import static org.junit.jupiter.api.Assertions.*;

public class TestSaslRpc extends JindoSingleClusterTestBase {
    SaslRpcServer.QualityOfProtection[] qop;
    SaslRpcServer.QualityOfProtection expectedQop;
    String saslPropertiesResolver;
    Configuration conf;

    public static final Log LOG =
            LogFactory.getLog(TestSaslRpc.class);

    static final String SERVER_PRINCIPAL_1 = "p1/foo@BAR";

    // If this is set to true AND the auth-method is not simple, secretManager
    // will be enabled.
    static Boolean enableSecretManager = null;
    // If this is set to true, secretManager will be forecefully enabled
    // irrespective of auth-method.
    static Boolean forceSecretManager = null;
    static Boolean clientFallBackToSimpleAllowed = true;

    @BeforeEach
    public void before() throws IOException {
        conf = new Configuration(BASE_CONF);
        // the specific tests for kerberos will enable kerberos.  forcing it
        // for all tests will cause tests to fail if the user has a TGT
        conf.set(HADOOP_SECURITY_AUTHENTICATION, SIMPLE.toString());
        if (saslPropertiesResolver != null) {
            conf.set(CommonConfigurationKeys.HADOOP_SECURITY_SASL_PROPS_RESOLVER_CLASS,
                    saslPropertiesResolver);
        }
        UserGroupInformation.setConfiguration(conf);
        enableSecretManager = null;
        forceSecretManager = null;
        clientFallBackToSimpleAllowed = true;
    }

    public static class TestTokenIdentifier extends TokenIdentifier {
        private Text tokenid;
        private Text realUser;
        static final Text KIND_NAME = new Text("test.token");

        public TestTokenIdentifier() {
            this(new Text(), new Text());
        }

        public TestTokenIdentifier(Text tokenid) {
            this(tokenid, new Text());
        }

        public TestTokenIdentifier(Text tokenid, Text realUser) {
            this.tokenid = tokenid == null ? new Text() : tokenid;
            this.realUser = realUser == null ? new Text() : realUser;
        }

        public Text getKind() {
            return KIND_NAME;
        }

        public UserGroupInformation getUser() {
            if (this.realUser.toString().isEmpty()) {
                return UserGroupInformation.createRemoteUser(this.tokenid.toString());
            } else {
                UserGroupInformation realUgi = UserGroupInformation.createRemoteUser(this.realUser.toString());
                return UserGroupInformation.createProxyUser(this.tokenid.toString(), realUgi);
            }
        }

        public void readFields(DataInput in) throws IOException {
            this.tokenid.readFields(in);
            this.realUser.readFields(in);
        }

        public void write(DataOutput out) throws IOException {
            this.tokenid.write(out);
            this.realUser.write(out);
        }
    }

    public static class TestTokenSecretManager extends SecretManager<TestTokenIdentifier> {
        public TestTokenSecretManager() {
        }

        @Override
        protected byte[] createPassword(TestTokenIdentifier testTokenIdentifier) {
            return testTokenIdentifier.getBytes();
        }

        public byte[] retrievePassword(TestTokenIdentifier id) throws InvalidToken {
            throw new InvalidToken("Token is invalid");
        }

        @Override
        public TestTokenIdentifier createIdentifier() {
            return null;
        }
    }

    @Test
    public void testSimpleServer() throws Exception {
        // config.put("hadoop.user.group.static.mapping.overrides", "client=supergroup");  // TODO: check
        // create cluster
        InetSocketAddress serverAddr =
                new InetSocketAddress("localhost", cluster.getNameNodePort());

        final Configuration serverConf = new Configuration(conf);
        serverConf.set(HADOOP_SECURITY_AUTHENTICATION, SIMPLE.toString());
        UserGroupInformation.setConfiguration(serverConf);

        final TestTokenSecretManager sm = new TestTokenSecretManager();

        final Configuration clientConf = new Configuration(conf);
        clientConf.set(HADOOP_SECURITY_AUTHENTICATION, SIMPLE.toString());
        clientConf.setBoolean(
                CommonConfigurationKeys.IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_KEY,
                clientFallBackToSimpleAllowed);
        UserGroupInformation.setConfiguration(clientConf);

        final UserGroupInformation clientUgi =
                UserGroupInformation.createRemoteUser("client");
        clientUgi.setAuthenticationMethod(SIMPLE);

        TestTokenIdentifier tokenId = new TestTokenIdentifier(
                new Text(clientUgi.getUserName()));
        Token<TestTokenIdentifier> token = new Token<>(tokenId, sm);

        clientUgi.addToken(token);


        LOG.info("trying ugi:" + clientUgi + " tokens:" + clientUgi.getTokens());
        clientUgi.doAs(new PrivilegedExceptionAction<String>() {
            @Override
            public String run() throws IOException {
                try {
                    DFSClient client = new DFSClient(serverAddr, conf);
                    FileSystem fs = getDefaultFS();
                    HdfsFileStatus fileInfo = client.getFileInfo(makeTestPathStr("noSuchFile"));
                    assertNull(fileInfo, "Non-existant file should result in null");

                    Path path1 = makeTestPath("name1");
                    Path path2 = makeTestPath("name1/name2");
                    assertTrue(fs.mkdirs(path1));
                    FSDataOutputStream out = fs.create(path2, false);
                    out.close();
                    fileInfo = client.getFileInfo(path1.toString());
                    // konna : getChildrenNum() is not implemented in JfsFileStatus
//                    assertEquals(1, fileInfo.getChildrenNum());
                    return SIMPLE.toString();
                } catch (RemoteException ex) {
                    fail("should not throw exception ");
                    throw (RemoteException) ex.getCause();
                } finally {
                }
            }
        });
    }
}
