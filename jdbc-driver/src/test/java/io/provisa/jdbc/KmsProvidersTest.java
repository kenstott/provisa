package io.provisa.jdbc;

import com.azure.security.keyvault.keys.cryptography.CryptographyClient;
import com.google.cloud.kms.v1.KeyManagementServiceClient;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.kms.KmsClient;

import java.sql.SQLException;
import java.util.Base64;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class KmsProvidersTest {

    @Test
    void forName_local_buildsLocalProvider() throws Exception {
        String b64 = Base64.getEncoder().encodeToString(new byte[32]);
        KmsProvider p = KmsProviders.forName("local", null, Map.of("kms_master_key", b64));
        assertInstanceOf(LocalKmsProvider.class, p);
    }

    @Test
    void forName_local_missingMasterKey_failsLoud() {
        assertThrows(SQLException.class, () -> KmsProviders.forName("local", null, Map.of()));
    }

    @Test
    void forName_unknownProvider_failsClosed() {
        SQLException ex = assertThrows(SQLException.class,
                () -> KmsProviders.forName("vault", "k", Map.of()));
        assertTrue(ex.getMessage().contains("Unknown kms_provider"));
    }

    @Test
    void forName_cloudProvider_missingKeyId_failsLoud() {
        assertThrows(SQLException.class, () -> KmsProviders.forName("aws", null, Map.of()));
        assertThrows(SQLException.class, () -> KmsProviders.forName("azure", "", Map.of()));
        assertThrows(SQLException.class, () -> KmsProviders.forName("gcp", null, Map.of()));
    }

    @Test
    void buildWithClient_selectsCorrectProviderPerName() throws Exception {
        assertInstanceOf(AwsKmsProvider.class,
                KmsProviders.buildWithClient("aws", "arn", mock(KmsClient.class)));
        assertInstanceOf(AzureKmsProvider.class,
                KmsProviders.buildWithClient("azure", "kid", mock(CryptographyClient.class)));
        assertInstanceOf(GcpKmsProvider.class,
                KmsProviders.buildWithClient("gcp", "kn", mock(KeyManagementServiceClient.class)));
    }

    @Test
    void requireSdk_present_noThrow() throws Exception {
        KmsProviders.requireSdk("java.lang.String", "aws", "software.amazon.awssdk:kms");
    }

    @Test
    void requireSdk_absent_failsLoud() {
        SQLException ex = assertThrows(SQLException.class,
                () -> KmsProviders.requireSdk("com.nonexistent.MissingSdk", "aws", "software.amazon.awssdk:kms"));
        assertTrue(ex.getMessage().contains("requires its cloud SDK on the classpath"));
    }
}
