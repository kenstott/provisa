package io.provisa.jdbc;

import java.sql.SQLException;
import java.util.Base64;
import java.util.Map;

/**
 * Selects the client-owned {@link KmsProvider} for a JDBC connection by {@code kms_provider}
 * (REQ-690, REQ-694). Mirrors the Python {@code build_kms_provider}.
 *
 * <p>{@code local} is JDK-only (base64 master key). The {@code aws}/{@code azure}/{@code gcp}
 * cloud providers need their SDK on the runtime classpath (bundled as optional so the fat JAR
 * stays lean — the deployer supplies the SDK for their cloud). A named cloud provider whose SDK
 * is absent fails loud with a {@link SQLException}, never a silent skip. An unknown provider
 * fails closed.
 */
public final class KmsProviders {

    static final String AWS_MARKER = "software.amazon.awssdk.services.kms.KmsClient";
    static final String AZURE_MARKER = "com.azure.security.keyvault.keys.cryptography.CryptographyClient";
    static final String GCP_MARKER = "com.google.cloud.kms.v1.KeyManagementServiceClient";

    private KmsProviders() {
    }

    /**
     * Build the provider named by {@code kms_provider}, wiring the client-owned key {@code keyId}
     * (the {@code kms_key_arn}). {@code params} carries {@code kms_master_key} for {@code local}.
     */
    public static KmsProvider forName(String provider, String keyId, Map<String, String> params)
            throws SQLException {
        String p = provider == null ? "" : provider.toLowerCase();
        switch (p) {
            case "local": {
                String b64 = params == null ? null : params.get("kms_master_key");
                if (b64 == null) {
                    throw new SQLException("kms_provider=local requires kms_master_key (base64 32-byte key)");
                }
                return new LocalKmsProvider(Base64.getDecoder().decode(b64));
            }
            case "aws":
                requireKeyId(keyId, "aws");
                requireSdk(AWS_MARKER, "aws", "software.amazon.awssdk:kms");
                return buildWithClient("aws", keyId,
                        software.amazon.awssdk.services.kms.KmsClient.create());
            case "azure":
                requireKeyId(keyId, "azure");
                requireSdk(AZURE_MARKER, "azure", "com.azure:azure-security-keyvault-keys");
                return buildWithClient("azure", keyId,
                        new com.azure.security.keyvault.keys.cryptography.CryptographyClientBuilder()
                                .keyIdentifier(keyId)
                                .credential(new com.azure.identity.DefaultAzureCredentialBuilder().build())
                                .buildClient());
            case "gcp":
                requireKeyId(keyId, "gcp");
                requireSdk(GCP_MARKER, "gcp", "com.google.cloud:google-cloud-kms");
                try {
                    return buildWithClient("gcp", keyId,
                            com.google.cloud.kms.v1.KeyManagementServiceClient.create());
                } catch (java.io.IOException e) {
                    throw new SQLException("GCP KMS client init failed: " + e.getMessage(), e);
                }
            default:
                throw new SQLException("Unknown kms_provider: " + provider);
        }
    }

    /** Wrap an already-built SDK client in its provider (selection logic; unit-testable with mocks). */
    static KmsProvider buildWithClient(String provider, String keyId, Object client) throws SQLException {
        switch (provider.toLowerCase()) {
            case "aws":
                return new AwsKmsProvider(keyId,
                        (software.amazon.awssdk.services.kms.KmsClient) client);
            case "azure":
                return new AzureKmsProvider(keyId,
                        (com.azure.security.keyvault.keys.cryptography.CryptographyClient) client);
            case "gcp":
                return new GcpKmsProvider(keyId,
                        (com.google.cloud.kms.v1.KeyManagementServiceClient) client);
            default:
                throw new SQLException("Unknown kms_provider: " + provider);
        }
    }

    static void requireSdk(String markerClass, String provider, String coords) throws SQLException {
        try {
            Class.forName(markerClass);
        } catch (ClassNotFoundException e) {
            throw new SQLException("kms_provider=" + provider + " requires its cloud SDK on the classpath ("
                    + coords + "); add it to the driver runtime classpath.");
        }
    }

    private static void requireKeyId(String keyId, String provider) throws SQLException {
        if (keyId == null || keyId.isEmpty()) {
            throw new SQLException("kms_provider=" + provider + " requires kms_key_arn (client CMK identifier)");
        }
    }
}
