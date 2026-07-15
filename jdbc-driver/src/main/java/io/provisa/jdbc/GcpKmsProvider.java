package io.provisa.jdbc;

import com.google.cloud.kms.v1.DecryptResponse;
import com.google.cloud.kms.v1.KeyManagementServiceClient;
import com.google.protobuf.ByteString;

/**
 * GCP KMS client-owned key provider (REQ-690, REQ-694). Mirrors the Python
 * {@code GcpKmsProvider}: holds only the CryptoKey resource name and an injected
 * {@link KeyManagementServiceClient} handle — never raw key bytes — and wraps
 * {@code decrypt} on the CryptoKey to unwrap the DEK.
 *
 * <p>Revoking the IAM binding makes {@link #unwrapDek} fail — the client's instant-lockout
 * kill switch. Any SDK failure (including {@code PERMISSION_DENIED} on a revoked grant) maps
 * to {@link DecryptionException} (fail loud — never a silent plaintext passthrough).
 */
public final class GcpKmsProvider implements KmsProvider {

    private final String keyName;
    private final KeyManagementServiceClient client;

    public GcpKmsProvider(String keyName, KeyManagementServiceClient client) {
        if (keyName == null || keyName.isEmpty()) {
            throw new IllegalArgumentException("GcpKmsProvider requires a CryptoKey resource name (kms_key_arn)");
        }
        if (client == null) {
            throw new IllegalArgumentException("GcpKmsProvider requires a KeyManagementServiceClient");
        }
        this.keyName = keyName;
        this.client = client;
    }

    @Override
    public byte[] unwrapDek(byte[] wrapped) throws DecryptionException {
        try {
            DecryptResponse resp = client.decrypt(keyName, ByteString.copyFrom(wrapped));
            return resp.getPlaintext().toByteArray();
        } catch (Exception e) {
            throw new DecryptionException(
                    "GCP KMS unwrap failed (grant revoked or key unavailable): " + e.getMessage(), e);
        }
    }
}
