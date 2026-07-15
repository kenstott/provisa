package io.provisa.jdbc;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.DecryptRequest;
import software.amazon.awssdk.services.kms.model.DecryptResponse;

/**
 * AWS KMS client-owned CMK provider (REQ-690, REQ-694). Mirrors the Python
 * {@code AwsKmsProvider}: holds only the CMK ARN and an injected {@link KmsClient}
 * handle — never raw key bytes — and wraps {@code kms:Decrypt} to unwrap the DEK.
 *
 * <p>The scoped cross-account IAM grant only needs {@code kms:Decrypt}; revoking it
 * makes {@link #unwrapDek} fail — the client's instant-lockout kill switch. Any SDK
 * failure (including {@code AccessDeniedException} on a revoked grant) maps to
 * {@link DecryptionException} (fail loud — never a silent plaintext passthrough).
 */
public final class AwsKmsProvider implements KmsProvider {

    private final String keyArn;
    private final KmsClient client;

    public AwsKmsProvider(String keyArn, KmsClient client) {
        if (keyArn == null || keyArn.isEmpty()) {
            throw new IllegalArgumentException("AwsKmsProvider requires a key ARN (kms_key_arn)");
        }
        if (client == null) {
            throw new IllegalArgumentException("AwsKmsProvider requires a KmsClient");
        }
        this.keyArn = keyArn;
        this.client = client;
    }

    @Override
    public byte[] unwrapDek(byte[] wrapped) throws DecryptionException {
        try {
            DecryptResponse resp = client.decrypt(DecryptRequest.builder()
                    .keyId(keyArn)
                    .ciphertextBlob(SdkBytes.fromByteArray(wrapped))
                    .build());
            return resp.plaintext().asByteArray();
        } catch (Exception e) {
            throw new DecryptionException(
                    "AWS KMS unwrap failed (grant revoked or key unavailable): " + e.getMessage(), e);
        }
    }
}
