package io.provisa.jdbc;

/**
 * Client-owned KMS key operations for client-side decryption (REQ-690, REQ-694).
 *
 * <p>An implementation holds only a key identifier and a cloud SDK client — never raw
 * key material. {@link #unwrapDek} decrypts a wrapped Data Encryption Key with the
 * client's Customer Master Key (CMK). The concrete AWS KMS / Azure Key Vault / GCP KMS
 * providers wrap {@code kms:Decrypt} (a scoped cross-account grant); revoking that grant
 * makes {@code unwrapDek} fail — the client's instant lockout kill switch.
 *
 * <p>The concrete cloud providers require their respective cloud SDK on the classpath and
 * are documented in {@code docs/arch/jdbc-client-side-encryption.md}; {@link LocalKmsProvider}
 * is a JDK-only implementation used for tests and local envelope round-trips.
 */
public interface KmsProvider {

    /**
     * Decrypt a wrapped DEK with the client CMK. Returns the raw 32-byte AES-256 DEK.
     *
     * @throws DecryptionException when the grant is revoked or the key is unavailable.
     */
    byte[] unwrapDek(byte[] wrapped) throws DecryptionException;
}
