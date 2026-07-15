package io.provisa.jdbc;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.util.Arrays;

/**
 * JDK-only {@link KmsProvider} that unwraps a DEK with a local AES-256-GCM master key
 * (REQ-690). Used for tests and local envelope round-trips; the wrapped DEK format is
 * {@code nonce(12) || AESGCM(dek)} under the master key, matching the Python
 * {@code LocalKeychain} provider so an envelope produced by either side round-trips.
 *
 * <p>Production deployments use the AWS/Azure/GCP CMK providers (client-owned key,
 * REQ-694); this class never leaves the process and holds the master key only in memory.
 */
public final class LocalKmsProvider implements KmsProvider {

    private static final int NONCE_LEN = 12;
    private static final int TAG_BITS = 128;

    private final byte[] masterKey;

    public LocalKmsProvider(byte[] masterKey) {
        if (masterKey.length != 32) {
            throw new IllegalArgumentException("master key must be 32 bytes (AES-256)");
        }
        this.masterKey = masterKey.clone();
    }

    @Override
    public byte[] unwrapDek(byte[] wrapped) throws DecryptionException {
        try {
            byte[] nonce = Arrays.copyOfRange(wrapped, 0, NONCE_LEN);
            byte[] ct = Arrays.copyOfRange(wrapped, NONCE_LEN, wrapped.length);
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(masterKey, "AES"),
                    new GCMParameterSpec(TAG_BITS, nonce));
            return cipher.doFinal(ct);
        } catch (Exception e) {
            throw new DecryptionException("local DEK unwrap failed: " + e.getMessage(), e);
        }
    }
}
