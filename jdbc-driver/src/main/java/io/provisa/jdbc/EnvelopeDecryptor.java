package io.provisa.jdbc;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/**
 * Client-side envelope decryption for the Provisa JDBC driver (REQ-690).
 *
 * <p>The Provisa backend passes encrypted column blobs through undecrypted; this class
 * decrypts them driver-side, so a compromised backend never sees plaintext. Each blob is
 * a self-describing envelope matching the Python {@code provisa.encryption.envelope} wire
 * format:
 *
 * <pre>
 *   magic(1) | version(1) | len(wrapped_dek):u32-be | wrapped_dek | iv(12) | ciphertext+tag
 * </pre>
 *
 * <p>{@code wrapped_dek} is unwrapped by the client-owned {@link KmsProvider} (CMK).
 * Unwrapped DEKs are cached in-process with a short TTL to bound KMS round-trips on repeated
 * reads of the same blob. Failure is loud — a bad envelope, a revoked KMS grant, or a failed
 * AES-GCM tag throws {@link DecryptionException}; the driver never returns ciphertext.
 */
public final class EnvelopeDecryptor {

    private static final int MAGIC = 0xE1;
    private static final int VERSION = 1;
    private static final int IV_LEN = 12;
    private static final int DEK_LEN = 32;
    private static final int TAG_BITS = 128;

    private final KmsProvider provider;
    private final long ttlMillis;
    private final Map<String, CachedDek> cache = new HashMap<>();

    public EnvelopeDecryptor(KmsProvider provider, long dekCacheTtlSeconds) {
        this.provider = provider;
        this.ttlMillis = (long) (dekCacheTtlSeconds * 1000);
    }

    private static final class CachedDek {
        final byte[] dek;
        final long expiry;

        CachedDek(byte[] dek, long expiry) {
            this.dek = dek;
            this.expiry = expiry;
        }
    }

    /** Decrypt a base64-encoded envelope value to its UTF-8 plaintext string. */
    public String decryptField(String base64Value) throws DecryptionException {
        if (base64Value == null) {
            return null;
        }
        final byte[] blob;
        try {
            blob = Base64.getDecoder().decode(base64Value);
        } catch (IllegalArgumentException e) {
            throw new DecryptionException("encrypted field is not valid base64", e);
        }
        return new String(decrypt(blob), java.nio.charset.StandardCharsets.UTF_8);
    }

    /** Decrypt a raw envelope blob to plaintext bytes. */
    public byte[] decrypt(byte[] blob) throws DecryptionException {
        if (blob.length < 6) {
            throw new DecryptionException("blob too short to be a provisa envelope");
        }
        ByteBuffer buf = ByteBuffer.wrap(blob).order(ByteOrder.BIG_ENDIAN);
        int magic = buf.get() & 0xFF;
        int version = buf.get() & 0xFF;
        if (magic != MAGIC || version != VERSION) {
            throw new DecryptionException("not a provisa envelope blob (bad magic/version)");
        }
        long wlenL = buf.getInt() & 0xFFFFFFFFL;
        int wlen = (int) wlenL;
        if (wlen < 0 || 6 + wlen + IV_LEN > blob.length) {
            throw new DecryptionException("truncated provisa envelope blob");
        }
        byte[] wrapped = Arrays.copyOfRange(blob, 6, 6 + wlen);
        byte[] iv = Arrays.copyOfRange(blob, 6 + wlen, 6 + wlen + IV_LEN);
        byte[] ciphertext = Arrays.copyOfRange(blob, 6 + wlen + IV_LEN, blob.length);

        byte[] dek = unwrap(wrapped);
        try {
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(dek, "AES"),
                    new GCMParameterSpec(TAG_BITS, iv));
            return cipher.doFinal(ciphertext);
        } catch (Exception e) {
            throw new DecryptionException("AES-GCM authentication failed (tampered or wrong key)", e);
        }
    }

    private byte[] unwrap(byte[] wrapped) throws DecryptionException {
        String key = digest(wrapped);
        long now = System.currentTimeMillis();
        CachedDek hit = cache.get(key);
        if (hit != null && now < hit.expiry) {
            return hit.dek;
        }
        byte[] dek = provider.unwrapDek(wrapped);
        if (dek.length != DEK_LEN) {
            throw new DecryptionException("unwrapped DEK is " + dek.length + " bytes, expected " + DEK_LEN);
        }
        cache.put(key, new CachedDek(dek, now + ttlMillis));
        return dek;
    }

    private static String digest(byte[] wrapped) {
        try {
            byte[] d = MessageDigest.getInstance("SHA-256").digest(wrapped);
            return Base64.getEncoder().encodeToString(d);
        } catch (Exception e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }
}
