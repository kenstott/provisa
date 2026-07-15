package io.provisa.jdbc;

import org.junit.jupiter.api.Test;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Envelope round-trip parity: a DEK wrapped under a local master key (matching the Python
 * {@code LocalKeychain} / {@code provisa.encryption.envelope} wire format) unwraps identically
 * through {@link LocalKmsProvider} and the full {@link EnvelopeDecryptor} yields plaintext.
 * Failure is loud (REQ-690).
 */
class EnvelopeDecryptorTest {

    private static final int MAGIC = 0xE1;
    private static final int VERSION = 1;
    private static final SecureRandom RNG = new SecureRandom();

    private static byte[] aesGcmEncrypt(byte[] key, byte[] nonce, byte[] plaintext) throws Exception {
        Cipher c = Cipher.getInstance("AES/GCM/NoPadding");
        c.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(key, "AES"), new GCMParameterSpec(128, nonce));
        return c.doFinal(plaintext);
    }

    /** Wrap a DEK the way LocalKmsProvider expects: nonce(12) || AESGCM(masterKey)(dek). */
    private static byte[] wrapDek(byte[] masterKey, byte[] dek) throws Exception {
        byte[] nonce = new byte[12];
        RNG.nextBytes(nonce);
        byte[] ct = aesGcmEncrypt(masterKey, nonce, dek);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(nonce);
        out.write(ct);
        return out.toByteArray();
    }

    private static byte[] envelope(byte[] wrapped, byte[] iv, byte[] ciphertext) {
        ByteBuffer buf = ByteBuffer.allocate(6 + wrapped.length + iv.length + ciphertext.length)
                .order(ByteOrder.BIG_ENDIAN);
        buf.put((byte) MAGIC).put((byte) VERSION).putInt(wrapped.length);
        buf.put(wrapped).put(iv).put(ciphertext);
        return buf.array();
    }

    @Test
    void localProvider_unwrapsDekIdentically() throws Exception {
        byte[] masterKey = new byte[32];
        byte[] dek = new byte[32];
        RNG.nextBytes(masterKey);
        RNG.nextBytes(dek);

        byte[] wrapped = wrapDek(masterKey, dek);
        assertArrayEquals(dek, new LocalKmsProvider(masterKey).unwrapDek(wrapped));
    }

    @Test
    void fullEnvelope_decryptsToPlaintext() throws Exception {
        byte[] masterKey = new byte[32];
        byte[] dek = new byte[32];
        RNG.nextBytes(masterKey);
        RNG.nextBytes(dek);
        String plaintext = "sensitive-value-42";

        byte[] wrapped = wrapDek(masterKey, dek);
        byte[] iv = new byte[12];
        RNG.nextBytes(iv);
        byte[] ciphertext = aesGcmEncrypt(dek, iv, plaintext.getBytes(StandardCharsets.UTF_8));
        String b64 = Base64.getEncoder().encodeToString(envelope(wrapped, iv, ciphertext));

        EnvelopeDecryptor dec = new EnvelopeDecryptor(new LocalKmsProvider(masterKey), 300);
        assertEquals(plaintext, dec.decryptField(b64));
    }

    @Test
    void wrongMasterKey_failsLoud() throws Exception {
        byte[] masterKey = new byte[32];
        byte[] wrongKey = new byte[32];
        byte[] dek = new byte[32];
        RNG.nextBytes(masterKey);
        RNG.nextBytes(wrongKey);
        RNG.nextBytes(dek);

        byte[] wrapped = wrapDek(masterKey, dek);
        byte[] iv = new byte[12];
        RNG.nextBytes(iv);
        byte[] ciphertext = aesGcmEncrypt(dek, iv, "x".getBytes(StandardCharsets.UTF_8));
        String b64 = Base64.getEncoder().encodeToString(envelope(wrapped, iv, ciphertext));

        EnvelopeDecryptor dec = new EnvelopeDecryptor(new LocalKmsProvider(wrongKey), 300);
        assertThrows(DecryptionException.class, () -> dec.decryptField(b64));
    }

    @Test
    void tamperedCiphertext_failsLoud() throws Exception {
        byte[] masterKey = new byte[32];
        byte[] dek = new byte[32];
        RNG.nextBytes(masterKey);
        RNG.nextBytes(dek);

        byte[] wrapped = wrapDek(masterKey, dek);
        byte[] iv = new byte[12];
        RNG.nextBytes(iv);
        byte[] ciphertext = aesGcmEncrypt(dek, iv, "x".getBytes(StandardCharsets.UTF_8));
        ciphertext[0] ^= 0x01;
        String b64 = Base64.getEncoder().encodeToString(envelope(wrapped, iv, ciphertext));

        EnvelopeDecryptor dec = new EnvelopeDecryptor(new LocalKmsProvider(masterKey), 300);
        assertThrows(DecryptionException.class, () -> dec.decryptField(b64));
    }
}
