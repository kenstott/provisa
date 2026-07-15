package io.provisa.jdbc;

import java.sql.SQLException;

/**
 * Client-side decryption failed (REQ-690).
 *
 * <p>A security path never silently returns ciphertext or plaintext-on-failure — a
 * decrypt that cannot unwrap the DEK or fails the AES-GCM authentication tag throws
 * this exception up through the JDBC {@code getString}/{@code getObject} call.
 */
public class DecryptionException extends SQLException {
    public DecryptionException(String message) {
        super(message);
    }

    public DecryptionException(String message, Throwable cause) {
        super(message, cause);
    }
}
