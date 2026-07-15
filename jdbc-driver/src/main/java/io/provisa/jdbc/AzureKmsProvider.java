package io.provisa.jdbc;

import com.azure.security.keyvault.keys.cryptography.CryptographyClient;
import com.azure.security.keyvault.keys.cryptography.models.DecryptResult;
import com.azure.security.keyvault.keys.cryptography.models.EncryptionAlgorithm;

/**
 * Azure Key Vault client-owned key provider (REQ-690, REQ-694). Mirrors the Python
 * {@code AzureKeyVaultProvider}: holds only the CMK key-id and an injected
 * {@link CryptographyClient} handle — never raw key bytes — and unwraps the DEK by
 * decrypting the wrapped blob with the CMK ({@code RSA-OAEP-256}).
 *
 * <p>Revoking the Key Vault access policy makes {@link #unwrapDek} fail — the client's
 * instant-lockout kill switch. Any SDK failure (including a forbidden/revoked grant)
 * maps to {@link DecryptionException} (fail loud — never a silent plaintext passthrough).
 */
public final class AzureKmsProvider implements KmsProvider {

    private final String keyId;
    private final CryptographyClient client;
    private final EncryptionAlgorithm algorithm;

    public AzureKmsProvider(String keyId, CryptographyClient client) {
        this(keyId, client, EncryptionAlgorithm.RSA_OAEP_256);
    }

    public AzureKmsProvider(String keyId, CryptographyClient client, EncryptionAlgorithm algorithm) {
        if (keyId == null || keyId.isEmpty()) {
            throw new IllegalArgumentException("AzureKmsProvider requires a key id (kms_key_arn)");
        }
        if (client == null) {
            throw new IllegalArgumentException("AzureKmsProvider requires a CryptographyClient");
        }
        this.keyId = keyId;
        this.client = client;
        this.algorithm = algorithm;
    }

    @Override
    public byte[] unwrapDek(byte[] wrapped) throws DecryptionException {
        try {
            DecryptResult result = client.decrypt(algorithm, wrapped);
            return result.getPlainText();
        } catch (Exception e) {
            throw new DecryptionException(
                    "Azure Key Vault unwrap failed (grant revoked or key unavailable): "
                    + e.getMessage() + " [key=" + keyId + "]", e);
        }
    }
}
