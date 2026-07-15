package io.provisa.jdbc;

import com.azure.security.keyvault.keys.cryptography.CryptographyClient;
import com.azure.security.keyvault.keys.cryptography.models.DecryptResult;
import com.azure.security.keyvault.keys.cryptography.models.EncryptionAlgorithm;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class AzureKmsProviderTest {

    private static final String KEY_ID = "https://vault.vault.azure.net/keys/cmk/ver";

    @Test
    void unwrapDek_delegatesToCryptographyDecrypt_returnsDek() throws Exception {
        byte[] wrapped = "wrapped-blob".getBytes();
        byte[] dek = new byte[32];
        for (int i = 0; i < 32; i++) dek[i] = (byte) (i + 1);

        DecryptResult result = mock(DecryptResult.class);
        when(result.getPlainText()).thenReturn(dek);
        CryptographyClient client = mock(CryptographyClient.class);
        when(client.decrypt(eq(EncryptionAlgorithm.RSA_OAEP_256), eq(wrapped))).thenReturn(result);

        AzureKmsProvider provider = new AzureKmsProvider(KEY_ID, client);
        assertArrayEquals(dek, provider.unwrapDek(wrapped));
        verify(client).decrypt(EncryptionAlgorithm.RSA_OAEP_256, wrapped);
    }

    @Test
    void unwrapDek_mapsRevokedGrantToDecryptionException() {
        CryptographyClient client = mock(CryptographyClient.class);
        when(client.decrypt(any(EncryptionAlgorithm.class), any(byte[].class)))
                .thenThrow(new RuntimeException("Forbidden: caller lacks keys/decrypt permission"));

        AzureKmsProvider provider = new AzureKmsProvider(KEY_ID, client);
        DecryptionException ex = assertThrows(DecryptionException.class,
                () -> provider.unwrapDek("x".getBytes()));
        assertTrue(ex.getMessage().contains("Forbidden"));
    }

    @Test
    void constructor_rejectsMissingKeyOrClient() {
        assertThrows(IllegalArgumentException.class, () -> new AzureKmsProvider(null, mock(CryptographyClient.class)));
        assertThrows(IllegalArgumentException.class, () -> new AzureKmsProvider(KEY_ID, null));
    }

    @Test
    void holdsNoRawKeyMaterial() {
        AzureKmsProvider provider = new AzureKmsProvider(KEY_ID, mock(CryptographyClient.class));
        for (Field f : AzureKmsProvider.class.getDeclaredFields()) {
            assertNotEquals(byte[].class, f.getType(), "provider must not hold raw key bytes: " + f.getName());
        }
    }
}
