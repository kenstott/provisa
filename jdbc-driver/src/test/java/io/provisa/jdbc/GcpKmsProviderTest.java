package io.provisa.jdbc;

import com.google.cloud.kms.v1.DecryptResponse;
import com.google.cloud.kms.v1.KeyManagementServiceClient;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class GcpKmsProviderTest {

    private static final String KEY_NAME =
            "projects/p/locations/global/keyRings/r/cryptoKeys/cmk";

    @Test
    void unwrapDek_delegatesToKmsDecrypt_returnsDek() throws Exception {
        byte[] wrapped = "wrapped-blob".getBytes();
        byte[] dek = new byte[32];
        for (int i = 0; i < 32; i++) dek[i] = (byte) (i + 2);

        KeyManagementServiceClient client = mock(KeyManagementServiceClient.class);
        DecryptResponse resp = DecryptResponse.newBuilder()
                .setPlaintext(ByteString.copyFrom(dek))
                .build();
        when(client.decrypt(eq(KEY_NAME), eq(ByteString.copyFrom(wrapped)))).thenReturn(resp);

        GcpKmsProvider provider = new GcpKmsProvider(KEY_NAME, client);
        assertArrayEquals(dek, provider.unwrapDek(wrapped));
        verify(client).decrypt(KEY_NAME, ByteString.copyFrom(wrapped));
    }

    @Test
    void unwrapDek_mapsPermissionDeniedToDecryptionException() {
        KeyManagementServiceClient client = mock(KeyManagementServiceClient.class);
        when(client.decrypt(anyString(), any(ByteString.class)))
                .thenThrow(new RuntimeException("PERMISSION_DENIED: grant revoked"));

        GcpKmsProvider provider = new GcpKmsProvider(KEY_NAME, client);
        DecryptionException ex = assertThrows(DecryptionException.class,
                () -> provider.unwrapDek("x".getBytes()));
        assertTrue(ex.getMessage().contains("grant revoked"));
    }

    @Test
    void constructor_rejectsMissingKeyOrClient() {
        assertThrows(IllegalArgumentException.class,
                () -> new GcpKmsProvider(null, mock(KeyManagementServiceClient.class)));
        assertThrows(IllegalArgumentException.class, () -> new GcpKmsProvider(KEY_NAME, null));
    }

    @Test
    void holdsNoRawKeyMaterial() {
        GcpKmsProvider provider = new GcpKmsProvider(KEY_NAME, mock(KeyManagementServiceClient.class));
        for (Field f : GcpKmsProvider.class.getDeclaredFields()) {
            assertNotEquals(byte[].class, f.getType(), "provider must not hold raw key bytes: " + f.getName());
        }
    }
}
