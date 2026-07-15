package io.provisa.jdbc;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.DecryptRequest;
import software.amazon.awssdk.services.kms.model.DecryptResponse;
import software.amazon.awssdk.services.kms.model.KmsException;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class AwsKmsProviderTest {

    private static final String ARN = "arn:aws:kms:us-east-1:123456789012:key/abc";

    @Test
    void unwrapDek_delegatesToKmsDecrypt_returnsPlaintextDek() throws Exception {
        byte[] wrapped = "wrapped-blob".getBytes();
        byte[] dek = new byte[32];
        for (int i = 0; i < 32; i++) dek[i] = (byte) i;

        KmsClient client = mock(KmsClient.class);
        when(client.decrypt(any(DecryptRequest.class)))
                .thenReturn(DecryptResponse.builder()
                        .keyId(ARN)
                        .plaintext(SdkBytes.fromByteArray(dek))
                        .build());

        AwsKmsProvider provider = new AwsKmsProvider(ARN, client);
        byte[] out = provider.unwrapDek(wrapped);

        assertArrayEquals(dek, out);
        ArgumentCaptor<DecryptRequest> cap = ArgumentCaptor.forClass(DecryptRequest.class);
        verify(client).decrypt(cap.capture());
        assertEquals(ARN, cap.getValue().keyId());
        assertArrayEquals(wrapped, cap.getValue().ciphertextBlob().asByteArray());
    }

    @Test
    void unwrapDek_mapsAccessDeniedToDecryptionException() {
        KmsClient client = mock(KmsClient.class);
        when(client.decrypt(any(DecryptRequest.class)))
                .thenThrow(KmsException.builder().message("AccessDeniedException: grant revoked").build());

        AwsKmsProvider provider = new AwsKmsProvider(ARN, client);
        DecryptionException ex = assertThrows(DecryptionException.class,
                () -> provider.unwrapDek("x".getBytes()));
        assertTrue(ex.getMessage().contains("grant revoked"));
    }

    @Test
    void constructor_rejectsMissingArnOrClient() {
        assertThrows(IllegalArgumentException.class, () -> new AwsKmsProvider(null, mock(KmsClient.class)));
        assertThrows(IllegalArgumentException.class, () -> new AwsKmsProvider(ARN, null));
    }

    @Test
    void holdsNoRawKeyMaterial() {
        AwsKmsProvider provider = new AwsKmsProvider(ARN, mock(KmsClient.class));
        for (Field f : AwsKmsProvider.class.getDeclaredFields()) {
            assertNotEquals(byte[].class, f.getType(), "provider must not hold raw key bytes: " + f.getName());
        }
    }
}
