# JDBC client-side decryption (REQ-690)

The Provisa JDBC driver decrypts result columns flagged encrypted in the column
metadata. The backend returns ciphertext; the driver holds the key relationship and
decrypts locally, so a compromised backend cannot read result data.

## Encrypted-column contract

A column is flagged encrypted through Arrow field metadata on the Flight/IPC schema:

```
field.metadata["provisa_encrypted"] = "true"
```

`ArrowStreamResultSet` reads this flag at construction and decrypts those columns in
`getString` / `getObject`. Column values are base64-encoded envelope blobs using the
same wire format as the Python client and `provisa.encryption.envelope`:

```
magic(1) | version(1) | len(wrapped_dek):u32-be | wrapped_dek | iv(12) | ciphertext+tag
```

## Connection parameters

Set via the JDBC URL query string or `Properties`:

- `kms_provider` — `aws` | `azure` | `gcp` | `local`
- `kms_key_arn` — the client-owned CMK identifier
- `kms_master_key` — base64 32-byte key, `local` provider only (tests / local use)

Example:

```
jdbc:provisa://host:8001?kms_provider=aws&kms_key_arn=arn:aws:kms:us-east-1:...:key/abc
```

## Classes

- `EnvelopeDecryptor` — parses the envelope, unwraps the DEK through a `KmsProvider`,
  caches unwrapped DEKs in-process with a TTL, and AES-256-GCM decrypts. A bad
  envelope, a revoked grant, or a failed authentication tag throws
  `DecryptionException`; the driver never returns ciphertext.
- `KmsProvider` — client-owned CMK operation (`unwrapDek`). `LocalKmsProvider` is the
  JDK-only implementation used for tests and local round-trips.
- `AwsKmsProvider` — unwraps the DEK with `KmsClient.decrypt` (AWS SDK for Java v2).
- `AzureKmsProvider` — unwraps the DEK with `CryptographyClient.decrypt`
  (`RSA-OAEP-256`, Azure Key Vault Keys).
- `GcpKmsProvider` — unwraps the DEK with `KeyManagementServiceClient.decrypt`
  (google-cloud-kms).
- `KmsProviders` — factory selecting the provider by `kms_provider`, wired into
  `ProvisaConnection.configureEncryption`.

Each cloud provider holds only the CMK identifier plus an injected SDK client — never
raw key material — and maps any SDK failure (including an `AccessDenied` /
`PERMISSION_DENIED` / forbidden result on a revoked grant) to `DecryptionException`.
Revoking the `kms:Decrypt` (or Key Vault / IAM) grant is an instant lockout kill
switch (REQ-694).

## Cloud SDK classpath

The three cloud SDKs (`software.amazon.awssdk:kms`,
`com.azure:azure-security-keyvault-keys`, `com.google.cloud:google-cloud-kms`) are
`provided`-scope dependencies: on the compile and test classpath but not bundled into
the shaded fat JAR, so the shipped driver stays lean. The deployer supplies the SDK
for their cloud on the runtime classpath. Naming a `kms_provider` whose SDK is absent
fails loud (`SQLException: kms_provider=<x> requires its cloud SDK on the classpath`),
never a silent skip.

## High-security mode (REQ-693)

When `security.mode=high`, the pgwire server is not started. JDBC clients reach data
over the KMS-gated HTTP/Flight path and send `X-Provisa-KMS-Key` (the `kms_key_arn`)
so the high-security middleware admits the connection; a client without it is refused.
