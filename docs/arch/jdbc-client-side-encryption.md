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

## Deferred

The AWS KMS, Azure Key Vault, and GCP KMS concrete `KmsProvider` implementations
require the respective cloud SDK on the driver classpath and a live cross-account
grant. Those, plus the end-to-end JDBC round-trip test (a real pgwire/Flight
connection against an encrypted backend response), are deferred to the REQ-690 target
of 2027-Q1. The envelope decryptor, the encrypted-column metadata contract, the
connection-parameter parsing, and the `local` provider are implemented and compile in
the in-repo `jdbc-driver` module.

## High-security mode (REQ-693)

When `security.mode=high`, the pgwire server is not started. JDBC clients reach data
over the KMS-gated HTTP/Flight path and send `X-Provisa-KMS-Key` (the `kms_key_arn`)
so the high-security middleware admits the connection; a client without it is refused.
