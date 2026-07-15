# Copyright (c) 2026 Kenneth Stott
# Canary: 5d5b9a0e-7c41-4f2c-9d3a-1e6b8c0f2a77
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Client-side envelope decryption for Provisa clients (REQ-691, REQ-692, REQ-694).

The Provisa backend passes encrypted column blobs through undecrypted; the client
holds the key material relationship and decrypts locally. Each blob is a
self-describing envelope:

    magic(1) | version(1) | len(wrapped_dek):u32-be | wrapped_dek | iv(12) | ciphertext+tag

``wrapped_dek`` is a KMS-wrapped Data Encryption Key. Only the client's own CMK
(AWS KMS / Azure Key Vault / GCP KMS — REQ-694) can unwrap it, so a compromised
backend cannot read the payload. Unwrapped DEKs are cached in-process with a
short TTL to bound KMS round-trips (REQ-691). Provisa never stores raw key
material: the providers hold only a key identifier plus a cloud SDK client and
call ``Decrypt`` / ``GenerateDataKey`` on demand; a client revoke of the KMS
grant is instant lockout (REQ-694).

Failure is loud: a decrypt that cannot unwrap the DEK or fails the AES-GCM
authentication tag raises ``DecryptionError`` — a security path never silently
returns ciphertext or plaintext-on-failure.
"""

from __future__ import annotations

import base64
import hashlib
import struct
import time
from abc import ABC, abstractmethod
from typing import Any

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

_MAGIC = 0xE1  # marks a provisa envelope blob (must match provisa.encryption.envelope)
_VERSION = 1
_HEADER = struct.Struct(">BBI")  # magic, version, wrapped_dek_len
_IV_LEN = 12
_DEK_LEN = 32  # AES-256


class DecryptionError(Exception):
    """Client-side decryption failed. Never swallowed — surfaces to the caller."""


def split_envelope(blob: bytes) -> tuple[bytes, bytes, bytes]:
    """Split a provisa envelope blob into ``(wrapped_dek, iv, ciphertext+tag)``.

    Raises ``DecryptionError`` when the framing is not a provisa envelope.
    """
    if len(blob) < _HEADER.size:
        raise DecryptionError("blob too short to be a provisa envelope")
    magic, version, wlen = _HEADER.unpack_from(blob)
    if magic != _MAGIC or version != _VERSION:
        raise DecryptionError("not a provisa envelope blob (bad magic/version)")
    off = _HEADER.size
    wrapped = blob[off : off + wlen]
    off += wlen
    iv = blob[off : off + _IV_LEN]
    ciphertext = blob[off + _IV_LEN :]
    if len(wrapped) != wlen or len(iv) != _IV_LEN:
        raise DecryptionError("truncated provisa envelope blob")
    return wrapped, iv, ciphertext


# -- KMS provider abstraction (REQ-694) -------------------------------------------------------


class KmsProvider(ABC):
    """Client-owned Customer Master Key operations (REQ-694).

    Implementations hold only a key identifier and a cloud SDK client — never raw
    key material. ``unwrap_dek`` decrypts a wrapped DEK via the CMK; ``wrap_dek``
    (used for round-trip tests / client-side encrypt) generates a fresh DEK under
    the CMK. Revoking the cloud grant makes both fail — the client's kill switch.
    """

    @abstractmethod
    def unwrap_dek(self, wrapped: bytes) -> bytes:
        """Decrypt a wrapped DEK with the client CMK. Returns the raw 32-byte DEK."""

    @abstractmethod
    def generate_data_key(self) -> tuple[bytes, bytes]:
        """Return ``(plaintext_dek, wrapped_dek)`` freshly generated under the CMK."""


class AwsKmsProvider(KmsProvider):  # REQ-694
    """AWS KMS client-owned CMK. Wraps ``kms:Decrypt`` / ``kms:GenerateDataKey``.

    The scoped cross-account IAM grant only needs those two actions. ``client`` is
    injectable so tests supply a fake; production lazily builds a boto3 KMS client.
    """

    def __init__(self, key_arn: str, *, region: str | None = None, client: "Any" = None) -> None:
        if not key_arn:
            raise ValueError("AwsKmsProvider requires a key_arn (kms_key_arn)")
        self._key_arn = key_arn
        self._region = region
        self._client = client  # only a client handle + arn — never raw key bytes

    def _kms(self):
        if self._client is None:
            import boto3  # noqa: PLC0415

            self._client = boto3.client("kms", region_name=self._region)
        return self._client

    def unwrap_dek(self, wrapped: bytes) -> bytes:
        resp = self._kms().decrypt(KeyId=self._key_arn, CiphertextBlob=wrapped)
        return resp["Plaintext"]

    def generate_data_key(self) -> tuple[bytes, bytes]:
        resp = self._kms().generate_data_key(KeyId=self._key_arn, KeySpec="AES_256")
        return resp["Plaintext"], resp["CiphertextBlob"]


class AzureKeyVaultProvider(KmsProvider):  # REQ-694
    """Azure Key Vault client-owned key. Wraps ``unwrapKey`` / ``wrapKey``."""

    def __init__(self, key_id: str, *, client: "Any" = None) -> None:
        if not key_id:
            raise ValueError("AzureKeyVaultProvider requires a key_id (kms_key_arn)")
        self._key_id = key_id
        self._client = client  # CryptographyClient handle — never raw key bytes

    def _crypto(self):
        if self._client is None:
            from azure.identity import DefaultAzureCredential  # noqa: PLC0415  # pyright: ignore[reportMissingImports]
            from azure.keyvault.keys.crypto import CryptographyClient  # noqa: PLC0415  # pyright: ignore[reportMissingImports]

            self._client = CryptographyClient(self._key_id, DefaultAzureCredential())
        return self._client

    def unwrap_dek(self, wrapped: bytes) -> bytes:
        from azure.keyvault.keys.crypto import KeyWrapAlgorithm  # noqa: PLC0415  # pyright: ignore[reportMissingImports]

        return self._crypto().unwrap_key(KeyWrapAlgorithm.rsa_oaep_256, wrapped).key

    def generate_data_key(self) -> tuple[bytes, bytes]:
        import os  # noqa: PLC0415

        from azure.keyvault.keys.crypto import KeyWrapAlgorithm  # noqa: PLC0415  # pyright: ignore[reportMissingImports]

        dek = os.urandom(_DEK_LEN)
        wrapped = self._crypto().wrap_key(KeyWrapAlgorithm.rsa_oaep_256, dek).encrypted_key
        return dek, wrapped


class GcpKmsProvider(KmsProvider):  # REQ-694
    """GCP KMS client-owned key. Wraps ``decrypt`` / ``encrypt`` on a CryptoKey."""

    def __init__(self, key_name: str, *, client: "Any" = None) -> None:
        if not key_name:
            raise ValueError("GcpKmsProvider requires a key_name (kms_key_arn)")
        self._key_name = key_name
        self._client = client  # KeyManagementServiceClient handle — never raw key bytes

    def _kms(self):
        if self._client is None:
            from google.cloud import kms  # noqa: PLC0415  # pyright: ignore[reportMissingImports,reportAttributeAccessIssue]

            self._client = kms.KeyManagementServiceClient()
        return self._client

    def unwrap_dek(self, wrapped: bytes) -> bytes:
        resp = self._kms().decrypt(request={"name": self._key_name, "ciphertext": wrapped})
        return resp.plaintext

    def generate_data_key(self) -> tuple[bytes, bytes]:
        import os  # noqa: PLC0415

        dek = os.urandom(_DEK_LEN)
        resp = self._kms().encrypt(request={"name": self._key_name, "plaintext": dek})
        return dek, resp.ciphertext


def build_kms_provider(
    provider: str,
    key_arn: str,
    *,
    region: str | None = None,
    client: "Any" = None,
) -> KmsProvider:
    """Build a KMS provider from a connection param. Unknown providers fail closed."""
    name = (provider or "").lower()
    if name in ("aws", "aws_kms", "kms"):
        return AwsKmsProvider(key_arn, region=region, client=client)
    if name in ("azure", "azure_key_vault", "keyvault"):
        return AzureKeyVaultProvider(key_arn, client=client)
    if name in ("gcp", "gcp_kms", "google", "google_kms"):
        return GcpKmsProvider(key_arn, client=client)
    raise ValueError(
        f"Unknown kms_provider {provider!r}. Supported: aws, azure, gcp (client-owned CMK)."
    )


# -- DEK cache (REQ-691) ----------------------------------------------------------------------


class _DekCache:
    """Bounded TTL cache: wrapped-DEK digest → (unwrapped DEK, expiry)."""

    def __init__(self, ttl_seconds: float) -> None:
        self._ttl = ttl_seconds
        self._entries: dict[bytes, tuple[bytes, float]] = {}

    def get(self, wrapped: bytes, now: float) -> bytes | None:
        key = hashlib.sha256(wrapped).digest()
        hit = self._entries.get(key)
        if hit is None:
            return None
        dek, expiry = hit
        if now >= expiry:
            del self._entries[key]
            return None
        return dek

    def put(self, wrapped: bytes, dek: bytes, now: float) -> None:
        for k in [k for k, (_, exp) in self._entries.items() if exp <= now]:
            del self._entries[k]
        self._entries[hashlib.sha256(wrapped).digest()] = (dek, now + self._ttl)


# -- Client encryption service (REQ-691) ------------------------------------------------------


class ClientEncryptionService:
    """Envelope decrypt/encrypt over a client-owned KMS provider.

    ``decrypt`` parses a provisa envelope, unwraps its DEK (cache first, then KMS),
    and AES-256-GCM decrypts. Any failure raises ``DecryptionError`` — never a
    silent passthrough.
    """

    def __init__(self, provider: KmsProvider, *, dek_cache_ttl: float = 300.0) -> None:
        self._provider = provider
        self._cache = _DekCache(dek_cache_ttl)

    def _unwrap(self, wrapped: bytes, now: float) -> bytes:
        dek = self._cache.get(wrapped, now)
        if dek is not None:
            return dek
        try:
            dek = self._provider.unwrap_dek(wrapped)
        except Exception as exc:  # noqa: BLE001 - fail loud: any KMS/grant failure is a decrypt failure
            raise DecryptionError(
                f"KMS unwrap failed (grant revoked or key unavailable): {exc}"
            ) from exc
        if len(dek) != _DEK_LEN:
            raise DecryptionError(f"unwrapped DEK is {len(dek)} bytes, expected {_DEK_LEN}")
        self._cache.put(wrapped, dek, now)
        return dek

    def decrypt(self, blob: bytes) -> bytes:
        wrapped, iv, ciphertext = split_envelope(blob)
        dek = self._unwrap(wrapped, time.monotonic())
        try:
            return AESGCM(dek).decrypt(iv, ciphertext, None)
        except InvalidTag as exc:
            raise DecryptionError("AES-GCM authentication failed (tampered or wrong key)") from exc

    def encrypt(self, plaintext: bytes) -> bytes:
        """Client-side encrypt (round-trip / write path). Fresh DEK per payload."""
        import os  # noqa: PLC0415

        dek, wrapped = self._provider.generate_data_key()
        iv = os.urandom(_IV_LEN)
        ciphertext = AESGCM(dek).encrypt(iv, plaintext, None)
        return _HEADER.pack(_MAGIC, _VERSION, len(wrapped)) + wrapped + iv + ciphertext

    def decrypt_field(self, value: Any) -> Any:
        """Decrypt one column/field value flagged encrypted.

        Accepts a base64 str or raw bytes envelope; returns the decrypted UTF-8
        string (JSON payloads round-trip as text). ``None`` passes through (NULL).
        """
        if value is None:
            return None
        if isinstance(value, str):
            try:
                blob = base64.b64decode(value, validate=True)
            except (ValueError, base64.binascii.Error) as exc:  # type: ignore[attr-defined]
                raise DecryptionError("encrypted field is not valid base64") from exc
        elif isinstance(value, (bytes, bytearray)):
            blob = bytes(value)
        else:
            raise DecryptionError(
                f"encrypted field has non-decryptable type {type(value).__name__}"
            )
        return self.decrypt(blob).decode("utf-8")


def build_client_encryption(
    *,
    kms_provider: str | None,
    kms_key_arn: str | None,
    dek_cache_ttl: float = 300.0,
    region: str | None = None,
    _client: "Any" = None,
) -> ClientEncryptionService | None:
    """Build a client EncryptionService from connection params, or None when unconfigured.

    Both ``kms_provider`` and ``kms_key_arn`` are required together; supplying one
    without the other is an error (fail closed — never a half-configured client).
    """
    if not kms_provider and not kms_key_arn:
        return None
    if not kms_provider or not kms_key_arn:
        raise ValueError("client-side decryption needs both kms_provider and kms_key_arn")
    provider = build_kms_provider(kms_provider, kms_key_arn, region=region, client=_client)
    return ClientEncryptionService(provider, dek_cache_ttl=dek_cache_ttl)


def decrypt_rows(
    rows: list[dict],
    encrypted_columns: list[str] | set[str],
    svc: ClientEncryptionService,
) -> list[dict]:
    """Decrypt the flagged columns of each dict row in place (REQ-691).

    ``encrypted_columns`` is the server-supplied column-metadata flag set. Rows not
    containing a flagged column are left untouched; a present flagged value that
    fails to decrypt raises ``DecryptionError`` (loud).
    """
    cols = set(encrypted_columns)
    if not cols:
        return rows
    for row in rows:
        for col in cols:
            if col in row:
                row[col] = svc.decrypt_field(row[col])
    return rows
