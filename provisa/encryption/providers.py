# Copyright (c) 2026 Kenneth Stott
# Canary: becef0b4-6356-4366-ba72-ae7d8064de7f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Master-key providers for envelope encryption (REQ-684, REQ-685).

A ``MasterKeyProvider`` wraps/unwraps per-payload Data Encryption Keys (DEKs) with
an org master key. Envelope encryption (``envelope.py``) generates a fresh DEK per
payload and asks the provider to wrap it; only the wrapped DEK is stored. The
provider is where the trust boundary lives — LocalKeychain keeps the master key on
the machine (OS keychain / configured secret); the cloud KMS variants (REQ-690-694)
keep it in AWS/Azure/GCP and never expose it.
"""

from __future__ import annotations

import base64
import os
from abc import ABC, abstractmethod

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

_MASTER_KEY_ENV = "PROVISA_ENCRYPTION_KEY"  # base64-encoded 32-byte master key
_KEYCHAIN_SERVICE = "provisa-encryption"
_MASTER_KEY_BYTES = 32  # AES-256
_NONCE_LEN = 12  # AES-GCM nonce


class MasterKeyProvider(ABC):  # REQ-684
    """Wrap/unwrap DEKs with the org master key."""

    @abstractmethod
    def wrap_dek(self, dek: bytes) -> bytes: ...

    @abstractmethod
    def unwrap_dek(self, wrapped: bytes) -> bytes: ...


class NullMasterKey(MasterKeyProvider):  # REQ-684
    """Passthrough master key — the DEK is stored unwrapped. Dev/test only."""

    def wrap_dek(self, dek: bytes) -> bytes:
        return dek

    def unwrap_dek(self, wrapped: bytes) -> bytes:
        return wrapped


class LocalKeychain(MasterKeyProvider):  # REQ-684
    """AES-256-GCM DEK wrapping with a 32-byte master key held on this machine.

    The master key is retrieved (in order) from the OS keychain via ``keyring`` if
    available, else the ``PROVISA_ENCRYPTION_KEY`` env var (base64). It never leaves
    the process. A wrapped DEK is ``nonce(12) || AESGCM(dek)`` under the master key.
    """

    def __init__(self, master_key: bytes) -> None:
        if len(master_key) != _MASTER_KEY_BYTES:
            raise ValueError(
                f"LocalKeychain master key must be {_MASTER_KEY_BYTES} bytes (AES-256), "
                f"got {len(master_key)}"
            )
        self._aes = AESGCM(master_key)

    def wrap_dek(self, dek: bytes) -> bytes:
        nonce = os.urandom(_NONCE_LEN)
        return nonce + self._aes.encrypt(nonce, dek, None)

    def unwrap_dek(self, wrapped: bytes) -> bytes:
        nonce, ct = wrapped[:_NONCE_LEN], wrapped[_NONCE_LEN:]
        return self._aes.decrypt(nonce, ct, None)

    @classmethod
    def from_config(cls, key_id: str | None = None) -> "LocalKeychain":
        """Load the master key from the OS keychain (keyring) or the env fallback."""
        raw = _load_from_keychain(key_id) or os.environ.get(_MASTER_KEY_ENV)
        if not raw:
            raise RuntimeError(
                "LocalKeychain: no master key found in the OS keychain or "
                f"{_MASTER_KEY_ENV}. Provision a 32-byte base64 key first."
            )
        key = base64.b64decode(raw)
        return cls(key)


def _load_from_keychain(key_id: str | None) -> str | None:
    """Return the base64 master key from the OS keychain, or None when unavailable."""
    try:
        import keyring  # noqa: PLC0415
    except ImportError:
        return None
    return keyring.get_password(_KEYCHAIN_SERVICE, key_id or "master")


class AwsKmsMasterKey(MasterKeyProvider):  # REQ-690
    """DEK wrapping via AWS KMS (or any KMS-compatible endpoint).

    ``KMS.Encrypt``/``Decrypt`` wrap and unwrap the DEK under a CMK identified by
    ``key_arn`` — the master key never leaves KMS. ``endpoint_url`` targets a
    KMS-compatible service (LocalStack, a private/enterprise KMS gateway); leave
    blank for real AWS. Auth is standard boto3 credential resolution.
    """

    def __init__(
        self, key_arn: str, *, region: str | None = None, endpoint_url: str | None = None
    ) -> None:
        if not key_arn:
            raise ValueError("aws_kms provider requires a key_arn")
        import boto3  # noqa: PLC0415

        self._key_arn = key_arn
        self._kms = boto3.client(
            "kms", region_name=region or None, endpoint_url=endpoint_url or None
        )

    def wrap_dek(self, dek: bytes) -> bytes:
        return self._kms.encrypt(KeyId=self._key_arn, Plaintext=dek)["CiphertextBlob"]

    def unwrap_dek(self, wrapped: bytes) -> bytes:
        # KeyId pins decryption to the expected CMK (defence-in-depth vs. ciphertext swapping).
        return self._kms.decrypt(CiphertextBlob=wrapped, KeyId=self._key_arn)["Plaintext"]


class HashiCorpVaultMasterKey(MasterKeyProvider):  # REQ-691
    """DEK wrapping via HashiCorp Vault's Transit secrets engine.

    ``transit/encrypt/<key>`` and ``transit/decrypt/<key>`` wrap/unwrap the DEK
    without exposing the key. ``url`` is the Vault address (VAULT_ADDR) — inherently
    an enterprise-custom endpoint — and ``token`` the auth token (falls back to
    VAULT_TOKEN). ``mount`` selects the Transit mount; ``namespace`` targets a Vault
    Enterprise namespace.
    """

    def __init__(
        self,
        key_name: str,
        *,
        url: str | None = None,
        token: str | None = None,
        mount: str = "transit",
        namespace: str | None = None,
    ) -> None:
        if not key_name:
            raise ValueError("hashicorp_vault provider requires a key_name")
        import hvac  # noqa: PLC0415

        self._key = key_name
        self._mount = mount or "transit"
        self._client = hvac.Client(
            url=url or None, token=token or None, namespace=namespace or None
        )

    def wrap_dek(self, dek: bytes) -> bytes:
        b64 = base64.b64encode(dek).decode("ascii")
        resp = self._client.secrets.transit.encrypt_data(
            name=self._key, plaintext=b64, mount_point=self._mount
        )
        return resp["data"]["ciphertext"].encode("ascii")  # vault:v1:… token

    def unwrap_dek(self, wrapped: bytes) -> bytes:
        resp = self._client.secrets.transit.decrypt_data(
            name=self._key, ciphertext=wrapped.decode("ascii"), mount_point=self._mount
        )
        return base64.b64decode(resp["data"]["plaintext"])


class AzureKeyVaultMasterKey(MasterKeyProvider):  # REQ-692
    """DEK wrapping via Azure Key Vault (RSA-OAEP-256 key-wrap).

    The master key stays in Key Vault; ``wrap``/``unwrap`` are remote operations.
    ``vault_url`` is the vault endpoint and ``key_name`` the key. Auth uses
    ``DefaultAzureCredential`` (``az login`` / managed identity / env).
    """

    def __init__(self, vault_url: str, key_name: str) -> None:
        if not vault_url or not key_name:
            raise ValueError("azure_key_vault provider requires vault_url and key_name")
        from azure.identity import DefaultAzureCredential  # noqa: PLC0415
        from azure.keyvault.keys import KeyClient  # noqa: PLC0415
        from azure.keyvault.keys.crypto import (  # noqa: PLC0415
            CryptographyClient,
            KeyWrapAlgorithm,
        )

        cred = DefaultAzureCredential()
        key = KeyClient(vault_url=vault_url, credential=cred).get_key(key_name)
        self._crypto = CryptographyClient(key, credential=cred)
        self._alg = KeyWrapAlgorithm.rsa_oaep_256

    def wrap_dek(self, dek: bytes) -> bytes:
        return self._crypto.wrap_key(self._alg, dek).encrypted_key

    def unwrap_dek(self, wrapped: bytes) -> bytes:
        return self._crypto.unwrap_key(self._alg, wrapped).key


class GcpKmsMasterKey(MasterKeyProvider):  # REQ-693
    """DEK wrapping via Google Cloud KMS (symmetric encrypt/decrypt).

    ``key_name`` is the full resource path
    (``projects/P/locations/L/keyRings/R/cryptoKeys/K``); the master key never
    leaves KMS. Auth uses Application Default Credentials
    (``GOOGLE_APPLICATION_CREDENTIALS`` / workload identity).
    """

    def __init__(self, key_name: str) -> None:
        if not key_name:
            raise ValueError("gcp_kms provider requires a key_name (full cryptoKey resource path)")
        from google.cloud import kms  # noqa: PLC0415

        self._key_name = key_name
        self._client = kms.KeyManagementServiceClient()

    def wrap_dek(self, dek: bytes) -> bytes:
        return self._client.encrypt(request={"name": self._key_name, "plaintext": dek}).ciphertext

    def unwrap_dek(self, wrapped: bytes) -> bytes:
        return self._client.decrypt(
            request={"name": self._key_name, "ciphertext": wrapped}
        ).plaintext


# -- key management (REQ-918): provisioning + presence for the admin UI ----------------------


def master_key_present(key_id: str | None = None) -> bool:
    """Whether a LocalKeychain master key is available (OS keychain or the env fallback)."""
    return bool(_load_from_keychain(key_id) or os.environ.get(_MASTER_KEY_ENV))


def generate_master_key_b64() -> str:
    """Generate a fresh 32-byte (AES-256) master key, base64-encoded."""
    return base64.b64encode(os.urandom(_MASTER_KEY_BYTES)).decode("ascii")


def store_master_key(key_b64: str, key_id: str | None = None) -> bool:
    """Store a base64 master key in the OS keychain under ``key_id``. Returns True on success,
    False when no OS keychain is available (the caller then supplies it via the env var)."""
    if len(base64.b64decode(key_b64)) != _MASTER_KEY_BYTES:
        raise ValueError(f"master key must decode to {_MASTER_KEY_BYTES} bytes (AES-256)")
    try:
        import keyring  # noqa: PLC0415
    except ImportError:
        return False
    keyring.set_password(_KEYCHAIN_SERVICE, key_id or "master", key_b64)
    return True
