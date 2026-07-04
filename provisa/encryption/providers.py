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
