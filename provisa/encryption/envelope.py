# Copyright (c) 2026 Kenneth Stott
# Canary: 93cf104e-e2f3-4a1f-9921-689071587ffe
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Envelope encryption (REQ-685).

Each payload gets a fresh Data Encryption Key (DEK). The DEK encrypts the payload
with AES-256-GCM; the DEK itself is wrapped by the org master key via a
``MasterKeyProvider`` (REQ-684). Only the wrapped DEK is stored alongside the
ciphertext, so the master key never touches the payload and a compromised stored
blob cannot be decrypted without the provider.

Storage format (self-describing so any provider's wrapped-DEK length works):

    magic(1) | version(1) | len(wrapped_dek):u32-be | wrapped_dek | iv(12) | ciphertext+tag

Unwrapped DEKs are cached in-process with a short TTL to bound master-key /
KMS round-trips on repeated reads of the same blob.
"""

from __future__ import annotations

import hashlib
import os
import struct
import time

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from provisa.encryption.providers import MasterKeyProvider
from provisa.encryption.service import EncryptionService

_MAGIC = 0xE1  # marks a provisa envelope blob
_VERSION = 1
_HEADER = struct.Struct(">BBI")  # magic, version, wrapped_dek_len
_IV_LEN = 12
_DEK_LEN = 32  # AES-256


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
        # Opportunistically evict expired entries to bound growth.
        for k in [k for k, (_, exp) in self._entries.items() if exp <= now]:
            del self._entries[k]
        self._entries[hashlib.sha256(wrapped).digest()] = (dek, now + self._ttl)


class EnvelopeEncryption(EncryptionService):  # REQ-685
    """AES-256-GCM envelope encryption over a pluggable master-key provider."""

    def __init__(self, provider: MasterKeyProvider, *, dek_cache_ttl: float = 300.0) -> None:
        self._provider = provider
        self._cache = _DekCache(dek_cache_ttl)

    def encrypt(self, plaintext: bytes) -> bytes:
        dek = os.urandom(_DEK_LEN)
        iv = os.urandom(_IV_LEN)
        ciphertext = AESGCM(dek).encrypt(iv, plaintext, None)
        wrapped = self._provider.wrap_dek(dek)
        return _HEADER.pack(_MAGIC, _VERSION, len(wrapped)) + wrapped + iv + ciphertext

    def decrypt(self, blob: bytes) -> bytes:
        magic, version, wlen = _HEADER.unpack_from(blob)
        if magic != _MAGIC or version != _VERSION:
            raise ValueError("not a provisa envelope blob (bad magic/version)")
        off = _HEADER.size
        wrapped = blob[off : off + wlen]
        off += wlen
        iv = blob[off : off + _IV_LEN]
        ciphertext = blob[off + _IV_LEN :]
        now = time.monotonic()
        dek = self._cache.get(wrapped, now)
        if dek is None:
            dek = self._provider.unwrap_dek(wrapped)
            self._cache.put(wrapped, dek, now)
        return AESGCM(dek).decrypt(iv, ciphertext, None)
