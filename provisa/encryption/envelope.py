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

    v1: magic(1) | version(1) | len(wrapped_dek):u32-be | wrapped_dek | iv(12) | ciphertext+tag
    v2: magic(1) | version(2) | len(key_id):u8 | key_id | len(wrapped_dek):u32-be | wrapped_dek
        | iv(12) | ciphertext+tag

Version 2 names the KEY that wrapped the DEK (REQ-1574). An org holds a ring of keys rather than
one key, so that rotating is immediate and is not a re-encryption of everything already stored: the
new key wraps new writes while a blob written under a retired key still says which key to ask for.
A v1 blob names no key because there was only ever one -- the deployment's.

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
_VERSION_KEYED = 2  # REQ-1574: the blob names the ring key that wrapped its DEK
_HEADER = struct.Struct(">BBI")  # magic, version, wrapped_dek_len
_KEYED_PREFIX = struct.Struct(">BBB")  # magic, version, key_id_len
_LEN = struct.Struct(">I")  # wrapped_dek_len
_MAX_KEY_ID = 255  # one byte of length
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
        key_id, wrapped, iv, ciphertext = _split(blob)
        if key_id is not None:
            # REQ-1574: a keyed blob was written by a RING, and this service holds one provider that
            # is not necessarily the named entry. Opening it with the only key at hand would be a
            # wrong answer dressed as a right one.
            raise ValueError(
                f"envelope blob names ring key {key_id!r}; a single-key service cannot open it"
            )
        now = time.monotonic()
        dek = self._cache.get(wrapped, now)
        if dek is None:
            dek = self._provider.unwrap_dek(wrapped)
            self._cache.put(wrapped, dek, now)
        return AESGCM(dek).decrypt(iv, ciphertext, None)

    def unwrap(self, wrapped: bytes) -> bytes:
        """Unwrap a wrapped DEK to its raw bytes via the master-key provider (REQ-687).

        Backs the authenticated redirect-unwrap call: a client that holds a ciphertext
        object + wrapped DEK still cannot read it without the master key, which never
        leaves this provider.
        """
        return self._provider.unwrap_dek(wrapped)


def split_envelope(blob: bytes) -> tuple[bytes, bytes, bytes]:
    """Split a provisa envelope blob into ``(wrapped_dek, iv, ciphertext+tag)``.

    The blob is self-describing (see module docstring); this is the framing a client
    parses to decrypt a redirect payload once it has unwrapped the DEK (REQ-687).
    """
    return _split(blob)[1:]


def envelope_key_id(blob: bytes) -> str | None:
    """The ring key that wrapped this blob's DEK, or ``None`` for a v1 blob (REQ-1574).

    ``None`` is not "unknown": a v1 blob was written when a deployment had exactly one key, so the
    absence of a name IS the name. A ring resolves it to the entry it was adopted under.
    """
    return _split(blob)[0]


def _split(blob: bytes) -> tuple[str | None, bytes, bytes, bytes]:
    """``(key_id, wrapped_dek, iv, ciphertext+tag)`` for either envelope version."""
    magic, version = blob[0], blob[1]
    if magic != _MAGIC or version not in (_VERSION, _VERSION_KEYED):
        raise ValueError("not a provisa envelope blob (bad magic/version)")
    if version == _VERSION:
        _, _, wlen = _HEADER.unpack_from(blob)
        off = _HEADER.size
        key_id = None
    else:
        _, _, klen = _KEYED_PREFIX.unpack_from(blob)
        off = _KEYED_PREFIX.size
        key_id = blob[off : off + klen].decode("ascii")
        off += klen
        (wlen,) = _LEN.unpack_from(blob, off)
        off += _LEN.size
    wrapped = blob[off : off + wlen]
    off += wlen
    iv = blob[off : off + _IV_LEN]
    ciphertext = blob[off + _IV_LEN :]
    return key_id, wrapped, iv, ciphertext


class RingEnvelopeEncryption(EncryptionService):  # REQ-1574
    """Envelope encryption over a RING of master keys, one of them active.

    The service an org gets once it has set a key of its own. ``encrypt`` wraps under the active
    entry and stamps its id into the blob; ``decrypt`` reads the id back and asks that entry, so a
    key that has been rotated away still decrypts what it wrote. A blob naming an entry this ring
    does not hold RAISES -- it is never opened with whichever key happens to be active, which would
    be a wrong answer rather than a missing one.

    ``v1_key_id`` names the ring entry that adopts unnamed (v1) blobs: what the org's data was
    written under before it held a ring. ``None`` means the ring refuses them, which is correct for
    a ring whose first key predates any write.
    """

    def __init__(
        self,
        active_key_id: str,
        providers: dict[str, MasterKeyProvider],
        *,
        v1_key_id: str | None = None,
        dek_cache_ttl: float = 300.0,
    ) -> None:
        if active_key_id not in providers:
            raise ValueError(f"active key {active_key_id!r} is not in the ring")
        if len(active_key_id.encode("ascii")) > _MAX_KEY_ID:
            raise ValueError(f"key id {active_key_id!r} is longer than {_MAX_KEY_ID} bytes")
        self._active = active_key_id
        self._providers = providers
        self._v1_key_id = v1_key_id
        self._cache = _DekCache(dek_cache_ttl)

    @property
    def active_key_id(self) -> str:
        return self._active

    def encrypt(self, plaintext: bytes) -> bytes:
        dek = os.urandom(_DEK_LEN)
        iv = os.urandom(_IV_LEN)
        ciphertext = AESGCM(dek).encrypt(iv, plaintext, None)
        wrapped = self._providers[self._active].wrap_dek(dek)
        key_id = self._active.encode("ascii")
        return (
            _KEYED_PREFIX.pack(_MAGIC, _VERSION_KEYED, len(key_id))
            + key_id
            + _LEN.pack(len(wrapped))
            + wrapped
            + iv
            + ciphertext
        )

    def decrypt(self, blob: bytes) -> bytes:
        key_id, wrapped, iv, ciphertext = _split(blob)
        named = key_id if key_id is not None else self._v1_key_id
        if named is None:
            raise ValueError(
                "envelope blob names no key and this ring adopts no unnamed blobs (REQ-1574)"
            )
        provider = self._providers.get(named)
        if provider is None:
            raise ValueError(f"envelope blob names key {named!r}, which this ring does not hold")
        now = time.monotonic()
        cache_key = named.encode("ascii") + wrapped
        dek = self._cache.get(cache_key, now)
        if dek is None:
            dek = provider.unwrap_dek(wrapped)
            self._cache.put(cache_key, dek, now)
        return AESGCM(dek).decrypt(iv, ciphertext, None)

    def unwrap(self, wrapped: bytes) -> bytes:
        """Unwrap a DEK wrapped by the ACTIVE entry (REQ-687 redirect unwrap)."""
        return self._providers[self._active].unwrap_dek(wrapped)
