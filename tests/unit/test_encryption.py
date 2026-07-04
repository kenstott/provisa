# Copyright (c) 2026 Kenneth Stott
# Canary: 7b9d1f3a-5c2e-4048-8a6b-1d3f5a7c9e0b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for the encryption core (REQ-684, REQ-685).

Pure crypto — no cloud, no infra. Covers the service abstraction, NullEncryption
passthrough, LocalKeychain AES-256-GCM envelope encryption, DEK-per-payload,
tamper/wrong-key rejection, the DEK cache, and the fail-closed factory.
"""

from __future__ import annotations

import base64
import os

import pytest

from provisa.encryption import (
    EnvelopeEncryption,
    NullEncryption,
    build_encryption_service,
)
from provisa.encryption.providers import LocalKeychain, NullMasterKey


def _key32() -> bytes:
    return os.urandom(32)


# --- NullEncryption (REQ-684) --------------------------------------------------


def test_null_encryption_is_passthrough():
    n = NullEncryption()
    assert n.encrypt(b"secret") == b"secret"
    assert n.decrypt(b"secret") == b"secret"


def test_null_roundtrip_arbitrary_bytes():
    n = NullEncryption()
    for p in (b"", b"\x00\x01\x02", os.urandom(1000)):
        assert n.decrypt(n.encrypt(p)) == p


# --- LocalKeychain provider (REQ-684) ------------------------------------------


def test_local_keychain_requires_32_byte_key():
    with pytest.raises(ValueError, match="32 bytes"):
        LocalKeychain(b"short")


def test_local_keychain_wrap_unwrap_roundtrip():
    kc = LocalKeychain(_key32())
    dek = os.urandom(32)
    wrapped = kc.wrap_dek(dek)
    assert wrapped != dek
    assert kc.unwrap_dek(wrapped) == dek


def test_local_keychain_wrap_is_nondeterministic():
    kc = LocalKeychain(_key32())
    dek = os.urandom(32)
    assert kc.wrap_dek(dek) != kc.wrap_dek(dek)  # random nonce per wrap


# --- Envelope encryption (REQ-685) ---------------------------------------------


def _svc() -> EnvelopeEncryption:
    return EnvelopeEncryption(LocalKeychain(_key32()))


def test_envelope_roundtrip():
    svc = _svc()
    for p in (b"", b"api-key-12345", os.urandom(5000)):
        assert svc.decrypt(svc.encrypt(p)) == p


def test_envelope_ciphertext_differs_from_plaintext():
    svc = _svc()
    assert svc.encrypt(b"credentials") != b"credentials"


def test_fresh_dek_per_payload():
    # Two encryptions of the same plaintext must differ (unique DEK + IV).
    svc = _svc()
    assert svc.encrypt(b"same") != svc.encrypt(b"same")


def test_tamper_is_rejected():
    svc = _svc()
    blob = bytearray(svc.encrypt(b"payload"))
    blob[-1] ^= 0x01
    with pytest.raises(Exception):
        svc.decrypt(bytes(blob))


def test_wrong_master_key_cannot_decrypt():
    a = EnvelopeEncryption(LocalKeychain(_key32()))
    b = EnvelopeEncryption(LocalKeychain(_key32()))
    blob = a.encrypt(b"secret")
    with pytest.raises(Exception):
        b.decrypt(blob)


def test_non_envelope_blob_rejected():
    svc = _svc()
    with pytest.raises(ValueError, match="magic"):
        svc.decrypt(b"not-an-envelope-blob")


def test_null_master_key_envelope_roundtrips():
    # Envelope path with an unwrapped DEK still round-trips (still AES-GCM on data).
    svc = EnvelopeEncryption(NullMasterKey())
    assert svc.decrypt(svc.encrypt(b"x")) == b"x"


def test_dek_cache_returns_same_plaintext_on_repeat_decrypt():
    svc = _svc()
    blob = svc.encrypt(b"cached")
    assert svc.decrypt(blob) == b"cached"
    assert svc.decrypt(blob) == b"cached"  # second read hits the DEK cache


# --- Factory (REQ-684) ----------------------------------------------------------


def test_factory_null_default():
    assert isinstance(build_encryption_service(None), NullEncryption)
    assert isinstance(build_encryption_service("null"), NullEncryption)


def test_factory_local_from_env(monkeypatch):
    monkeypatch.setenv("PROVISA_ENCRYPTION_KEY", base64.b64encode(_key32()).decode())
    svc = build_encryption_service("local")
    assert isinstance(svc, EnvelopeEncryption)
    assert svc.decrypt(svc.encrypt(b"x")) == b"x"


def test_factory_local_missing_key_raises(monkeypatch):
    monkeypatch.delenv("PROVISA_ENCRYPTION_KEY", raising=False)
    with pytest.raises(RuntimeError, match="master key"):
        build_encryption_service("local")


def test_factory_unknown_provider_fails_closed():
    with pytest.raises(ValueError, match="Unknown encryption provider"):
        build_encryption_service("mystery-kms")
