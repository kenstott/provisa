# Copyright (c) 2026 Kenneth Stott
# Canary: 3f7b2e91-5a4c-4d68-9e02-1c8a6f3b7d95
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1802: provisa.core.secrets_store's auto-mint fallback (``_cipher``) must succeed on a host
with no OS keychain backend at all — the exact failure a user hit trying to save an org secret
("No encryption master key, and this host has no keychain to hold one...") when ``keyring`` was
not installed. It now falls through to the file keystore instead of raising.
"""

from __future__ import annotations

import sys

import pytest

from provisa.core.secrets_store import _cipher
from provisa.encryption.runtime import reset_encryption
from provisa.encryption.service import NullEncryption


@pytest.fixture(autouse=True)
def _isolated(tmp_path, monkeypatch):
    monkeypatch.setenv("PROVISA_DATA_DIR", str(tmp_path))
    monkeypatch.setitem(sys.modules, "keyring", None)  # simulate: no OS keychain package at all
    reset_encryption()
    yield
    reset_encryption()


def test_auto_mints_a_key_via_the_file_keystore_when_no_deployment_service_is_configured():
    # Before the fix, this raised RuntimeError("No encryption master key, and this host has
    # no keychain to hold one...") whenever no keyring backend existed.
    service = _cipher()
    assert not isinstance(service, NullEncryption)
    plaintext = b"KGAT_some_kaggle_token"
    assert service.decrypt(service.encrypt(plaintext)) == plaintext


def test_second_call_reuses_the_same_minted_key():
    service_a = _cipher()
    blob = service_a.encrypt(b"a secret")
    service_b = _cipher()
    assert service_b.decrypt(blob) == b"a secret"


def test_uses_the_configured_deployment_service_when_one_exists(monkeypatch):
    from provisa.encryption import configure_encryption

    monkeypatch.setattr(
        "provisa.encryption.factory.build_encryption_service",
        lambda *a, **k: _EnvelopeStub(),
    )
    configure_encryption("local", key_id="whatever")
    service = _cipher()
    assert isinstance(service, _EnvelopeStub)


class _EnvelopeStub:
    def encrypt(self, plaintext: bytes) -> bytes:
        return plaintext

    def decrypt(self, blob: bytes) -> bytes:
        return blob
