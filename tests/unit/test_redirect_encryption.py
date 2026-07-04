# Copyright (c) 2026 Kenneth Stott
# Canary: 4d7a1c9e-2b60-4f83-9c11-7ae5f0d3b8a2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-687: bulk-result client-side envelope encryption round-trip.

Proves the role-bound grant contract end-to-end without S3: the redirect body is
encrypted to a self-describing blob (the S3 object is ciphertext), the response
metadata carries only a master-sealed grant, and a payload is readable ONLY after
the server opens the grant (verifying the caller's role) and hands back the DEK.
The bucket/URL alone, and any role other than the creator, cannot decrypt it.
"""

from __future__ import annotations

import base64
import json

import pytest
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from provisa.encryption import (
    EnvelopeEncryption,
    configure_encryption,
    encryption_service,
    reset_encryption,
    split_envelope,
)
from provisa.encryption.providers import LocalKeychain
from provisa.executor.redirect import _encrypt_payload


@pytest.fixture(autouse=True)
def _reset():
    reset_encryption()
    yield
    reset_encryption()


def _install_envelope(monkeypatch):
    monkeypatch.setenv("PROVISA_ENCRYPTION_KEY", base64.b64encode(bytes(range(32))).decode())
    svc = configure_encryption("local")
    assert isinstance(svc, EnvelopeEncryption)
    return svc


def _open_grant(meta):
    """Mirror the server-side grant open the /data/redirect/unwrap endpoint performs."""
    svc = encryption_service()
    return json.loads(svc.decrypt(base64.b64decode(meta["grant"])))


_PLAINTEXT = b'{"row": 1, "secret": "super-secret-value"}\n{"row": 2}'


def test_object_is_ciphertext_and_metadata_carries_only_a_grant(monkeypatch):
    _install_envelope(monkeypatch)
    blob, meta = _encrypt_payload(_PLAINTEXT, "analyst")
    # The S3 object is ciphertext — the plaintext secret never appears.
    assert b"super-secret-value" not in blob
    assert meta["scheme"] == "provisa-envelope-v1"
    assert meta["alg"] == "AES-256-GCM"
    assert meta["unwrap_endpoint"] == "/data/redirect/unwrap"
    # The grant is opaque — the DEK/role never appear in the clear in the metadata.
    grant_bytes = base64.b64decode(meta["grant"])
    assert b"analyst" not in grant_bytes
    assert "dek" not in meta and "wrapped_dek" not in meta


def test_client_decrypts_only_after_server_opens_grant(monkeypatch):
    _install_envelope(monkeypatch)
    blob, meta = _encrypt_payload(_PLAINTEXT, "analyst")

    # Server opens the grant (master key), scoped to the creating role.
    payload = _open_grant(meta)
    assert payload["role"] == "analyst"
    dek = base64.b64decode(payload["dek"])

    # Client side: has the ciphertext blob + the returned DEK → decrypts locally.
    _wrapped, iv, ciphertext = split_envelope(blob)
    assert AESGCM(dek).decrypt(iv, ciphertext, None) == _PLAINTEXT


def test_grant_is_bound_to_creating_role(monkeypatch):
    _install_envelope(monkeypatch)
    _blob, meta = _encrypt_payload(_PLAINTEXT, "analyst")
    # The endpoint compares this to the caller; a different role is rejected there.
    assert _open_grant(meta)["role"] == "analyst"


def test_encrypt_requires_envelope_provider():
    # NullEncryption (default) cannot protect the payload — fail closed.
    with pytest.raises(RuntimeError, match="envelope encryption provider"):
        _encrypt_payload(_PLAINTEXT, "analyst")


def test_foreign_master_key_cannot_open_grant(monkeypatch):
    _install_envelope(monkeypatch)
    _blob, meta = _encrypt_payload(_PLAINTEXT, "analyst")
    # A different master key (e.g. a stolen bucket + attacker's own provider) cannot open it.
    other = EnvelopeEncryption(LocalKeychain(bytes(range(32, 64))))
    with pytest.raises(Exception):
        other.decrypt(base64.b64decode(meta["grant"]))
