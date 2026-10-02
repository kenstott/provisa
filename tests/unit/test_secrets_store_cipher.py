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

import asyncio
import base64
import fcntl
import os
import subprocess
import sys
import time
from contextlib import asynccontextmanager
from pathlib import Path

import pytest

from provisa.core.secrets_store import ORG_OWNER, VaultKeyError, _cipher, _decrypted
from provisa.encryption.providers import (
    _file_keystore_path,
    generate_master_key_b64,
    master_key_present,
    mint_master_key,
    store_master_key,
)
from provisa.encryption.runtime import reset_encryption
from provisa.encryption.service import NullEncryption


@pytest.fixture(autouse=True)
def _isolated(tmp_path, monkeypatch):
    monkeypatch.setenv("PROVISA_DATA_DIR", str(tmp_path))
    # The deployment's key, when set, is the one in force: these tests are about a host's own.
    monkeypatch.delenv("PROVISA_ENCRYPTION_KEY", raising=False)
    monkeypatch.setitem(sys.modules, "keyring", None)  # simulate: no OS keychain package at all
    reset_encryption()
    yield
    reset_encryption()


def test_auto_mints_a_key_via_the_file_keystore_when_no_deployment_service_is_configured():
    # Before the fix, this raised RuntimeError("No encryption master key, and this host has
    # no keychain to hold one...") whenever no keyring backend existed.
    service = _cipher(mint=True)
    assert not isinstance(service, NullEncryption)
    plaintext = b"KGAT_some_kaggle_token"
    assert service.decrypt(service.encrypt(plaintext)) == plaintext


def test_second_call_reuses_the_same_minted_key():
    service_a = _cipher(mint=True)
    blob = service_a.encrypt(b"a secret")
    service_b = _cipher(mint=False)
    assert service_b.decrypt(blob) == b"a secret"


def test_uses_the_configured_deployment_service_when_one_exists(monkeypatch):
    from provisa.encryption import configure_encryption

    monkeypatch.setattr(
        "provisa.encryption.factory.build_encryption_service",
        lambda *a, **k: _EnvelopeStub(),
    )
    configure_encryption("local", key_id="whatever")
    service = _cipher(mint=False)
    assert isinstance(service, _EnvelopeStub)


class _EnvelopeStub:
    def encrypt(self, plaintext: bytes) -> bytes:
        return plaintext

    def decrypt(self, blob: bytes) -> bytes:
        return blob


# --- one master key per deployment; a worker never mints on read ------------------------------


class _Vault:
    """The one query ``_decrypted`` makes, answered from a list of (name, blob) rows."""

    def __init__(self, rows: list[tuple[str, bytes]]) -> None:
        self._rows = rows

    @asynccontextmanager
    async def acquire(self):
        yield self

    async def execute_core(self, _stmt):
        return self

    def fetchall(self):
        return self._rows


def _read(vault: _Vault) -> dict[str, str]:
    return asyncio.run(_decrypted(vault, "acme", ORG_OWNER))  # type: ignore[arg-type]


def test_reading_a_vault_that_holds_secrets_never_mints_a_key():
    """A worker with no key, facing a vault written under one, was not given the deployment's
    key. It says so; it does not make a second key that opens nothing."""
    blob = _cipher(mint=True).encrypt(b"s3cret")
    _file_keystore_path(None).unlink()  # this worker: the same vault, no key
    assert not master_key_present()
    with pytest.raises(VaultKeyError) as raised:
        _read(_Vault([("PG_PASSWORD", blob)]))
    message = str(raised.value)
    assert "holds no encryption master key" in message
    assert "PROVISA_ENCRYPTION_KEY" in message
    assert str(_file_keystore_path(None)) in message
    assert not master_key_present()


def test_an_empty_vault_is_read_without_a_key():
    assert _read(_Vault([])) == {}
    assert not master_key_present()


def test_a_vault_written_under_another_key_says_so():
    blob = _cipher(mint=True).encrypt(b"s3cret")
    store_master_key(generate_master_key_b64())  # this worker holds a different key
    with pytest.raises(VaultKeyError) as raised:
        _read(_Vault([("PG_PASSWORD", blob)]))
    message = str(raised.value)
    assert "is not the one the vault of org 'acme' was written under" in message
    assert "PROVISA_ENCRYPTION_KEY" in message
    assert str(_file_keystore_path(None)) in message


def test_the_deployments_key_wins_over_this_hosts_own(monkeypatch):
    """Workers on separate hosts share a key only by being given one. A host that minted its own
    before the variable was set must use the deployment's."""
    mint_master_key()  # this host's own key, in its file keystore
    own = _cipher(mint=False).encrypt(b"written under the host key")
    deployment_key = base64.b64encode(bytes(range(32))).decode()
    monkeypatch.setenv("PROVISA_ENCRYPTION_KEY", deployment_key)
    blob = _cipher(mint=True).encrypt(b"s3cret")
    assert _file_keystore_path(None).read_text() != deployment_key  # nothing was overwritten
    assert _read(_Vault([("PG_PASSWORD", blob)])) == {"PG_PASSWORD": "s3cret"}
    with pytest.raises(VaultKeyError):
        _read(_Vault([("OLD", own)]))


def test_minting_does_not_replace_a_key_that_exists():
    mint_master_key()
    first = _file_keystore_path(None).read_text()
    mint_master_key()
    assert _file_keystore_path(None).read_text() == first


def test_two_workers_minting_together_end_with_one_key(tmp_path):
    """The check and the store are one step under the keystore's lock: a worker that arrives
    while another holds it waits, then finds that worker's key and mints none."""
    keystore = _file_keystore_path(None)
    keystore.parent.mkdir(parents=True, mode=0o700)
    env = {
        **{k: v for k, v in os.environ.items() if k != "PROVISA_ENCRYPTION_KEY"},
        "PROVISA_DATA_DIR": str(tmp_path),
        "PYTHON_KEYRING_BACKEND": "keyring.backends.fail.Keyring",
    }
    with open(keystore.parent / "master.lock", "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)  # this test is the worker that got there first
        other = subprocess.Popen(
            [
                sys.executable,
                "-c",
                "from provisa.encryption.providers import mint_master_key; mint_master_key()",
            ],
            cwd=str(Path(__file__).parents[2]),
            env=env,
        )
        try:
            deadline = time.monotonic() + 3.0
            while time.monotonic() < deadline:
                assert other.poll() is None, "the second worker did not wait for the lock"
                assert not keystore.exists(), "the second worker minted while the lock was held"
                time.sleep(0.1)
            mine = generate_master_key_b64()
            keystore.write_text(mine)
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)
    assert other.wait(timeout=60) == 0
    assert keystore.read_text() == mine
