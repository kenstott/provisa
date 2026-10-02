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
from pathlib import Path

import pytest

from provisa.core.secrets_store import ORG_OWNER, VaultKeyError, _cipher, _decrypted, put
from provisa.encryption.providers import (
    _file_keystore_path,
    generate_master_key_b64,
    master_key_fingerprint,
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


@pytest.fixture
def plane(tmp_path):
    """A platform control plane (SQLite) holding the vault and the deployment key's record."""
    from provisa.core.database import Database, create_engine_from_url
    from provisa.core.schema_admin import (
        deployment_encryption_key,
        metadata,
        secrets_store,
    )

    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'platform.db'}")
    with engine.begin() as conn:
        metadata.create_all(conn, tables=[secrets_store, deployment_encryption_key])
    db = Database(engine, name="admin")
    yield db
    engine.dispose()


def _put(plane, name: str = "PG_PASSWORD", value: str = "s3cret") -> None:
    asyncio.run(put(plane, "acme", name, value, owner_id=ORG_OWNER))


def _read(plane) -> dict[str, str]:
    return asyncio.run(_decrypted(plane, "acme", ORG_OWNER))


def _recorded(plane) -> list[str]:
    from provisa.core.schema_admin import deployment_encryption_key

    with plane.engine.connect() as conn:
        return [r[0] for r in conn.execute(deployment_encryption_key.select()).fetchall()]


def _secret_names(plane) -> list[str]:
    from provisa.core.schema_admin import secrets_store

    with plane.engine.connect() as conn:
        return [r.name for r in conn.execute(secrets_store.select()).fetchall()]


def test_the_first_secret_records_the_deployment_keys_fingerprint(plane):
    from provisa.core.org_encryption import fingerprint

    assert _recorded(plane) == []
    _put(plane)
    raw = base64.b64decode(_file_keystore_path(None).read_text())
    assert _recorded(plane) == ["master"]
    with plane.engine.connect() as conn:
        from provisa.core.schema_admin import deployment_encryption_key as t

        stored = conn.execute(t.select()).fetchone().fingerprint
    assert stored == fingerprint(raw) == master_key_fingerprint()
    assert _read(plane) == {"PG_PASSWORD": "s3cret"}
    _put(plane, "OTHER", "x")  # a later write leaves the record as it is
    with plane.engine.connect() as conn:
        assert conn.execute(t.select()).fetchone().fingerprint == stored


def test_reading_a_vault_that_holds_secrets_never_mints_a_key(plane):
    """A worker with no key, facing a vault written under one, was not given the deployment's
    key. It says so; it does not make a second key that opens nothing."""
    _put(plane)
    recorded = master_key_fingerprint()
    _file_keystore_path(None).unlink()  # this worker: the same vault, no key
    assert not master_key_present()
    with pytest.raises(VaultKeyError) as raised:
        _read(plane)
    message = str(raised.value)
    assert "holds no encryption master key" in message
    assert f"fingerprint {recorded[:8]}" in message
    assert "PROVISA_ENCRYPTION_KEY" in message
    assert str(_file_keystore_path(None)) in message
    assert not master_key_present()


def test_no_worker_mints_once_the_deployment_has_a_key(plane):
    """A write on a worker without the key is refused too: the deployment's key exists, so
    this worker is to be given it, not to make another."""
    _put(plane)
    _file_keystore_path(None).unlink()
    with pytest.raises(VaultKeyError, match="holds no encryption master key"):
        _put(plane, "SECOND", "x")
    assert not master_key_present()
    assert _secret_names(plane) == ["PG_PASSWORD"]


def test_an_empty_vault_is_read_without_a_key(plane):
    assert _read(plane) == {}
    assert not master_key_present()
    assert _recorded(plane) == []


def test_a_worker_holding_another_key_refuses_naming_both_fingerprints(plane):
    _put(plane)
    theirs = master_key_fingerprint()
    store_master_key(generate_master_key_b64())  # this worker holds a different key
    mine = master_key_fingerprint()
    assert mine != theirs
    for use in (lambda: _read(plane), lambda: _put(plane, "SECOND", "x")):
        with pytest.raises(VaultKeyError) as raised:
            use()
        message = str(raised.value)
        assert f"fingerprint {mine[:8]}" in message and f"fingerprint {theirs[:8]}" in message
        assert "PROVISA_ENCRYPTION_KEY" in message
        assert str(_file_keystore_path(None)) in message
    assert _secret_names(plane) == ["PG_PASSWORD"]


def test_the_deployments_key_wins_over_this_hosts_own(plane, monkeypatch):
    """Workers on separate hosts share a key only by being given one. A host that minted its own
    before the variable was set uses the deployment's, and it is the one recorded."""
    from provisa.core.org_encryption import fingerprint

    mint_master_key()  # this host's own key, in its file keystore
    own = _file_keystore_path(None).read_text()
    deployment_key = bytes(range(32))
    monkeypatch.setenv("PROVISA_ENCRYPTION_KEY", base64.b64encode(deployment_key).decode())
    _put(plane)
    assert _file_keystore_path(None).read_text() == own  # nothing was overwritten
    assert master_key_fingerprint() == fingerprint(deployment_key)
    assert _read(plane) == {"PG_PASSWORD": "s3cret"}
    # A worker of the same deployment that was NOT given the variable holds only its host key.
    monkeypatch.delenv("PROVISA_ENCRYPTION_KEY")
    with pytest.raises(VaultKeyError, match="is not the one this deployment's secrets"):
        _read(plane)


def test_two_hosts_storing_a_first_secret_together_end_with_one_deployment_key(plane):
    """Separate hosts share no key store, so each mints. Both try to record; one row wins, and
    the other host finds its key is not the deployment's — its secret is not stored."""
    from provisa.core.schema_admin import deployment_encryption_key as t

    with plane.engine.begin() as conn:  # the other host recorded its key first
        conn.execute(t.insert().values(key_id="master", fingerprint="0123456789abcdef"))
    with pytest.raises(VaultKeyError) as raised:
        _put(plane)
    assert "fingerprint 01234567" in str(raised.value)
    assert _secret_names(plane) == []
    with plane.engine.connect() as conn:
        assert conn.execute(t.select()).fetchone().fingerprint == "0123456789abcdef"


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
