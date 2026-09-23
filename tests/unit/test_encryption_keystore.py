# Copyright (c) 2026 Kenneth Stott
# Canary: 5d8a2c17-9f6e-4b30-a1d4-7e2c8f9b6a13
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1802: the master-key store falls back to a file under this host's Provisa data directory
when no OS keychain backend is usable (no ``keyring`` package, or an installed one with no
working backend) — so the admin UI's "Generate key" action never requires an operator to fall
back to hand-setting PROVISA_ENCRYPTION_KEY.
"""

from __future__ import annotations

import base64
import stat
import sys

import pytest

from provisa.encryption.providers import (
    _MASTER_KEY_BYTES,
    _file_keystore_path,
    generate_master_key_b64,
    master_key_present,
    store_master_key,
)


@pytest.fixture(autouse=True)
def _isolated_data_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("PROVISA_DATA_DIR", str(tmp_path))
    yield


def _no_keyring_package(monkeypatch):
    """Simulate ``keyring`` not being installed at all."""
    monkeypatch.setitem(sys.modules, "keyring", None)


def _keyring_with_broken_backend(monkeypatch):
    """Simulate ``keyring`` installed but its backend raising (no session keyring, locked
    desktop, sandboxed CI) — a different failure shape than ImportError, same outcome."""
    import types

    class _NoBackend(Exception):
        pass

    fake = types.SimpleNamespace(
        set_password=lambda *a, **k: (_ for _ in ()).throw(_NoBackend("no backend")),
        get_password=lambda *a, **k: (_ for _ in ()).throw(_NoBackend("no backend")),
    )
    monkeypatch.setitem(sys.modules, "keyring", fake)


class TestFileKeystoreFallback:
    def test_store_and_load_round_trip_with_no_keyring_package(self, monkeypatch):
        _no_keyring_package(monkeypatch)
        key_b64 = generate_master_key_b64()
        assert store_master_key(key_b64, "master") is True
        assert master_key_present("master") is True

        from provisa.encryption.providers import _load_from_keychain

        assert _load_from_keychain("master") == key_b64

    def test_store_and_load_round_trip_with_a_broken_keyring_backend(self, monkeypatch):
        _keyring_with_broken_backend(monkeypatch)
        key_b64 = generate_master_key_b64()
        assert store_master_key(key_b64, "master") is True

        from provisa.encryption.providers import _load_from_keychain

        assert _load_from_keychain("master") == key_b64

    def test_file_is_written_with_owner_only_permissions(self, monkeypatch):
        _no_keyring_package(monkeypatch)
        store_master_key(generate_master_key_b64(), "master")
        path = _file_keystore_path("master")
        assert path.exists()
        mode = stat.S_IMODE(path.stat().st_mode)
        assert mode == 0o600

    def test_distinct_key_ids_get_distinct_files(self, monkeypatch):
        _no_keyring_package(monkeypatch)
        key_a = generate_master_key_b64()
        key_b = generate_master_key_b64()
        store_master_key(key_a, "master")
        store_master_key(key_b, "org-42")

        from provisa.encryption.providers import _load_from_keychain

        assert _load_from_keychain("master") == key_a
        assert _load_from_keychain("org-42") == key_b

    def test_no_key_present_before_anything_is_stored(self, monkeypatch):
        _no_keyring_package(monkeypatch)
        assert master_key_present("master") is False

    def test_rejects_wrong_length_key(self, monkeypatch):
        _no_keyring_package(monkeypatch)
        bad = base64.b64encode(b"too short").decode("ascii")
        with pytest.raises(ValueError, match=str(_MASTER_KEY_BYTES)):
            store_master_key(bad, "master")
