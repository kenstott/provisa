# Copyright (c) 2026 Kenneth Stott
# Canary: 2a4289ca-72f8-4317-be49-d1e054c1af63
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The pg engine's fake functions (REQ-1494): PL/Python calling the same Python as DuckDB, the key
read by fingerprint from the key directory; the bundle's PL/Python linked to the running
interpreter, and the postmaster started with its home and paths without changing this process."""

from __future__ import annotations

import os

import duckdb
import pytest

from provisa.fakes import digest as digest_mod
from provisa.fakes import pg_functions
from provisa.fakes.digest import FakeKeyMissing, fingerprint
from provisa.fakes.duckdb_functions import register

_KEY = b"q" * 32


@pytest.fixture
def key_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("PROVISA_FAKE_KEY_DIR", str(tmp_path))
    monkeypatch.setattr(pg_functions, "_keys", {})
    (tmp_path / f"{fingerprint(_KEY)}.key").write_text(_KEY.hex())
    return tmp_path


def test_postgres_and_duckdb_compute_the_same_digest_and_fakes(key_dir, monkeypatch):
    monkeypatch.setattr(digest_mod, "_key", _KEY)
    con = duckdb.connect()
    register(con)
    fp = fingerprint(_KEY)
    for value in ("ann@example.com", "42", ""):
        (duck,) = con.execute("SELECT provisa_digest(?, ?)", [fp, value]).fetchone()
        assert pg_functions.pg_digest(fp, value) == duck
        (tag,) = con.execute("SELECT provisa_digest_tag(?)", [duck]).fetchone()
        assert pg_functions.pg_digest_tag(duck) == tag
        (fake,) = con.execute("SELECT provisa_fake_method('email', '{}', ?, 11)", [duck]).fetchone()
        assert pg_functions.pg_fake_method("email", "{}", duck, 11) == fake
    assert pg_functions.pg_digest(fp, None) is None


def test_a_key_the_engine_does_not_hold_is_refused_by_fingerprint(key_dir):
    with pytest.raises(FakeKeyMissing, match="holds no fake key 0000000000000000"):
        pg_functions.pg_digest("0000000000000000", "x")
    (key_dir / "1111111111111111.key").write_text((b"z" * 32).hex())
    with pytest.raises(FakeKeyMissing, match="holds another key"):
        pg_functions.pg_digest("1111111111111111", "x")


def test_plpython_is_linked_to_the_running_interpreter(tmp_path):
    from provisa.pg_extensions.staging import _libpython, link_interpreter

    platform = "darwin-arm64" if os.uname().sysname == "Darwin" else "linux-x64"
    link_interpreter(tmp_path, platform)
    name = "libpython3.12.dylib" if platform.startswith("darwin") else "libpython3.12.so.1.0"
    assert (tmp_path / name).resolve() == _libpython().resolve()
    link_interpreter(tmp_path, platform)  # idempotent


def test_the_postmaster_environment_is_set_for_the_start_alone(tmp_path, monkeypatch):
    import pgserver

    from provisa.core import control_plane_pg

    fake_pkg = tmp_path / "pgserver"
    (fake_pkg / "pginstall" / "lib" / "postgresql").mkdir(parents=True)
    (fake_pkg / "pginstall" / "lib" / "postgresql" / "plpython3.so").write_text("")
    monkeypatch.setattr(pgserver, "__file__", str(fake_pkg / "__init__.py"))
    monkeypatch.delenv("PYTHONHOME", raising=False)
    with control_plane_pg._postmaster_environment():
        assert os.environ["PYTHONHOME"]
        assert "PYTHONPATH" in os.environ
    assert "PYTHONHOME" not in os.environ
