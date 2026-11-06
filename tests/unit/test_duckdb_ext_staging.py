# Copyright (c) 2026 Kenneth Stott
# Canary: d717b13c-e102-44cb-960f-f3b7e77e6f03
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
"""Offline staging of the bundled DuckDB extensions (provisa-duckdb-ext PyPI package).

stage_bundled_extensions must copy the running platform's blobs out of the package into a writable
dir WITHOUT touching extensions.duckdb.org, and fail loud (BundledExtensionsMissing) when the package
carries no blobs for this exact DuckDB version+platform. The package is mocked so the test is hermetic
(no dependency on the embedded extra being installed).
"""

from __future__ import annotations

import sys
import types

import duckdb
import pytest

from provisa.federation.duckdb_extensions import (
    BundledExtensionsMissing,
    stage_bundled_extensions,
)


def _duckdb_version_platform() -> tuple[str, str]:
    con = duckdb.connect()
    try:
        return (
            con.execute("SELECT version()").fetchone()[0],
            con.execute("PRAGMA platform").fetchone()[0],
        )
    finally:
        con.close()


def _install_fake_package(monkeypatch, ext_root):
    mod = types.ModuleType("provisa_duckdb_ext")
    mod.ext_root = lambda: ext_root  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "provisa_duckdb_ext", mod)


def test_stage_copies_current_platform_blobs(tmp_path, monkeypatch):
    ver, plat = _duckdb_version_platform()
    pkg = tmp_path / "pkg_ext"
    (pkg / ver / plat).mkdir(parents=True)
    (pkg / ver / plat / "sqlite_scanner.duckdb_extension").write_bytes(b"FAKE-BLOB")
    _install_fake_package(monkeypatch, pkg)

    target = tmp_path / "staged"
    out = stage_bundled_extensions(target)

    staged = out / ver / plat / "sqlite_scanner.duckdb_extension"
    assert staged.exists() and staged.read_bytes() == b"FAKE-BLOB"
    # Idempotent — a second call over the existing file does not raise.
    assert stage_bundled_extensions(target) == target


def test_missing_platform_blobs_fails_loud(tmp_path, monkeypatch):
    # Package present but no blobs for this version/platform → packaging defect, never a silent network fetch.
    _install_fake_package(monkeypatch, tmp_path / "empty_pkg")
    with pytest.raises(BundledExtensionsMissing):
        stage_bundled_extensions(tmp_path / "staged")


def test_package_absent_raises_module_not_found(tmp_path, monkeypatch):
    # A dev checkout without the embedded extra: caller (cli) treats this as "fall back to network".
    monkeypatch.setitem(
        sys.modules, "provisa_duckdb_ext", None
    )  # force ModuleNotFoundError on import
    with pytest.raises(ModuleNotFoundError):
        stage_bundled_extensions(tmp_path / "staged")


def test_stage_copies_a_named_repositorys_extension_with_its_info(tmp_path, monkeypatch):
    # crypto on DuckDB 2.0 is Query.Farm's: DuckDB keeps it under repositories/query_farm/ and
    # loads it by the .info beside it, so both are staged in that layout.
    ver, plat = _duckdb_version_platform()
    pkg = tmp_path / "pkg_ext"
    repo = pkg / ver / plat / "repositories" / "query_farm"
    repo.mkdir(parents=True)
    (repo / "crypto.duckdb_extension").write_bytes(b"FAKE-CRYPTO")
    (repo / "crypto.duckdb_extension.info").write_bytes(b"FAKE-INFO")
    _install_fake_package(monkeypatch, pkg)

    out = stage_bundled_extensions(tmp_path / "staged")

    staged = out / ver / plat / "repositories" / "query_farm"
    assert (staged / "crypto.duckdb_extension").read_bytes() == b"FAKE-CRYPTO"
    assert (staged / "crypto.duckdb_extension.info").read_bytes() == b"FAKE-INFO"


def test_load_staged_crypto_unstaged_is_false_never_an_error(tmp_path, monkeypatch):
    # Nothing staged: whichever LOAD this DuckDB parses (LOAD crypto FROM query_farm on 2.0, plain
    # LOAD before it, where FROM is a parser error) fails, and the caller refuses by name.
    from provisa.federation.duckdb_extensions import load_staged

    monkeypatch.setenv("PROVISA_DUCKDB_EXT_DIR", str(tmp_path / "empty"))
    con = duckdb.connect()
    try:
        assert load_staged(con, "crypto") is False
    finally:
        con.close()
