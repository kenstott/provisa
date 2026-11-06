# Copyright (c) 2026 Kenneth Stott
# Canary: d717b13c-e102-44cb-960f-f3b7e77e6f03
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
"""Offline staging of the bundled DuckDB extensions (the embedded extra's PyPI packages).

stage_bundled_extensions must copy the running platform's blobs into a writable dir WITHOUT touching
extensions.duckdb.org: the core extensions from their third-party per-extension wheels (REQ-1142
amendment, TEMPORARY EXTENSION SOURCES FOR DUCKDB 2.0), every other one from provisa-duckdb-ext. It
fails loud (BundledExtensionsMissing), naming each, when a package is absent or carries no blobs for
this exact DuckDB version+platform. The packages are faked so the test is hermetic (no dependency on
the embedded extra being installed).
"""

from __future__ import annotations

import sys
import types

import duckdb
import pytest

from provisa.federation.duckdb_extensions import (
    CORE_EXTENSION_WHEELS,
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


def _install_fake_core_wheels(monkeypatch, site, version, skip=()):
    """The third-party wheels as installed: an empty package per extension, holding the binary at
    <package>/extensions/<version>/<name>.duckdb_extension -- no platform dir: the wheel is the
    platform's own."""
    for extension, package in CORE_EXTENSION_WHEELS.items():
        if extension in skip:
            continue
        pkg = site / package
        (pkg / "extensions" / version).mkdir(parents=True)
        (pkg / "__init__.py").write_text("")
        (pkg / "extensions" / version / f"{extension}.duckdb_extension").write_bytes(
            f"CORE-{extension}".encode()
        )
    monkeypatch.syspath_prepend(str(site))


def _provisa_ext(tmp_path, ver, plat):
    pkg = tmp_path / "pkg_ext"
    (pkg / ver / plat).mkdir(parents=True)
    (pkg / ver / plat / "mssql.duckdb_extension").write_bytes(b"FAKE-MSSQL")
    (pkg / ver / plat / "mssql.duckdb_extension.info").write_bytes(b"FAKE-INFO")
    return pkg


def test_the_core_extensions_are_the_third_party_wheels():
    assert CORE_EXTENSION_WHEELS == {
        "postgres_scanner": "duckdb_extension_postgres_scanner",
        "httpfs": "duckdb_extension_httpfs",
        "sqlite_scanner": "duckdb_extension_sqlite_scanner",
        "iceberg": "duckdb_extension_iceberg",
        "azure": "duckdb_extension_azure",
        "delta": "duckdb_extension_delta",
        "avro": "duckdb_extension_avro",  # iceberg loads it
    }


def test_stage_takes_core_from_the_third_party_wheels_and_the_rest_from_provisa_duckdb_ext(
    tmp_path, monkeypatch
):
    ver, plat = _duckdb_version_platform()
    _install_fake_package(monkeypatch, _provisa_ext(tmp_path, ver, plat))
    _install_fake_core_wheels(monkeypatch, tmp_path / "site", ver)

    target = tmp_path / "staged"
    out = stage_bundled_extensions(target)

    staged = out / ver / plat
    for extension in CORE_EXTENSION_WHEELS:
        blob = staged / f"{extension}.duckdb_extension"
        assert blob.read_bytes() == f"CORE-{extension}".encode(), extension
    assert (staged / "mssql.duckdb_extension").read_bytes() == b"FAKE-MSSQL"
    # Idempotent — a second call over the existing files does not raise.
    assert stage_bundled_extensions(target) == target


def test_a_third_party_wheel_never_imported_only_located(tmp_path, monkeypatch):
    # Its package is found, not run: code in its __init__ never executes.
    ver, plat = _duckdb_version_platform()
    _install_fake_package(monkeypatch, _provisa_ext(tmp_path, ver, plat))
    _install_fake_core_wheels(monkeypatch, tmp_path / "site", ver)
    trap = tmp_path / "site" / "duckdb_extension_httpfs" / "__init__.py"
    trap.write_text("raise RuntimeError('imported')\n")
    monkeypatch.delitem(sys.modules, "duckdb_extension_httpfs", raising=False)

    stage_bundled_extensions(tmp_path / "staged")
    assert "duckdb_extension_httpfs" not in sys.modules


def test_a_missing_third_party_wheel_is_refused_by_name(tmp_path, monkeypatch):
    ver, plat = _duckdb_version_platform()
    _install_fake_package(monkeypatch, _provisa_ext(tmp_path, ver, plat))
    _install_fake_core_wheels(monkeypatch, tmp_path / "site", ver, skip=("delta",))
    for package in CORE_EXTENSION_WHEELS.values():
        monkeypatch.delitem(sys.modules, package, raising=False)

    with pytest.raises(
        BundledExtensionsMissing, match="delta: duckdb-extension-delta is not installed"
    ):
        stage_bundled_extensions(tmp_path / "staged")
    assert not (tmp_path / "staged").exists()  # nothing staged by halves


def test_a_third_party_wheel_of_another_duckdb_is_refused_by_name(tmp_path, monkeypatch):
    ver, plat = _duckdb_version_platform()
    _install_fake_package(monkeypatch, _provisa_ext(tmp_path, ver, plat))
    _install_fake_core_wheels(monkeypatch, tmp_path / "site", "v0.0.1")

    with pytest.raises(
        BundledExtensionsMissing, match="iceberg: duckdb-extension-iceberg has none"
    ):
        stage_bundled_extensions(tmp_path / "staged")


def test_missing_platform_blobs_fails_loud(tmp_path, monkeypatch):
    # Package present but no blobs for this version/platform → packaging defect, never a silent network fetch.
    ver, _plat = _duckdb_version_platform()
    _install_fake_package(monkeypatch, tmp_path / "empty_pkg")
    _install_fake_core_wheels(monkeypatch, tmp_path / "site", ver)
    with pytest.raises(BundledExtensionsMissing, match="provisa-duckdb-ext has none"):
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
    _install_fake_core_wheels(monkeypatch, tmp_path / "site", ver)

    out = stage_bundled_extensions(tmp_path / "staged")

    staged = out / ver / plat / "repositories" / "query_farm"
    assert (staged / "crypto.duckdb_extension").read_bytes() == b"FAKE-CRYPTO"
    assert (staged / "crypto.duckdb_extension.info").read_bytes() == b"FAKE-INFO"


def test_load_staged_crypto_unstaged_is_false_never_an_error(tmp_path, monkeypatch):
    # Nothing staged: LOAD crypto FROM query_farm fails, and the caller refuses by name.
    from provisa.federation.duckdb_extensions import load_staged

    monkeypatch.setenv("PROVISA_DUCKDB_EXT_DIR", str(tmp_path / "empty"))
    con = duckdb.connect()
    try:
        assert load_staged(con, "crypto") is False
    finally:
        con.close()


def test_load_staged_loads_crypto_from_its_repository_only(monkeypatch):
    # DuckDB 2.0 loads crypto from Query.Farm's staged copy alone: no plain LOAD is tried after it.
    from provisa.federation import duckdb_extensions

    class _Con:
        def __init__(self):
            self.statements: list[str] = []

        def execute(self, statement):
            self.statements.append(statement)
            raise duckdb.IOException("IO Error: not found")

    monkeypatch.delenv("PROVISA_DUCKDB_EXT_DIR", raising=False)
    monkeypatch.setattr(duckdb_extensions, "extension_directory", lambda: None)
    con = _Con()
    assert duckdb_extensions.load_staged(con, "crypto") is False  # type: ignore[arg-type]
    assert con.statements == ["LOAD crypto FROM query_farm"]


def test_a_bundled_extension_without_its_info_is_refused_by_name(tmp_path, monkeypatch):
    # DuckDB 2.0 does not trust a community extension's signature without the .info naming its
    # repository (provisa-duckdb-ext 0.2.0 shipped them so, and none loaded): staging one would
    # only move the refusal to the first query.
    ver, plat = _duckdb_version_platform()
    pkg = _provisa_ext(tmp_path, ver, plat)
    (pkg / ver / plat / "mongo.duckdb_extension").write_bytes(b"FAKE-MONGO")
    _install_fake_package(monkeypatch, pkg)
    _install_fake_core_wheels(monkeypatch, tmp_path / "site", ver)
    for package in CORE_EXTENSION_WHEELS.values():
        monkeypatch.delitem(sys.modules, package, raising=False)
    with pytest.raises(BundledExtensionsMissing, match=r"mongo: no \.info"):
        stage_bundled_extensions(tmp_path / "staged")
