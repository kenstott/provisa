# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1142 (Amended 2026-11-06, ATTACHABLE MEANS STAGED): on a staged extension directory the
DuckDB engine counts a connector attachable only when every extension its way of reaching needs is
staged for the running DuckDB version and platform, read from the staged extensions' manifest. A
source whose extension is not staged lands when it is landable, and is otherwise refused by name
when it is added, naming the extension. REQ-1998 (Amended 2026-11-06): a MotherDuck source is
refused by name, MotherDuck not yet supporting the DuckDB version Provisa runs."""

# Requirements: REQ-1142, REQ-1998

from __future__ import annotations

import json
import sys
import types

import duckdb
import pytest

from provisa.federation import duckdb_extensions
from provisa.federation.duckdb_extensions import (
    CORE_EXTENSION_WHEELS,
    MANIFEST,
    StagedManifestMissing,
    stage_bundled_extensions,
    staged_extensions,
)
from provisa.federation.engine import UnreachableSource, build_engine

#: What the embedded extra stages, by platform (provisa-duckdb-ext 0.2.0 + the core wheels).
_CORE = ("postgres_scanner", "httpfs", "sqlite_scanner", "iceberg", "azure", "delta", "avro")
_LINUX = (*_CORE, "bigquery", "mongo", "mssql", "snowflake", "crypto")
_MACOS = (*_LINUX, "firebird")
_WINDOWS = (*_CORE, "airport", "mssql", "snowflake", "crypto")


def _version_platform() -> tuple[str, str]:
    con = duckdb.connect()
    try:
        return (
            con.execute("SELECT version()").fetchall()[0][0],
            con.execute("PRAGMA platform").fetchall()[0][0],
        )
    finally:
        con.close()


def _staged(tmp_path, monkeypatch, extensions) -> None:
    """An extension directory staged with ``extensions`` for the running DuckDB and platform."""
    ver, plat = _version_platform()
    where = tmp_path / "ext" / ver / plat
    where.mkdir(parents=True)
    (where / MANIFEST).write_text(
        json.dumps({"duckdb": ver, "platform": plat, "extensions": sorted(extensions)})
    )
    monkeypatch.setenv("PROVISA_DUCKDB_EXT_DIR", str(tmp_path / "ext"))


def _source(source_type: str, **fields):
    from provisa.core.models import Source

    base = {"id": f"s-{source_type}", "type": source_type, "host": "h", "port": 1}
    base.update(fields)
    return Source.model_validate(base)


# --- the manifest ---------------------------------------------------------------------------


def _fake_bundle(tmp_path, monkeypatch, names) -> None:
    ver, plat = _version_platform()
    pkg = tmp_path / "pkg" / ver / plat
    pkg.mkdir(parents=True)
    for name in names:
        (pkg / f"{name}.duckdb_extension").write_bytes(b"X")
    (pkg / "repositories" / "query_farm").mkdir(parents=True)
    (pkg / "repositories" / "query_farm" / "crypto.duckdb_extension").write_bytes(b"X")
    mod = types.ModuleType("provisa_duckdb_ext")
    mod.ext_root = lambda: tmp_path / "pkg"  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "provisa_duckdb_ext", mod)
    site = tmp_path / "site"
    for extension, package in CORE_EXTENSION_WHEELS.items():
        (site / package / "extensions" / ver).mkdir(parents=True)
        (site / package / "__init__.py").write_text("")
        (site / package / "extensions" / ver / f"{extension}.duckdb_extension").write_bytes(b"X")
        monkeypatch.delitem(sys.modules, package, raising=False)
    monkeypatch.syspath_prepend(str(site))


def test_staging_writes_the_manifest_of_what_it_staged(tmp_path, monkeypatch):
    _fake_bundle(tmp_path, monkeypatch, ("mssql", "mongo"))
    target = stage_bundled_extensions(tmp_path / "staged")
    ver, plat = _version_platform()
    manifest = json.loads((target / ver / plat / MANIFEST).read_text())
    assert manifest == {
        "duckdb": ver,
        "platform": plat,
        "extensions": sorted({*CORE_EXTENSION_WHEELS, "mssql", "mongo", "crypto"}),
    }


def test_the_manifest_is_what_is_read_for_the_running_duckdb(tmp_path, monkeypatch):
    _staged(tmp_path, monkeypatch, ("postgres_scanner", "mssql"))
    assert staged_extensions() == frozenset({"postgres", "postgres_scanner", "mssql"})


def test_no_staged_directory_is_duckdbs_own_install_path(monkeypatch):
    monkeypatch.delenv("PROVISA_DUCKDB_EXT_DIR", raising=False)
    assert staged_extensions() is None


def test_a_staged_directory_without_a_manifest_is_refused_by_name(tmp_path, monkeypatch):
    monkeypatch.setenv("PROVISA_DUCKDB_EXT_DIR", str(tmp_path / "empty"))
    with pytest.raises(StagedManifestMissing, match=MANIFEST):
        staged_extensions()


# --- staged: attaches -----------------------------------------------------------------------


def test_a_staged_extension_attaches(tmp_path, monkeypatch):
    from provisa.federation.strategy import Strategy, federate

    _staged(tmp_path, monkeypatch, _LINUX)
    engine = build_engine("duckdb")
    assert engine.refusal("sqlserver") is None
    assert engine.connectors["sqlserver"].reads_in_place
    assert federate(_source("mongodb"), engine) is Strategy.VIRTUAL


def test_staged_means_every_way_the_extension_offers(tmp_path, monkeypatch):
    # postgres staged: the Postgres attach and every pgwire-bridge connector are attachable.
    _staged(tmp_path, monkeypatch, ("postgres_scanner",))
    engine = build_engine("duckdb")
    for source_type in ("postgresql", "sharepoint", "splunk", "files", "jira", "govdata"):
        assert engine.connectors[source_type].reads_in_place, source_type


def test_firebird_attaches_where_it_is_staged(tmp_path, monkeypatch):
    _staged(tmp_path, monkeypatch, _MACOS)
    engine = build_engine("duckdb")
    assert engine.refusal("firebird") is None
    assert engine.connectors["firebird"].reads_in_place


# --- not staged, landable: lands ------------------------------------------------------------


def test_an_unstaged_extension_lands_a_landable_source(tmp_path, monkeypatch):
    from provisa.federation.strategy import Strategy, federate

    _staged(tmp_path, monkeypatch, _WINDOWS)  # no mongo on Windows
    engine = build_engine("duckdb")
    assert engine.refusal("mongodb") is None
    assert not engine.connectors["mongodb"].reads_in_place
    assert federate(_source("mongodb"), engine) is Strategy.MATERIALIZED


# --- not staged, not landable: refused by name at registration ------------------------------


def test_gsheets_has_no_build_for_this_duckdb_and_is_refused_by_name(tmp_path, monkeypatch):
    _staged(tmp_path, monkeypatch, _MACOS)
    ver, plat = _version_platform()
    engine = build_engine("duckdb")
    why = engine.refusal("google_sheets")
    assert why is not None
    assert "gsheets" in why and ver in why and plat in why
    assert "google_sheets" not in engine.connectors
    assert not engine.reachable("google_sheets")
    with pytest.raises(UnreachableSource, match="gsheets"):
        engine.connector_for("google_sheets")


def test_firebird_on_linux_is_refused_by_name(tmp_path, monkeypatch):
    # Left out of the Linux bundle; its rows are read through the same extension, so it cannot
    # land either.
    _staged(tmp_path, monkeypatch, _LINUX)
    engine = build_engine("duckdb")
    why = engine.refusal("firebird")
    assert why is not None and "firebird" in why
    assert "firebird" not in engine.connectors
    with pytest.raises(UnreachableSource, match="firebird"):
        engine.connector_for("firebird")


def test_iceberg_needs_avro_staged_too(tmp_path, monkeypatch):
    _staged(tmp_path, monkeypatch, tuple(e for e in _LINUX if e != "avro"))
    engine = build_engine("duckdb")
    why = engine.refusal("iceberg")
    assert why is not None and "avro" in why


def test_registration_refuses_an_unattachable_source_before_it_is_saved(tmp_path, monkeypatch):
    from provisa.api.admin.schema_common import engine_extension_refusal

    _staged(tmp_path, monkeypatch, _LINUX)
    state = types.SimpleNamespace(
        federation_engine=types.SimpleNamespace(engine=build_engine("duckdb"))
    )
    refused = engine_extension_refusal(state, "my-sheet", "google_sheets")
    assert refused is not None
    assert refused.success is False
    assert refused.code == "schema.source_extension_unavailable"
    assert "gsheets" in refused.message and "my-sheet" in refused.message
    assert engine_extension_refusal(state, "pg", "postgresql") is None


# --- the server tier: DuckDB's own install path ----------------------------------------------


def test_with_no_staged_directory_every_extension_connector_stays(monkeypatch):
    monkeypatch.delenv("PROVISA_DUCKDB_EXT_DIR", raising=False)
    engine = build_engine("duckdb")
    for source_type in ("postgresql", "sqlserver", "firebird", "google_sheets", "iceberg"):
        assert engine.refusal(source_type) is None, source_type
        assert engine.connectors[source_type].reads_in_place, source_type


# --- MotherDuck (REQ-1998, MOTHERDUCK WAITS FOR DUCKDB 2.0 SUPPORT) ---------------------------


@pytest.mark.parametrize("staged", [None, _MACOS])
def test_motherduck_is_refused_by_name_on_every_tier(tmp_path, monkeypatch, staged):
    if staged is None:
        monkeypatch.delenv("PROVISA_DUCKDB_EXT_DIR", raising=False)
    else:
        _staged(tmp_path, monkeypatch, staged)
    engine = build_engine("duckdb")
    why = engine.refusal("motherduck")
    assert why is not None
    assert "MotherDuck does not yet support DuckDB" in why
    assert duckdb.__version__ in why
    assert "motherduck" not in engine.connectors
    with pytest.raises(UnreachableSource, match="MotherDuck does not yet support"):
        engine.connector_for("motherduck")


def test_the_motherduck_extension_is_never_installed(monkeypatch):
    # Refused before anything is attached: no INSTALL motherduck reaches DuckDB.
    loaded: list[str] = []
    monkeypatch.setattr(
        duckdb_extensions, "install_and_load", lambda con, ext, **_: loaded.append(ext)
    )
    monkeypatch.delenv("PROVISA_DUCKDB_EXT_DIR", raising=False)
    engine = build_engine("duckdb")
    with pytest.raises(UnreachableSource):
        engine.connector_for_source(_source("motherduck", password="t", database="db"))
    assert loaded == []
