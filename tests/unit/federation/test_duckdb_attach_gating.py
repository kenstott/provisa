# Copyright (c) 2026 Kenneth Stott
# Canary: f8b0cd0f-aaea-40fa-9c08-44cee3dc7e37
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1142 (Amended 2026-11-06, ATTACHABLE MEANS STAGED; NOT ATTACHABLE MEANS REPLICA): on a
staged extension directory the DuckDB engine counts a connector attachable only when every
extension its way of reaching needs is staged for the running DuckDB version and platform, read
from the staged extensions' manifest. A source it cannot read in place is a replica wherever
Provisa lands it; it is unreachable only when no reader in the deployment can read it, and that is
shown when it is added, naming what is missing. REQ-1998 (Amended 2026-11-06, MOTHERDUCK IS A
REPLICA WHERE IT CANNOT ATTACH; MOTHERDUCK LANDS THROUGH ITS POSTGRES ENDPOINT): on DuckDB 2.0 a
MotherDuck source, and a Google Sheets source (gsheets has no 2.0 build), each needs its per-type
reader, and names it while the deployment lacks it."""

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
        (pkg / f"{name}.duckdb_extension.info").write_bytes(b"I")
    (pkg / "repositories" / "query_farm").mkdir(parents=True)
    (pkg / "repositories" / "query_farm" / "crypto.duckdb_extension").write_bytes(b"X")
    (pkg / "repositories" / "query_farm" / "crypto.duckdb_extension.info").write_bytes(b"I")
    (pkg / "repositories" / "query_farm" / "query_farm.duckdb_extension.repo.json").write_text("{}")
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
    assert engine.unreachable_reason("sqlserver") is None
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
    assert engine.unreachable_reason("firebird") is None
    assert engine.connectors["firebird"].reads_in_place


# --- not staged, landable: a replica ------------------------------------------------------------


def test_an_unstaged_extension_makes_a_landable_source_a_replica(tmp_path, monkeypatch):
    from provisa.federation.strategy import Strategy, federate

    _staged(tmp_path, monkeypatch, _WINDOWS)  # no mongo on Windows
    engine = build_engine("duckdb")
    assert engine.unreachable_reason("mongodb") is None
    assert not engine.connectors["mongodb"].reads_in_place
    assert federate(_source("mongodb"), engine) is Strategy.MATERIALIZED


# --- not staged, no reader: unreachable, shown when added ------------------------------


def test_gsheets_has_no_build_for_this_duckdb_and_no_reader_so_is_unreachable(
    tmp_path, monkeypatch
):
    _staged(tmp_path, monkeypatch, _MACOS)
    ver, plat = _version_platform()
    engine = build_engine("duckdb")
    why = engine.unreachable_reason("google_sheets")
    assert why is not None
    assert "the Google Sheets API reader" in why
    assert "gsheets" in why and ver in why and plat in why
    assert "google_sheets" not in engine.connectors
    assert not engine.reachable("google_sheets")
    with pytest.raises(UnreachableSource, match="gsheets"):
        engine.connector_for("google_sheets")


def test_firebird_on_linux_is_unreachable_naming_its_extension(tmp_path, monkeypatch):
    # Left out of the Linux bundle; its loader reads through the same extension, so no reader
    # lands it either.
    _staged(tmp_path, monkeypatch, _LINUX)
    engine = build_engine("duckdb")
    why = engine.unreachable_reason("firebird")
    assert why is not None and "firebird" in why
    assert "firebird" not in engine.connectors
    with pytest.raises(UnreachableSource, match="firebird"):
        engine.connector_for("firebird")


def test_firebird_with_provisas_own_driver_is_a_replica_where_it_is_not_staged(
    tmp_path, monkeypatch
):
    # provisa[firebird] installs a direct driver: it lands firebird where the extension is absent.
    from provisa.executor.drivers import registry
    from provisa.federation.strategy import Strategy, federate

    monkeypatch.setattr(registry, "available_drivers", lambda: {"firebird"})
    _staged(tmp_path, monkeypatch, _LINUX)
    engine = build_engine("duckdb")
    assert engine.unreachable_reason("firebird") is None
    assert not engine.connectors["firebird"].reads_in_place
    assert federate(_source("firebird"), engine) is Strategy.MATERIALIZED


def test_iceberg_needs_avro_staged_too(tmp_path, monkeypatch):
    _staged(tmp_path, monkeypatch, tuple(e for e in _LINUX if e != "avro"))
    engine = build_engine("duckdb")
    why = engine.unreachable_reason("iceberg")
    assert why is not None and "avro" in why


def test_an_unreachable_source_is_shown_when_added_naming_what_is_missing(tmp_path, monkeypatch):
    from provisa.api.admin.schema_common import engine_unreachable

    _staged(tmp_path, monkeypatch, _LINUX)
    state = types.SimpleNamespace(
        federation_engine=types.SimpleNamespace(engine=build_engine("duckdb"))
    )
    shown = engine_unreachable(state, "my-sheet", "google_sheets")
    assert shown is not None
    assert shown.success is False
    assert shown.code == "schema.source_unreachable"
    assert "gsheets" in shown.message and "my-sheet" in shown.message
    assert engine_unreachable(state, "pg", "postgresql") is None  # staged: attaches
    assert engine_unreachable(state, "mongo-1", "mongodb") is None  # not staged on Windows ...


def test_a_replica_source_is_added(tmp_path, monkeypatch):
    # ... and a source the engine cannot read in place but Provisa lands is added, a replica.
    from provisa.api.admin.schema_common import engine_unreachable

    _staged(tmp_path, monkeypatch, _WINDOWS)  # no mongo on Windows
    state = types.SimpleNamespace(
        federation_engine=types.SimpleNamespace(engine=build_engine("duckdb"))
    )
    assert engine_unreachable(state, "mongo-1", "mongodb") is None


# --- the server tier: DuckDB's own install path ----------------------------------------------


def test_with_no_staged_directory_every_extension_connector_stays(monkeypatch):
    monkeypatch.delenv("PROVISA_DUCKDB_EXT_DIR", raising=False)
    engine = build_engine("duckdb")
    for source_type in ("postgresql", "sqlserver", "firebird", "google_sheets", "iceberg"):
        assert engine.unreachable_reason(source_type) is None, source_type
        assert engine.connectors[source_type].reads_in_place, source_type


# --- MotherDuck (REQ-1998: a replica where it cannot attach, landed through its Postgres endpoint) ---


@pytest.mark.parametrize("staged", [None, _MACOS])
def test_motherduck_on_duckdb_2_needs_its_postgres_endpoint_reader_on_every_tier(
    tmp_path, monkeypatch, staged
):
    if staged is None:
        monkeypatch.delenv("PROVISA_DUCKDB_EXT_DIR", raising=False)
    else:
        _staged(tmp_path, monkeypatch, staged)
    engine = build_engine("duckdb")
    why = engine.unreachable_reason("motherduck")
    assert why is not None
    assert "MotherDuck's Postgres-endpoint reader" in why
    assert duckdb.__version__ in why and "1.5.6" in why
    assert "motherduck" not in engine.connectors
    with pytest.raises(UnreachableSource, match="Postgres-endpoint reader"):
        engine.connector_for("motherduck")


def test_motherduck_attaches_where_its_extension_loads_on_the_engines_duckdb(monkeypatch):
    # The DuckDB MotherDuck supports (<= 1.5.6) attaches it in place, as before the pin.
    from provisa.federation.connector_duckdb import DuckDBMotherDuckConnector

    monkeypatch.delenv("PROVISA_DUCKDB_EXT_DIR", raising=False)
    monkeypatch.setattr(duckdb, "__version__", "1.5.5")
    engine = build_engine("duckdb")
    assert engine.unreachable_reason("motherduck") is None
    assert isinstance(engine.connectors["motherduck"], DuckDBMotherDuckConnector)


def test_the_motherduck_extension_is_never_installed_on_duckdb_2(monkeypatch):
    # Unreachable before anything is attached: no INSTALL motherduck reaches DuckDB.
    loaded: list[str] = []
    monkeypatch.setattr(
        duckdb_extensions, "install_and_load", lambda con, ext, **_: loaded.append(ext)
    )
    monkeypatch.delenv("PROVISA_DUCKDB_EXT_DIR", raising=False)
    engine = build_engine("duckdb")
    with pytest.raises(UnreachableSource):
        engine.connector_for_source(_source("motherduck", password="t", database="db"))
    assert loaded == []
