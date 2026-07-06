# Copyright (c) 2026 Kenneth Stott
# Canary: 1d7a9c40-3b28-4e75-8f02-6c2a0d4f9b83
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-840/841/842/843: federation engine + connector abstraction."""

from __future__ import annotations

import pytest

from provisa.core.models import Source, SourceType
from provisa.federation.connector import Mechanism, TrinoPostgresConnector
from provisa.federation.engine import (
    DriverClass,
    FederationEngine,
    UnreachableSource,
    build_duckdb_engine,
    build_sqlalchemy_engine,
    build_trino_engine,
)


def _src(sid: str, type_: SourceType, **kw) -> Source:
    fields = {"host": "h", "port": 5432, "database": "db", "username": "u", **kw}
    return Source(id=sid, type=type_, **fields)


_PG = _src("orders_pg", SourceType.postgresql)
_MYSQL = _src("inv_mysql", SourceType.mysql)


# ---- reachability (REQ-840) -------------------------------------------------


def test_reachability_is_connector_presence():
    eng = build_duckdb_engine()
    assert eng.reachable("postgresql") is True
    assert eng.reachable("csv") is True
    assert eng.reachable("mysql") is False  # no DuckDB mysql connector


def test_connector_for_unreachable_raises():
    eng = build_duckdb_engine()
    with pytest.raises(UnreachableSource):
        eng.connector_for("oracle")


# ---- driver classes defined by collection contents (REQ-840) ----------------


def test_driver_classes():
    assert build_trino_engine().driver_class() is DriverClass.BROAD
    assert build_duckdb_engine().driver_class() is DriverClass.PARTIAL
    assert build_sqlalchemy_engine("postgresql://h/db").driver_class() is DriverClass.SELF_ONLY


def test_duckdb_reaches_file_and_db_sources():
    # REQ-840: DuckDB attaches postgres/sqlite in place and scans csv/parquet files.
    eng = build_duckdb_engine()
    for st in ("postgresql", "sqlite", "csv", "parquet"):
        assert eng.reachable(st) is True


def test_duckdb_sqlite_attach_details():
    entry = build_duckdb_engine().resolve(_src("inq", SourceType.sqlite, path="/data/inq.sqlite"))
    assert "ATTACH" in entry.details["attach"] and "TYPE sqlite" in entry.details["attach"]


def test_duckdb_parquet_view_details():
    entry = build_duckdb_engine().resolve(_src("prod", SourceType.parquet, path="/data/p.parquet"))
    assert "read_parquet" in entry.details["view_ddl"]


def test_mpp_is_declared_and_orthogonal_to_reach():
    # REQ-894/895: MPP is orthogonal to reach — a SELF_ONLY engine can still be MPP
    # (declared explicitly), while DuckDB reaches several sources yet is single-node.
    assert build_trino_engine().mpp is True
    assert build_duckdb_engine().mpp is False
    from provisa.federation.connector import WarehouseNativeConnector

    self_only_mpp = FederationEngine(
        "wh",
        [WarehouseNativeConnector("wh", "wh")],
        native_store="wh",
        driver_class=DriverClass.SELF_ONLY,
        mpp=True,
    )
    assert self_only_mpp.driver_class() is DriverClass.SELF_ONLY and self_only_mpp.mpp is True


def test_build_engine_selects_the_four_engines(monkeypatch):
    # REQ-840: one factory picks the engine by name; the four engines are trino/pg/duckdb/sqlalchemy.
    from provisa.federation.engine import build_engine

    monkeypatch.delenv("PROVISA_ENGINE", raising=False)
    monkeypatch.delenv("PROVISA_ENGINE_URL", raising=False)
    assert build_engine().name == "trino"  # default
    assert build_engine("duckdb").name == "duckdb"
    assert build_engine("pg").name == "postgres"
    monkeypatch.setenv("PROVISA_ENGINE", "duckdb")
    assert build_engine().name == "duckdb"  # env-selected
    for gone in ("embedded-pg", "snowflake", "bogus"):
        with pytest.raises(ValueError, match="unknown PROVISA_ENGINE"):
            build_engine(gone)


def test_sqlalchemy_engine_is_url_defined_self_only(monkeypatch):
    # REQ-905: the sqlalchemy engine is any RDB with a working SQLAlchemy URI — self-only,
    # zero connectors; its scheme names the native store.
    from provisa.federation.engine import build_engine, build_sqlalchemy_engine
    from provisa.federation.strategy import Strategy, federate

    eng = build_sqlalchemy_engine("postgresql+psycopg2://h/db")
    assert eng.driver_class() is DriverClass.SELF_ONLY and eng.native_store == "postgresql"
    for st in (SourceType.csv, SourceType.sqlite, SourceType.postgresql):
        assert federate(_src("x", st), eng) is Strategy.MATERIALIZED  # landed, not attached
    monkeypatch.setenv("PROVISA_ENGINE", "sqlalchemy")
    monkeypatch.setenv("PROVISA_ENGINE_URL", "mysql+pymysql://u:p@h/db")
    assert build_engine().native_store == "mysql"  # env URL drives the store
    monkeypatch.delenv("PROVISA_ENGINE_URL")
    with pytest.raises(ValueError, match="requires a URL"):
        build_engine()


def test_pg_engine_registers_all_prebuilt_connectors():
    # REQ-904: one Postgres engine with every prebuilt connector def; optimistic before discovery.
    from provisa.federation.engine import build_pg_engine

    eng = build_pg_engine()
    assert eng.driver_class() is DriverClass.PARTIAL
    for st in ("postgresql", "csv", "parquet", "json"):
        assert eng.reachable(st) is True


def _fake_pg(*, extensions=(), available=(), preload=""):
    """An async fetch double over a Postgres: canned answers for the probe queries."""

    async def fetch(sql: str):
        if "shared_preload_libraries" in sql:
            return [{"v": preload}]
        if "pg_extension" in sql:
            ext = sql.split("extname = '")[1].split("'")[0]
            return [{"?": 1}] if ext in extensions else []
        if "pg_available_extensions" in sql:
            name = sql.split("name = '")[1].split("'")[0]
            return [{"?": 1}] if name in available else []
        return []

    return fetch


@pytest.mark.asyncio
async def test_discover_disables_connectors_whose_probe_fails():
    # REQ-904: probe is availability truth — pg_duckdb present but NOT preloaded -> disabled.
    from provisa.federation.engine import build_pg_engine

    eng = build_pg_engine()
    # postgres_fdw + file_fdw installable; pg_duckdb installed but not in shared_preload_libraries.
    fetch = _fake_pg(available=("postgres_fdw", "file_fdw"), extensions=("pg_duckdb",), preload="")
    report = await eng.discover(fetch)
    assert eng.reachable("postgresql") is True  # postgres_fdw probes available
    assert eng.reachable("csv") is True  # falls back to file_fdw (pg_duckdb_csv failed)
    assert eng.reachable("parquet") is False and eng.reachable("json") is False  # pg_duckdb only
    assert report["pg_duckdb_csv"].available is False
    assert "shared_preload_libraries" in report["pg_duckdb_csv"].reason
    with pytest.raises(UnreachableSource):  # unavailable -> explicit, never a silent land
        eng.resolve(_src("p", SourceType.parquet, path="/x.parquet"))


@pytest.mark.asyncio
async def test_discover_pg_duckdb_wins_csv_when_available():
    # REQ-904: with pg_duckdb preloaded, it owns csv (precedence) and unlocks parquet/json.
    from provisa.federation.connector import PgDuckdbCsvConnector
    from provisa.federation.engine import build_pg_engine

    eng = build_pg_engine()
    fetch = _fake_pg(extensions=("pg_duckdb", "file_fdw"), preload="pg_duckdb")
    await eng.discover(fetch)
    assert isinstance(eng.connector_for("csv"), PgDuckdbCsvConnector)
    for st in ("csv", "parquet", "json"):
        assert eng.reachable(st) is True


@pytest.mark.asyncio
async def test_discover_override_strikes_connector_without_probing():
    # REQ-904: a struck connector is never probed; csv then falls back to file_fdw.
    from provisa.federation.connector import FileFdwConnector
    from provisa.federation.engine import build_pg_engine

    eng = build_pg_engine()
    fetch = _fake_pg(extensions=("pg_duckdb", "file_fdw"), preload="pg_duckdb")
    report = await eng.discover(fetch, disabled=frozenset({"pg_duckdb_csv"}))
    assert "not probed" in report["pg_duckdb_csv"].reason
    assert isinstance(eng.connector_for("csv"), FileFdwConnector)  # struck -> fallback wins csv
    assert eng.reachable("parquet") is True  # other pg_duckdb connectors still probed/available


def test_swapping_engine_swaps_reachability():
    trino, duck = build_trino_engine(), build_duckdb_engine()
    assert trino.reachable("mysql") and not duck.reachable("mysql")


# ---- mechanism is fixed by the connector (REQ-841/842) ----------------------


def test_trino_postgres_attach_details():
    entry = build_trino_engine().resolve(_PG)
    assert entry.mechanism is Mechanism.ATTACH
    assert entry.engine == "trino" and entry.source_type == "postgresql"
    assert entry.details["connection-url"] == "jdbc:postgresql://h:5432/db"


def test_duckdb_postgres_attach_dsn():
    entry = build_duckdb_engine().resolve(_PG)
    assert entry.mechanism is Mechanism.ATTACH
    assert "ATTACH" in entry.details["attach"] and "TYPE postgres" in entry.details["attach"]


def test_warehouse_native_lands_into_self():
    eng = build_sqlalchemy_engine("postgresql://h/db")
    entry = eng.resolve(_src("pg", SourceType.postgresql))
    assert entry.mechanism is Mechanism.LAND
    assert entry.details == {}  # already native — nothing to attach


def test_unreachable_source_rejected_at_resolve():
    # Trino has no connector for oracle in this build → rejected, not landed.
    with pytest.raises(UnreachableSource):
        build_trino_engine().resolve(_src("ora", SourceType.oracle))


# ---- derived catalog + reconcile (REQ-843) ----------------------------------


def test_reconcile_projects_only_reachable():
    eng = build_duckdb_engine()
    entries = eng.reconcile([_PG, _MYSQL])  # mysql unreachable on duckdb
    names = {e.name for e in entries}
    assert names == {"orders_pg"}
    assert eng.catalog.get("orders_pg") is not None
    assert eng.catalog.get("inv_mysql") is None


def test_on_asset_create_and_drop():
    eng = build_trino_engine()
    eng.on_asset_create(_PG)
    assert eng.catalog.get("orders_pg") is not None
    eng.on_asset_drop("orders_pg")
    assert eng.catalog.get("orders_pg") is None


def test_ensure_entry_reprojects_when_stale():
    eng = build_trino_engine()
    # Seed a stale entry, then ensure_entry must re-project from the registry (never keep stale).
    stale = eng.resolve(_PG)
    eng.catalog.add(stale)
    updated = _src("orders_pg", SourceType.postgresql, database="db2")
    fresh = eng.ensure_entry(updated)
    assert fresh.details["connection-url"].endswith("/db2")
    assert eng.catalog.get("orders_pg").details["connection-url"].endswith("/db2")


def test_reconcile_rebuilds_full_projection():
    eng = build_trino_engine()
    eng.on_asset_create(_PG)
    eng.on_asset_create(_MYSQL)
    # A reconcile with only _PG drops the mysql entry (registry is source of truth).
    eng.reconcile([_PG])
    assert eng.catalog.get("orders_pg") is not None
    assert eng.catalog.get("inv_mysql") is None


def test_direct_engine_construction_from_connectors():
    eng = FederationEngine("custom", [TrinoPostgresConnector()])
    assert eng.reachable("postgresql")
    assert not eng.reachable("mysql")
