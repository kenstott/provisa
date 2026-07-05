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
    build_snowflake_engine,
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
    assert build_snowflake_engine().driver_class() is DriverClass.SELF_ONLY


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
    # REQ-894/895: Snowflake is SELF_ONLY reach yet MPP; DuckDB reaches several sources, single-node.
    assert build_trino_engine().mpp is True
    assert build_snowflake_engine().mpp is True
    assert build_duckdb_engine().mpp is False


def test_embedded_pg_lands_every_source_type():
    # REQ-893: stock embedded PG (no FDWs) is SELF_ONLY — every source lands into its native store.
    from provisa.federation.engine import build_embedded_pg_engine
    from provisa.federation.strategy import Strategy, federate

    eng = build_embedded_pg_engine()
    assert eng.driver_class() is DriverClass.SELF_ONLY and eng.mpp is False
    for st in (SourceType.csv, SourceType.sqlite, SourceType.postgresql):
        assert federate(_src("x", st), eng) is Strategy.MATERIALIZED  # landed, not attached


def test_pg_fdw_engine_attaches_in_place():
    # REQ-893: the FDW engine reaches postgres/csv via ATTACH (in place), not materialization.
    from provisa.federation.engine import build_postgres_engine

    eng = build_postgres_engine()
    assert eng.driver_class() is DriverClass.PARTIAL
    assert eng.reachable("postgresql") and eng.reachable("csv")


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
    eng = build_snowflake_engine()
    entry = eng.resolve(_src("sf", SourceType.snowflake))
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
