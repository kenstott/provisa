# Copyright (c) 2026 Kenneth Stott
# Canary: e5a71b0c-9d34-4f28-8b16-7c0af2e91d45
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Step definitions for REQ-1177 (config-driven custom source connectors) and REQ-1178 (config-driven
ClickHouse connectors + SQLite/Hudi OOTB reach).

Each step drives the REAL native runtime (DuckDB in-process, embedded chdb ClickHouse) or the real
engine builder against an operator-authored descriptor loaded via PROVISA_CUSTOM_CONNECTORS — proving
a new source_type becomes reachable with no code change, and that descriptor typos fail loud.
"""

from __future__ import annotations

import asyncio
import sqlite3
import textwrap
from pathlib import Path
from types import SimpleNamespace

import pytest

from pytest_bdd import given, when, then, scenarios

duckdb = pytest.importorskip("duckdb")
openpyxl = pytest.importorskip("openpyxl")
chdb = pytest.importorskip("chdb")

from provisa.core.catalog import _to_catalog_name  # noqa: E402
from provisa.federation.custom_connectors import (  # noqa: E402
    _probe_clickhouse_engine,
    load_custom_connectors,
)
from provisa.federation.duckdb_runtime import DuckDBFederationRuntime  # noqa: E402
from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime  # noqa: E402
from provisa.federation.engine import build_pg_engine  # noqa: E402

scenarios("../features/REQ-1177.feature")
scenarios("../features/REQ-1178.feature")


@pytest.fixture
def cc_ctx():
    """Per-scenario context holding runtimes to close and cross-step state."""
    state: dict = {"runtimes": []}
    yield state
    for rt in state["runtimes"]:
        rt.close()


def _write(tmp_path: Path, body: str) -> Path:
    p = tmp_path / "custom_connectors.yaml"
    p.write_text(textwrap.dedent(body))
    return p


def _sqlite(path: Path) -> None:
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE widget(id INTEGER, name TEXT, qty INTEGER)")
    con.executemany(
        "INSERT INTO widget VALUES (?,?,?)", [(1, "sprocket", 10), (2, "gear", 20), (3, "cog", 30)]
    )
    con.commit()
    con.close()


def _phys(source) -> str:
    return f'"{_to_catalog_name(source.id)}"."{source.schema_name}"."{source.table_name}"'


# --------------------------------------------------------------------------- REQ-1177


@given("an operator declares a custom source connector in config/custom_connectors.yaml with no code change")
def _declares(cc_ctx):
    cc_ctx["declared"] = True


@when("the descriptor is a duckdb_attach for a new source_type over a real ducklake catalog")
def _duckdb_attach(cc_ctx, tmp_path, monkeypatch):
    cat = tmp_path / "cat.ducklake"
    data = tmp_path / "lake_data"
    data.mkdir()
    seed = duckdb.connect()
    seed.execute(f"ATTACH 'ducklake:{cat}' AS lake (DATA_PATH '{data}')")
    seed.execute("CREATE TABLE lake.events(id INTEGER, kind VARCHAR)")
    seed.execute("INSERT INTO lake.events VALUES (1,'shipped'),(2,'placed'),(3,'shipped')")
    seed.close()

    cfg = _write(
        tmp_path,
        f"""
        connectors:
          - engine: duckdb
            source_type: ducklake
            kind: duckdb_attach
            extension: ducklake
            install_from_community: false
            probe_symbol: ducklake_snapshots
            mechanism: attach_rw
            attach_template: "ATTACH 'ducklake:{{path}}' AS \\"{{alias}}\\" (DATA_PATH '{{data_path}}')"
            remote_schema: main
        """,
    )
    monkeypatch.setenv("PROVISA_CUSTOM_CONNECTORS", str(cfg))
    rt = DuckDBFederationRuntime()
    cc_ctx["runtimes"].append(rt)
    src = SimpleNamespace(
        id="lake", type=SimpleNamespace(value="ducklake"), schema_name="main",
        table_name="events", path=str(cat), federation_hints={"data_path": str(data)},
    )
    rt.attach_source(src)
    cc_ctx["duckdb_attach"] = (rt, src)


@then("the DuckDB engine reaches that source_type and the runtime attaches it and returns its rows")
def _duckdb_attach_rows(cc_ctx):
    rt, src = cc_ctx["duckdb_attach"]
    assert rt._engine.reachable("ducklake")
    res = rt.run_sync(f"SELECT id, kind FROM {_phys(src)} ORDER BY id")
    assert [r[0] for r in res.rows] == [1, 2, 3]
    assert res.rows[0][1] == "shipped"


@when("the descriptor is a duckdb_scan for a new source_type over a real .xlsx via read_xlsx")
def _duckdb_scan(cc_ctx, tmp_path, monkeypatch):
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Q1"
    ws.append(["id", "name"])
    ws.append([1, "alice"])
    ws.append([2, "bob"])
    xlsx = tmp_path / "sales.xlsx"
    wb.save(xlsx)

    cfg = _write(
        tmp_path,
        """
        connectors:
          - engine: duckdb
            source_type: excel
            kind: duckdb_scan
            extension: excel
            install_from_community: false
            probe_symbol: read_xlsx
            scan_template: "read_xlsx('{path}', sheet='{sheet}', header=true)"
        """,
    )
    monkeypatch.setenv("PROVISA_CUSTOM_CONNECTORS", str(cfg))
    rt = DuckDBFederationRuntime()
    cc_ctx["runtimes"].append(rt)
    src = SimpleNamespace(
        id="sales", type=SimpleNamespace(value="excel"), schema_name="main",
        table_name="sales", path=str(xlsx), federation_hints={"sheet": "Q1"},
    )
    rt.attach_source(src)
    cc_ctx["duckdb_scan"] = (rt, src)


@then("the DuckDB engine reaches that source_type and the runtime scans it in place and returns its rows")
def _duckdb_scan_rows(cc_ctx):
    rt, src = cc_ctx["duckdb_scan"]
    assert rt._engine.reachable("excel")
    res = rt.run_sync(f"SELECT id, name FROM {_phys(src)} ORDER BY id")
    assert [r[0] for r in res.rows] == [1, 2]
    assert res.rows[1][1] == "bob"


@when("the descriptor is a generic pg_fdw for a new source_type")
def _pg_fdw(cc_ctx, tmp_path, monkeypatch):
    cfg = _write(
        tmp_path,
        """
        connectors:
          - engine: postgres
            source_type: pgfdw_custom
            kind: pg_fdw
            extension: postgres_fdw
            mechanism: attach_r
            supports_import: true
            server_options:
              host: "{host}"
              port: "{port}"
              dbname: "{database}"
            user_mapping:
              user: "{username}"
            remote_schema: "{schema}"
        """,
    )
    monkeypatch.setenv("PROVISA_CUSTOM_CONNECTORS", str(cfg))
    engine = build_pg_engine()
    src = SimpleNamespace(
        id="wid", type=SimpleNamespace(value="pgfdw_custom"), host="db.internal", port=5432,
        database="warehouse", username="reader", password="", schema_name="inventory",
        table_name="widgets", federation_hints={"schema": "demo_remote"},
    )
    cc_ctx["pg"] = (engine, src)


@then("the Postgres engine reaches that source_type and its connector emits standard SQL/MED IMPORT FOREIGN SCHEMA DDL")
def _pg_fdw_ddl(cc_ctx):
    engine, src = cc_ctx["pg"]
    assert engine.reachable("pgfdw_custom")
    details = engine.resolve(src).details
    ddl = "\n".join(details["attach_ddl"])
    assert "CREATE SERVER" in ddl and "FOREIGN DATA WRAPPER postgres_fdw" in ddl
    assert "CREATE USER MAPPING" in ddl
    assert "IMPORT FOREIGN SCHEMA demo_remote" in ddl


@then("a descriptor naming an unknown kind fails loud at load rather than leaving the source_type silently unreachable")
def _unknown_kind_fails_loud(cc_ctx, tmp_path, monkeypatch):
    cfg = _write(
        tmp_path,
        """
        connectors:
          - engine: duckdb
            source_type: bogus
            kind: duckdb_teleport
        """,
    )
    monkeypatch.setenv("PROVISA_CUSTOM_CONNECTORS", str(cfg))
    with pytest.raises(ValueError, match="unknown kind"):
        load_custom_connectors("duckdb")


# --------------------------------------------------------------------------- REQ-1178


@given("a ClickHouse federation engine")
def _ch_engine(cc_ctx):
    cc_ctx["ch"] = True


@when("an operator registers a SQLite source (OOTB, no config)")
def _ch_sqlite_ootb(cc_ctx, tmp_path):
    db = tmp_path / "shop.db"
    _sqlite(db)
    rt = ClickHouseFederationRuntime.embedded()
    cc_ctx["runtimes"].append(rt)
    src = SimpleNamespace(
        id="shop", type=SimpleNamespace(value="sqlite"), path=str(db),
        schema_name="inv", table_name="widget", federation_hints={},
    )
    rt.attach_source(src)
    cc_ctx["ch_ootb"] = rt


@then("the engine reaches sqlite live and the runtime returns its federated rows")
def _ch_sqlite_rows(cc_ctx):
    rt = cc_ctx["ch_ootb"]
    assert rt._engine.reachable("sqlite")
    rows = rt.run_sync('SELECT "name" FROM "inv"."widget" WHERE "qty" >= 20 ORDER BY "id"')
    assert [r[0] for r in rows.rows] == ["gear", "cog"]


@when("an operator declares a clickhouse_database/table/scan connector for a new source_type in config/custom_connectors.yaml")
def _ch_config_driven(cc_ctx, tmp_path, monkeypatch):
    db = tmp_path / "ledger.db"
    _sqlite(db)
    cfg = _write(
        tmp_path,
        """
        connectors:
          - engine: clickhouse
            source_type: sqlite_custom
            kind: clickhouse_database
            ch_engine: SQLite
            engine_template: "SQLite('{path}')"
        """,
    )
    monkeypatch.setenv("PROVISA_CUSTOM_CONNECTORS", str(cfg))
    rt = ClickHouseFederationRuntime.embedded()  # builds its engine AFTER the env override
    cc_ctx["runtimes"].append(rt)
    src = SimpleNamespace(
        id="ledger", type=SimpleNamespace(value="sqlite_custom"), path=str(db),
        schema_name="fin", table_name="widget", federation_hints={},
    )
    rt.attach_source(src)
    cc_ctx["ch_custom"] = (rt, src)


@then("the engine reaches that new source_type with no code change")
def _ch_config_rows(cc_ctx):
    rt, src = cc_ctx["ch_custom"]
    assert rt._engine.reachable("sqlite_custom")
    rows = rt.run_sync(f'SELECT "id", "name" FROM {_phys_ch(src)} ORDER BY "id"')
    assert [(r[0], r[1]) for r in rows.rows] == [(1, "sprocket"), (2, "gear"), (3, "cog")]


def _phys_ch(source) -> str:
    return f'"{source.schema_name}"."{source.table_name}"'


@then("an absent ClickHouse integration engine fails loud at attach time")
def _ch_absent_fails_loud(cc_ctx):
    rt = cc_ctx["ch_custom"][0]

    async def _fetch(sql):  # a rows-returning probe over the live embedded engine
        return rt.run_sync(sql).rows

    loop = asyncio.new_event_loop()
    try:
        result = loop.run_until_complete(_probe_clickhouse_engine(_fetch, "NoSuchEngine9000"))
    finally:
        loop.close()
    assert result.available is False
    assert result.remediation
