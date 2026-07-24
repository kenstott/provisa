# Copyright (c) 2026 Kenneth Stott
# Canary: fb697c7d-e7c8-445b-9536-660f56cfecbb
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-827: cross-VENDOR governance-parity — the DIFFERENTIAL harness (Gap 1).

test_governance_parity_e2e.py proves parity for DuckDB and (optionally) one Postgres.
The per-engine ``test_*_federation_engine_e2e.py`` tests each drive the real pipeline for
ONE warehouse and never compare engines. So the highest-cost engines — Snowflake,
Databricks, BigQuery, Fabric — are validated only against themselves; a subtly-wrong
pushdown (case-folding collation, three-valued NULL logic, decimal coercion) that changes
which ROWS survive an RLS predicate would pass every existing test.

This harness closes that: it runs the SAME governed query (RLS ``region = 'west'``) through
the SAME pipeline (compile -> govern -> physical-rewrite -> transpile(dialect)) on EVERY
reachable engine and certifies each engine's governed rows against an engine-independent
ground truth via ``compare_governed_results``. Because every engine is diffed against the
same truth, agreement is transitive: all-certified == mutually-identical.

The golden dataset is chosen to STRESS the semantic edges that a capability lie would leak
through:
  - 'West' / 'WEST' rows      -> a case-insensitive engine wrongly admits them (collation)
  - a NULL-region row         -> a mis-handled three-valued predicate wrongly admits it
  - two 'west' rows, same     -> multiset / row-identity is preserved (id distinguishes)
    amount, different id

DuckDB runs in-process and is ALWAYS exercised (never a silent no-op). Each warehouse lane
skips individually — with an explicit reason — when its driver or credentials are absent; a
skip is never reported as a pass.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any, Callable

import pytest

from provisa.compiler.context import build_context
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.parser import parse_query
from provisa.compiler.rls import RLSContext
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.sql_gen import compile_query
from provisa.compiler.sql_rewrite import (
    rewrite_semantic_to_catalog_physical,
    rewrite_semantic_to_physical,
)
from provisa.compiler.stage2 import apply_governance, build_governance_context
from provisa.federation.conformance import compare_governed_results
from provisa.transpiler.transpile import transpile

pytestmark = [pytest.mark.integration]

_ADMIN = {"id": "admin", "capabilities": ["admin"], "domain_access": ["*"]}
_SCHEMA = "public"
_TABLE = "orders"

# --- golden dataset + the governed query (REQ-827) ---------------------------
# orders(id, region, amount); the analyst identity carries an RLS row filter region = 'west'.
_GOLDEN = [
    {"id": 1, "region": "west", "amount": 100.0},
    {"id": 2, "region": "West", "amount": 200.5},  # capital W: case-insensitive engine leaks this
    {"id": 3, "region": "west", "amount": 300.25},
    {"id": 4, "region": None, "amount": 400.0},  # NULL: bad 3-valued logic leaks this
    {"id": 5, "region": "west", "amount": 100.0},  # dup (region,amount) of id=1, distinct id
    {"id": 6, "region": "WEST", "amount": 500.0},  # all-caps: case-insensitive engine leaks this
]

_RLS = RLSContext(rules={1: "region = 'west'"}, domain_rules={})

# Engine-independent ground truth: exactly the rows a CORRECT engine returns for the governed
# query — case-sensitive equality, NULL excluded by three-valued logic. ids {1, 3, 5}.
_TRUTH = [
    (r["id"], r["region"], round(float(r["amount"]), 6))
    for r in _GOLDEN
    if r["region"] == "west"
]


def _canon(row: dict[str, Any] | tuple) -> tuple:
    """Normalize a returned row to (id:int, region:str|None, amount:float|None).

    Case-insensitive key lookup (Snowflake echoes quoted-lowercase, T-SQL/others vary) and
    numeric coercion (NUMBER/DECIMAL come back as Decimal) so comparison is on VALUES, not
    on a dialect's type-repr — decimal *precision* beyond 6 places is not a governance edge,
    but row membership and masking are exact.
    """
    if isinstance(row, dict):
        m = {k.lower(): v for k, v in row.items()}
        rid, region, amount = m["id"], m["region"], m["amount"]
    else:
        rid, region, amount = row
    return (
        int(rid),
        None if region is None else str(region),
        None if amount is None else round(float(amount), 6),
    )


def _col(n: str, d: str = "varchar", nl: bool = True) -> ColumnMetadata:
    return ColumnMetadata(column_name=n, data_type=d, is_nullable=nl)


def _schema_input(source_id: str, source_type: str, catalog: str | None) -> SchemaInput:
    kw: dict[str, Any] = {}
    if catalog is not None:
        kw["source_catalogs"] = {source_id: catalog}
    return SchemaInput(
        tables=[
            {
                "id": 1,
                "source_id": source_id,
                "domain_id": "sales",
                "schema_name": _SCHEMA,
                "table_name": _TABLE,
                "columns": [
                    {"column_name": c, "visible_to": ["admin"]}
                    for c in ("id", "region", "amount")
                ],
            }
        ],
        relationships=[],
        column_types={1: [_col("id", "integer", False), _col("region"), _col("amount", "double")]},
        naming_rules=[],
        role=_ADMIN,
        domains=[{"id": "sales", "description": "Sales"}],
        source_types={source_id: source_type},
        **kw,
    )


# --- vendor lanes ------------------------------------------------------------
# Adding a vendor = one Lane. Each lane owns only what genuinely differs per engine:
# how it connects, seeds the golden rows, executes governed SQL, and tears down. The
# compile+certify path is shared, so parity is asserted the SAME way for every engine.


@dataclass
class Lane:
    id: str
    dialect: str
    source_type: str
    driver_module: str  # importorskip target; "" for the always-on in-process reference
    cred_env: tuple[str, ...]
    catalog_rewrite: bool  # catalog-qualified physical name (warehouses) vs bare schema.table
    connect: Callable[[], Any]
    seed: Callable[[Any], Any]  # may be sync or async
    fetch: Callable[[Any, str], list]  # (runtime, sql) -> list[dict|tuple]
    teardown: Callable[[Any], None]
    source_id: str = ""
    catalog: str | None = None
    extra_env: tuple[str, ...] = field(default_factory=tuple)

    def compile_governed(self) -> str:
        si = _schema_input(self.source_id, self.source_type, self.catalog)
        ctx = build_context(si)
        compiled = compile_query(
            parse_query(generate_schema(si), "{ orders { id region amount } }", {}), ctx
        )[0]
        gov = apply_governance(
            compiled.sql,
            build_governance_context("admin", _RLS, {}, ctx, si.tables, role=_ADMIN),
        )
        rewrite = (
            rewrite_semantic_to_catalog_physical if self.catalog_rewrite else rewrite_semantic_to_physical
        )
        return transpile(rewrite(gov, ctx), self.dialect)

    def skip_reason(self) -> str | None:
        for v in self.cred_env:
            if not os.environ.get(v):
                return f"{self.id}: credential {v} not set"
        return None


# ---- DuckDB (in-process reference — ALWAYS runs) ----------------------------
def _duckdb_connect():
    import duckdb

    con = duckdb.connect()
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {_SCHEMA}")
    con.execute(f"CREATE TABLE {_SCHEMA}.{_TABLE} (id INTEGER, region VARCHAR, amount DOUBLE)")
    con.executemany(
        f"INSERT INTO {_SCHEMA}.{_TABLE} VALUES (?, ?, ?)",
        [(r["id"], r["region"], r["amount"]) for r in _GOLDEN],
    )
    return con


def _duckdb_fetch(con, sql) -> list[dict]:
    rows = con.execute(sql).fetchall()
    cols = [d[0] for d in con.description]
    return [dict(zip(cols, r)) for r in rows]


_DUCKDB = Lane(
    id="duckdb",
    dialect="duckdb",
    source_type="sqlite",  # a db-typed source rewrites to bare schema.table (main.orders)
    driver_module="duckdb",
    cred_env=(),
    catalog_rewrite=False,
    source_id="parity-duck",
    catalog=None,
    connect=_duckdb_connect,
    seed=lambda _rt: None,  # seeded on connect
    fetch=_duckdb_fetch,
    teardown=lambda con: con.close(),
)


# ---- Snowflake --------------------------------------------------------------
def _sf_connect():
    from urllib.parse import quote

    from provisa.federation.snowflake_runtime import SnowflakeFederationRuntime

    acct = os.environ["SNOWFLAKE_ACCOUNT"]
    user = quote(os.environ["SNOWFLAKE_USER"], safe="")
    pw = quote(os.environ["SNOWFLAKE_PASSWORD"], safe="")
    wh = os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
    cat = "parity_sf"
    return SnowflakeFederationRuntime(
        url=f"snowflake://{user}:{pw}@{acct}/{cat}/{_SCHEMA}?warehouse={wh}"
    )


def _sf_seed(rt):
    cat = "parity_sf"
    fq = f'"{cat}"."{_SCHEMA}"."{_TABLE}"'
    cur = rt.connection.cursor()
    try:
        cur.execute(f'CREATE DATABASE IF NOT EXISTS "{cat}"')
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{cat}"."{_SCHEMA}"')
        cur.execute(f'CREATE OR REPLACE TABLE {fq} ("id" NUMBER, "region" STRING, "amount" FLOAT)')
        cur.executemany(
            f"INSERT INTO {fq} VALUES (%s, %s, %s)",
            [(r["id"], r["region"], r["amount"]) for r in _GOLDEN],
        )
    finally:
        cur.close()


def _sf_teardown(rt):
    cur = rt.connection.cursor()
    try:
        cur.execute('DROP DATABASE IF EXISTS "parity_sf"')
    finally:
        cur.close()
    rt.close()


_SNOWFLAKE = Lane(
    id="snowflake",
    dialect="snowflake",
    source_type="snowflake",
    driver_module="snowflake.connector",
    cred_env=("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"),
    catalog_rewrite=True,
    source_id="parity-sf",  # -> catalog parity_sf (compiler naming: hyphen -> underscore)
    catalog=None,  # derived from source id
    connect=_sf_connect,
    seed=_sf_seed,
    fetch=lambda rt, sql: rt.run_arrow(sql).to_pylist(),
    teardown=_sf_teardown,
)


# ---- Databricks -------------------------------------------------------------
def _dbx_connect():
    from provisa.federation.databricks_runtime import DatabricksFederationRuntime

    return DatabricksFederationRuntime(
        url=(
            f"databricks://token:{os.environ['DATABRICKS_TOKEN']}"
            f"@{os.environ['DATABRICKS_SERVER_HOSTNAME']}"
            f"?http_path={os.environ['DATABRICKS_HTTP_PATH']}"
        )
    )


async def _dbx_seed(rt):
    src = SimpleNamespace(id="parity-dbx", type="databricks", schema_name=_SCHEMA, table_name=_TABLE)
    await rt.materialize_source(
        src, [("id", "bigint"), ("region", "text"), ("amount", "double")], _GOLDEN, change_signal="ttl"
    )


def _dbx_teardown(rt):
    from provisa.core.catalog import _to_catalog_name

    cur = rt.connection.cursor()
    try:
        cur.execute(f"DROP CATALOG IF EXISTS `{_to_catalog_name('parity-dbx')}` CASCADE")
    finally:
        cur.close()
    rt.close()


_DATABRICKS = Lane(
    id="databricks",
    dialect="databricks",
    source_type="databricks",
    driver_module="databricks.sql",
    cred_env=("DATABRICKS_SERVER_HOSTNAME", "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN"),
    catalog_rewrite=True,
    source_id="parity-dbx",
    catalog=None,
    connect=_dbx_connect,
    seed=_dbx_seed,
    fetch=lambda rt, sql: rt.run_arrow(sql).to_pylist(),
    teardown=_dbx_teardown,
)


# ---- BigQuery ---------------------------------------------------------------
_BQ_DS = "provisa_parity_it"


def _bq_connect():
    from provisa.federation.bigquery_runtime import BigQueryFederationRuntime

    proj = os.environ["GOOGLE_CLOUD_PROJECT"]
    return BigQueryFederationRuntime(url=f"bigquery://{proj}?location=US")


async def _bq_seed(rt):
    from provisa.core.models import SourceType

    src = SimpleNamespace(id="parity-bq", type=SourceType.bigquery, schema_name=_BQ_DS, table_name=_TABLE)
    await rt.materialize_source(
        src, [("id", "bigint"), ("region", "text"), ("amount", "double")], _GOLDEN, change_signal="ttl"
    )


def _bq_teardown(rt):
    proj = os.environ["GOOGLE_CLOUD_PROJECT"]
    rt.connection.query(f"DROP SCHEMA IF EXISTS `{proj}`.`{_BQ_DS}` CASCADE").result()
    rt.close()


_BIGQUERY = Lane(
    id="bigquery",
    dialect="bigquery",
    source_type="bigquery",
    driver_module="google.cloud.bigquery",
    cred_env=("GOOGLE_CLOUD_PROJECT", "GOOGLE_APPLICATION_CREDENTIALS"),
    catalog_rewrite=True,
    source_id="parity-bq",
    catalog=os.environ.get("GOOGLE_CLOUD_PROJECT"),  # BigQuery: catalog is the project
    connect=_bq_connect,
    seed=_bq_seed,
    fetch=lambda rt, sql: rt.run_arrow(sql).to_pylist(),
    teardown=_bq_teardown,
)


# ---- Microsoft Fabric (T-SQL warehouse) -------------------------------------
_FAB_SCH = "provisa_parity_it"


def _fab_connect():
    from provisa.federation.mssql_warehouse_runtime import MssqlWarehouseRuntime

    return MssqlWarehouseRuntime(
        server=os.environ["FABRIC_SQL_SERVER"], database=os.environ["FABRIC_DATABASE"], engine_name="fabric"
    )


async def _fab_seed(rt):
    from provisa.core.models import SourceType

    src = SimpleNamespace(id="parity-fab", type=SourceType.parquet, schema_name=_FAB_SCH, table_name=_TABLE)
    await rt.materialize_source(
        src, [("id", "bigint"), ("region", "text"), ("amount", "double")], _GOLDEN, change_signal="ttl"
    )


def _fab_teardown(rt):
    cur = rt.connection.cursor()
    try:
        cur.execute(f"DROP TABLE IF EXISTS [{_FAB_SCH}].[{_TABLE}]")
        cur.execute(f"DROP SCHEMA IF EXISTS [{_FAB_SCH}]")
        rt.connection.commit()
    finally:
        cur.close()
    rt.close()


_FABRIC = Lane(
    id="fabric",
    dialect="tsql",
    source_type="fabric",
    driver_module="pyodbc",
    cred_env=("FABRIC_SQL_SERVER", "FABRIC_DATABASE"),
    catalog_rewrite=True,
    source_id="parity-fab",
    catalog=os.environ.get("FABRIC_DATABASE"),  # T-SQL: catalog is the warehouse database
    connect=_fab_connect,
    seed=_fab_seed,
    fetch=lambda rt, sql: rt.run_arrow(sql).to_pylist(),
    teardown=_fab_teardown,
)


_LANES = [_DUCKDB, _SNOWFLAKE, _DATABRICKS, _BIGQUERY, _FABRIC]


async def _run_lane(lane: Lane) -> list[tuple]:
    """Connect -> seed -> execute governed SQL -> return canonicalized rows. Always tears down."""
    import inspect

    rt = lane.connect()
    try:
        seeded = lane.seed(rt)
        if inspect.isawaitable(seeded):
            await seeded
        rows = lane.fetch(rt, lane.compile_governed())
        return sorted(_canon(r) for r in rows)
    finally:
        lane.teardown(rt)


# --- the parity suite --------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("lane", _LANES, ids=[lane.id for lane in _LANES])
async def test_engine_governed_rows_certify_against_ground_truth(lane: Lane):
    """Each reachable engine's governed rows must diff clean against the engine-independent
    ground truth. A collation/NULL/coercion bug that changes which rows survive the RLS
    predicate diverges here — the failure per-engine tests cannot surface."""
    pytest.importorskip(lane.driver_module, reason=f"{lane.id}: driver not installed")
    reason = lane.skip_reason()
    if reason is not None:
        pytest.skip(reason)  # explicit per-lane skip — never a silent pass

    rows = await _run_lane(lane)
    result = compare_governed_results(sorted(_TRUTH), rows)
    assert result.certified, f"{lane.id} diverged from ground truth: {result.divergences}"


@pytest.mark.asyncio
async def test_duckdb_reference_lane_always_runs():
    """Guard against the whole file degenerating to all-skips: the in-process DuckDB lane must
    execute and certify on every run, so cross-vendor coverage is never silently reported as a
    pass when no warehouse creds are present."""
    rows = await _run_lane(_DUCKDB)
    assert compare_governed_results(sorted(_TRUTH), rows).certified
    assert len(rows) == 3  # ids {1,3,5} survived the RLS predicate


def test_ground_truth_excludes_case_and_null_variants():
    """The golden set MUST contain the case/NULL edge rows the harness relies on to catch a
    capability lie — otherwise a green run would prove nothing about collation/NULL handling."""
    regions = {r["region"] for r in _GOLDEN}
    assert {"West", "WEST"} <= regions and None in regions  # case + NULL edges present
    assert sorted(_TRUTH) == sorted([(1, "west", 100.0), (3, "west", 300.25), (5, "west", 100.0)])


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
