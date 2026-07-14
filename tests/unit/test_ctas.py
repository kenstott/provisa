# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""CTAS cluster (REQ-996..1002): parse/recognize, resolve+validate, route, transactional land, coerce.

Pure contracts are tested directly; the transactional land (REQ-1001) runs against a real SQLite
store so the write face is exercised for real (the engine is never involved)."""

from __future__ import annotations

import pytest

from provisa.executor.ctas import (
    CtasAmbiguousPlacement,
    CtasCatalogMismatch,
    CtasError,
    CtasNameCollision,
    CtasNoWritableTarget,
    CtasRoute,
    CtasStatement,
    ResolvedTarget,
    WritableSource,
    coerce_result_columns,
    decide_ctas_route,
    execute_ctas,
    parse_ctas,
    resolve_ctas,
)
from provisa.executor.result import QueryResult

_SNOW = WritableSource(
    source_id="snowflake_prod", source_type="snowflake", catalog="snowflake_prod"
)
_STAGE = WritableSource(
    source_id="snowflake_staging", source_type="snowflake", catalog="snowflake_staging"
)


# --- REQ-996: recognize CTAS --------------------------------------------------------------------


def test_parse_ctas_recognizes_move():
    stmt = parse_ctas("CREATE TABLE schema_b.new_table AS SELECT col1, col2 FROM schema_a.table_a")
    assert stmt is not None
    assert (stmt.catalog, stmt.schema, stmt.table) == (None, "schema_b", "new_table")
    assert "SELECT" in stmt.select_sql and "table_a" in stmt.select_sql


def test_parse_ctas_reads_catalog_off_grammar():
    stmt = parse_ctas("CREATE TABLE snowflake.schema_a.t AS SELECT 1 AS x")
    assert stmt is not None
    assert (stmt.catalog, stmt.schema, stmt.table) == ("snowflake", "schema_a", "t")


def test_parse_plain_create_table_is_not_ctas():
    assert parse_ctas("CREATE TABLE s.t (id INT, name TEXT)") is None


def test_parse_non_create_is_not_ctas():
    assert parse_ctas("SELECT 1") is None


def test_parse_ctas_requires_schema_qualification():
    with pytest.raises(CtasError, match="schema-qualified"):
        parse_ctas("CREATE TABLE bare AS SELECT 1 AS x")


# --- REQ-998: global schema.table uniqueness ----------------------------------------------------


def test_resolve_rejects_name_collision():
    stmt = CtasStatement(None, "schema_a", "users", "SELECT 1")
    with pytest.raises(CtasNameCollision, match="already exists"):
        resolve_ctas(stmt, {"schema_a.users"}, {"schema_a": [_SNOW]})


def test_resolve_accepts_unique_name():
    stmt = CtasStatement(None, "schema_a", "new_table", "SELECT 1")
    resolved = resolve_ctas(stmt, {"schema_a.users"}, {"schema_a": [_SNOW]})
    assert resolved.source is _SNOW


# --- REQ-999: catalog names the source (self-documenting / mismatch is an error) -----------------


def test_resolve_catalog_match_proceeds():
    stmt = CtasStatement("snowflake_prod", "schema_a", "t", "SELECT 1")
    resolved = resolve_ctas(stmt, set(), {"schema_a": [_SNOW]})
    assert resolved.source is _SNOW


def test_resolve_catalog_mismatch_rejected():
    stmt = CtasStatement("bigquery", "schema_a", "t", "SELECT 1")
    with pytest.raises(CtasCatalogMismatch, match="catalog"):
        resolve_ctas(stmt, set(), {"schema_a": [_SNOW]})


# --- REQ-1000: placement disambiguation at CREATE -----------------------------------------------


def test_resolve_single_writable_no_catalog_needed():
    stmt = CtasStatement(None, "schema_unique", "t", "SELECT 1")
    resolved = resolve_ctas(stmt, set(), {"schema_unique": [_SNOW]})
    assert resolved.source is _SNOW


def test_resolve_multi_writable_without_catalog_rejected():
    stmt = CtasStatement(None, "schema_shared", "t", "SELECT 1")
    with pytest.raises(CtasAmbiguousPlacement, match="multiple writable sources"):
        resolve_ctas(stmt, set(), {"schema_shared": [_SNOW, _STAGE]})


def test_resolve_multi_writable_catalog_disambiguates():
    stmt = CtasStatement("snowflake_prod", "schema_shared", "t", "SELECT 1")
    resolved = resolve_ctas(stmt, set(), {"schema_shared": [_SNOW, _STAGE]})
    assert resolved.source is _SNOW


def test_resolve_no_writable_source_rejected():
    stmt = CtasStatement(None, "schema_ro", "t", "SELECT 1")
    with pytest.raises(CtasNoWritableTarget, match="no writable source"):
        resolve_ctas(stmt, set(), {})


# --- REQ-997: same-engine zero-copy vs cross-engine routing -------------------------------------


def test_route_same_engine_is_zero_copy():
    target = ResolvedTarget("s", "t", _SNOW)
    assert decide_ctas_route("snowflake_prod", target.source.source_id) is CtasRoute.ZERO_COPY


def test_route_cross_engine_lands():
    target = ResolvedTarget("s", "t", _SNOW)
    assert decide_ctas_route("bigquery_src", target.source.source_id) is CtasRoute.CROSS_ENGINE


def test_route_federated_select_is_cross_engine():
    # A multi-source SELECT has no single engine key → never treated as same-engine.
    assert decide_ctas_route(None, "snowflake_prod") is CtasRoute.CROSS_ENGINE


# --- REQ-1002: cross-engine result-schema → target DDL type coercion ----------------------------


def test_coerce_complex_and_decimal_types():
    cols = coerce_result_columns(
        ["tags", "meta", "amount"],
        ["array(varchar)", "row(name varchar, value integer)", "decimal(38,10)"],
        "trino",
    )
    assert cols == [("tags", "text"), ("meta", "text"), ("amount", "numeric")]


def test_coerce_missing_types_is_rejected():
    with pytest.raises(CtasError, match="requires result column types"):
        coerce_result_columns(["a"], None, "trino")


def test_coerce_length_mismatch_rejected():
    with pytest.raises(CtasError, match="result schema mismatch"):
        coerce_result_columns(["a", "b"], ["integer"], "trino")


# --- REQ-996 happy path: orchestrator on both routes --------------------------------------------


@pytest.mark.asyncio
async def test_execute_ctas_zero_copy_pushes_native_ddl():
    stmt = parse_ctas("CREATE TABLE s.t AS SELECT 1 AS x")
    resolved = ResolvedTarget("s", "t", _SNOW)
    captured = {}

    async def _engine_ctas(sql: str) -> int:
        captured["sql"] = sql
        return -1

    async def _select():  # must NOT be called on the zero-copy path
        raise AssertionError("zero-copy must not spool through the SELECT")

    async def _land(**_):
        raise AssertionError("zero-copy must not land")

    n = await execute_ctas(
        stmt,
        resolved,
        CtasRoute.ZERO_COPY,
        run_engine_ctas=_engine_ctas,
        run_select=_select,
        land=_land,
        source_platform="snowflake",
    )
    assert n == -1
    assert captured["sql"] == "CREATE TABLE s.t AS SELECT 1 AS x"


@pytest.mark.asyncio
async def test_execute_ctas_cross_engine_spools_and_lands():
    stmt = parse_ctas("CREATE TABLE s.t AS SELECT id, tags FROM a.b")
    resolved = ResolvedTarget("s", "t", _SNOW)
    landed = {}

    async def _engine_ctas(_sql: str) -> int:
        raise AssertionError("cross-engine must not push native CTAS")

    async def _select():
        return QueryResult(
            rows=[(1, "x"), (2, "y")],
            column_names=["id", "tags"],
            column_types=["integer", "array(varchar)"],
        )

    async def _land(*, schema, table, columns, rows):
        landed.update(schema=schema, table=table, columns=columns, rows=rows)
        return f"{schema}.{table}"

    n = await execute_ctas(
        stmt,
        resolved,
        CtasRoute.CROSS_ENGINE,
        run_engine_ctas=_engine_ctas,
        run_select=_select,
        land=_land,
        source_platform="trino",
    )
    assert n == 2
    assert landed["columns"] == [("id", "integer"), ("tags", "text")]
    assert landed["rows"] == [{"id": 1, "tags": "x"}, {"id": 2, "tags": "y"}]


# --- REQ-1001: transactional temp → swap ---------------------------------------------------------

from provisa.federation import store_writer  # noqa: E402

_COLS = [("id", "bigint"), ("status", "text")]


def _dsn(tmp_path) -> str:
    return f"sqlite+aiosqlite:///{tmp_path / 'store.db'}"


async def _table_names(dsn: str) -> list[str]:
    async with store_writer.store_connection(dsn) as conn:
        rows = await conn.fetch("SELECT name FROM sqlite_master WHERE type='table'")
    return [r[0] for r in rows]


@pytest.mark.asyncio
async def test_land_ctas_happy_path_swaps_into_place(tmp_path):
    dsn = _dsn(tmp_path)
    loc = await store_writer.land_ctas(
        dsn,
        schema="",
        table="snap",
        columns=_COLS,
        rows=[{"id": 1, "status": "a"}, {"id": 2, "status": "b"}],
    )
    assert loc == "snap"
    assert "snap" in await _table_names(dsn)
    assert not [t for t in await _table_names(dsn) if t.startswith("__ctas_")]
    async with store_writer.store_connection(dsn) as conn:
        rows = await conn.fetch("SELECT id, status FROM snap ORDER BY id")
    assert [(r[0], r[1]) for r in rows] == [(1, "a"), (2, "b")]


@pytest.mark.asyncio
async def test_land_ctas_midload_failure_leaves_no_partial_table(tmp_path, monkeypatch):
    dsn = _dsn(tmp_path)

    async def _boom(_conn, _tbl, _rows):
        raise RuntimeError("bulk load failed at 50%")

    monkeypatch.setattr(store_writer, "_bulk_insert", _boom)
    with pytest.raises(RuntimeError, match="bulk load failed"):
        await store_writer.land_ctas(
            dsn, schema="", table="snap", columns=_COLS, rows=[{"id": 1, "status": "a"}]
        )
    names = await _table_names(dsn)
    assert "snap" not in names  # target never created
    assert not [t for t in names if t.startswith("__ctas_")]  # temp cleaned up


# --- pgwire recognition regex -------------------------------------------------------------------


def test_pgwire_ctas_regex_matches_ctas_not_plain_ddl():
    from provisa.pgwire.server import _CTAS_RE, _DDL_RE

    assert _CTAS_RE.match("CREATE TABLE s.t AS SELECT 1")
    assert _CTAS_RE.match("create table s.t as with cte as (select 1) select * from cte")
    assert not _CTAS_RE.match("CREATE TABLE s.t (id INT)")
    # plain column-def DDL still routes to the DDL handler
    assert _DDL_RE.match("CREATE TABLE s.t (id INT)")
