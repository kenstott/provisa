# Copyright (c) 2026 Kenneth Stott
# Canary: 3b7f0c52-9d1e-4a86-8e47-c5a2f6d10b93
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PGWIRE CATALOG AND TYPES (REQ-2167, amended 2026-10-26), part 3: the casts tools issue, sent
as PostgreSQL 17 sends them -- the result column's type OID and typmod, and its text and binary
encodings.

Each statement runs as the conformance probe runs it (scripts/conformance/pgwire_catalog_types.py):
one ``exec_params`` over the extended protocol, once with text results and once with binary,
through a real pgwire server whose engine is a real DuckDB runtime (the statement's physical SQL is
the DuckDB transpile every engine read takes) and whose catalog is the real one. The expected
values are PostgreSQL 17.11's, taken from the PostgreSQL column of the Conformance (pgwire) run
40692101045 where it recorded them, and otherwise PostgreSQL's documented output for the type
(its ``*out`` and ``*send`` functions)."""

# Requirements: REQ-2167

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import psycopg
import pytest
from psycopg import pq

from provisa.federation.duckdb_runtime import DuckDBFederationRuntime
from provisa.transpiler.transpile import transpile_to_duckdb
from tests.pgwire_describe_parity import RuntimeEngine
from tests.role_state import with_surfaces
from tests.unit.pgwire.test_extended_protocol_portals import _engine_plan
from tests.unit.pgwire.test_wire_protocol import _free_port, _make_server

ROLE = "alice"

#: statement -> (type OID, typmod, text, binary hex), as PostgreSQL 17.11 sends them.
POSTGRES: dict[str, tuple[int, int, str | None, str | None]] = {
    "SELECT 1.5::float4 AS v": (700, -1, "1.5", "3fc00000"),
    "SELECT '12345.6789'::numeric AS v": (1700, -1, "12345.6789", "0003000100000004000109291a85"),
    # numeric(10,2): typmod ((10 << 16) | 2) + 4; 12345.68 is base-10000 digits 1, 2345, 6800.
    "SELECT '12345.6789'::numeric(10,2) AS v": (
        1700,
        655366,
        "12345.68",
        "000300010000000200010929" + "1a90",
    ),
    "SELECT 'x'::varchar AS v": (1043, -1, "x", "78"),
    "SELECT 'x'::varchar(10) AS v": (1043, 14, "x", "78"),
    "SELECT 'x'::char(3) AS v": (1042, 7, "x  ", "782020"),
    "SELECT 'x'::bpchar AS v": (1042, -1, "x", "78"),
    "SELECT 1::varchar AS v": (1043, -1, "1", "31"),
    "SELECT 'x'::name AS v": (19, -1, "x", "78"),
    "SELECT '2024-06-15 12:30:45+00'::timestamptz AS v": (
        1184,
        -1,
        "2024-06-15 12:30:45+00",
        "0002bdebab75a740",
    ),
    "SELECT '{\"a\": 1}'::jsonb AS v": (3802, -1, '{"a": 1}', "017b2261223a20317d"),
    "SELECT '\\xdeadbeef'::bytea AS v": (17, -1, "\\xdeadbeef", "deadbeef"),
    # array_send: ndim, has-nulls, element type, then per dimension its length and lower bound,
    # then each element's length and its own send form.
    "SELECT '{1,2}'::int4[] AS v": (
        1007,
        -1,
        "{1,2}",
        "000000010000000000000017000000020000000100000004000000010000000400000002",
    ),
    "SELECT '{a,b}'::text[] AS v": (
        1009,
        -1,
        "{a,b}",
        "000000010000000000000019000000020000000100000001610000000162",
    ),
    "SELECT 1::oid AS v": (26, -1, "1", "00000001"),
    "SELECT 23::regtype AS v": (2206, -1, "integer", "00000017"),
    "SELECT 'int4'::regtype AS v": (2206, -1, "integer", "00000017"),
    "SELECT 23::regtype::text AS v": (25, -1, "integer", "integer".encode().hex()),
    "SELECT 'pg_catalog.pg_class'::regclass AS v": (2205, -1, "pg_class", "000004eb"),
    "SELECT 'pg_catalog.pg_class'::regclass::oid AS v": (26, -1, "1259", "000004eb"),
    "SELECT 'pg_catalog'::regnamespace AS v": (4089, -1, "pg_catalog", "0000000b"),
    # An unknown-typed output column is text (PostgreSQL 10 and later).
    "SELECT NULL AS v": (25, -1, None, None),
    # inet_send: family (2, PGSQL_AF_INET), bits, is_cidr, address length, the address.
    "SELECT '192.168.0.1'::inet AS v": (869, -1, "192.168.0.1", "02200004c0a80001"),
    "SELECT pg_catalog.format_type(23, NULL) AS v": (25, -1, "integer", "integer".encode().hex()),
    "SELECT pg_catalog.format_type(1043, 24) AS v": (
        25,
        -1,
        "character varying(20)",
        "character varying(20)".encode().hex(),
    ),
    # No relation has oid 0: the function answers NULL.
    "SELECT pg_catalog.pg_table_is_visible(0) AS v": (16, -1, None, None),
    "SELECT pg_catalog.quote_ident('x') AS v": (25, -1, "x", "78"),
    # session_user names the role the session acts as (REQ-588), as a name.
    "SELECT session_user AS v": (19, -1, ROLE, ROLE.encode().hex()),
}


def _run(conn: psycopg.Connection, sql: str, fmt: int) -> tuple[int, int, str | None, str | None]:
    res = conn.pgconn.exec_params(sql.encode(), [], result_format=fmt)
    assert res.status == pq.ExecStatus.TUPLES_OK, (res.error_message or b"").decode()
    assert res.nfields == 1 and res.ntuples == 1
    assert res.fformat(0) == fmt
    value = res.get_value(0, 0)
    if fmt == 0:
        shown = None if value is None else value.decode()
    else:
        shown = None if value is None else value.hex()
    return res.ftype(0), res.fmod(0), shown, shown


@pytest.fixture(scope="module")
def pgwire():
    port = _free_port()
    server = _make_server(port)
    yield port
    server.shutdown()


@pytest.fixture
def conn(pgwire):
    from provisa.pgwire.server import ProvisaSession

    engine = RuntimeEngine(DuckDBFederationRuntime(), "duckdb")
    ctx = MagicMock()
    ctx.tables = {}
    state = with_surfaces(MagicMock())
    state.contexts = {ROLE: ctx}
    state.schema_build_cache = {"column_types": {}}
    state.auth_config = None
    state.auth_middleware_active = False
    state.multitenancy = False
    state.federation_engine = engine

    async def _govern(text, role_id, params=None, wire_formats=None, *, deliver, as_of=None):
        return _engine_plan(transpile_to_duckdb(text))

    async def _no_cache(plan, st):
        return None

    with (
        patch("provisa.api.app.state", state),
        patch("provisa.pgwire._pipeline.govern_pgwire_plan", _govern),
        patch("provisa.pgwire._pipeline.prepare_residency_and_check_cache", _no_cache),
        patch.object(ProvisaSession, "_finalize_audit", lambda self, governed, status, **kw: None),
    ):
        c = psycopg.connect(
            host="127.0.0.1",
            port=pgwire,
            user=ROLE,
            password="x",
            dbname="provisa",
            autocommit=True,
        )
        try:
            c.execute("SET TimeZone TO 'UTC'")
            yield SimpleNamespace(conn=c, engine=engine)
        finally:
            c.close()


@pytest.mark.parametrize("sql", list(POSTGRES))
def test_a_cast_is_sent_as_postgresql_sends_it(conn: Any, sql: str):
    oid, typmod, text, binary = POSTGRES[sql]
    t_oid, t_mod, t_text, _ = _run(conn.conn, sql, 0)
    b_oid, b_mod, _, b_bin = _run(conn.conn, sql, 1)
    assert (t_oid, t_mod, t_text) == (oid, typmod, text)
    assert (b_oid, b_mod, b_bin) == (oid, typmod, binary)


# --- a registered column is sent as its registered type (REQ-1961, amended 2026-10-28) ----------
#
# PGWIRE SENDS NUMERIC(P,S); VARCHAR AND CHAR KEEP THEIR TYPE: a registered numeric(p,s) column is
# described with its typmod, and a column whose native type is varchar, varchar(n), char(n) or
# bpchar (the hidden attribute registration keeps) with that type's OID and length typmod; the
# catalog's pg_attribute says the same. The fixture is the conformance job's
# (scripts/conformance/pgwire_catalog_types.py), registered as registration records it; the
# values are the PostgreSQL column of the Conformance (pgwire) run 40811310936, except c_numeric's
# typmod: the unsized numeric registration samples into numeric(38,4) is described as that, by
# design (by_design/pgwire.yaml).

from provisa.compiler.introspect import ColumnMetadata  # noqa: E402

FIXTURE_TABLE_ID = 1

#: (column, DuckDB type, the value as the source delivers it, registered column)
_FIXTURE: list[tuple[str, str, str, ColumnMetadata]] = [
    ("c_text", "VARCHAR", "'text value'", ColumnMetadata("c_text", "text", True)),
    (
        "c_numeric",
        "DECIMAL(38,4)",
        "12345.6789",
        ColumnMetadata("c_numeric", "numeric(38,4)", True),
    ),
    (
        "c_numeric_10_2",
        "DECIMAL(10,2)",
        "12345.67",
        ColumnMetadata("c_numeric_10_2", "numeric(10,2)", True),
    ),
    (
        "c_varchar",
        "VARCHAR",
        "'varchar value'",
        ColumnMetadata("c_varchar", "text", True, native_type="character varying"),
    ),
    (
        "c_varchar_20",
        "VARCHAR",
        "'bounded'",
        ColumnMetadata("c_varchar_20", "text", True, native_type="character varying(20)"),
    ),
    # PostgreSQL delivers a character(n) value padded to its length.
    (
        "c_bpchar_5",
        "VARCHAR",
        "'ab   '",
        ColumnMetadata("c_bpchar_5", "text", True, native_type="character(5)"),
    ),
    # An oid column registers as oid (the conformance fixture's c_oid): it is described as oid,
    # and the catalog listing it is built.
    ("c_oid", "INTEGER", "12345", ColumnMetadata("c_oid", "oid", True)),
]

#: column -> (type OID, typmod, text, binary hex).
REGISTERED: dict[str, tuple[int, int, str, str]] = {
    "c_text": (25, -1, "text value", "text value".encode().hex()),
    # numeric(38,4): typmod ((38 << 16) | 4) + 4.
    "c_numeric": (1700, 2490376, "12345.6789", "0003000100000004000109291a85"),
    # 12345.67 is base-10000 digits 1, 2345, 6700, display scale 2.
    "c_numeric_10_2": (1700, 655366, "12345.67", "000300010000000200010929" + "1a2c"),
    "c_varchar": (1043, -1, "varchar value", "varchar value".encode().hex()),
    "c_varchar_20": (1043, 24, "bounded", "bounded".encode().hex()),
    "c_bpchar_5": (1042, 9, "ab   ", "6162202020"),
    "c_oid": (26, -1, "12345", "00003039"),
}


def _fixture_ctx() -> Any:
    from provisa.compiler.sql_types import CompilationContext, TableMeta

    meta = TableMeta(
        table_id=FIXTURE_TABLE_ID,
        field_name="types_fixture",
        type_name="TypesFixture",
        source_id="conformance-pg",
        catalog_name="conformance_pg",
        schema_name="conformance",
        table_name="types_fixture",
        domain_id="conformance",
        source_type="postgresql",
    )
    return CompilationContext(tables={"types_fixture": meta})


@pytest.fixture
def registered(pgwire):
    from provisa.pgwire._pipeline import _kept_result_shape
    from provisa.pgwire.server import ProvisaSession

    runtime = DuckDBFederationRuntime()
    runtime._con.execute("CREATE SCHEMA conformance")
    columns = ", ".join(f"{name} {duck}" for name, duck, _, _ in _FIXTURE)
    values = ", ".join(value for _, _, value, _ in _FIXTURE)
    runtime._con.execute(f"CREATE TABLE conformance.types_fixture ({columns})")
    runtime._con.execute(f"INSERT INTO conformance.types_fixture VALUES ({values})")
    engine = RuntimeEngine(runtime, "duckdb")
    ctx = _fixture_ctx()
    state = with_surfaces(MagicMock())
    state.contexts = {ROLE: ctx}
    state.schema_build_cache = {"column_types": {FIXTURE_TABLE_ID: [c for *_, c in _FIXTURE]}}
    state.auth_config = None
    state.auth_middleware_active = False
    state.multitenancy = False
    state.federation_engine = engine
    state.engine_conn = None  # the catalog's row-count estimates are not asked of an engine
    table_map = {"conformance.types_fixture": FIXTURE_TABLE_ID, "types_fixture": FIXTURE_TABLE_ID}

    async def _govern(text, role_id, params=None, wire_formats=None, *, deliver, as_of=None):
        plan = _engine_plan(transpile_to_duckdb(text))
        plan.result_shape = _kept_result_shape({}, text, table_map, ctx, state)
        return plan

    async def _no_cache(plan, st):
        return None

    with (
        patch("provisa.api.app.state", state),
        patch("provisa.pgwire._pipeline.govern_pgwire_plan", _govern),
        patch("provisa.pgwire._pipeline.prepare_residency_and_check_cache", _no_cache),
        patch.object(ProvisaSession, "_finalize_audit", lambda self, governed, status, **kw: None),
    ):
        c = psycopg.connect(
            host="127.0.0.1",
            port=pgwire,
            user=ROLE,
            password="x",
            dbname="provisa",
            autocommit=True,
        )
        try:
            yield c
        finally:
            c.close()


@pytest.mark.parametrize("column", list(REGISTERED))
def test_a_registered_column_is_sent_as_its_registered_type(registered: Any, column: str):
    oid, typmod, text, binary = REGISTERED[column]
    sql = f"SELECT {column} FROM conformance.types_fixture"
    t_oid, t_mod, t_text, _ = _run(registered, sql, 0)
    b_oid, b_mod, _, b_bin = _run(registered, sql, 1)
    assert (t_oid, t_mod, t_text) == (oid, typmod, text)
    assert (b_oid, b_mod, b_bin) == (oid, typmod, binary)


def test_a_described_registered_column_is_sent_as_its_registered_type():
    # A statement Described first (Describe(Statement)) is sent by its description: the shape
    # derived from registered metadata types each column, whatever the engine answers.
    from buenavista.postgres import BVTYPE_TO_PGTYPE

    from provisa.compiler.result_shape import derive_result_shape
    from provisa.executor.result import QueryResult
    from provisa.pgwire.server import ProvisaQueryResult

    columns = list(REGISTERED)
    sql = f"SELECT {', '.join(columns)} FROM conformance.types_fixture"
    table_map = {"conformance.types_fixture": FIXTURE_TABLE_ID}
    column_types = {FIXTURE_TABLE_ID: [c for *_, c in _FIXTURE]}
    shape = derive_result_shape(sql, table_map, _fixture_ctx(), column_types)
    engine = QueryResult(
        rows=[("text value", 12345.6789, 12345.67, "varchar value", "bounded", "ab")],
        column_names=columns,
        column_types=["varchar", "double", "double", "varchar", "varchar", "varchar"],
    )
    result = ProvisaQueryResult(engine, sql, shape)
    described = [
        (
            result.column_type_oid(i) or BVTYPE_TO_PGTYPE[result.column(i)[1]][0],
            result.column_typmod(i),
        )
        for i in range(len(columns))
    ]
    assert described == [(REGISTERED[c][0], REGISTERED[c][1]) for c in columns]
    # A character(n) value is sent padded to its length, as bpchar's input pads it.
    assert list(result.rows())[0][-1] == "ab   "


def test_the_catalog_lists_a_registered_column_with_its_type_and_typmod(registered: Any):
    rows = registered.execute(
        "SELECT a.attname, a.atttypid::int8, a.atttypmod, "
        "pg_catalog.format_type(a.atttypid, a.atttypmod) "
        "FROM pg_catalog.pg_attribute a "
        "JOIN pg_catalog.pg_class c ON c.oid = a.attrelid "
        "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = 'conformance' AND c.relname = 'types_fixture' AND a.attnum > 0 "
        "ORDER BY a.attnum"
    ).fetchall()
    assert rows == [
        ("c_text", 25, -1, "text"),
        ("c_numeric", 1700, 2490376, "numeric(38,4)"),
        ("c_numeric_10_2", 1700, 655366, "numeric(10,2)"),
        ("c_varchar", 1043, -1, "character varying"),
        ("c_varchar_20", 1043, 24, "character varying(20)"),
        ("c_bpchar_5", 1042, 9, "character(5)"),
        ("c_oid", 26, -1, "oid"),
        ("_name_", 1043, -1, "character varying"),
        ("_domain_", 1043, -1, "character varying"),
    ]
