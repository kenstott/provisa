# Copyright (c) 2026 Kenneth Stott
# Canary: e18b4d73-6a29-4c05-b9f7-35d2c1e8a064
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL's money over pgwire (REQ-2167, REQ-2171): every statement run as the conformance
probe runs it, through a real pgwire server on a real DuckDB engine, against what PostgreSQL 17.11
answered it: type OID, each row's text, the first row's binary form, or its error.

The registered statements read a table two ways, as the engine can hold one: an attached
PostgreSQL's money is a VARCHAR in its text form (DuckDB's postgres extension), a landed one the
IR's numeric(21,4). Either way ``price + price`` is money from the plan to the wire."""

# Requirements: REQ-2167, REQ-2171
# ruff: noqa: F811 -- the fixtures are conn and pgwire of the module they are imported from

from __future__ import annotations

import re
from typing import Any
from unittest.mock import MagicMock, patch

import psycopg
import pytest
from psycopg import pq

from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.native_subtype import mark_subtype_operations
from provisa.compiler.result_shape import derive_result_shape
from provisa.compiler.sql_types import CompilationContext, TableMeta
from provisa.core import pg_money
from provisa.core.pg_error import PostgresError
from provisa.federation.duckdb_runtime import DuckDBFederationRuntime
from provisa.pgwire._pipeline import _kept_result_shape
from provisa.pgwire.server import ProvisaSession
from provisa.transpiler.native_subtype import SubtypeOperationRefused
from provisa.transpiler.pg_money import MONEY, lowered, money_type
from provisa.transpiler.transpile import transpile, transpile_to_duckdb
from tests.pgwire_describe_parity import RuntimeEngine
from tests.role_state import with_surfaces
from tests.unit.pgwire.pg17_money_answers import COLUMNS, RECORDED
from tests.unit.pgwire.test_extended_protocol_portals import _engine_plan
from tests.unit.pgwire.test_type_conversions import ROLE, conn, pgwire  # noqa: F401

import sqlglot

_TABLE_ID = 1


def _rows(res: Any) -> list[Any]:
    return [None if (v := res.get_value(i, 0)) is None else v.decode() for i in range(res.ntuples)]


def _problem(connection: Any, sql: str, shown: str, expected: tuple) -> str | None:
    text = connection.pgconn.exec_params(shown.encode(), [], result_format=0)
    binary = connection.pgconn.exec_params(shown.encode(), [], result_format=1)
    if expected[0] == "err":
        if text.status != pq.ExecStatus.FATAL_ERROR:
            return f"answered {_rows(text)}, PostgreSQL {expected[1:]}"
        said = (
            text.error_field(pq.DiagnosticField.SQLSTATE).decode(),
            text.error_field(pq.DiagnosticField.MESSAGE_PRIMARY).decode(),
        )
        return None if said == expected[1:] else f"{said}, PostgreSQL {expected[1:]}"
    _, oid, rows, wire = expected
    if text.status != pq.ExecStatus.TUPLES_OK:
        return (text.error_message or b"").decode()
    if binary.status != pq.ExecStatus.TUPLES_OK:
        return "binary: " + (binary.error_message or b"").decode()
    if (text.ftype(0), binary.ftype(0)) != (oid, oid):
        return f"type {text.ftype(0)}/{binary.ftype(0)}, PostgreSQL {oid}"
    got = _rows(text)
    if got != rows:
        return f"rows {got}, PostgreSQL {rows}"
    raw = binary.get_value(0, 0) if binary.ntuples else None
    hexed = None if raw is None else raw.hex()
    return None if hexed == wire else f"binary {hexed}, PostgreSQL {wire}"


# One test over all of them: conn builds a DuckDB runtime per test (pg_function_families).
def test_every_money_statement_answers_what_postgresql_17_answered(conn: Any):
    problems = {
        sql: problem
        for sql, expected in RECORDED.items()
        if (problem := _problem(conn.conn, sql, sql, expected)) is not None
    }
    assert not problems


def _context() -> CompilationContext:
    meta = TableMeta(
        table_id=_TABLE_ID,
        field_name="m",
        type_name="M",
        source_id="conformance-pg",
        catalog_name="conformance_pg",
        schema_name="conformance",
        table_name="m",
        domain_id="conformance",
        source_type="postgresql",
    )
    return CompilationContext(tables={"m": meta})


_COLUMN_TYPES = [
    ColumnMetadata("id", "integer", False),
    ColumnMetadata("price", "numeric(21,4)", True, subtype="money"),
    ColumnMetadata("qty", "integer", True),
    ColumnMetadata("rate", "double", True),
    ColumnMetadata("label", "text", True),
]
_SUBTYPE_COLUMNS = {("conformance", "m"): {"price": MONEY}}
_MAP = {"conformance.m": _TABLE_ID, "m": _TABLE_ID}


def _physical(sql: str) -> str:
    """As governance leaves it: the table qualified, the money column's uses marked."""
    qualified = re.sub(r"\bFROM m\b", "FROM conformance.m", sql)
    return mark_subtype_operations(qualified, _SUBTYPE_COLUMNS)


@pytest.fixture(params=["VARCHAR", "DECIMAL(21,4)"], ids=["attached", "landed"])
def money_table(request: Any, pgwire: int):
    runtime = DuckDBFederationRuntime()
    held = request.param
    runtime._con.execute("CREATE SCHEMA conformance")
    runtime._con.execute(
        f"CREATE TABLE conformance.m "
        f"(id INTEGER, price {held}, qty INTEGER, rate DOUBLE, label VARCHAR)"
    )
    written = held == "VARCHAR"
    prices = ("$1,234.56", "-$5.00", "$0.35") if written else ("1234.56", "-5.00", "0.35")
    runtime._con.execute(
        "INSERT INTO conformance.m VALUES "
        f"(1, '{prices[0]}', 3, 1.5, '$2.50'), (2, '{prices[1]}', 2, 0.25, '$1,000.10'), "
        f"(3, '{prices[2]}', 7, 0.5, '$0.01'), (4, NULL, 1, 2.0, NULL)"
    )
    engine = RuntimeEngine(runtime, "duckdb")
    ctx = _context()
    state = with_surfaces(MagicMock())
    state.contexts = {ROLE: ctx}
    state.schema_build_cache = {"column_types": {_TABLE_ID: _COLUMN_TYPES}}
    state.auth_config = None
    state.auth_middleware_active = False
    state.multitenancy = False
    state.federation_engine = engine
    state.engine_conn = None

    async def _govern(text, role_id, params=None, wire_formats=None, *, deliver, as_of=None):
        plan = _engine_plan(transpile_to_duckdb(_physical(text)))
        plan.result_shape = _kept_result_shape(
            {}, re.sub(r"\bFROM m\b", "FROM conformance.m", text), _MAP, ctx, state
        )
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


def test_a_registered_money_column_answers_what_postgresql_17_answered(money_table: Any):
    problems = {
        sql: problem
        for sql, expected in COLUMNS.items()
        if (problem := _problem(money_table, sql, sql, expected)) is not None
    }
    assert not problems


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        ("SELECT price + price AS v FROM conformance.m", "money"),
        ("SELECT price * 2 AS v FROM conformance.m", "money"),
        ("SELECT 2 * price AS v FROM conformance.m", "money"),
        ("SELECT price / 3 AS v FROM conformance.m", "money"),
        ("SELECT price / price AS v FROM conformance.m", "double precision"),
        ("SELECT sum(price) AS v FROM conformance.m", "money"),
        ("SELECT (price + price) / 2 AS v FROM conformance.m", "money"),
        ("SELECT d + d AS v FROM (SELECT price + price AS d FROM conformance.m) x", "money"),
        ("WITH c AS (SELECT price AS p FROM conformance.m) SELECT p * 2 AS v FROM c", "money"),
    ],
)
def test_the_plan_types_a_money_expression_as_money_not_its_base(sql: str, expected: str):
    ctx = _context()
    ctx.physical_to_sql = {}
    shape = derive_result_shape(sql, _MAP, ctx, {_TABLE_ID: _COLUMN_TYPES})
    assert [t for _, t in shape] == [expected]


def test_a_described_money_expression_is_described_as_money(money_table: Any):
    prepared = money_table.pgconn.prepare(
        b"money_probe", b"SELECT price + price FROM m ORDER BY id"
    )
    assert prepared.status == pq.ExecStatus.COMMAND_OK
    described = money_table.pgconn.describe_prepared(b"money_probe")
    assert described.ftype(0) == 790
    executed = money_table.pgconn.exec_prepared(b"money_probe", [], result_format=1)
    assert executed.ftype(0) == 790
    assert executed.get_value(0, 0).hex() == COLUMNS["SELECT price + price FROM m ORDER BY id"][3]


def test_lc_monetary_is_reported_and_fixed(conn: Any):
    shown = conn.conn.pgconn.exec_params(b"SHOW lc_monetary", [], result_format=0)
    assert _rows(shown) == ["en_US.UTF-8"]
    current = conn.conn.pgconn.exec_params(
        b"SELECT current_setting('lc_monetary') AS v", [], result_format=0
    )
    assert _rows(current) == ["en_US.UTF-8"]
    listed = conn.conn.pgconn.exec_params(
        b"SELECT setting FROM pg_catalog.pg_settings WHERE name = 'lc_monetary'",
        [],
        result_format=0,
    )
    assert _rows(listed) == ["en_US.UTF-8"]


def test_the_catalog_lists_money_as_its_own_type(conn: Any):
    listed = conn.conn.pgconn.exec_params(
        b"SELECT oid, typname, typlen FROM pg_catalog.pg_type WHERE typname = 'money'",
        [],
        result_format=0,
    )
    assert (listed.get_value(0, 0), listed.get_value(0, 1), listed.get_value(0, 2)) == (
        b"790",
        b"money",
        b"8",
    )


# --- the dialects ------------------------------------------------------------------------------


def test_postgresql_answers_money_with_its_own_operators():
    physical = mark_subtype_operations("SELECT price / 3 AS v FROM conformance.m", _SUBTYPE_COLUMNS)
    written = transpile(physical, "postgres")
    assert "CAST(price AS MONEY) / 3" in written
    assert "provisa_subtype" not in written


def test_a_literal_on_postgresql_stays_postgresqls_input():
    assert lowered("SELECT '$10'::money / 3", "postgres") == "SELECT '$10'::money / 3"


def test_money_is_computed_over_the_base_on_trino():
    physical = mark_subtype_operations(
        "SELECT price + price AS v FROM conformance.m", _SUBTYPE_COLUMNS
    )
    written = transpile(physical, "trino")
    assert "DECIMAL(21, 4)" in written and "fail(" in written
    assert "provisa_subtype" not in written


def test_a_divisor_of_unknown_kind_is_refused_by_name_with_no_typeof():
    physical = mark_subtype_operations(
        "SELECT price / qty AS v FROM conformance.m", _SUBTYPE_COLUMNS
    )
    with pytest.raises(SubtypeOperationRefused, match="money"):
        transpile(physical, "mysql")


def test_a_statement_naming_no_money_is_left_as_written():
    sql = "select  'cash' , name  from t where name = 'x'"
    assert lowered(sql, "duckdb") == sql


def test_a_text_comparison_beside_money_keeps_its_text():
    written = lowered("SELECT '$1'::money AS m, a FROM t WHERE a = 'x'", "duckdb")
    assert "a = 'x'" in written


def test_money_type_needs_the_statement_to_say_it():
    tree = sqlglot.parse_one("SELECT a + b", read="postgres")
    assert money_type(tree.selects[0]) is None


# --- money's input and output, as cash.c ---------------------------------------------------------


@pytest.mark.parametrize(
    ("text", "cents"),
    [
        ("$1,234.56", 123456),
        ("(5)", -500),
        ("5-", -500),
        ("+5", 500),
        ("1.005", 101),
        ("1.2349", 123),
        ("", 0),
    ],
)
def test_cash_in(text: str, cents: int):
    assert pg_money.cash_in(text) == cents


def test_cash_in_refuses_a_value_outside_int8():
    with pytest.raises(PostgresError, match="out of range for type money"):
        pg_money.cash_in("99999999999999999999")


@pytest.mark.parametrize(
    ("cents", "text"), [(0, "$0.00"), (-1, "-$0.01"), (123456, "$1,234.56"), (-(2**63), None)]
)
def test_cash_out(cents: int, text: str | None):
    assert pg_money.cash_out(cents) == (text or "-$92,233,720,368,547,758.08")
