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
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock, patch

import psycopg
import pytest
from psycopg import pq

from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.native_subtype import mark_subtype_operations
from provisa.compiler.parameter_types import parameter_types
from provisa.compiler.result_shape import derive_result_shape
from provisa.compiler.sql_types import CompilationContext, TableMeta
from provisa.core import pg_money
from provisa.core.pg_error import PostgresError
from provisa.federation.duckdb_runtime import DuckDBFederationRuntime
from provisa.pgwire._pipeline import _Described, _kept_result_shape
from provisa.pgwire.server import ProvisaSession
from provisa.transpiler.native_subtype import SubtypeOperationRefused
from provisa.transpiler.pg_money import MONEY, lowered, money_type
from provisa.transpiler.transpile import transpile, transpile_to_duckdb
from tests.pgwire_describe_parity import RuntimeEngine
from tests.role_state import with_surfaces
from tests.unit.pgwire.pg17_money_answers import COLUMNS, RECORDED
from tests.unit.pgwire.test_extended_protocol_live import _answered
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

    async def _resident(st, plan):
        return None

    async def _describe(text, role_id, *, as_of=None):
        physical = re.sub(r"\bFROM m\b", "FROM conformance.m", text)
        ctx.physical_to_sql = {}
        columns = {_TABLE_ID: _COLUMN_TYPES}
        shape = derive_result_shape(physical, _MAP, ctx, columns)
        parameters = parameter_types(physical, _MAP, ctx, columns)
        return _Described(shape, ("governed", text), parameters)  # type: ignore[arg-type]

    async def _plan(held, params, wire_formats=None, *, deliver):
        plan = await _govern(held[1], ROLE, params, deliver=deliver)
        plan.exec_params = params or []
        return plan

    with (
        patch("provisa.api.app.state", state),
        patch("provisa.pgwire._pipeline.govern_pgwire_plan", _govern),
        patch("provisa.pgwire._pipeline.describe_pgwire_statement", _describe),
        patch("provisa.pgwire._pipeline.plan_pgwire_statement", _plan),
        patch(
            "provisa.pgwire._pipeline.governed_statement_is_current",
            lambda held, st, as_of=None: True,
        ),
        patch("provisa.pgwire._pipeline.prepare_residency_and_check_cache", _no_cache),
        patch("provisa.federation.query_residency.prepare_engine_residency", _resident),
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


def test_a_described_money_expression_is_described_as_money(money_table: Any):
    pgconn = money_table.pgconn
    sql = "SELECT price + price FROM m ORDER BY id"
    pgconn.send_prepare(b"money_probe", sql.encode())
    _answered(pgconn, pq.ExecStatus.COMMAND_OK)
    pgconn.send_describe_prepared(b"money_probe")
    assert _answered(pgconn, pq.ExecStatus.COMMAND_OK).ftype(0) == 790
    pgconn.send_query_prepared(b"money_probe", None, result_format=1)
    executed = _answered(pgconn, pq.ExecStatus.TUPLES_OK)
    assert executed.ftype(0) == 790
    assert executed.get_value(0, 0).hex() == COLUMNS[sql][3]


#: Sorted as money, not as its text form, however the ORDER BY names the column: the table's prices
#: are $1,234.56, -$5.00, $0.35 and NULL (PostgreSQL 16.11 answered the order).
_ORDERED_BY_NAME = {
    "SELECT price AS p FROM m ORDER BY p": ["-$5.00", "$0.35", "$1,234.56", None],
    "SELECT price AS p FROM m ORDER BY p DESC": [None, "$1,234.56", "$0.35", "-$5.00"],
    "SELECT price p FROM m ORDER BY p DESC NULLS LAST": ["$1,234.56", "$0.35", "-$5.00", None],
    "SELECT price FROM m ORDER BY 1": ["-$5.00", "$0.35", "$1,234.56", None],
    "SELECT id, price AS p FROM m ORDER BY 2 DESC NULLS LAST": None,
    "SELECT price + price AS d FROM m ORDER BY d": ["-$10.00", "$0.70", "$2,469.12", None],
}


@pytest.mark.parametrize("sql", list(_ORDERED_BY_NAME))
def test_order_by_an_alias_or_position_sorts_money_as_money(money_table: Any, sql: str):
    res = money_table.pgconn.exec_params(sql.encode(), [], result_format=0)
    assert res.status == pq.ExecStatus.TUPLES_OK, (res.error_message or b"").decode()
    last = res.nfields - 1
    got = [
        None if (v := res.get_value(i, last)) is None else v.decode() for i in range(res.ntuples)
    ]
    assert got == (_ORDERED_BY_NAME[sql] or ["$1,234.56", "$0.35", "-$5.00", None])


def test_an_uncast_parameter_compared_with_money_is_money(money_table: Any):
    pgconn = money_table.pgconn
    pgconn.send_prepare(b"money_param", b"SELECT id FROM m WHERE price > $1 ORDER BY id")
    _answered(pgconn, pq.ExecStatus.COMMAND_OK)
    pgconn.send_describe_prepared(b"money_param")
    described = _answered(pgconn, pq.ExecStatus.COMMAND_OK)
    assert [described.param_type(i) for i in range(described.nparams)] == [790]
    pgconn.send_query_prepared(b"money_param", [b"$1.00"])
    executed = _answered(pgconn, pq.ExecStatus.TUPLES_OK)
    assert [executed.get_value(i, 0) for i in range(executed.ntuples)] == [b"1"]


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


def test_a_described_money_expression_is_sent_as_it_was_described():
    # Describe(Statement) answers from the shape, and each Execute encodes rows to it, whatever
    # decimal the engine computed the expression in.
    from buenavista.postgres import BVTYPE_TO_PGTYPE

    from provisa.executor.result import QueryResult
    from provisa.pgwire.server import ProvisaQueryResult, _StatementDescription

    sql = "SELECT price + price AS v, price / price AS r FROM conformance.m"
    ctx = _context()
    ctx.physical_to_sql = {}
    shape = derive_result_shape(sql, _MAP, ctx, {_TABLE_ID: _COLUMN_TYPES})
    description = _StatementDescription(shape, None, sql)
    engine = QueryResult(
        rows=[(Decimal("2469.1200"), 1.0)],
        column_names=["v", "r"],
        column_types=["decimal(21,4)", "double"],
    )
    result = ProvisaQueryResult(engine, sql, shape)
    for described in (description, result):
        oids = [
            described.column_type_oid(i) or BVTYPE_TO_PGTYPE[described.column(i)[1]][0]
            for i in range(2)
        ]
        assert oids == [790, 701]
    encoder = BVTYPE_TO_PGTYPE[result.column(0)[1]]
    assert encoder[1](Decimal("2469.1200")) == "$2,469.12"
    assert encoder[2](Decimal("2469.1200")).hex() == "000000000003c480"


def test_a_dollar_amount_in_a_literal_is_no_placeholder():
    from provisa.pgwire.parameters import placeholder_oids

    assert placeholder_oids("SELECT '$10'::money / 3") == []
    assert placeholder_oids("SELECT $1::text, 'a''$2'") == [25]


@pytest.mark.parametrize(
    ("value", "text"),
    [
        (1.0, "1"),
        (1e14, "100000000000000"),
        (1e15, "1e+15"),
        (-1234567890123456.0, "-1.234567890123456e+15"),
        (float("inf"), "Infinity"),
        (float("nan"), "NaN"),
    ],
)
def test_a_double_is_sent_in_postgresqls_text(value: float, text: str):
    from buenavista.core import BVType
    from buenavista.postgres import BVTYPE_TO_PGTYPE

    assert BVTYPE_TO_PGTYPE[BVType.FLOAT][1](value) == text


def test_a_bound_parameter_is_read_as_moneys_input():
    written = lowered("SELECT $1::money AS v", "duckdb")
    assert "MONEY_INPUT($1)" in written.upper()
    assert "PROVISA_CASH_IN" not in written.upper()


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        ("SELECT pg_typeof('$1'::money + '$2'::money) AS v", "money"),
        ("SELECT pg_typeof('$1'::money * 2) AS v", "money"),
        ("SELECT pg_typeof(sum('$1'::money)) AS v", "money"),
        ("SELECT pg_typeof('$1'::money / '$2'::money) AS v", "double precision"),
    ],
)
def test_pg_typeof_names_what_money_operators_return(conn: Any, sql: str, expected: str):
    res = conn.conn.pgconn.exec_params(sql.encode(), [], result_format=0)
    assert _rows(res) == [expected]


#: money[], OID 791: the array of money, as PostgreSQL 16.11 answered it. array_send of a money
#: element is its int8 cents.
_MONEY_ARRAYS = {
    "SELECT '{$1.00}'::money[] AS v": (
        "{$1.00}",
        "0000000100000000000003160000000100000001000000080000000000000064",
    ),
    "SELECT '{$1.00,-$2.50}'::money[] AS v": ("{$1.00,-$2.50}", None),
    "SELECT '{\"$1,234.56\",NULL}'::money[] AS v": ('{"$1,234.56",NULL}', None),
    "SELECT ARRAY['$1'::money, NULL] AS v": ("{$1.00,NULL}", None),
    "SELECT ARRAY['$1', '$2.50']::money[] AS v": ("{$1.00,$2.50}", None),
    "SELECT '{}'::money[] AS v": ("{}", None),
}


@pytest.mark.parametrize("sql", list(_MONEY_ARRAYS))
def test_a_money_array_is_sent_as_oid_791(conn: Any, sql: str):
    text, binary = _MONEY_ARRAYS[sql]
    as_text = conn.conn.pgconn.exec_params(sql.encode(), [], result_format=0)
    assert as_text.status == pq.ExecStatus.TUPLES_OK, (as_text.error_message or b"").decode()
    assert (as_text.ftype(0), as_text.get_value(0, 0).decode()) == (791, text)
    as_binary = conn.conn.pgconn.exec_params(sql.encode(), [], result_format=1)
    assert as_binary.status == pq.ExecStatus.TUPLES_OK, (as_binary.error_message or b"").decode()
    assert as_binary.ftype(0) == 791
    if binary is not None:
        assert as_binary.get_value(0, 0).hex() == binary


def test_a_malformed_money_array_is_postgresqls_error(conn: Any):
    res = conn.conn.pgconn.exec_params(b"SELECT 'x'::money[] AS v", [], result_format=0)
    assert res.status == pq.ExecStatus.FATAL_ERROR
    assert res.error_field(pq.DiagnosticField.SQLSTATE) == b"22P02"


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
    assert "DECIMAL(21, 4)" in written and "fail(" in written.lower()
    assert "provisa_subtype" not in written


def test_a_divisor_of_unknown_kind_is_refused_by_name_with_no_typeof():
    physical = mark_subtype_operations(
        "SELECT price / qty AS v FROM conformance.m", _SUBTYPE_COLUMNS
    )
    with pytest.raises(SubtypeOperationRefused, match="money"):
        transpile(physical, "mysql")


_DIALECTS = (
    "postgres",
    "duckdb",
    "trino",
    "athena",
    "mysql",
    "tsql",
    "oracle",
    "snowflake",
    "bigquery",
    "clickhouse",
    "databricks",
    "sqlite",
)
_MONEY_ARGS = "'$1'::money, '$2'::money"
_CALLS = (
    *(
        f"{name}({_MONEY_ARGS})"
        for name in (
            "cash_pl",
            "cash_mi",
            "cash_cmp",
            "cash_eq",
            "cash_ne",
            "cash_lt",
            "cash_le",
            "cash_gt",
            "cash_ge",
            "cashlarger",
            "cashsmaller",
            "cash_div_cash",
        )
    ),
    *(
        f"cash_{op}_{kind}('$1'::money, 2)"
        for op in ("mul", "div")
        for kind in ("flt4", "flt8", "int2", "int4", "int8")
    ),
    *(f"{kind}_mul_cash(2, '$1'::money)" for kind in ("flt4", "flt8", "int2", "int4", "int8")),
    "cash_words('$1'::money)",
    "cash_send('$1'::money)",
    "cash_out('$1'::money)",
    "cash_in('$1')",
    "money(1.5)",
    "'$1'::money + '$2'::money",
    "'$1'::money * 1.5",
    "'$1'::money / 3",
    "'$1'::money / '$2'::money",
    "'$1'::money::text",
    "'$1'::money::numeric",
)


@pytest.mark.parametrize("dialect", _DIALECTS)
def test_every_money_function_compiles_or_is_refused_by_name_on_every_dialect(dialect: str):
    """REQ-2171, HOW THE LIMITS ARE TESTED: never a raw engine error, here or at the engine."""
    for call in _CALLS:
        try:
            written = transpile(f"SELECT {call} AS v", dialect)
        except SubtypeOperationRefused as refused:
            assert refused.params["subtype"] == "money"
            continue
        assert "provisa_subtype" not in written
        if dialect != "postgres":
            # No cast to the type, and no call of a family function, reaches a dialect without it.
            assert not re.search(r"AS MONEY\b|(?<!provisa_)cash", written, re.IGNORECASE)


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
