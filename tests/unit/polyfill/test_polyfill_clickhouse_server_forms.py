# Copyright (c) 2026 Kenneth Stott
# Canary: f9f7e6f0-34e6-4cc4-bd88-039dbbe13081
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""ClickHouse's forms both of its legs take: embedded chDB and the 24.3 server (REQ-2236; the
polyfill triage of 2026-11-22, groups G4 to G6 and G11).

The server (24.3) refuses what chDB takes: a result column of type Nothing (an untyped NULL) in
its Arrow output, a constant empty or NULL pattern of replaceAll and replaceRegexpAll, an array of
a Nullable text's split, and an if of a Nullable(Nothing) branch inside a lambda; its reverseUTF8
reverses a four-byte character's bytes. Each form here is answered on chDB against PostgreSQL's
answer (PostgreSQL 17), and its written SQL is held to what the server takes. The grid's rows state
a raise only where a form raises at a value PostgreSQL answers: PostgreSQL's own errors, and errors
of a value PostgreSQL never holds, are no bound."""

# Requirements: REQ-2236

from __future__ import annotations

import sys
from pathlib import Path

import pytest
import sqlglot
import sqlglot.expressions as exp

from provisa.core.pg_error import from_engine
from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime
from provisa.transpiler.function_macros import RowRefused
from provisa.transpiler.polyfill import GRID
from provisa.transpiler.polyfill.clickhouse import typed_null_columns
from provisa.transpiler.transpile import transpile

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts" / "conformance"))

import function_edges  # noqa: E402
import polyfill_check  # noqa: E402

#: The NULL calls whose ClickHouse form was an untyped NULL column (G6): each answers NULL.
NULL_CALLS = [
    'SELECT pg_catalog."array_cat"(NULL::integer[], NULL::integer[]) AS v',
    'SELECT pg_catalog."array_dims"(NULL::integer[]) AS v',
    'SELECT pg_catalog."any_value_transfn"(NULL::integer, NULL::integer) AS v',
    'SELECT pg_catalog."isfinite"(NULL::date) AS v',
    'SELECT pg_catalog."timestamptz_larger"(NULL::timestamp with time zone, '
    "NULL::timestamp with time zone) AS v",
    'SELECT pg_catalog."substr"(NULL::bytea, NULL::integer, NULL::integer) AS v',
    'SELECT pg_catalog."in_range"(NULL::numeric, NULL::numeric, NULL::numeric, NULL::boolean, '
    "NULL::boolean) AS v",
    'SELECT pg_catalog."datetime_pl"(NULL::date, NULL::time without time zone) AS v',
    'SELECT pg_catalog."timedate_pl"(NULL::time without time zone, NULL::date) AS v',
    'SELECT pg_catalog."timestamp"(NULL::date, NULL::time without time zone) AS v',
    'SELECT pg_catalog."json_object"(NULL::text[]) AS v',
]

ANSWERED = [  # (statement, PostgreSQL's text of its one row)
    ("SELECT pg_catalog.\"replace\"('abc'::text, 'b'::text, 'z'::text) AS v", "azc"),
    ("SELECT pg_catalog.\"replace\"('abc'::text, ''::text, 'z'::text) AS v", "abc"),
    ("SELECT pg_catalog.\"replace\"('abc'::text, 'b'::text, NULL::text) AS v", None),
    ("SELECT pg_catalog.\"replace\"('abc'::text, ''::text, NULL::text) AS v", None),
    ('SELECT pg_catalog."replace"(NULL::text, NULL::text, NULL::text) AS v', None),
    ("SELECT pg_catalog.\"replace\"('a b a'::text, 'a'::text, ''::text) AS v", " b "),
    ('SELECT pg_catalog."btrim"(NULL::text, NULL::text) AS v', None),
    ("SELECT pg_catalog.\"btrim\"('xabcx'::text, NULL::text) AS v", None),
    ("SELECT pg_catalog.\"btrim\"('xabcx'::text, ''::text) AS v", "xabcx"),
    ("SELECT pg_catalog.\"btrim\"('xabcx'::text, 'x'::text) AS v", "abc"),
    ("SELECT pg_catalog.\"ltrim\"('xabcx'::text, NULL::text) AS v", None),
    ("SELECT pg_catalog.\"rtrim\"('xabcx'::text, NULL::text) AS v", None),
    ("SELECT pg_catalog.\"reverse\"('日本語テキスト😀'::text) AS v", "😀トスキテ語本日"),
    ("SELECT pg_catalog.\"reverse\"('abc'::text) AS v", "cba"),
    ("SELECT pg_catalog.\"reverse\"(''::text) AS v", ""),
    ('SELECT pg_catalog."reverse"(NULL::text) AS v', None),
    ('SELECT pg_catalog."int4"(2.5::double precision) AS v', 2),
    ('SELECT pg_catalog."int4"(3.5::double precision) AS v', 4),
    ('SELECT pg_catalog."int4"(-2.5::double precision) AS v', -2),
    ('SELECT pg_catalog."int2"(32767.4::double precision) AS v', 32767),
    ('SELECT pg_catalog."int8"(2.5::real) AS v', 2),
    ('SELECT pg_catalog."int8"(NULL::double precision) AS v', None),
    ("SELECT pg_catalog.\"btnamecmp\"('abc'::name, 'abc'::name) AS v", 0),
    ("SELECT pg_catalog.\"btnamecmp\"(NULL::name, 'abc'::name) AS v", None),
    ("SELECT pg_catalog.\"byteacmp\"('\\x00ff00'::bytea, '\\x00ff01'::bytea) AS v", -1),
    ("SELECT pg_catalog.\"pg_snapshot_xmin\"('10:20:10,14,15'::pg_snapshot) AS v", 10),
    ("SELECT pg_catalog.\"pg_snapshot_xmax\"('10:20:10,14,15'::pg_snapshot) AS v", 20),
    ('SELECT pg_catalog."pg_snapshot_xmin"(NULL::pg_snapshot) AS v', None),
]

RAISED = [  # (statement, PostgreSQL's SQLSTATE, or the row's refusal's)
    ("SELECT pg_catalog.\"int4\"('NaN'::double precision) AS v", "22003"),
    ("SELECT pg_catalog.\"int4\"('-Infinity'::double precision) AS v", "22003"),
    ('SELECT pg_catalog."int2"(32767.5::double precision) AS v', "22003"),
    ("SELECT pg_catalog.\"int8\"('Infinity'::real) AS v", "22003"),
    ('SELECT pg_catalog."int8"(9223372036854775807::double precision) AS v', "22003"),
    # A date Date32 does not hold: the date row's refusal, not toDate32's parse error.
    (
        "SELECT pg_catalog.\"datetime_pl\"('4713-01-01 BC'::date, '12:00'::time without time "
        "zone) AS v",
        "0A000",
    ),
    (
        "SELECT pg_catalog.\"timestamp\"('5874897-12-31'::date, '12:00'::time without time "
        "zone) AS v",
        "0A000",
    ),
]


@pytest.fixture(scope="module")
def runtime() -> ClickHouseFederationRuntime:
    return ClickHouseFederationRuntime.embedded()


def _answer(runtime: ClickHouseFederationRuntime, sql: str) -> object:
    ((value,),) = list(runtime.run_sync(transpile(sql, "clickhouse", generated=False)).iter_rows())
    return value


@pytest.mark.parametrize("sql", NULL_CALLS)
def test_a_null_call_answers_null(runtime, sql):
    assert _answer(runtime, sql) is None


@pytest.mark.parametrize("sql", NULL_CALLS)
def test_a_null_call_has_no_column_of_type_nothing(sql):
    written = sqlglot.parse_one(transpile(sql, "clickhouse", generated=False), read="clickhouse")
    assert isinstance(written, exp.Select)
    for column in written.selects:
        value = column.unalias()
        assert not isinstance(value, exp.Null), written.sql("clickhouse")
        assert not (
            isinstance(value, exp.Binary)
            and any(isinstance(side, exp.Null) for side in (value.this, value.expression))
        ), written.sql("clickhouse")


def test_a_null_column_postgresql_types_by_no_call_is_a_null_of_string():
    assert (
        typed_null_columns("SELECT 1 AS a, NULL AS b", "SELECT 1 AS a, NULL AS b")
        == "SELECT 1 AS a, CAST(NULL AS Nullable(String)) AS b"
    )


def test_a_null_column_postgresql_types_is_a_null_the_call_s_type_describes():
    assert (
        typed_null_columns(
            "SELECT 1 AS a, NULL AS b", "SELECT 1 AS a, cardinality(NULL::int[]) AS b"
        )
        == "SELECT 1 AS a, CAST(NULL AS Nullable(FixedString(1))) AS b"
    )


def test_a_set_operation_s_null_takes_its_type_from_the_other_branch():
    written = "SELECT NULL AS v UNION ALL SELECT 1"
    assert typed_null_columns(written, written) == written


def test_a_statement_of_no_null_column_is_unchanged():
    written = "SELECT x FROM t WHERE y IS NULL"
    assert typed_null_columns(written, written) == written


@pytest.mark.parametrize(("sql", "want"), ANSWERED)
def test_clickhouse_answers_postgresql_s(runtime, sql, want):
    got = _answer(runtime, sql)
    assert got == want, (want, got)


@pytest.mark.parametrize(("sql", "state"), RAISED)
def test_clickhouse_raises_postgresql_s_error(runtime, sql, state):
    with pytest.raises(Exception) as raised:
        _answer(runtime, sql)
    if isinstance(raised.value, RowRefused):
        assert raised.value.sqlstate == state
        return
    error = from_engine(raised.value)
    assert error is not None and error.sqlstate == state, str(raised.value)[:300]


def test_a_snapshot_is_split_as_not_null():
    written = transpile(
        "SELECT pg_catalog.\"pg_snapshot_xip\"('10:20:10,14,15'::pg_snapshot) AS v",
        "clickhouse",
        generated=False,
    )
    # sqlglot writes ClickHouse's splitByChar as splitByString
    assert "splitByString(':', assumeNotNull(" in written
    assert "splitByString(':', CAST(" not in written


def test_a_byte_comparison_s_null_branch_is_typed():
    written = transpile(
        "SELECT pg_catalog.\"btnamecmp\"('a'::name, 'b'::name) AS v", "clickhouse", generated=False
    )
    assert "THEN CAST(NULL AS Nullable(Int32))" in written
    assert "THEN NULL ELSE" not in written


def _stated(function: str, overload: str) -> list[str]:
    applied = GRID.read(polyfill_check.call_of(function, overload), "clickhouse")
    assert applied is not None
    return function_edges.stated_raises(
        tuple(applied.feature.read.limitations.get("clickhouse", ()))
    )


@pytest.mark.parametrize(
    ("function", "overload"),
    [
        # A jsonb column's text that is no JSON: PostgreSQL never holds it.
        ("int4", "bigint"),
        ("float8", "integer"),
        ("bool", "integer"),
        # PostgreSQL's own errors, raised where PostgreSQL raises them.
        ("interval_pl", "interval, interval"),
        ("get_bit", "bytea, bigint"),
        ("like", "bytea, bytea"),
        ("substr", "bytea, integer"),
        ("overlay", "bytea, bytea, integer"),
    ],
)
def test_a_row_states_no_raise_of_what_is_no_bound(function, overload):
    assert _stated(function, overload) == []


@pytest.mark.parametrize("function", ["exp", "ln"])
def test_the_server_s_approximation_is_a_stated_difference(function):
    applied = GRID.read(polyfill_check.call_of(function, "double precision"), "clickhouse")
    assert applied is not None
    # The row's measured refusals (another overload's, such as numeric's) are limitations beside
    # it; the difference is the one limitation that refuses nothing.
    limitations = applied.feature.read.limitations["clickhouse"]
    (stated,) = [lim for lim in limitations if lim.refusal is None]
    assert "24.3" in stated.text


#: NULL calls whose ClickHouse form is an untyped NULL column, with the result type oid PostgreSQL
#: 17 describes each by (the conformance functions measure's base calls, REQ-2236).
DESCRIBED_NULL_CALLS = [
    ('SELECT pg_catalog."array_length"(NULL::integer[], NULL::integer) AS v', 23),
    ('SELECT pg_catalog."any_value_transfn"(NULL::integer, NULL::integer) AS v', 23),
    ('SELECT pg_catalog."arraycontains"(NULL::integer[], NULL::integer[]) AS v', 16),
    ('SELECT pg_catalog."booland_statefunc"(NULL::boolean, NULL::boolean) AS v', 16),
    ('SELECT pg_catalog."array_cat"(NULL::integer[], NULL::integer[]) AS v', 1007),
    (
        'SELECT pg_catalog."float8_accum"(NULL::double precision[], NULL::double precision) AS v',
        1022,
    ),
    ('SELECT pg_catalog."string_to_array"(NULL::text, NULL::text) AS v', 1009),
    ('SELECT pg_catalog."xid8_larger"(NULL::xid8, NULL::xid8) AS v', 5069),
    (
        'SELECT pg_catalog."timestamptz_larger"(NULL::timestamp with time zone, '
        "NULL::timestamp with time zone) AS v",
        1184,
    ),
]


def _described_oids(runtime: ClickHouseFederationRuntime, sql: str) -> list[int | None]:
    """The result type oids pgwire describes ``sql``'s result by, served by embedded ClickHouse."""
    from provisa.pgwire.server import BVTYPE_TO_OID, _describe_casts, _sql_type_to_bvtype, _type_oid

    served = runtime.run_sync(transpile(sql, "clickhouse")).column_types
    types = [_sql_type_to_bvtype(t) for t in served]
    oids: list[int | None] = [_type_oid(t) for t in served]
    _describe_casts(sql, types, oids, [-1] * len(served), [], served, [False] * len(served))
    # A column no oid describes is sent as its wire type's (pgwire's RowDescription).
    return [BVTYPE_TO_OID[t] if o is None else o for t, o in zip(types, oids)]


@pytest.mark.parametrize(("sql", "oid"), DESCRIBED_NULL_CALLS)
def test_a_null_call_is_described_as_postgresql_types_it(runtime, sql, oid):
    assert _described_oids(runtime, sql) == [oid]


@pytest.mark.parametrize(("sql", "oid"), DESCRIBED_NULL_CALLS)
def test_a_null_call_s_column_is_one_the_server_s_arrow_output_types(sql, oid):
    # The stand-in reaches Arrow as a binary (FixedString), as a Nothing reaches embedded
    # ClickHouse's: 24.3's Arrow output has no type for Nothing, and pgwire reads a binary NULL
    # column as untyped, described by PostgreSQL's type of the call.
    (column,) = sqlglot.parse_one(transpile(sql, "clickhouse"), read="clickhouse").selects
    value = column.unalias()
    assert isinstance(value, exp.Cast) and isinstance(value.this, exp.Null)
    assert value.to.sql(dialect="clickhouse") == "Nullable(FixedString(1))"


def test_a_null_column_postgresql_types_by_no_call_is_text(runtime):
    assert _described_oids(runtime, "SELECT NULL AS v") == [25]
