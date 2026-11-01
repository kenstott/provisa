# Copyright (c) 2026 Kenneth Stott
# Canary: 3f9a1c72-5d4e-4b86-9e21-7c0d8b6a4f13
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A function answers as PostgreSQL does or is refused by name (REQ-2171, AN ANSWER THAT DIFFERS IS
REFUSED): the wrong answers the function support measure found, each as the statement it found it
with, against postgres:17.11's answer."""

# Requirements: REQ-2171

from __future__ import annotations

import duckdb
import pytest
import sqlglot
from sqlglot import exp

from provisa.pgwire.cast_types import result_casts
from provisa.pgwire.function_types import call_result_type
from provisa.transpiler.function_macros import FunctionRefused
from provisa.transpiler.pg_literals import exact_inputs, values_cast
from provisa.transpiler.transpile import transpile, transpile_to_duckdb

ENGINES = ("trino", "clickhouse", "duckdb")


def _on_duckdb(sql: str):
    return duckdb.sql(transpile_to_duckdb(sql)).fetchall()[0][0]


@pytest.mark.parametrize(
    ("sql", "kept"),
    [
        ("SELECT 2.5::numeric", "CAST(2.5 AS DECIMAL(2, 1))"),
        ("SELECT (-7.125)::numeric", "CAST((-7.125) AS DECIMAL(4, 3))"),
        ("SELECT 1000::numeric", "CAST(1000 AS DECIMAL(4, 0))"),
        ("SELECT CAST('12.50' AS numeric)", "CAST('12.50' AS DECIMAL(4, 2))"),
        ("SELECT 'abc'::character", "CAST('a' AS CHAR)"),
        ("SELECT 'abcd'::char(2)", "CAST('ab' AS CHAR(2))"),
    ],
)
def test_a_literal_is_read_as_its_types_input(sql, kept):
    assert kept in exact_inputs(sql)


def test_a_statement_with_nothing_to_read_is_left_as_written():
    sql = "SELECT  x::numeric  FROM t"
    assert exact_inputs(sql) == sql


@pytest.mark.parametrize("dialect", ["trino", "clickhouse"])
def test_a_numeric_literal_keeps_its_scale(dialect):
    written = transpile("SELECT 2.5::numeric AS v", dialect)
    assert "DECIMAL(2, 1)" in written.replace("Decimal", "DECIMAL")


@pytest.mark.parametrize(
    ("sql", "answer"),
    [
        ("SELECT length('abc'::character) AS v", 1),
        ("SELECT min(c1) AS v FROM (VALUES (2.5::numeric), ((-7.125)::numeric)) AS t(c1)", -7.125),
        ("SELECT substr('Hello World'::text, (-7)::integer) AS v", "Hello World"),
        ("SELECT substr('Hello'::text, (-1)::integer, 3) AS v", "H"),
        ("SELECT to_hex((-7)::integer) AS v", "fffffff9"),
        ("SELECT to_hex((-7)::bigint) AS v", "fffffffffffffff9"),
        ("SELECT round(0.5::double precision) AS v", 0),
        ("SELECT array_position('{1,2,3}'::integer[], 3::integer, 3::integer) AS v", 3),
        ("SELECT array_remove('{-7,NULL,42}'::integer[], 42::integer) AS v", [-7, None]),
        ("SELECT array_cat(NULL::integer[], NULL::integer[]) AS v", None),
        ("SELECT array_cat('{1}'::integer[], NULL::integer[]) AS v", [1]),
    ],
)
def test_on_duckdb_it_answers_as_postgresql_does(sql, answer):
    value = _on_duckdb(sql)
    assert (float(value) if isinstance(answer, float) else value) == answer


@pytest.mark.parametrize(
    ("sql", "text"),
    [
        ("SELECT round(2.5::numeric, 3::integer) AS v", "2.500"),
        ("SELECT trunc(2.5::numeric, 3) AS v", "2.500"),
        ("SELECT round(1234.5::numeric, (-2)::integer) AS v", "1200"),
    ],
)
def test_a_said_scale_is_the_answers_scale(sql, text):
    assert str(_on_duckdb(sql)) == text


def test_a_scale_only_a_row_knows_is_refused():
    with pytest.raises(FunctionRefused, match="round"):
        transpile("SELECT round(2.5::numeric, n) AS v FROM t", "duckdb")


@pytest.mark.parametrize("dialect", ENGINES)
@pytest.mark.parametrize(
    "sql",
    [
        "SELECT ln(1000::numeric) AS v",
        "SELECT log(2.5::numeric, 2.5::numeric) AS v",
        "SELECT power(2.5::numeric, 2.5::numeric) AS v",
        "SELECT sqrt(2.5::numeric) AS v",
        "SELECT avg(c1) AS v FROM (VALUES (3::bigint)) AS t(c1)",
        "SELECT stddev(c1) AS v FROM (VALUES (2.5::numeric)) AS t(c1)",
        "SELECT regexp_replace('abc'::text, 'b'::text, 'x'::text, 3::integer) AS v",
    ],
)
def test_an_exact_answer_no_form_writes_is_refused_by_name(sql, dialect):
    with pytest.raises(FunctionRefused):
        transpile(sql, dialect)


def test_an_average_of_floats_is_the_engines_own():
    assert "AVG" in transpile("SELECT avg(c1) FROM (VALUES (1.5::float8)) AS t(c1)", "duckdb")


@pytest.mark.parametrize(
    "function",
    ["time_eq('1:00'::time, '1:00'::time)", "pg_lsn_larger('0/1'::pg_lsn, '0/2'::pg_lsn)"],
)
def test_a_row_replay_found_another_answer_of_is_refused_there(function):
    with pytest.raises(FunctionRefused):
        transpile(f"SELECT {function} AS v", "clickhouse")
    assert transpile(f"SELECT {function} AS v", "trino")


def test_a_clickhouse_timestamp_keeps_its_fraction():
    written = transpile("SELECT now() AS a, NULL::timestamp AS b", "clickhouse")
    assert "now64(6, 'UTC')" in written
    assert "Nullable(DateTime64(6, 'UTC'))" in written


def test_a_clickhouse_refusal_raises_where_a_nullable_cast_reads_null():
    written = transpile("SELECT masklen('::1'::inet) AS v", "clickhouse")
    assert "accurateCast(materialize(" in written


def test_money_divided_by_an_integer_truncates():
    assert "/" in transpile("SELECT cash_div_int4('1.00'::money, 3) AS v", "trino")
    assert "DOUBLE" not in transpile("SELECT cash_div_int4('1.00'::money, 3) AS v", "trino")


def test_a_null_shape_is_a_null_on_trino():
    written = transpile("SELECT center(NULL::box) AS v", "trino")
    assert written.startswith("SELECT CASE WHEN")


def test_a_column_of_a_values_list_is_the_type_its_first_row_says():
    tree = sqlglot.parse_one(
        "SELECT c1 FROM (VALUES (3::bigint), (4::bigint)) AS t(c1)", read="postgres"
    )
    said = values_cast(next(iter(tree.find_all(exp.Column))))
    assert said is not None and said.to.is_type(exp.DataType.Type.BIGINT)


@pytest.mark.parametrize(
    ("sql", "oid"),
    [
        ("SELECT max(c1) AS v FROM (VALUES (12345::oid)) AS t(c1)", 26),
        ("SELECT sum(c1) AS v FROM (VALUES (3::bigint)) AS t(c1)", 1700),
        ("SELECT sum(c1) AS v FROM (VALUES (0.5::real)) AS t(c1)", 700),
        ("SELECT max(NULL::character) AS v", 1042),
        ("SELECT jsonb_build_object('k'::text, NULL::integer) AS v", 3802),
        ("SELECT ntile(NULL::integer) OVER () AS v", 23),
        ("SELECT anyelement_out(NULL::integer) AS v", 2275),
        ("SELECT timestamp_larger(NULL::timestamp, NULL::timestamp) AS v", 1114),
    ],
)
def test_a_call_is_described_by_the_overload_its_arguments_name(sql, oid):
    casts = result_casts(sql)
    assert casts is not None and casts[0] is not None and casts[0].oid == oid


def test_an_enum_is_not_every_type():
    tree = sqlglot.parse_one('SELECT pg_catalog."min"(NULL::oid)', read="postgres")
    assert call_result_type(tree.selects[0]) == "oid"


def test_a_statement_after_a_failed_one_has_its_own_answer():
    from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime

    runtime = ClickHouseFederationRuntime.embedded()
    with pytest.raises(RuntimeError):
        runtime.run_sync("SELECT throwIf(1, 'refused')")
    assert runtime.run_sync("SELECT 'a' AS v").rows == [("a",)]
