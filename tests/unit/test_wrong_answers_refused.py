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

from provisa.federation.duckdb_pg_scalars import register
from provisa.pgwire.cast_types import result_casts
from provisa.pgwire.function_types import call_result_type
from provisa.transpiler.function_macros import FunctionRefused
from provisa.transpiler.pg_forms_geometric import routines_apart
from provisa.transpiler.pg_literals import exact_inputs, values_cast
from provisa.transpiler.transpile import transpile, transpile_to_duckdb

ENGINES = ("trino", "clickhouse", "duckdb")


_CON = duckdb.connect()
register(_CON)


def _on_duckdb(sql: str):
    return _CON.execute(transpile_to_duckdb(sql)).fetchall()[0][0]


@pytest.mark.parametrize(
    ("sql", "kept"),
    [
        ("SELECT 2.5::numeric", "CAST(2.5 AS DECIMAL(20, 1))"),
        ("SELECT (-7.125)::numeric", "CAST((-7.125) AS DECIMAL(22, 3))"),
        ("SELECT 1000::numeric", "CAST(1000 AS DECIMAL(22, 0))"),
        ("SELECT CAST('12.50' AS numeric)", "CAST('12.50' AS DECIMAL(22, 2))"),
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
    assert "DECIMAL(20, 1)" in written.replace("Decimal", "DECIMAL")


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


@pytest.mark.parametrize(
    ("sql", "text"),
    [
        ("SELECT ln(1000::numeric) AS v", "6.9077552789821371"),
        ("SELECT log(2.5::numeric, 2.5::numeric) AS v", "1.0000000000000000"),
        ("SELECT log10(1000::numeric) AS v", "3.0000000000000000"),
        ("SELECT power(2.5::numeric, 2.5::numeric) AS v", "9.8821176880261854"),
        ("SELECT sqrt(2.5::numeric) AS v", "1.581138830084190"),
        ("SELECT exp((-7.125)::numeric) AS v", "0.0008047330101246133"),
        ("SELECT div(2.5::numeric, 2.5::numeric) AS v", "1"),
        ("SELECT pg_size_pretty(2.5::numeric) AS v", "2.5 bytes"),
        ('SELECT "numeric"(100::double precision) AS v', "100"),
        ("SELECT to_number('-7', 'S9') AS v", "-7"),
        ("SELECT regexp_replace('abc'::text, 'b'::text, 'x'::text, 3::integer) AS v", "abc"),
        (
            "SELECT avg(c1) AS v FROM (VALUES (3::bigint), (4::bigint)) AS t(c1)",
            "3.5000000000000000",
        ),
        (
            "SELECT var_samp(c1) AS v FROM (VALUES (1::integer), (2::integer), (4::integer)) AS t(c1)",
            "2.3333333333333333",
        ),
        (
            "SELECT stddev(c1) AS v FROM (VALUES (2.5::numeric), ((-7.125)::numeric)) AS t(c1)",
            "6.8059027689205199",
        ),
        (
            "SELECT max(c1) AS v FROM (VALUES ('abc'::text), ('Hello'::text), ('12'::text)) AS t(c1)",
            "Hello",
        ),
    ],
)
def test_an_exact_answer_on_duckdb_has_postgresqls_digits(sql, text):
    assert _on_duckdb(sql) == text


@pytest.mark.parametrize("dialect", ["trino", "clickhouse"])
def test_a_dialect_with_no_exact_digits_refuses_by_name(dialect):
    with pytest.raises(FunctionRefused, match="sqrt"):
        transpile("SELECT sqrt(2.5::numeric) AS v", dialect)


def test_an_order_the_key_cannot_say_is_refused_for_that_value():
    with pytest.raises(duckdb.Error, match="does not read this value"):
        _on_duckdb("SELECT max(c1) AS v FROM (VALUES ('a b'::text)) AS t(c1)")


@pytest.mark.parametrize(
    "sql", ["SELECT sqrt(NULL::numeric) AS v", "SELECT stddev(NULL::integer) AS v"]
)
def test_a_null_has_no_digits_to_differ_in(sql):
    assert transpile(sql, "duckdb")


def test_one_value_has_no_order():
    assert _on_duckdb("SELECT min('a'::character) AS v") == "a"


def test_an_average_of_floats_is_the_engines_own():
    assert "AVG" in transpile("SELECT avg(c1) FROM (VALUES (1.5::float8)) AS t(c1)", "duckdb")


def test_a_row_replay_found_another_answer_of_is_refused_there():
    function = "pg_lsn_pli('0/1'::pg_lsn, 2.5::numeric)"
    with pytest.raises(FunctionRefused):
        transpile(f"SELECT {function} AS v", "clickhouse")
    # Trino reads the form's 2^64 as no literal (run 43307392116): the grid refuses it by name.
    with pytest.raises(FunctionRefused, match=r"pg_lsn_pli\(\) has no form on trino"):
        transpile(f"SELECT {function} AS v", "trino")


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
    # The statement after its routine definitions (REQ-2171: Trino defines a shape's helpers).
    _, written = routines_apart(transpile("SELECT center(NULL::box) AS v", "trino"))
    assert written.startswith("SELECT CASE WHEN NULL IS NULL")


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


def test_a_clickhouse_time_is_read_as_its_text_says():
    from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime

    runtime = ClickHouseFederationRuntime.embedded()
    equal = "SELECT time_eq('23:59:59.5'::time, '23:59:59.5'::time) AS v"
    assert runtime.run_sync(transpile(equal, "clickhouse")).rows == [(1,)]
    missing = "SELECT time_eq(NULL::time, NULL::time) AS v"
    assert runtime.run_sync(transpile(missing, "clickhouse")).rows == [(None,)]


def test_a_null_scale_answers_null():
    assert _on_duckdb("SELECT round(NULL::numeric, NULL::integer) AS v") is None


def test_a_null_scale_truncates_to_null():
    assert _on_duckdb("SELECT trunc(1.25::numeric, NULL::integer) AS v") is None


def test_a_scale_still_rounds_and_truncates():
    assert str(_on_duckdb("SELECT round(1.255::numeric, 2) AS v")) == "1.26"
    assert str(_on_duckdb("SELECT trunc(1.257::numeric, 2) AS v")) == "1.25"


def test_a_clickhouse_time_is_a_time_on_the_wire():
    casts = result_casts("SELECT time_larger('12:30:45'::time, '12:30:45'::time) AS v")
    assert casts is not None and casts[0] is not None and casts[0].coerce is not None
    assert str(casts[0].coerce((45045500000).to_bytes(8, "little"))) == "12:30:45.500000"


@pytest.mark.parametrize("dialect", ["trino", "clickhouse"])
@pytest.mark.parametrize(
    "sql",
    [
        "SELECT hashoid(12345::oid) AS v",
        "SELECT btoidcmp(1::oid, 7::oid) AS v",
        "SELECT lo_get(1::oid) AS v",
    ],
)
def test_an_oid_is_held_as_a_bigint_and_reaches_a_call(sql, dialect):
    assert " oid" not in transpile(sql, dialect).lower()


@pytest.mark.parametrize("dialect", ["trino", "clickhouse"])
@pytest.mark.parametrize(
    "sql",
    [
        'SELECT pg_catalog."int8"(12345::oid) AS v',
        "SELECT pg_catalog.\"nameconcatoid\"('abc'::name, 12345::oid) AS v",
    ],
)
def test_the_calls_of_an_oid_a_replay_refused_stay_refused(sql, dialect):
    with pytest.raises(FunctionRefused):
        transpile(sql, dialect)
