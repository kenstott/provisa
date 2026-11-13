# Copyright (c) 2026 Kenneth Stott
# Canary: f78fe493-f16c-4450-8db8-c34acd82c921
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""ClickHouse's engine errors of run 43410717058, each now PostgreSQL's answer or a refusal by name
(REQ-2167, REQ-2236; polyfill grid).

Each statement is translated for ClickHouse and run on the embedded engine (chdb), in this
process, and its answer compared with PostgreSQL 17's. The groups, by ClickHouse's error:

1. & | # ~ << >> (a syntax error there): ClickHouse's bit functions, of the operands' type.
2. regexp_replace(s, p, r, 'g') in a form (replaceRegexpAll takes three arguments).
3. ROW(...) is ClickHouse's tuple; a refusal by name of a record is read as its own, not from the
   statement ClickHouse echoed.
4. A form's nested bindings (the network functions): a lambda of the name applied to the value,
   which ClickHouse reads within another, where a subquery within a subquery was refused.
5. intDiv, cume_dist, generateUUIDv4, min/max for bool_and/bool_or, arrayPushBack/Front, indexOf.
6. A NULL array: NULL of max, min, array_to_string; no rows of unnest; a bare NULL in a form.
7. lag, lead, nth_value of a NULL offset; ntile of a constant count, as a grid row.
8. numeric, bool, oid, xid, random(lo, hi), make_date, transaction_timestamp, generate_series,
   json_agg, numeric_mod, right: ClickHouse's forms.
9. Refused by name where ClickHouse has no form: a collated text order, an inet's host, family,
   broadcast, network, max and min, a range's lower, upper and unnest, string_agg of a delimiter
   that is no constant, json_object of arrays, three server functions."""

# Requirements: REQ-2167, REQ-2236, REQ-2171

from __future__ import annotations

import datetime
from decimal import Decimal
from typing import Any

import pytest

from provisa.core.pg_error import PostgresError, from_engine
from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime
from provisa.transpiler.function_macros import FunctionRefused
from provisa.transpiler.polyfill import GRID
from provisa.transpiler.transpile import transpile


@pytest.fixture(scope="module")
def runtime() -> ClickHouseFederationRuntime:
    return ClickHouseFederationRuntime.embedded()


def _rows(runtime: ClickHouseFederationRuntime, sql: str) -> list[tuple[Any, ...]]:
    return list(runtime.run_sync(transpile(sql, "clickhouse")).iter_rows())


def _one(runtime: ClickHouseFederationRuntime, sql: str) -> Any:
    (row,) = _rows(runtime, sql)
    return row[0]


def _raised(runtime: ClickHouseFederationRuntime, sql: str) -> PostgresError:
    with pytest.raises(Exception) as raised:
        _rows(runtime, sql)
    error = from_engine(raised.value)
    assert error is not None, str(raised.value)[:300]
    return error


def _refused(sql: str) -> str:
    with pytest.raises(FunctionRefused) as refused:
        transpile(sql, "clickhouse")
    message = str(refused.value)
    assert "has no form on clickhouse" in message or "does not read this value" in message
    return message


# --- 1. bit operators ----------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("sql", "answer"),
    [
        ("SELECT pg_catalog.int2and(5::smallint, 3::smallint) AS v", 1),
        ("SELECT pg_catalog.int4or(5, 3) AS v", 7),
        ("SELECT pg_catalog.int8xor(5::bigint, 3::bigint) AS v", 6),
        ("SELECT pg_catalog.int4not(5) AS v", -6),
        ("SELECT pg_catalog.int2not((-1)::smallint) AS v", 0),
        ("SELECT pg_catalog.int4shl(1, 33) AS v", 2),
        ("SELECT pg_catalog.int4shl(1, 31) AS v", -2147483648),
        ("SELECT pg_catalog.int4shr(-8, 1) AS v", -4),
        ("SELECT pg_catalog.int8shr((-9)::bigint, 2) AS v", -3),
        ("SELECT pg_catalog.int2shl(1::smallint, 15) AS v", -32768),
        ('SELECT pg_catalog."int4and"(NULL::integer, NULL::integer) AS v', None),
        ("SELECT pg_catalog.format_type(1700, 655366) AS v", "numeric(10,2)"),
        ("SELECT pg_catalog.xideqint4('5'::xid, 5) AS v", 1),
    ],
)
def test_a_bit_operation_is_clickhouses_bit_function(runtime, sql, answer):
    assert _one(runtime, sql) == answer


def test_no_bit_operator_reaches_clickhouse():
    written = transpile(
        "SELECT 5 & 3 AS a, 5 | 3 AS b, ~5 AS c, 5 # 3 AS d, 1 << 2 AS e", "clickhouse"
    )
    assert "bitAnd(5, 3)" in written and "bitOr(5, 3)" in written and "bitNot(5)" in written
    assert "bitXor(5, 3)" in written and "bitShiftLeft(1, 2)" in written
    assert not any(op in written for op in (" & ", " | ", "~", " ^ ", "<<"))


# --- 2. regexp_replace in a form, and the MAC address functions ---------------------------------


@pytest.mark.parametrize(
    ("sql", "answer"),
    [
        (
            "SELECT pg_catalog.macaddr_and('08:00:2b:01:02:03'::macaddr, "
            "'ff:ff:ff:00:00:00'::macaddr) AS v",
            "08:00:2b:00:00:00",
        ),
        (
            "SELECT pg_catalog.macaddr_or('08:00:2b:01:02:03'::macaddr, "
            "'ff:ff:ff:00:00:00'::macaddr) AS v",
            "ff:ff:ff:01:02:03",
        ),
        ("SELECT pg_catalog.macaddr_not('08:00:2b:01:02:03'::macaddr) AS v", "f7:ff:d4:fe:fd:fc"),
        (
            "SELECT pg_catalog.macaddr8_set7bit('08:00:2b:01:02:03:04:05'::macaddr8) AS v",
            "0a:00:2b:01:02:03:04:05",
        ),
        (
            "SELECT pg_catalog.macaddr_cmp('08:00:2b:01:02:03'::macaddr, "
            "'08-00-2b-01-02-03'::macaddr) AS v",
            0,
        ),
        (
            "SELECT pg_catalog.macaddr8('08:00:2b:01:02:03'::macaddr) AS v",
            "08:00:2b:ff:fe:01:02:03",
        ),
        (
            "SELECT pg_catalog.macaddr('08:00:2b:ff:fe:01:02:03'::macaddr8) AS v",
            "08:00:2b:01:02:03",
        ),
        ("SELECT pg_catalog.trunc('08:00:2b:01:02:03'::macaddr) AS v", "08:00:2b:00:00:00"),
        (
            "SELECT pg_catalog.trunc('08:00:2b:01:02:03:04:05'::macaddr8) AS v",
            "08:00:2b:00:00:00:00:00",
        ),
        ('SELECT pg_catalog."macaddr_eq"(NULL::macaddr, NULL::macaddr) AS v', None),
    ],
)
def test_a_mac_address_function_answers_postgresqls(runtime, sql, answer):
    assert _one(runtime, sql) == answer


def test_a_forms_every_match_is_replace_regexp_all():
    written = transpile(
        'SELECT pg_catalog."macaddr_eq"(NULL::macaddr, NULL::macaddr) AS v', "clickhouse"
    )
    assert "replaceRegexpAll(" in written
    assert ", 'g')" not in written


# --- 3. ROW, and a refusal read as its own -------------------------------------------------------


@pytest.mark.parametrize(
    ("sql", "answer"),
    [
        (
            "SELECT pg_catalog.record_lt(ROW(1, 2)::record, ROW(1, 5)::record) AS v",
            1,
        ),
        (
            "SELECT pg_catalog.record_eq(ROW(1, 'a'::text)::record, ROW(1, 'a'::text)::record) AS v",
            1,
        ),
        (
            "SELECT pg_catalog.record_ne(ROW(1, 'a'::text)::record, ROW(2, 'a'::text)::record) AS v",
            1,
        ),
        (
            "SELECT pg_catalog.record_ge(ROW(1, 2)::record, ROW(1, 5)::record) AS v",
            0,
        ),
    ],
)
def test_a_row_is_a_tuple_compared_field_by_field(runtime, sql, answer):
    assert _one(runtime, sql) == answer


@pytest.mark.parametrize("function", ["record_send", "record_out"])
def test_a_records_refusal_is_its_own_message(runtime, function):
    error = _raised(runtime, f"SELECT pg_catalog.{function}(ROW(1, 'a'::text)::record) AS v")
    assert (error.sqlstate, str(error)) == ("0A000", f"{function} does not read this value")


def test_a_mark_in_the_echoed_statement_is_no_raise():
    echoed = RuntimeError(
        "Code: 46. DB::Exception: Function with name `ROW` does not exist. In scope SELECT "
        "CONCAT('postgresql-error 0A000: ', 'record_send does not read this value', "
        "'<end of postgresql-error>') AS v. (UNKNOWN_FUNCTION)"
    )
    assert from_engine(echoed) is None


def test_a_raise_ends_where_clickhouse_writes_what_raised_it():
    raised = RuntimeError(
        'Code: 395. DB::Exception: postgresql-error 58P01: could not open directory "x": '
        "No such file or directory: while executing 'FUNCTION throwIf(1_UInt8 ...'"
    )
    error = from_engine(raised)
    assert error is not None
    assert str(error) == 'could not open directory "x": No such file or directory'


# --- 4. nested bindings: the network functions --------------------------------------------------


@pytest.mark.parametrize(
    ("sql", "answer"),
    [
        ("SELECT pg_catalog.network_eq('192.168.1.5'::inet, '192.168.1.5'::inet) AS v", 1),
        ("SELECT pg_catalog.network_lt('192.168.1.5'::inet, '192.168.1.6'::inet) AS v", 1),
        ("SELECT pg_catalog.network_cmp('192.168.1.0/24'::inet, '192.168.1.0/25'::inet) AS v", -1),
        ("SELECT pg_catalog.network_sub('192.168.1.5'::inet, '192.168.1.0/24'::inet) AS v", 1),
        (
            "SELECT pg_catalog.network_overlap('192.168.1.0/24'::inet, '192.168.0.0/16'::inet) AS v",
            1,
        ),
        ("SELECT pg_catalog.network_larger('10.0.0.1'::inet, '10.0.0.2'::inet) AS v", "10.0.0.2"),
        ("SELECT pg_catalog.set_masklen('192.168.1.5/24'::inet, 16) AS v", "192.168.1.5/16"),
        ("SELECT pg_catalog.set_masklen('192.168.1.0/24'::cidr, 16) AS v", "192.168.0.0/16"),
        ("SELECT pg_catalog.inetpl('192.168.1.5'::inet, 10::bigint) AS v", "192.168.1.15"),
        ("SELECT pg_catalog.inetmi_int8('192.168.1.5'::inet, 10::bigint) AS v", "192.168.0.251"),
        (
            "SELECT pg_catalog.int8pl_inet(10::bigint, '192.168.1.5/24'::inet) AS v",
            "192.168.1.15/24",
        ),
        ("SELECT pg_catalog.inetmi('192.168.1.5'::inet, '192.168.0.5'::inet) AS v", 256),
        ("SELECT pg_catalog.inetand('192.168.1.5'::inet, '255.255.0.0'::inet) AS v", "192.168.0.0"),
        (
            "SELECT pg_catalog.inetor('192.168.1.5'::inet, '0.0.255.255'::inet) AS v",
            "192.168.255.255",
        ),
        ('SELECT pg_catalog."network_eq"(NULL::inet, NULL::inet) AS v', None),
        ("SELECT '1.2.3.4'::inet AS v", "1.2.3.4"),
    ],
)
def test_a_network_function_answers_postgresqls(runtime, sql, answer):
    assert _one(runtime, sql) == answer


def test_a_network_function_of_a_column_answers_each_row(runtime):
    sql = (
        "SELECT pg_catalog.inetpl(c, 1::bigint) AS v "
        "FROM (VALUES ('10.1.2.3/8'::inet), ('10.0.0.255'::inet)) AS t(c)"
    )
    assert sorted(_rows(runtime, sql)) == [("10.0.1.0",), ("10.1.2.4/8",)]


@pytest.mark.parametrize(
    ("sql", "sqlstate", "message"),
    [
        (
            "SELECT pg_catalog.inetpl('255.255.255.255'::inet, 1::bigint) AS v",
            "22003",
            "result is out of range",
        ),
        (
            "SELECT pg_catalog.set_masklen('1.2.3.4'::inet, 33) AS v",
            "22023",
            "invalid mask length: 33",
        ),
    ],
)
def test_a_network_functions_error_is_raised_only_where_chosen(runtime, sql, sqlstate, message):
    error = _raised(runtime, sql)
    assert (error.sqlstate, str(error)) == (sqlstate, message)


def test_only_a_nested_binding_is_a_lambda():
    written = transpile(
        "SELECT pg_catalog.macaddr_not('08:00:2b:01:02:03'::macaddr) AS v", "clickhouse"
    )
    assert "arrayMap(" not in written


# --- 5. ClickHouse's names ----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("sql", "answer"),
    [
        ("SELECT pg_catalog.array_append('{1,2,3}'::integer[], 4::integer) AS v", [1, 2, 3, 4]),
        ("SELECT pg_catalog.array_prepend(0::integer, '{1,2,3}'::integer[]) AS v", [0, 1, 2, 3]),
        ("SELECT pg_catalog.array_position('{1,2,3}'::integer[], 3::integer) AS v", 3),
        ("SELECT pg_catalog.array_position('{1,2,3}'::integer[], 5::integer) AS v", None),
        (
            "SELECT pg_catalog.bool_and(c) AS v FROM (VALUES (true), (false), (NULL::boolean)) AS t(c)",
            False,
        ),
        (
            "SELECT pg_catalog.bool_or(c) AS v FROM (VALUES (true), (false), (NULL::boolean)) AS t(c)",
            True,
        ),
        ("SELECT pg_catalog.every(c) AS v FROM (VALUES (true), (NULL::boolean)) AS t(c)", True),
        ('SELECT pg_catalog."bool_and"(NULL::boolean) AS v', None),
        ("SELECT pg_catalog.cash_div_int4('10.00'::money, 3) AS v", Decimal("3.33")),
        ("SELECT pg_catalog.cash_div_int4('-10.00'::money, 3) AS v", Decimal("-3.33")),
    ],
)
def test_a_function_is_written_by_clickhouses_name(runtime, sql, answer):
    assert _one(runtime, sql) == answer


def test_cume_dist_and_a_uuid_are_clickhouses(runtime):
    sql = "SELECT pg_catalog.cume_dist() OVER (ORDER BY c) AS v FROM (VALUES (1), (2)) AS t(c)"
    assert sorted(_rows(runtime, sql)) == [(0.5,), (1.0,)]
    assert len(str(_one(runtime, 'SELECT pg_catalog."gen_random_uuid"() AS v'))) == 36


# --- 6. a NULL array ----------------------------------------------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        'SELECT pg_catalog."array_to_string"(NULL::integer[], NULL::text) AS v',
        'SELECT pg_catalog."array_to_string"(NULL::integer[], NULL::text, NULL::text) AS v',
        'SELECT pg_catalog."max"(NULL::integer[]) AS v',
        'SELECT pg_catalog."min"(NULL::integer[]) AS v',
        "SELECT CASE WHEN CAST(CAST(NULL AS INT) AS TEXT) IS NULL THEN CAST(NULL AS INT[]) "
        "ELSE CAST(ARRAY[] AS INT[]) END AS v",
    ],
)
def test_a_null_array_answers_null(runtime, sql):
    assert _one(runtime, sql) is None


def test_unnest_of_a_null_array_is_no_rows(runtime):
    assert _rows(runtime, 'SELECT pg_catalog."unnest"(NULL::integer[]) AS v') == []


def test_a_case_the_statements_nulls_decide_is_its_branch(runtime):
    sql = (
        "SELECT pg_catalog.tsrange(NULL::timestamp without time zone, "
        "NULL::timestamp without time zone, '[]'::text) AS v"
    )
    assert _one(runtime, sql) == "(,)"
    by_row = "SELECT CASE WHEN c IS NULL THEN 1 ELSE 2 END AS v FROM (VALUES (NULL::int), (1)) t(c)"
    assert sorted(_rows(runtime, by_row)) == [(1,), (2,)]


# --- 7. window functions ------------------------------------------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        'SELECT pg_catalog."lag"(NULL::integer, NULL::integer) OVER () AS v',
        'SELECT pg_catalog."lag"(NULL::integer, NULL::integer, NULL::integer) OVER () AS v',
        'SELECT pg_catalog."lead"(NULL::integer, NULL::integer) OVER () AS v',
        'SELECT pg_catalog."lead"(NULL::integer, NULL::integer, NULL::integer) OVER () AS v',
        'SELECT pg_catalog."nth_value"(NULL::integer, NULL::integer) OVER () AS v',
        'SELECT pg_catalog."ntile"(NULL::integer) OVER () AS v',
    ],
)
def test_a_window_function_of_a_null_count_answers_null(runtime, sql):
    assert _one(runtime, sql) is None


def test_ntile_is_clickhouses_over_the_whole_partition(runtime):
    assert _one(runtime, "SELECT ntile(3) OVER () AS v") == 1
    sql = "SELECT c, ntile(2) OVER (ORDER BY c) AS v FROM (VALUES (1), (2), (3)) AS t(c)"
    assert sorted(_rows(runtime, sql)) == [(1, 1), (2, 1), (3, 2)]


def test_ntile_of_no_bucket_is_postgresqls_error():
    with pytest.raises(FunctionRefused) as refused:
        transpile("SELECT ntile(0) OVER () AS v", "clickhouse")
    assert getattr(refused.value, "sqlstate", None) == "22014"


def test_ntile_of_a_column_is_refused_by_name():
    _refused("SELECT ntile(c) OVER (ORDER BY c) AS v FROM (VALUES (1)) AS t(c)")


def test_ntile_is_the_grids_row_on_every_engine():
    assert "ntile" in {f.name for f in GRID.features}
    assert "AS INT" in transpile("SELECT ntile(3) OVER () AS v", "duckdb")


# --- 8. forms -----------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("sql", "answer"),
    [
        ('SELECT pg_catalog."numeric"(5::bigint) AS v', Decimal(5)),
        ('SELECT pg_catalog."numeric"(NULL::integer) AS v', None),
        ('SELECT pg_catalog."bool"(0) AS v', False),
        ('SELECT pg_catalog."bool"(7) AS v', True),
        ('SELECT pg_catalog."oid"(5::bigint) AS v', 5),
        ('SELECT pg_catalog."oid"(NULL::bigint) AS v', None),
        ("SELECT pg_catalog.xid('4294967301'::xid8) AS v", 5),
        ('SELECT pg_catalog."random"(3, 3) AS v', 3),
        ('SELECT pg_catalog."random"(NULL::integer, NULL::integer) AS v', None),
        ("SELECT make_date(2024, 2, 29) AS v", datetime.date(2024, 2, 29)),
        ('SELECT pg_catalog."make_date"(NULL::integer, NULL::integer, NULL::integer) AS v', None),
        ('SELECT pg_catalog."numeric_mod"(1000::numeric, 1000::numeric) AS v', "0"),
        ('SELECT pg_catalog."numeric_mod"(1001.50::numeric, 1000::numeric) AS v', "1.50"),
        ('SELECT pg_catalog."numeric_mod"((-7.5)::numeric, 2::numeric) AS v', "-1.5"),
        ("SELECT pg_catalog.right('hello', 2) AS v", "lo"),
        ("SELECT pg_catalog.right('hello', -2) AS v", "llo"),
        ('SELECT pg_catalog."right"(NULL::text, NULL::integer) AS v', None),
        ('SELECT pg_catalog."json_agg"(NULL::integer) AS v', "[null]"),
        ("SELECT json_agg(c::text) AS v FROM (VALUES ('a'), ('b')) AS t(c)", '["a","b"]'),
    ],
)
def test_a_form_answers_postgresqls(runtime, sql, answer):
    assert _one(runtime, sql) == answer


def test_random_between_two_bounds_is_within_them(runtime):
    assert 1 <= _one(runtime, "SELECT pg_catalog.random(1, 10) AS v") <= 10


def test_transaction_timestamp_is_the_statements_time(runtime):
    assert isinstance(
        _one(runtime, 'SELECT pg_catalog."transaction_timestamp"() AS v'), datetime.datetime
    )


@pytest.mark.parametrize(
    ("sql", "rows"),
    [
        ('SELECT pg_catalog."generate_series"(NULL::integer, NULL::integer) AS v', []),
        ('SELECT pg_catalog."generate_series"(NULL::bigint, NULL::bigint, NULL::bigint) AS v', []),
        ("SELECT generate_series(1, 3) AS v", [(1,), (2,), (3,)]),
        ("SELECT generate_series(5, 1, -2) AS v", [(5,), (3,), (1,)]),
    ],
)
def test_generate_series_in_a_select_list_is_an_array_join(runtime, sql, rows):
    assert _rows(runtime, sql) == rows


@pytest.mark.parametrize(
    ("sql", "sqlstate", "message"),
    [
        ("SELECT generate_series(1, 3, 0) AS v", "22023", "step size cannot equal zero"),
        ('SELECT pg_catalog."oid"((-1)::bigint) AS v', "22003", "OID out of range"),
        (
            'SELECT pg_catalog."random"(5::bigint, 3::bigint) AS v',
            "22023",
            "lower bound must be less than or equal to upper bound",
        ),
        ("SELECT make_date(2023, 2, 29) AS v", "0A000", "make_date does not read this value"),
        (
            'SELECT pg_catalog."numeric_mod"(1::numeric, 0::numeric) AS v',
            "22012",
            "division by zero",
        ),
    ],
)
def test_a_form_raises_postgresqls_error(runtime, sql, sqlstate, message):
    error = _raised(runtime, sql)
    assert (error.sqlstate, str(error)) == (sqlstate, message)


# --- 9. refused by name -------------------------------------------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        'SELECT pg_catalog."text_lt"(NULL::text, NULL::text) AS v',
        'SELECT pg_catalog."bpcharcmp"(NULL::character, NULL::character) AS v',
        'SELECT pg_catalog."text_larger"(NULL::text, NULL::text) AS v',
        'SELECT pg_catalog."host"(NULL::inet) AS v',
        'SELECT pg_catalog."family"(NULL::inet) AS v',
        'SELECT pg_catalog."broadcast"(NULL::inet) AS v',
        'SELECT pg_catalog."network"(NULL::inet) AS v',
        'SELECT pg_catalog."max"(NULL::inet) AS v',
        'SELECT pg_catalog."min"(NULL::inet) AS v',
        'SELECT pg_catalog."lower"(NULL::int4range) AS v',
        'SELECT pg_catalog."upper"(NULL::int4multirange) AS v',
        'SELECT pg_catalog."unnest"(NULL::int4multirange) AS v',
        'SELECT pg_catalog."string_agg"(NULL::text, NULL::text) AS v',
        'SELECT pg_catalog."string_agg"(NULL::bytea, NULL::bytea) AS v',
        "SELECT pg_catalog.\"json_object\"('{a,b,c}'::text[], '{a,b,c}'::text[]) AS v",
        'SELECT pg_catalog."pg_conf_load_time"() AS v',
        'SELECT pg_catalog."pg_my_temp_schema"() AS v',
        'SELECT pg_catalog."pg_postmaster_start_time"() AS v',
        "SELECT generate_series(1.0, 2.0) AS v",
        "SELECT json_agg(c::date) AS v FROM (VALUES ('2020-01-01')) AS t(c)",
    ],
)
def test_what_clickhouse_has_no_form_of_is_refused_by_name(sql):
    _refused(sql)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT pg_catalog.lower('ABC') AS v",
        "SELECT pg_catalog.max(5) AS v",
        "SELECT string_agg(c, ',') AS v FROM (VALUES ('a'), ('b')) AS t(c)",
    ],
)
def test_what_clickhouse_has_is_not_refused(runtime, sql):
    assert _rows(runtime, sql)


def test_the_other_engines_keep_their_forms():
    assert "BOOL_AND" in transpile('SELECT pg_catalog."bool_and"(NULL::boolean) AS v', "trino")
    assert "&" in transpile("SELECT pg_catalog.int4and(5, 3) AS v", "duckdb")
    assert "COLLATE" in transpile("SELECT pg_catalog.text_lt('a'::text, 'b'::text) AS v", "duckdb")


# --- run 43424649612: the wrong answers and errors of the first form ------------------------------


@pytest.mark.parametrize(
    ("sql", "answer"),
    [
        # A strict function of NULL is NULL: ``x IS NOT NULL`` of a NULL is false (sqlglot's Is,
        # negated), which the fold of a decided CASE read as true.
        ('SELECT pg_catalog."isfinite"(NULL::date) AS v', None),
        ('SELECT pg_catalog."isfinite"(NULL::timestamp without time zone) AS v', None),
        ('SELECT pg_catalog."masklen"(NULL::inet) AS v', None),
        ('SELECT pg_catalog."hostmask"(NULL::inet) AS v', None),
        ('SELECT pg_catalog."network_sub"(NULL::inet, NULL::inet) AS v', None),
        ('SELECT pg_catalog."network_sup"(NULL::inet, NULL::inet) AS v', None),
        ("SELECT CASE WHEN CAST(NULL AS BIGINT) IS NOT NULL THEN FALSE END AS v", None),
        ('SELECT pg_catalog."num_nonnulls"(NULL::integer) AS v', 0),
        ('SELECT pg_catalog."regr_count"(NULL::double precision, NULL::double precision) AS v', 0),
        ('SELECT pg_catalog."rank"(NULL::integer) WITHIN GROUP (ORDER BY NULL::integer) AS v', 1),
        (
            'SELECT pg_catalog."dense_rank"(NULL::integer) WITHIN GROUP (ORDER BY NULL::integer) '
            "AS v",
            1,
        ),
        (
            'SELECT pg_catalog."cume_dist"(NULL::integer) WITHIN GROUP (ORDER BY NULL::integer) '
            "AS v",
            1.0,
        ),
        (
            'SELECT pg_catalog."percent_rank"(NULL::integer) WITHIN GROUP '
            "(ORDER BY NULL::integer) AS v",
            0.0,
        ),
        ('SELECT pg_catalog."int8shl"(3::bigint, 3::integer) AS v', 24),
        ('SELECT pg_catalog."int8shl"(1::bigint, 64) AS v', 1),
        (
            "SELECT pg_catalog.\"array_to_string\"('{1,2,3}'::integer[], 'abc'::text, "
            "'abc'::text) AS v",
            "1abc2abc3",
        ),
        ("SELECT array_to_string('{1,NULL,3}'::integer[], ',', '*') AS v", "1,*,3"),
        ("SELECT array_to_string('{1,NULL,3}'::integer[], ',') AS v", "1,3"),
    ],
)
def test_a_statement_of_run_43424649612_answers_postgresqls(runtime, sql, answer):
    assert _one(runtime, sql) == answer


@pytest.mark.parametrize(
    ("function", "answer"),
    [("ts_rank", 0.0607927 / 1.5849625 / 2), ("ts_rank_cd", 0.1 / 1.0986123 / 2)],
)
def test_a_rank_normalized_by_length_and_words_is_postgresqls(runtime, function, answer):
    sql = f"SELECT pg_catalog.\"{function}\"('cat:1 fat:2'::tsvector, 'cat'::tsquery, 3::integer) AS v"
    assert _one(runtime, sql) == pytest.approx(answer, rel=1e-6)


@pytest.mark.parametrize(
    "function",
    [f"record_{k}{op}" for k in ("", "image_") for op in ("eq", "ne", "lt", "le", "gt", "ge")],
)
def test_a_record_holding_null_is_refused_by_name(function):
    row = "ROW(NULL::integer, 'c'::text)::record"
    _refused(f'SELECT pg_catalog."{function}"({row}, {row}) AS v')


@pytest.mark.parametrize(
    ("function", "answer"),
    [
        ("record_eq", 0),
        ("record_ne", 1),
        ("record_lt", 1),
        ("record_ge", 0),
        ("record_image_le", 1),
    ],
)
def test_a_record_of_numbers_is_compared_as_its_tuple(runtime, function, answer):
    sql = f'SELECT pg_catalog."{function}"(ROW(1, -2)::record, ROW(1, 3)::record) AS v'
    assert _one(runtime, sql) == answer


def test_an_order_of_records_holding_text_is_refused_by_name():
    _refused(
        "SELECT pg_catalog.record_lt(ROW(1, 'a'::text)::record, ROW(1, 'B'::text)::record) AS v"
    )


_OFFSETS = (
    "FROM (VALUES (1, 3::integer, 1::integer, 3::integer), (2, (-7)::integer, 2::integer, "
    "(-7)::integer), (3, 42::integer, 1::integer, 42::integer)) AS t(c1, c2, c3, c4) ORDER BY c1"
)


@pytest.mark.parametrize(
    ("call", "rows"),
    [
        ('pg_catalog."lag"(c2, c3)', [None, None, -7]),
        ('pg_catalog."lag"(c2, c3, c4)', [3, -7, -7]),
        ('pg_catalog."lead"(c2, c3)', [-7, None, None]),
        ('pg_catalog."lead"(c2, c3, c4)', [-7, -7, 42]),
        ('pg_catalog."nth_value"(c2, c3)', [3, -7, 3]),
    ],
)
def test_a_window_offset_of_a_column_is_each_rows(runtime, call, rows):
    sql = f"SELECT {call} OVER (ORDER BY c1) AS v {_OFFSETS}"
    assert [row[0] for row in _rows(runtime, sql)] == rows


@pytest.mark.parametrize(
    ("sql", "sqlstate"),
    [
        (
            "SELECT nth_value(c, 0::integer) OVER (ORDER BY c) AS v FROM (VALUES (1)) AS t(c)",
            "22016",
        ),
        ("SELECT lag(c, -1) OVER (ORDER BY c) AS v FROM (VALUES (1)) AS t(c)", "0A000"),
    ],
)
def test_a_window_offset_below_its_least_is_raised(runtime, sql, sqlstate):
    assert _raised(runtime, sql).sqlstate == sqlstate


@pytest.mark.parametrize("dialect", ["duckdb", "trino"])
def test_numeric_mod_keeps_the_old_tables_form_elsewhere(dialect):
    written = transpile(
        'SELECT pg_catalog."numeric_mod"(NULL::numeric, NULL::numeric) AS v', dialect
    )
    assert "numeric_mod" not in written.lower()
