# Copyright (c) 2026 Kenneth Stott
# Canary: 8e4b1d63-2f7a-4c59-9a30-6d1e5b7c2f84
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""ClickHouse's rows of make_interval, make_time, max and min of times, to_timestamp of a double,
timestamp(timestamp, integer), timestamptz of a date, a timestamp or a date and a time,
int2int4_sum, int4 of a "char" and numeric of a double or a real
(REQ-2236; REQ-2171), each refused by name on ClickHouse before: the functions measure's own calls
and the edges the rows read, answered on the embedded engine (chdb) against the embedded
PostgreSQL's recorded answer (numbers compared by value)."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

import datetime
from decimal import Decimal, InvalidOperation

import pytest

from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime
from provisa.transpiler.function_macros import FunctionRefused
from provisa.transpiler.transpile import transpile

ANSWERED = [
    # max and min of times: shown as time_out's text
    (
        "SELECT pg_catalog.\"max\"(c1) AS v FROM (VALUES ('12:30:45'::time), ('01:00:00.5'::time), "
        "(NULL::time)) AS t(c1)",
        ["12:30:45"],
    ),
    (
        "SELECT pg_catalog.\"min\"(c1) AS v FROM (VALUES ('12:30:45'::time), ('01:00:00.5'::time), "
        "(NULL::time)) AS t(c1)",
        ["01:00:00.5"],
    ),
    ('SELECT pg_catalog."max"(c1) AS v FROM (VALUES (NULL::time)) AS t(c1)', [None]),
    # int4 of a "char" (chartoi4): its byte as a signed char
    ('SELECT pg_catalog."int4"(NULL::"char") AS v', [None]),
    ('SELECT pg_catalog."int4"(\'a\'::"char") AS v', ["97"]),
    ('SELECT pg_catalog."int4"(\'B\'::"char") AS v', ["66"]),
    ('SELECT pg_catalog."int4"(\'1\'::"char") AS v', ["49"]),
    ('SELECT pg_catalog."int4"(\'\'::"char") AS v', ["0"]),
    ('SELECT pg_catalog."int2int4_sum"(NULL::bigint[]) AS v', [None]),
    ("SELECT pg_catalog.\"int2int4_sum\"('{3,6}'::bigint[]) AS v", ["6"]),
    ("SELECT pg_catalog.\"int2int4_sum\"('{1,-7}'::bigint[]) AS v", ["-7"]),
    ("SELECT pg_catalog.\"int2int4_sum\"('{4,100}'::bigint[]) AS v", ["100"]),
    (
        'SELECT pg_catalog."make_interval"(NULL::integer, NULL::integer, NULL::integer, NULL::integer, NULL::integer, NULL::integer, NULL::double precision) AS v',
        [None],
    ),
    (
        'SELECT pg_catalog."make_interval"(3::integer, 3::integer, 3::integer, 3::integer, 3::integer, 3::integer, 0.5::double precision) AS v',
        ["3 years 3 mons 24 days 03:03:00.5"],
    ),
    (
        'SELECT pg_catalog."make_interval"((-7)::integer, (-7)::integer, (-7)::integer, (-7)::integer, (-7)::integer, (-7)::integer, (-2.25)::double precision) AS v',
        ["-7 years -7 mons -56 days -07:07:02.25"],
    ),
    (
        'SELECT pg_catalog."make_interval"(42::integer, 42::integer, 42::integer, 42::integer, 42::integer, 42::integer, 100::double precision) AS v',
        ["45 years 6 mons 336 days 42:43:40"],
    ),
    (
        'SELECT pg_catalog."make_interval"(NULL::integer, 3::integer, 3::integer, 3::integer, 3::integer, 3::integer, 0.5::double precision) AS v LIMIT 1001',
        [None],
    ),
    (
        'SELECT pg_catalog."make_interval"(3::integer, NULL::integer, 3::integer, 3::integer, 3::integer, 3::integer, 0.5::double precision) AS v LIMIT 1001',
        [None],
    ),
    (
        'SELECT pg_catalog."make_interval"(3::integer, 3::integer, NULL::integer, 3::integer, 3::integer, 3::integer, 0.5::double precision) AS v LIMIT 1001',
        [None],
    ),
    (
        'SELECT pg_catalog."make_interval"(3::integer, 3::integer, 3::integer, NULL::integer, 3::integer, 3::integer, 0.5::double precision) AS v LIMIT 1001',
        [None],
    ),
    (
        'SELECT pg_catalog."make_interval"(3::integer, 3::integer, 3::integer, 3::integer, NULL::integer, 3::integer, 0.5::double precision) AS v LIMIT 1001',
        [None],
    ),
    (
        'SELECT pg_catalog."make_interval"(3::integer, 3::integer, 3::integer, 3::integer, 3::integer, NULL::integer, 0.5::double precision) AS v LIMIT 1001',
        [None],
    ),
    (
        'SELECT pg_catalog."make_interval"(3::integer, 3::integer, 3::integer, 3::integer, 3::integer, 3::integer, NULL::double precision) AS v LIMIT 1001',
        [None],
    ),
    (
        'SELECT pg_catalog."make_time"(NULL::integer, NULL::integer, NULL::double precision) AS v',
        [None],
    ),
    (
        'SELECT pg_catalog."make_time"(12::integer, 30::integer, 45.5::double precision) AS v',
        ["12:30:45.5"],
    ),
    (
        'SELECT pg_catalog."make_time"(0::integer, 0::integer, 0::double precision) AS v',
        ["00:00:00"],
    ),
    (
        'SELECT pg_catalog."make_time"(23::integer, 59::integer, 59.5::double precision) AS v',
        ["23:59:59.5"],
    ),
    ('SELECT pg_catalog."numeric"(NULL::bigint) AS v', [None]),
    ('SELECT pg_catalog."numeric"(3::bigint) AS v', ["3"]),
    ('SELECT pg_catalog."numeric"((-7)::bigint) AS v', ["-7"]),
    ('SELECT pg_catalog."numeric"(5000000000::bigint) AS v', ["5000000000"]),
    (
        "SELECT pg_catalog.\"numeric\"('-9223372036854775808'::bigint) AS v LIMIT 1001",
        ["-9223372036854775808"],
    ),
    (
        "SELECT pg_catalog.\"numeric\"('9223372036854775807'::bigint) AS v LIMIT 1001",
        ["9223372036854775807"],
    ),
    ("SELECT pg_catalog.\"numeric\"('0'::bigint) AS v LIMIT 1001", ["0"]),
    ('SELECT pg_catalog."numeric"(NULL::double precision) AS v', [None]),
    ('SELECT pg_catalog."numeric"(0.5::double precision) AS v', ["0.5"]),
    ('SELECT pg_catalog."numeric"((-2.25)::double precision) AS v', ["-2.25"]),
    ('SELECT pg_catalog."numeric"(100::double precision) AS v', ["100"]),
    ("SELECT pg_catalog.\"numeric\"('-0'::double precision) AS v LIMIT 1001", ["0"]),
    ('SELECT pg_catalog."numeric"(NULL::integer) AS v', [None]),
    ('SELECT pg_catalog."numeric"(3::integer) AS v', ["3"]),
    ('SELECT pg_catalog."numeric"((-7)::integer) AS v', ["-7"]),
    ('SELECT pg_catalog."numeric"(42::integer) AS v', ["42"]),
    ("SELECT pg_catalog.\"numeric\"('-2147483648'::integer) AS v LIMIT 1001", ["-2147483648"]),
    ("SELECT pg_catalog.\"numeric\"('2147483647'::integer) AS v LIMIT 1001", ["2147483647"]),
    ("SELECT pg_catalog.\"numeric\"('0'::integer) AS v LIMIT 1001", ["0"]),
    ('SELECT pg_catalog."numeric"(NULL::jsonb) AS v', [None]),
    ("SELECT pg_catalog.\"numeric\"('3'::jsonb) AS v", ["3"]),
    (
        "SELECT pg_catalog.\"numeric\"('12345678901234567890123456789'::jsonb) AS v LIMIT 1001",
        ["12345678901234567890123456789"],
    ),
    ('SELECT pg_catalog."numeric"(NULL::money) AS v', [None]),
    ("SELECT pg_catalog.\"numeric\"('12.34'::money) AS v", ["12.34"]),
    ("SELECT pg_catalog.\"numeric\"('-5.00'::money) AS v", ["-5.00"]),
    ("SELECT pg_catalog.\"numeric\"('1000.00'::money) AS v", ["1000.00"]),
    (
        "SELECT pg_catalog.\"numeric\"('-92233720368547758.08'::money) AS v LIMIT 1001",
        ["-92233720368547758.08"],
    ),
    (
        "SELECT pg_catalog.\"numeric\"('92233720368547758.07'::money) AS v LIMIT 1001",
        ["92233720368547758.07"],
    ),
    ("SELECT pg_catalog.\"numeric\"('0'::money) AS v LIMIT 1001", ["0.00"]),
    ('SELECT pg_catalog."numeric"(NULL::real) AS v', [None]),
    ('SELECT pg_catalog."numeric"(0.5::real) AS v', ["0.5"]),
    ('SELECT pg_catalog."numeric"((-2.25)::real) AS v', ["-2.25"]),
    ('SELECT pg_catalog."numeric"(100::real) AS v', ["100"]),
    ("SELECT pg_catalog.\"numeric\"('-0'::real) AS v LIMIT 1001", ["0"]),
    ('SELECT pg_catalog."numeric"(NULL::smallint) AS v', [None]),
    ('SELECT pg_catalog."numeric"(3::smallint) AS v', ["3"]),
    ('SELECT pg_catalog."numeric"((-7)::smallint) AS v', ["-7"]),
    ('SELECT pg_catalog."numeric"(42::smallint) AS v', ["42"]),
    ("SELECT pg_catalog.\"numeric\"('-32768'::smallint) AS v LIMIT 1001", ["-32768"]),
    ("SELECT pg_catalog.\"numeric\"('32767'::smallint) AS v LIMIT 1001", ["32767"]),
    ("SELECT pg_catalog.\"numeric\"('0'::smallint) AS v LIMIT 1001", ["0"]),
    ("SELECT make_time(1, 2, 3.0000005)", ["01:02:03"]),
    ("SELECT int2int4_sum('{0,0}'::bigint[])", [None]),
]
RAISED = [
    # PostgreSQL 17 checks each step of make_interval (pg_mul_s32_overflow); 16 wrapped
    (
        "SELECT pg_catalog.\"make_interval\"('-2147483648'::integer, 3::integer, 3::integer, "
        "3::integer, 3::integer, 3::integer, 0.5::double precision) AS v",
        "22008",
    ),
    ("SELECT pg_catalog.\"numeric\"('9223372036854775808'::bigint) AS v LIMIT 1001", "22003"),
    ("SELECT pg_catalog.\"numeric\"('2147483648'::integer) AS v LIMIT 1001", "22003"),
    ('SELECT pg_catalog."numeric"(\'{"a": 1, "b": [1, 2]}\'::jsonb) AS v', "22023"),
    ('SELECT pg_catalog."numeric"(\'[1, "two", null]\'::jsonb) AS v', "22023"),
    ("SELECT pg_catalog.\"numeric\"('{}'::jsonb) AS v LIMIT 1001", "22023"),
    ("SELECT pg_catalog.\"numeric\"('[]'::jsonb) AS v LIMIT 1001", "22023"),
    ('SELECT pg_catalog."numeric"(\'"日本語😀"\'::jsonb) AS v LIMIT 1001', "22023"),
    ("SELECT pg_catalog.\"numeric\"('32768'::smallint) AS v LIMIT 1001", "22003"),
    ("SELECT make_time(24, 0, 0.5)", "22008"),
    ("SELECT make_time(1, 60, 0)", "22008"),
    ("SELECT make_time(-1, 2, 3)", "22008"),
    ("SELECT int2int4_sum('{2}'::bigint[])", "XX000"),
]


@pytest.fixture(scope="module")
def runtime() -> ClickHouseFederationRuntime:
    return ClickHouseFederationRuntime.embedded()


def _answers(runtime: ClickHouseFederationRuntime, sql: str) -> list[object]:
    return [row[0] for row in runtime.run_sync(transpile(sql, "clickhouse")).iter_rows()]


def _same(want: str | None, got: object) -> bool:
    if want is None or got is None:
        return want is got
    if str(got) == want:
        return True
    try:
        return Decimal(str(got)) == Decimal(want)
    except InvalidOperation:
        return False


@pytest.mark.parametrize(("sql", "want"), ANSWERED)
def test_clickhouse_answers_postgresql(runtime, sql, want):
    got = _answers(runtime, sql)
    assert len(got) == len(want) and all(map(_same, want, got)), (got, want)


@pytest.mark.parametrize(("sql", "sqlstate"), RAISED)
def test_clickhouse_raises_postgresqls_class(runtime, sql, sqlstate):
    try:
        _answers(runtime, sql)
    except FunctionRefused as refused:
        assert "no form" not in str(refused)
        assert getattr(refused, "sqlstate", "")[:2] == sqlstate[:2]
        return
    except Exception as raised:  # noqa: BLE001 -- the engine's raise carries PostgreSQL's error
        assert f"postgresql-error {sqlstate[:2]}" in str(raised), raised
        return
    pytest.fail(f"answered where PostgreSQL raises {sqlstate}")


#: to_timestamp of a double (float8_timestamptz), PostgreSQL's answer as an instant in UTC.
INSTANTS = [
    ('SELECT pg_catalog."to_timestamp"(NULL::double precision) AS v', None),
    ("SELECT pg_catalog.\"to_timestamp\"('1999'::double precision) AS v", "1970-01-01 00:33:19"),
    ("SELECT to_timestamp(0.5)", "1970-01-01 00:00:00.5"),
    ("SELECT to_timestamp(-1.0000005)", "1969-12-31 23:59:59"),  # rint after the 2000 epoch
    ("SELECT to_timestamp(1718454645.123456)", "2024-06-15 12:30:45.123456"),
    ("SELECT to_timestamp(' 1e9 '::double precision)", "2001-09-09 01:46:40"),
]


#: timestamptz of a date, a timestamp, or a date and a time, in the session zone UTC.
INSTANTS += [
    ('SELECT pg_catalog."timestamptz"(NULL::date) AS v', None),
    ("SELECT pg_catalog.\"timestamptz\"('2000-02-29'::date) AS v", "2000-02-29 00:00:00"),
    (
        "SELECT pg_catalog.\"timestamptz\"('2024-06-15 12:30:45'::timestamp without time zone) AS v",
        "2024-06-15 12:30:45",
    ),
    (
        "SELECT pg_catalog.\"timestamptz\"('2000-02-29'::date, '23:59:59.5'::time without time zone) "
        "AS v",
        "2000-02-29 23:59:59.5",
    ),
    (
        "SELECT pg_catalog.\"timestamptz\"('2024-06-15'::date, '24:00:00'::time without time zone) "
        "AS v",
        "2024-06-16 00:00:00",
    ),
    (
        "SELECT pg_catalog.\"timestamptz\"('2024-06-15'::date, NULL::time without time zone) AS v",
        None,
    ),
]


@pytest.mark.parametrize(("sql", "want"), INSTANTS)
def test_to_timestamp_of_a_double_is_postgresqls_instant(runtime, sql, want):
    (got,) = _answers(runtime, sql)
    if want is None:
        assert got is None
        return
    expected = datetime.datetime.fromisoformat(want).replace(tzinfo=datetime.UTC)
    assert got.astimezone(datetime.UTC) == expected


@pytest.mark.parametrize(
    ("sql", "sqlstate"),
    [
        ("SELECT to_timestamp('NaN'::double precision)", "22008"),
        ("SELECT pg_catalog.\"to_timestamp\"('15 Jun 2024'::double precision) AS v", "22P02"),
    ],
)
def test_to_timestamp_of_a_double_raises_postgresqls_class(sql, sqlstate):
    with pytest.raises(FunctionRefused) as refused:
        transpile(sql, "clickhouse")
    assert refused.value.sqlstate[:2] == sqlstate[:2]


#: timestamp(timestamp, integer) (AdjustTimestampForTypmod): half away from zero since 2000.
SCALED = [
    ('SELECT pg_catalog."timestamp"(NULL::timestamp, 3::integer) AS v', None),
    ("SELECT pg_catalog.\"timestamp\"('2024-06-15 12:30:45'::timestamp, NULL::integer) AS v", None),
    (
        "SELECT pg_catalog.\"timestamp\"('2024-06-15 12:30:45.5'::timestamp, 0) AS v",
        "2024-06-15 12:30:46",
    ),
    (
        "SELECT pg_catalog.\"timestamp\"('1990-01-01 00:00:00.5'::timestamp, 0) AS v",
        "1990-01-01 00:00:00",
    ),
    (
        "SELECT pg_catalog.\"timestamp\"('1990-01-01 00:00:00.12345'::timestamp, 4) AS v",
        "1990-01-01 00:00:00.1234",
    ),
    (
        "SELECT pg_catalog.\"timestamp\"('2024-06-15 12:30:45'::timestamp, 3) AS v",
        "2024-06-15 12:30:45",
    ),
]


@pytest.mark.parametrize(("sql", "want"), SCALED)
def test_a_timestamp_scaled_is_postgresqls(runtime, sql, want):
    (got,) = _answers(runtime, sql)
    if want is None:
        assert got is None
        return
    assert got.replace(tzinfo=None) == datetime.datetime.fromisoformat(want)


@pytest.mark.parametrize("precision", ["(-7)::integer", "7", "'2147483647'::integer"])
def test_a_precision_outside_0_to_6_raises_22023(runtime, precision):
    sql = f"SELECT pg_catalog.\"timestamp\"('2024-06-15 12:30:45'::timestamp, {precision}) AS v"
    with pytest.raises(Exception) as raised:  # noqa: PT011 -- the engine's raise of PostgreSQL's
        _answers(runtime, sql)
    assert "postgresql-error 22023" in str(raised.value)


def test_random_of_two_numerics_draws_between_them_at_the_larger_scale(runtime):
    for _ in range(20):
        (got,) = _answers(runtime, 'SELECT pg_catalog."random"(1.5::numeric, 2.25::numeric) AS v')
        assert Decimal("1.5") <= got <= Decimal("2.25")
        assert got.as_tuple().exponent >= -2


@pytest.mark.parametrize(
    ("sql", "want"),
    [
        ('SELECT pg_catalog."random"((-7)::numeric, (-7)::numeric) AS v', Decimal(-7)),
        ('SELECT pg_catalog."random"(NULL::numeric, NULL::numeric) AS v', None),
    ],
)
def test_random_of_two_numerics_edges(runtime, sql, want):
    assert _answers(runtime, sql) == [want]


def test_random_of_numerics_out_of_order_raises_22023():
    with pytest.raises(FunctionRefused) as refused:
        transpile('SELECT pg_catalog."random"(2::numeric, 1::numeric) AS v', "clickhouse")
    assert refused.value.sqlstate == "22023"
