# Copyright (c) 2026 Kenneth Stott
# Canary: 8e4b1d63-2f7a-4c59-9a30-6d1e5b7c2f84
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""ClickHouse's rows of make_interval, make_time, int2int4_sum and numeric of a double or a real
(REQ-2236; REQ-2171), each refused by name on ClickHouse before: the functions measure's own calls
and the edges the rows read, answered on the embedded engine (chdb) against the embedded
PostgreSQL's recorded answer (numbers compared by value)."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

from decimal import Decimal, InvalidOperation

import pytest

from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime
from provisa.transpiler.function_macros import FunctionRefused
from provisa.transpiler.transpile import transpile

ANSWERED = [
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
