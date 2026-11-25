# Copyright (c) 2026 Kenneth Stott
# Canary: 8e3b5d17-4c62-4a09-b1f8-2d7e9a6c0b53
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A date or a timestamp outside 1900..2299 on ClickHouse, held exactly (REQ-2236, maintainer
2026-11-25: hold rather than refuse): polyfill.held_instants.

Each statement is the function support run's (45668229003), at its date and timestamp edges
(4713-01-01 BC, 5874897-12-31, infinity), on a session on UTC. Each answer is compared with the
embedded PostgreSQL's (tests/embedded_pg.py): a timestamp by its microseconds from 1970, as
ClickHouse holds it (DateTime64(6, 'UTC')'s Int64, read from its Arrow column), any other value
by its text."""

# Requirements: REQ-2236, REQ-2167

from __future__ import annotations

import math
from decimal import Decimal
from typing import Any

import psycopg
import pyarrow as pa
import pytest

from provisa.core.pg_error import PostgresError
from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime
from provisa.transpiler.function_macros import RowRefused
from provisa.transpiler.polyfill import GRID, held_instants
from provisa.transpiler.transpile import transpile

_TS = "timestamp without time zone"
_TZ = "timestamp with time zone"


@pytest.fixture(scope="module")
def pg(embedded_postgres) -> Any:
    with psycopg.connect(embedded_postgres.dsn(), autocommit=True) as conn:
        conn.execute("SET TimeZone = 'UTC'")
        conn.execute("SET DateStyle = 'ISO, MDY'")
        yield conn


def _clickhouse(runtime: ClickHouseFederationRuntime, sql: str) -> Any:
    """ClickHouse's one answer: a timestamp as its microseconds from 1970, a time as its text."""
    table = runtime.run_arrow(transpile(sql, "clickhouse", generated=False))
    column = table.column(0)
    if pa.types.is_timestamp(column.type):
        return column.cast(pa.int64()).to_pylist()[0]
    return column.to_pylist()[0]


def _postgres(pg: Any, sql: str, stamp: bool) -> Any:
    """PostgreSQL's one answer: a timestamp as its microseconds from 1970 (infinity as the Int64
    end but one, as ClickHouse holds it), any other value as psycopg reads it."""
    if stamp:
        query = (
            "SELECT CASE WHEN NOT isfinite(v) THEN "
            "CASE WHEN v > 'epoch'::timestamp THEN 9223372036854775807 "
            "ELSE -9223372036854775807 END "
            "ELSE (EXTRACT(EPOCH FROM v) * 1000000)::numeric::bigint END "
            f"FROM ({sql.replace(' AS v', '::timestamp AS v', 1)}) AS q"
        )
        return pg.execute(query).fetchone()[0]
    return pg.execute(sql).fetchone()[0]


_STAMPS = [
    "SELECT pg_catalog.\"timestamp\"('4713-01-01 BC'::date) AS v",
    "SELECT pg_catalog.\"timestamp\"('infinity'::date) AS v",
    "SELECT pg_catalog.\"timestamp\"('-infinity'::date) AS v",
    f"SELECT pg_catalog.\"timestamp\"('4713-01-01 BC'::date, '12:30:45'::{'time without time zone'}) AS v",
    "SELECT pg_catalog.datetime_pl('4713-01-01 BC'::date, '12:30:45'::time without time zone) AS v",
    "SELECT pg_catalog.timedate_pl('12:30:45'::time without time zone, '4713-01-01 BC'::date) AS v",
    f"SELECT pg_catalog.\"timestamp\"('4713-01-01 00:00:00+00 BC'::{_TZ}) AS v",
    f"SELECT pg_catalog.timezone('UTC'::text, '4713-01-01 00:00:00+00 BC'::{_TZ}) AS v",
    f"SELECT pg_catalog.date_trunc('day'::text, '4713-01-01 12:34:56 BC'::{_TS}) AS v",
    f"SELECT pg_catalog.date_trunc('month'::text, '4713-03-15 12:34:56 BC'::{_TS}) AS v",
    f"SELECT pg_catalog.date_trunc('year'::text, '0044-03-15 12:34:56 BC'::{_TS}) AS v",
    f"SELECT pg_catalog.date_trunc('decade'::text, '0044-03-15 12:34:56 BC'::{_TS}) AS v",
    f"SELECT pg_catalog.date_trunc('century'::text, '0044-03-15 12:34:56 BC'::{_TS}) AS v",
    f"SELECT pg_catalog.date_trunc('millennium'::text, '0044-03-15 12:34:56 BC'::{_TS}) AS v",
    f"SELECT pg_catalog.date_trunc('century'::text, '12345-06-15 12:34:56'::{_TS}) AS v",
    f"SELECT pg_catalog.date_trunc('hour'::text, '4713-01-01 12:34:56.789 BC'::{_TZ}) AS v",
    "SELECT pg_catalog.date_pl_interval('4713-01-01 BC'::date, '1 day 02:03:04'::interval) AS v",
    "SELECT pg_catalog.date_mi_interval('4713-01-01 BC'::date, '1 day 02:03:04'::interval) AS v",
    "SELECT pg_catalog.interval_pl_date('1 day 02:03:04'::interval, '4713-01-01 BC'::date) AS v",
    f"SELECT pg_catalog.timestamp_pl_interval('4713-01-01 00:00:00 BC'::{_TS}, '1 mon 3 days'::interval) AS v",
    f"SELECT pg_catalog.timestamp_mi_interval('4713-01-01 00:00:00 BC'::{_TS}, '1 day 02:03:04'::interval) AS v",
    f"SELECT pg_catalog.interval_pl_timestamp('1 day 02:03:04'::interval, '4713-01-01 00:00:00 BC'::{_TS}) AS v",
    f"SELECT pg_catalog.timestamptz('4713-01-01 00:00:00.123456+00 BC'::{_TZ}, 3::integer) AS v",
    f"SELECT pg_catalog.\"max\"(c1) AS v FROM (VALUES ('4713-01-01 00:00:00 BC'::{_TS}), ('2024-06-15 12:30:45'::{_TS})) AS t(c1)",
    f"SELECT pg_catalog.\"min\"(c1) AS v FROM (VALUES ('4713-01-01 00:00:00+00 BC'::{_TZ}), ('2024-06-15 12:30:45+00'::{_TZ})) AS t(c1)",
    f"SELECT pg_catalog.\"max\"(c1) AS v FROM (VALUES ('infinity'::{_TS}), ('2024-06-15 12:30:45'::{_TS})) AS t(c1)",
]  # fmt: skip


@pytest.mark.parametrize("sql", _STAMPS)
def test_a_timestamp_answer_is_postgresql_s_microseconds(runtime, pg, sql):
    assert _clickhouse(runtime, sql) == _postgres(pg, sql, stamp=True)


_VALUES = [
    "SELECT pg_catalog.isfinite('4713-01-01 BC'::date) AS v",
    "SELECT pg_catalog.isfinite('5874897-12-31'::date) AS v",
    "SELECT pg_catalog.isfinite('infinity'::date) AS v",
    f"SELECT pg_catalog.isfinite('4713-01-01 00:00:00 BC'::{_TS}) AS v",
    f"SELECT pg_catalog.isfinite('-infinity'::{_TS}) AS v",
    f"SELECT pg_catalog.isfinite('4713-01-01 00:00:00+00 BC'::{_TZ}) AS v",
    "SELECT pg_catalog.date_mi('4713-01-01 BC'::date, '2024-06-15'::date) AS v",
    "SELECT pg_catalog.date_mi('2024-06-15'::date, '5874897-12-31'::date) AS v",
    "SELECT pg_catalog.date_mi(NULL::date, '4713-01-01 BC'::date) AS v",
    "SELECT pg_catalog.date_part('year'::text, '4713-01-01 BC'::date) AS v",
    "SELECT pg_catalog.date_part('year'::text, 'infinity'::date) AS v",
    "SELECT pg_catalog.date_part('month'::text, '-infinity'::date) AS v",
    f"SELECT pg_catalog.date_part('epoch'::text, '4713-01-01 00:00:00 BC'::{_TS}) AS v",
    f"SELECT pg_catalog.date_part('second'::text, '4713-01-01 12:34:56.789 BC'::{_TS}) AS v",
    f"SELECT pg_catalog.date_part('dow'::text, '4713-01-01 00:00:00 BC'::{_TS}) AS v",
    f"SELECT pg_catalog.date_part('isodow'::text, '4713-01-01 00:00:00 BC'::{_TS}) AS v",
    f"SELECT pg_catalog.date_part('doy'::text, '4713-12-31 00:00:00 BC'::{_TS}) AS v",
    f"SELECT pg_catalog.date_part('quarter'::text, '4713-08-31 00:00:00 BC'::{_TS}) AS v",
    "SELECT pg_catalog.\"extract\"('year'::text, '4713-01-01 BC'::date) AS v",
    "SELECT pg_catalog.\"extract\"('year'::text, '5874897-12-31'::date) AS v",
    "SELECT pg_catalog.\"extract\"('decade'::text, '0001-01-01 BC'::date) AS v",
    "SELECT pg_catalog.\"extract\"('century'::text, '0001-01-01 BC'::date) AS v",
    "SELECT pg_catalog.\"extract\"('millennium'::text, '4713-01-01 BC'::date) AS v",
    "SELECT pg_catalog.\"extract\"('epoch'::text, '4713-01-01 BC'::date) AS v",
    f"SELECT pg_catalog.\"extract\"('second'::text, '4713-01-01 12:34:56.789 BC'::{_TS}) AS v",
    f"SELECT pg_catalog.\"extract\"('epoch'::text, '4713-01-01 12:34:56.789+00 BC'::{_TZ}) AS v",
    "SELECT pg_catalog.daterange_subdiff('4713-01-01 BC'::date, '2024-06-15'::date) AS v",
    f"SELECT pg_catalog.tsrange_subdiff('4713-01-01 00:00:00 BC'::{_TS}, '2024-06-15 12:30:45'::{_TS}) AS v",
    f"SELECT pg_catalog.tstzrange_subdiff('4713-01-01 00:00:00+00 BC'::{_TZ}, '2024-06-15 12:30:45+00'::{_TZ}) AS v",
    "SELECT pg_catalog.daterange('4713-01-01 BC'::date, '2024-06-15'::date)::text AS v",
    "SELECT pg_catalog.daterange('4713-01-01 BC'::date, '2024-06-15'::date, '[)'::text)::text AS v",
    f"SELECT pg_catalog.tsrange('4713-01-01 00:00:00 BC'::{_TS}, '2024-06-15 12:30:45'::{_TS})::text AS v",
]  # fmt: skip


def _same(want: Any, got: Any) -> bool:
    if isinstance(want, float) and isinstance(got, float):
        return want == got or (math.isnan(want) and math.isnan(got))
    if isinstance(want, Decimal) and isinstance(got, Decimal):
        return want == got and want.as_tuple().exponent == got.as_tuple().exponent
    return want == got and type(want) is type(got)


@pytest.mark.parametrize("sql", _VALUES)
def test_any_other_answer_is_postgresql_s(runtime, pg, sql):
    want, got = _postgres(pg, sql, stamp=False), _clickhouse(runtime, sql)
    assert _same(want, got), (want, got)


def test_a_time_of_a_timestamp_bc_is_its_time_of_day(runtime, pg):
    sql = f"SELECT pg_catalog.\"time\"('4713-01-01 12:34:56.5 BC'::{_TS}) AS v"
    as_text = sql.replace(" AS v", "::text AS v", 1)  # time_out's text, as the select list shows
    assert _clickhouse(runtime, sql) == _postgres(pg, as_text, stamp=False)


@pytest.mark.parametrize(
    ("sql", "state", "message"),
    [
        ("SELECT pg_catalog.date_pli('5874897-12-31'::date, 3::integer) AS v", "22008", "date out of range"),
        ("SELECT pg_catalog.date_mi('infinity'::date, '4713-01-01 BC'::date) AS v", "22008", "cannot subtract infinite dates"),
        (f"SELECT pg_catalog.timestamptz('4713-01-01 00:00:00+00 BC'::{_TZ}, '-2147483648'::integer) AS v", "22023", "timestamp(-2147483648) precision must be between 0 and 6"),
        ("SELECT pg_catalog.daterange('infinity'::date, '2024-06-15'::date, '[)'::text) AS v", "22000", "range lower bound must be less than or equal to range upper bound"),
        (f"SELECT pg_catalog.tsrange('infinity'::{_TS}, '2024-06-15 12:30:45'::{_TS}, '[)'::text) AS v", "22000", "range lower bound must be less than or equal to range upper bound"),
    ],
)  # fmt: skip
def test_postgresql_s_error_is_raised_at_transpile(pg, sql, state, message):
    with pytest.raises(psycopg.Error) as pg_error:
        pg.execute(sql)
    assert pg_error.value.sqlstate == state
    with pytest.raises((RowRefused, PostgresError)) as raised:
        transpile(sql, "clickhouse", generated=False)
    assert (raised.value.sqlstate, str(raised.value)) == (state, message)


def test_a_timestamp_past_the_tail_of_an_int64_is_refused_by_name():
    """294247-01-10 04:00:54.775806 is the last microsecond an Int64 from 1970 holds, as on
    DuckDB: a timestamp answer past it is refused by name, its bound stated."""
    sql = f"SELECT pg_catalog.date_trunc('day'::text, '294276-12-31 23:59:59.999999'::{_TS}) AS v"
    with pytest.raises(RowRefused) as refused:
        transpile(sql, "clickhouse", generated=False)
    assert refused.value.sqlstate == "0A000"
    assert held_instants.HELD_LAST in str(refused.value)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT pg_catalog.date_pli('4713-01-01 BC'::date, 3::integer) AS v",
        f"SELECT pg_catalog.\"date\"('4713-01-01 00:00:00 BC'::{_TS}) AS v",
    ],
)
def test_a_date_answer_outside_date32_is_refused_by_name(sql):
    """No ClickHouse type holds a date outside 1900..2299 (Date32 clamps any other): the answer is
    refused by name, its bound stated."""
    with pytest.raises(RowRefused) as refused:
        transpile(sql, "clickhouse", generated=False)
    assert refused.value.sqlstate == "0A000"
    assert "1900-01-01 .. 2299-12-31" in str(refused.value)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT pg_catalog.date_mi('2024-06-15'::date, '2024-01-01'::date) AS v",
        f"SELECT pg_catalog.date_trunc('day'::text, '2024-06-15 12:30:45'::{_TS}) AS v",
        "SELECT pg_catalog.date_part('year'::text, d) AS v FROM t",
    ],
)
def test_a_call_inside_1900_to_2299_keeps_its_row_s_form(sql):
    """Only a call of a value ClickHouse's calendar does not read is answered here."""
    import sqlglot

    tree = sqlglot.parse_one(sql, read="postgres")
    assert held_instants.folded(sql, tree) is tree
    assert all(held_instants.held(n, "clickhouse") is None for n in tree.walk())


def test_the_hold_is_the_grid_s_row_and_states_its_bounds():
    (feature,) = [f for f in GRID.features if f.family == held_instants.FAMILY]
    (limitation,) = feature.read.limitations["clickhouse"]
    assert held_instants.HELD_LAST in limitation.text
    assert "1900-01-01 .. 2299-12-31" in limitation.text
