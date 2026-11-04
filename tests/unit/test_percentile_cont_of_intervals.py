# Copyright (c) 2026 Kenneth Stott
# Canary: 7c1e4a92-3b6d-4f08-9d25-e8a0f61b3c47
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""percentile_cont of intervals answers as PostgreSQL 17 does (REQ-2167): DuckDB's interpolates
microseconds, which carries months and days into hours; PostgreSQL's interval_lerp subtracts the
two neighbours field by field and multiplies as interval_mul cascades. Each statement is answered
by DuckDB through the transpiler and by PostgreSQL itself. A fraction written as an array's text
('{0.25,0.5}') is read as the array it is."""

# Requirements: REQ-2167

# ruff: noqa: F811 -- the fixtures are conn and pgwire of the module they are imported from

from __future__ import annotations

import random
from typing import Any

import duckdb
import psycopg2
import pytest

from provisa.transpiler.pg_type_literals import interval_text
from provisa.transpiler.transpile import transpile_to_duckdb
from tests.unit.pgwire.test_type_conversions import conn, pgwire  # noqa: F401

pytestmark = pytest.mark.unit

_FIXTURE = (
    "(VALUES ('1 day 02:03:04'::interval), ('3 mons'::interval), ('-1 year 2 days'::interval)) "
    "AS t(c1)"
)
_MIXED = (
    "(VALUES ('1 year 2 mons 3 days 04:05:06.789'::interval), ('-7 days 00:00:00.000001'::interval),"
    " ('25 mons -40 days 99:00:00'::interval), (NULL::interval), ('00:00:00.5'::interval)) AS t(c1)"
)


def _random_values(seed: int) -> str:
    rng = random.Random(seed)
    rows = ", ".join(
        f"('{rng.randint(-40, 40)} mons {rng.randint(-90, 90)} days "
        f"{rng.randint(-200 * 3600, 200 * 3600)} seconds {rng.randint(0, 999999)} microseconds'"
        "::interval)"
        for _ in range(rng.randint(2, 9))
    )
    return f"(VALUES {rows}) AS t(c1)"


STATEMENTS = [
    # The function check's call (run 41836627668): a fraction array as its text.
    "SELECT pg_catalog.\"percentile_cont\"('{0.25,0.5}'::double precision[]) "
    f"WITHIN GROUP (ORDER BY c1) AS v FROM {_FIXTURE}",
    f'SELECT pg_catalog."percentile_cont"(0.25::double precision) WITHIN GROUP (ORDER BY c1) AS v '
    f"FROM {_FIXTURE}",
    f"SELECT percentile_cont(ARRAY[0, 0.1, 0.5, 0.75, 1]::float8[]) WITHIN GROUP (ORDER BY c1) AS v "
    f"FROM {_MIXED}",
    f"SELECT percentile_cont(0.3) WITHIN GROUP (ORDER BY c1 DESC) AS v FROM {_MIXED}",
    f"SELECT percentile_cont(ARRAY[0.3, 0.9]) WITHIN GROUP (ORDER BY c1 DESC) AS v FROM {_MIXED}",
    "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY c1) AS v FROM "
    "(VALUES (NULL::interval)) AS t(c1)",
    "SELECT percentile_cont(ARRAY[0.5]) WITHIN GROUP (ORDER BY c1) AS v FROM "
    "(VALUES (NULL::interval)) AS t(c1)",
    "SELECT g, percentile_cont(0.5) WITHIN GROUP (ORDER BY c1) AS v FROM (VALUES (1, '1 mon'::interval), "
    "(1, '3 days 12:00:00'::interval), (2, '-5 mons'::interval)) AS t(g, c1) GROUP BY g ORDER BY g",
    *(
        f"SELECT percentile_cont(ARRAY[0.1, 0.333333333333, 0.5, 0.9]::float8[]) WITHIN GROUP (ORDER BY c1) "
        f"AS v FROM {_random_values(seed)}"
        for seed in range(12)
    ),
    # Not an interval: the text of a fraction array is read for every key's type.
    "SELECT percentile_cont('{0.25,0.5}'::double precision[]) WITHIN GROUP (ORDER BY c1) AS v "
    "FROM (VALUES (0.5::double precision), ((-2.25)::double precision), (100::double precision)) "
    "AS t(c1)",
]


@pytest.fixture(scope="module")
def postgres(embedded_postgres: Any) -> Any:
    connected = psycopg2.connect(embedded_postgres.dsn())
    connected.autocommit = True
    # An interval and its array as interval_out writes them, not the driver's timedelta.
    for oid in (1186, 1187):
        as_text = psycopg2.extensions.new_type((oid,), f"TEXT_{oid}", lambda value, _: value)
        psycopg2.extensions.register_type(as_text, connected)
    yield connected
    connected.close()


def _postgresql(connection: Any, sql: str) -> list[tuple]:
    cursor = connection.cursor()
    try:
        cursor.execute("SET IntervalStyle TO postgres")
        cursor.execute(sql)
        return [tuple(_pg_text(v) for v in row) for row in cursor.fetchall()]
    finally:
        cursor.close()


def _pg_text(value: Any) -> Any:
    if isinstance(value, list):
        return [_pg_text(v) for v in value]
    return repr(value) if isinstance(value, float) else value


def _interval_out(fields: Any) -> str | None:
    """A NULL interval's fields are each NULL."""
    if fields is None or None in fields:
        return None
    return interval_text(*(int(f) for f in fields))


_FIELDS = (
    "[date_part('year', {x}) * 12 + date_part('month', {x}), date_part('day', {x}), "
    "date_part('hour', {x}) * 3600000000 + date_part('minute', {x}) * 60000000 + "
    "date_part('microseconds', {x})]"
)


def _selected(name: str, kind: str) -> str:
    """A column of DuckDB's answer, an interval as its months, days and microseconds."""
    column = f'r."{name}"'
    if kind == "INTERVAL":
        return _FIELDS.format(x=column)
    if kind == "INTERVAL[]":
        return f"list_transform({column}, _z -> {_FIELDS.format(x='_z')})"
    return column


def _cell(value: Any, kind: str) -> Any:
    if value is None:
        return None
    if kind == "INTERVAL":
        return _interval_out(value)
    if kind == "INTERVAL[]":
        return "{" + ",".join(f'"{_interval_out(v)}"' for v in value) + "}"
    return _pg_text(value)


def _duckdb(sql: str) -> list[tuple]:
    """DuckDB's answer, each interval as interval_out writes its months, days and microseconds:
    its driver's timedelta has no months."""
    connection = duckdb.connect()
    written = transpile_to_duckdb(sql)
    relation = connection.sql(written)
    kinds = [str(kind) for kind in relation.types]
    selected = ", ".join(_selected(n, k) for n, k in zip(relation.columns, kinds))
    rows = connection.execute(f"SELECT {selected} FROM ({written}) AS r").fetchall()
    return [tuple(_cell(v, k) for v, k in zip(row, kinds)) for row in rows]


@pytest.mark.parametrize("sql", STATEMENTS)
def test_duckdb_answers_percentile_cont_as_postgresql(postgres: Any, sql: str):
    assert _duckdb(sql) == _postgresql(postgres, sql), transpile_to_duckdb(sql)


def test_the_function_check_s_call_reaches_the_wire_as_postgresql_s(postgres: Any, conn: Any):
    """Run 41836627668: Provisa answered {"-178 days -10:58:28",...}, PostgreSQL
    {"-6 mons +2 days -10:58:28","1 day 02:03:04"} of type interval[]."""
    sql = STATEMENTS[0]
    res = conn.conn.pgconn.exec_params(sql.encode(), [], result_format=0)
    assert (res.ftype(0), res.get_value(0, 0).decode()) == (
        1187,
        _postgresql(postgres, sql)[0][0],
    )
