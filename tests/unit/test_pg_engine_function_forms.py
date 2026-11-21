# Copyright (c) 2026 Kenneth Stott
# Canary: 3f9b2c71-5d84-4e1a-a6c0-9e2d7b4f1c58
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""On the PostgreSQL engine a pg_catalog function is PostgreSQL's own (REQ-2167, REQ-2171): the
statement Provisa writes for it answers as the client's statement answers on PostgreSQL. Each case
was a defect of the function-support run on that engine: an ordered-set aggregate's name written
"RANK", which no function is; overlaps written as the name sqlglot reads it by; a character's
length of one dropped with its cast."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

from typing import Any

import psycopg2
import pytest

from provisa.transpiler.transpile import transpile

pytestmark = pytest.mark.unit


@pytest.fixture(scope="module")
def pg(embedded_postgres):
    con = psycopg2.connect(embedded_postgres.dsn())
    con.autocommit = True
    yield con
    con.close()


def _answer(con: Any, sql: str) -> Any:
    cur = con.cursor()
    try:
        cur.execute("SET TimeZone TO 'UTC'")
        cur.execute(sql)
        return cur.fetchall(), [d.type_code for d in cur.description]
    except psycopg2.Error as error:
        return error.pgcode, error.diag.message_primary
    finally:
        cur.close()


ORDERED_SET = [
    'SELECT pg_catalog."{f}"(NULL::integer) WITHIN GROUP (ORDER BY NULL::integer) AS v',
    'SELECT pg_catalog."{f}"(2) WITHIN GROUP (ORDER BY x) AS v FROM (VALUES (1), (2), (3)) t(x)',
    "SELECT pg_catalog.{f}(2) WITHIN GROUP (ORDER BY x) AS v FROM (VALUES (1), (2), (3)) t(x)",
]
STATEMENTS = [
    *(
        s.format(f=f)
        for f in ("rank", "dense_rank", "percent_rank", "cume_dist")
        for s in ORDERED_SET
    ),
    'SELECT pg_catalog."mode"() WITHIN GROUP (ORDER BY NULL::integer) AS v',
    'SELECT pg_catalog."mode"() WITHIN GROUP (ORDER BY x) AS v FROM (VALUES (1), (2), (2)) t(x)',
    *(
        f'SELECT pg_catalog."percentile_{kind}"({fraction}) WITHIN GROUP (ORDER BY {by}) AS v'
        for kind, fraction, by in (
            ("cont", "NULL::double precision", "NULL::double precision"),
            ("cont", "NULL::double precision[]", "NULL::double precision"),
            ("cont", "NULL::double precision", "NULL::interval"),
            ("cont", "NULL::double precision[]", "NULL::interval"),
            ("disc", "NULL::double precision", "NULL::integer"),
            ("disc", "NULL::double precision[]", "NULL::integer"),
            ("cont", "0.5", "x::float8"),
            ("disc", "ARRAY[0.25, 0.75]", "x"),
        )
    ),
    *(
        f'SELECT pg_catalog."overlaps"(NULL::{a}, NULL::{b}, NULL::{a}, NULL::{d}) AS v'
        for t in (
            "timestamp without time zone",
            "timestamp with time zone",
            "time without time zone",
        )
        for a, b, d in (
            (t, "interval", "interval"),
            (t, "interval", t),
            (t, t, "interval"),
            (t, t, t),
        )
    ),
    'SELECT pg_catalog."overlaps"(NULL::time with time zone, NULL::time with time zone, '
    "NULL::time with time zone, NULL::time with time zone) AS v",
    "SELECT pg_catalog.\"overlaps\"('2001-02-16'::timestamp, '2001-12-21'::timestamp, "
    "'2001-10-30'::timestamp, '2002-10-30'::timestamp) AS v",
    "SELECT overlaps('10:00'::time, '1 hour'::interval, '10:30'::time, '11:00'::time) AS v",
    "SELECT (DATE '2001-02-16', DATE '2001-12-21') OVERLAPS (DATE '2001-10-30', DATE '2002-10-30') AS v",
    "SELECT pg_catalog.\"bpcharout\"('abc'::character) AS v",
    "SELECT pg_catalog.\"bpcharout\"('abc'::char) AS v",
    "SELECT pg_catalog.\"bpcharout\"('abc'::character(2)) AS v",
    "SELECT pg_catalog.\"bpcharout\"('abc'::bpchar) AS v",
    "SELECT 'abc'::character AS v",
]


@pytest.mark.parametrize("sql", STATEMENTS)
def test_the_postgresql_engine_answers_as_postgresql(pg: Any, sql: str):
    written = transpile(sql, "postgres")
    assert _answer(pg, written) == _answer(pg, sql), written
