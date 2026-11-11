# Copyright (c) 2026 Kenneth Stott
# Canary: a5c2c89a-8cc0-4e7a-8d67-2435cc0960fa
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The functions conformance run's wrong answers and engine errors on DuckDB (run 42935119044),
each sent as the probe sends it through a real pgwire server on the DuckDB engine: answered as
PostgreSQL 17.11 answers the same statement, or, where DuckDB cannot give that answer, refused by
name (REQ-2167)."""

# Requirements: REQ-2167

# ruff: noqa: F811 -- the fixtures are conn and pgwire of the module they are imported from

from __future__ import annotations

from typing import Any

import pytest
from psycopg import pq

from scripts.conformance.pgwire_function_probe import refused_by_name
from tests.unit.pgwire.test_type_conversions import conn, pgwire  # noqa: F401

INT4, NUMERIC, BOOL, BIT = 23, 1700, 16, 1560
INT4RANGE, INT8RANGE, NUMRANGE = 3904, 3926, 3906

#: (statement, PostgreSQL 17's result type, its rows' text)
ANSWERS: list[tuple[str, int, list[Any]]] = [
    # A NULL range's parts are NULLs of the function's own result type, not untyped (text).
    *(
        (f'SELECT pg_catalog."{name}"(NULL::int4range) AS v', BOOL, [None])
        for name in ("isempty", "lower_inc", "lower_inf", "upper_inc", "upper_inf")
    ),
    ('SELECT pg_catalog."upper_inc"(NULL::int4multirange) AS v', BOOL, [None]),
    ('SELECT pg_catalog."lower"(NULL::int4range) AS v', INT4, [None]),
    ('SELECT pg_catalog."upper"(NULL::int4range) AS v', INT4, [None]),
    ('SELECT pg_catalog."lower"(NULL::int4multirange) AS v', INT4, [None]),
    ('SELECT pg_catalog."upper"(NULL::int4multirange) AS v', INT4, [None]),
    ('SELECT pg_catalog."range_adjacent"(NULL::int4range, NULL::int4range) AS v', BOOL, [None]),
    ('SELECT pg_catalog."range_cmp"(NULL::int4range, NULL::int4range) AS v', INT4, [None]),
    # A bound written as a cast negative number is that number.
    ('SELECT pg_catalog."int4range"((-7)::integer, (-7)::integer) AS v', INT4RANGE, ["empty"]),
    ('SELECT pg_catalog."int8range"((-7)::bigint, (-7)::bigint) AS v', INT8RANGE, ["empty"]),
    ('SELECT pg_catalog."int4range"((-7)::integer, 3::integer) AS v', INT4RANGE, ["[-7,3)"]),
    (
        'SELECT pg_catalog."numrange"((-7.125)::numeric, (-7.125)::numeric) AS v',
        NUMRANGE,
        ["empty"],
    ),
    # ceil and its kin of a numeric literal named by pg_catalog: a DECIMAL, not its digits' text.
    ('SELECT pg_catalog."ceil"(2.5::numeric) AS v', NUMERIC, ["3"]),
    ('SELECT pg_catalog."floor"(2.5::numeric) AS v', NUMERIC, ["2"]),
    ('SELECT pg_catalog."round"(2.5::numeric) AS v', NUMERIC, ["3"]),
    ('SELECT pg_catalog."trunc"(2.5::numeric) AS v', NUMERIC, ["2"]),
    # bit(integer, typmod): a typmod of 0 or below is one bit (bitfromint4).
    ('SELECT pg_catalog."bit"((-7)::integer, (-7)::integer) AS v', BIT, ["1"]),
    ('SELECT pg_catalog."bit"((-7)::bigint, (-7)::integer) AS v', BIT, ["1"]),
    ('SELECT pg_catalog."bit"(5::integer, 4::integer) AS v', BIT, ["0101"]),
    # bit(bit, length, explicit): an explicit cast pads with zeros on the right, or truncates.
    ("SELECT pg_catalog.\"bit\"('1'::bit, 3::integer, true::boolean) AS v", BIT, ["100"]),
    ("SELECT pg_catalog.\"bit\"('101'::bit(3), 2::integer, true::boolean) AS v", BIT, ["10"]),
    ("SELECT pg_catalog.\"bit\"('101'::bit(3), 0::integer, false::boolean) AS v", BIT, ["101"]),
    # numeric(numeric, typmod): a typmod below the header leaves the value as it is.
    ('SELECT pg_catalog."numeric"(2.5::numeric, 3::integer) AS v', NUMERIC, ["2.5"]),
]


def _answer(conn: Any, sql: str) -> Any:
    return conn.conn.pgconn.exec_params(sql.encode(), [], result_format=0)


@pytest.mark.parametrize(("sql", "oid", "rows"), ANSWERS, ids=[a[0] for a in ANSWERS])
def test_duckdb_answers_as_postgresql_17(conn: Any, sql: str, oid: int, rows: list[Any]):
    res = _answer(conn, sql)
    assert res.status == pq.ExecStatus.TUPLES_OK, (res.error_message or b"").decode()
    got = [None if (v := res.get_value(i, 0)) is None else v.decode() for i in range(res.ntuples)]
    assert (res.ftype(0), got) == (oid, rows)


#: (statement, PostgreSQL's SQLSTATE, its message): PostgreSQL's own error, said as it says it.
ERRORS: list[tuple[str, str, str]] = [
    (
        "SELECT pg_catalog.\"bit\"('1'::bit, 3::integer, false::boolean) AS v",
        "22026",
        "bit string length 1 does not match type bit(3)",
    ),
]


@pytest.mark.parametrize(("sql", "sqlstate", "message"), ERRORS, ids=[e[0] for e in ERRORS])
def test_duckdb_errs_as_postgresql_17(conn: Any, sql: str, sqlstate: str, message: str):
    res = _answer(conn, sql)
    assert res.status == pq.ExecStatus.FATAL_ERROR
    said = (
        res.error_field(pq.DiagnosticField.SQLSTATE).decode(),
        res.error_field(pq.DiagnosticField.MESSAGE_PRIMARY).decode(),
    )
    assert said == (sqlstate, message)


#: Statements whose PostgreSQL answer DuckDB cannot give: each refused naming its function.
REFUSED: list[tuple[str, str]] = [
    # A scale DuckDB's DECIMAL (38 digits at most) has no room for.
    ('SELECT pg_catalog."round"(1000::numeric, 42::integer) AS v', "round"),
    ('SELECT pg_catalog."trunc"(1000::numeric, 42::integer) AS v', "trunc"),
]


@pytest.mark.parametrize(("sql", "function"), REFUSED, ids=[r[0] for r in REFUSED])
def test_duckdb_refuses_by_name(conn: Any, sql: str, function: str):
    res = _answer(conn, sql)
    assert res.status == pq.ExecStatus.FATAL_ERROR
    error = (res.error_message or b"").decode()
    assert refused_by_name(error), error
    assert error.split(":", 1)[1].strip().startswith(function), error
