# Copyright (c) 2026 Kenneth Stott
# Canary: 4c1a911b-aab0-456c-bd00-38b1ad090e3b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A NULL OR AN INTEGER'S EXTREME ON DUCKDB (REQ-2236, amended 2026-11-23): an edge value
recognisable from the input alone, where PostgreSQL's answer is fixed, is guarded: the form
answers PostgreSQL's constant there, or raises its SQLSTATE, and otherwise runs the engine form.
Each statement is one the functions measure runs (J6 of the DuckDB edge classification), each
answer PostgreSQL 17's."""

# Requirements: REQ-2236, REQ-2167
# ruff: noqa: F811 -- the fixtures are conn and pgwire of the module they are imported from

from __future__ import annotations

from typing import Any

import pytest
from psycopg import pq

from tests.unit.pgwire.test_type_conversions import conn, pgwire  # noqa: F401

_LEAST = "'-2147483648'::integer"
_GREATEST = "'2147483647'::integer"
_ARRAY = "'{1,2,3}'::integer[]"
_TIME = "'12:30:45'::time without time zone"
_TIMETZ = "'12:30:45+02'::time with time zone"


def _call(name: str, *args: str) -> str:
    return f'SELECT pg_catalog."{name}"({", ".join(args)}) AS v'


def _of_pairs(name: str, rows: str) -> str:
    return f'SELECT pg_catalog."{name}"(c1, c2 ORDER BY c1) AS v FROM (VALUES {rows}) AS t(c1, c2)'


_NULL_TWICE = "(3::integer, NULL::integer), (3::integer, NULL::integer)"
_ONE_NULL = "(3::integer, 1::integer), (3::integer, NULL::integer)"

#: statement -> the rows PostgreSQL 17 answers, each its text (None a NULL).
ANSWERED: dict[str, list[str | None]] = {
    # strict: a NULL argument answers NULL
    _call("array_to_string", _ARRAY, "NULL::text"): [None],
    _call("array_to_string", _ARRAY, "NULL::text", "'abc'::text"): [None],
    _call("bitshiftright", "'1'::bit", "NULL::integer"): [None],
    _call("bitshiftleft", "'101'::bit varying", "NULL::integer"): [None],
    _call("substr", "'abc'::text", "NULL::integer"): [None],
    _call("substr", "'abc'::text", "NULL::integer", "3::integer"): [None],
    _call("substr", "'abc'::text", "2::integer", "NULL::integer"): [None],
    _call("substr", "'\\xdeadbeef'::bytea", "3::integer", "NULL::integer"): [None],
    _call("substr", "'\\xdeadbeef'::bytea", "NULL::integer"): [None],
    _call("varbit", "'101'::bit varying", "3::integer", "NULL::boolean"): [None],
    _call("json_object", "'{a,b,c}'::text[]", "NULL::text[]"): [None],
    _call("jsonb_object", "'{a,b,c}'::text[]", "NULL::text[]"): [None],
    _call("pt_contained_poly", "NULL::point", "'((0,0),(1,0),(1,1))'::polygon"): [None],
    _call("generate_series", "2.5::numeric", "2.5::numeric", "NULL::numeric"): [],
    # an integer's extreme
    _call("bit", "3::integer", _GREATEST): ["1"],
    _call("bit", "3::bigint", _GREATEST): ["1"],
    _call("bitshiftleft", "'1'::bit", _LEAST): ["0"],
    _call("bitshiftright", "'1'::bit", _LEAST): ["0"],
    _call("bitshiftleft", "'101'::bit varying", _GREATEST): ["000"],
    _call("bitshiftright", "'101'::bit varying", _LEAST): ["000"],
    _call("bitshiftleft", "'101'::bit varying", "1::integer"): ["010"],
    _call("bitshiftright", "'101'::bit varying", "-1::integer"): ["010"],
    _call("intervaltypmodout", _LEAST): [""],
    _call("intervaltypmodout", _GREATEST): [""],
    _call("numerictypmodout", _LEAST): [""],
    _call("numerictypmodout", _GREATEST): ["(32767,-5)"],
    _call("numerictypmodout", "'0'::integer"): [""],
    _call("numeric", "2.5::numeric", _LEAST): ["2.5"],
    _call("numeric", "2.5::numeric", _GREATEST): ["0"],
    _call("numeric", "2.5::numeric", "'0'::integer"): ["2.5"],
    _call("time", _TIME, _LEAST): ["12:30:45"],
    _call("timetz", _TIMETZ, _LEAST): ["12:30:45+02"],
    _call("right", "'abc'::text", _LEAST): ["abc"],
    _call("substr", "'abc'::text", _GREATEST, "3::integer"): [""],
    _call("varbit", "'101'::bit varying", "'0'::integer", "true::boolean"): ["101"],
    _call("varbit", "'101'::bit varying", "2::integer", "true::boolean"): ["10"],
}

#: statement -> the SQLSTATE and message PostgreSQL 17 raises.
RAISED: dict[str, tuple[str, str]] = {
    _call("array_position", _ARRAY, "3::integer", "NULL::integer"): (
        "22004",
        "initial position must not be null",
    ),
    _call("pg_get_publication_tables", "VARIADIC '{NULL}'::text[]"): (
        "22004",
        "null array element not allowed in this context",
    ),
    _of_pairs("json_object_agg_unique_strict", _NULL_TWICE): (
        "22030",
        'duplicate JSON object key value: "3"',
    ),
    _of_pairs("json_object_agg_unique_strict", _ONE_NULL): (
        "22030",
        'duplicate JSON object key value: "3"',
    ),
    _of_pairs("jsonb_object_agg_unique_strict", _NULL_TWICE): (
        "22030",
        "duplicate JSON object key value",
    ),
    _call("chr", "'0'::integer"): ("54000", "null character not permitted"),
    _call("chr", _GREATEST): ("54000", "requested character too large for encoding: 2147483647"),
    _call("chr", "55296::integer"): ("54000", "requested character not valid for encoding: 55296"),
    _call("chr", "-1::integer"): ("22023", "character number must be positive"),
    _call("regexp_count", "'abc'::text", "'b'::text", _LEAST): (
        "22023",
        'invalid value for parameter "start": -2147483648',
    ),
    _call("like_escape", "NULL::bytea", "'\\'::bytea"): (
        "22P02",
        "invalid input syntax for type bytea",
    ),
    _call("substr", "'abc'::text", "2::integer", _LEAST): (
        "22011",
        "negative substring length not allowed",
    ),
    _call("varbit", "'101'::bit varying", "2::integer", "false::boolean"): (
        "22001",
        "bit string too long for type bit varying(2)",
    ),
}


def _result(conn: Any, sql: str) -> Any:
    return conn.conn.pgconn.exec_params(f"{sql} LIMIT 1001".encode(), [], result_format=0)


def _field(res: Any, field: pq.DiagnosticField) -> str:
    return (res.error_field(field) or b"").decode()


@pytest.mark.parametrize(("sql", "rows"), list(ANSWERED.items()), ids=list(ANSWERED))
def test_an_edge_answers_as_postgresql_answers_it(conn: Any, sql: str, rows: list[str | None]):
    res = _result(conn, sql)
    assert res.status == pq.ExecStatus.TUPLES_OK, (
        f"raised {_field(res, pq.DiagnosticField.MESSAGE_PRIMARY)}"
    )
    got = [res.get_value(r, 0) for r in range(res.ntuples)]
    assert [None if v is None else v.decode() for v in got] == rows


@pytest.mark.parametrize(("sql", "raised"), list(RAISED.items()), ids=list(RAISED))
def test_an_edge_raises_as_postgresql_raises_it(conn: Any, sql: str, raised: tuple[str, str]):
    res = _result(conn, sql)
    assert res.status == pq.ExecStatus.FATAL_ERROR, "answered where PostgreSQL raises"
    said = (
        _field(res, pq.DiagnosticField.SQLSTATE),
        _field(res, pq.DiagnosticField.MESSAGE_PRIMARY),
    )
    assert said == raised
