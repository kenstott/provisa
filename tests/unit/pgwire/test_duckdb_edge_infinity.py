# Copyright (c) 2026 Kenneth Stott
# Canary: 3e8b5d27-91c4-4f6a-b0d2-7a4c1e9f2b63
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""AN INFINITE DATE OR TIMESTAMP ON DUCKDB (REQ-2236, amended 2026-11-23): DuckDB holds the
infinities, and an edge recognisable from the input alone, where PostgreSQL's answer is fixed, is
guarded: the form answers PostgreSQL's constant there, or raises its SQLSTATE, and otherwise runs
the engine form. A range writes an infinite bound bare, as range_out does. Each statement is one
the functions measure runs (J4 of the DuckDB edge classification), each answer PostgreSQL 17's."""

# Requirements: REQ-2236, REQ-2167
# ruff: noqa: F811 -- the fixtures are conn and pgwire of the module they are imported from

from __future__ import annotations

from typing import Any

import pytest
from psycopg import pq

from tests.unit.pgwire.test_type_conversions import conn, pgwire  # noqa: F401

_DATE = "'2024-06-15'::date"
_TS = "'2024-06-15 12:30:45'::timestamp without time zone"
_TSTZ = "'2024-06-15 12:30:45+00'::timestamp with time zone"
_INF_DATE = "'infinity'::date"
_INF_TS = "'infinity'::timestamp without time zone"
_INF_TSTZ = "'infinity'::timestamp with time zone"


def _call(name: str, *args: str) -> str:
    return f'SELECT pg_catalog."{name}"({", ".join(args)}) AS v'


#: statement -> the rows PostgreSQL 17 answers, each its text (None a NULL).
ANSWERED: dict[str, list[str | None]] = {
    # subdiff: the stored values' difference, an infinity its sentinel
    _call("daterange_subdiff", _INF_DATE, _DATE): ["2147474715"],
    _call("daterange_subdiff", "'-infinity'::date", _DATE): ["-2147492580"],
    _call("daterange_subdiff", "'2024-06-20'::date", _DATE): ["5"],
    _call("tsrange_subdiff", _INF_TS, _TS): ["9222600267009.775"],
    _call("tstzrange_subdiff", _INF_TSTZ, _TSTZ): ["9222600267009.775"],
    _call("tstzrange_subdiff", "'2024-06-16 12:30:45+00'::timestamp with time zone", _TSTZ): [
        "86400"
    ],
    # range_out: an infinite bound written bare
    _call("tsrange", _TS, _INF_TS, "'[)'::text"): ['["2024-06-15 12:30:45",infinity)'],
    _call("tstzrange", _TSTZ, _INF_TSTZ, "'[)'::text"): ['["2024-06-15 12:30:45+00",infinity)'],
    _call("tsrange", "'-infinity'::timestamp without time zone", _TS, "'[)'::text"): [
        '[-infinity,"2024-06-15 12:30:45")'
    ],
    # date_part: an infinity of the value's sign of a unit that grows without end, else NULL
    _call("date_part", "'year'::text", _INF_TSTZ): ["Infinity"],
    _call("date_part", "'year'::text", _INF_DATE): ["Infinity"],
    _call("date_part", "'epoch'::text", "'-infinity'::timestamp without time zone"): ["-Infinity"],
    _call("date_part", "'month'::text", _INF_TS): [None],
    _call("date_part", "'year'::text", _TSTZ): ["2024"],
    # date_bin: an infinite source answers as it is
    _call("date_bin", "'15 minutes'::interval", _INF_TS, _TS): ["infinity"],
    _call("date_bin", "'15 minutes'::interval", "'2024-06-15 12:37:45'::timestamp", _TS): [
        "2024-06-15 12:30:45"
    ],
    _call("date_mi", "'2024-06-20'::date", _DATE): ["5"],
}

#: statement -> the SQLSTATE and message PostgreSQL 17 raises.
RAISED: dict[str, tuple[str, str]] = {
    _call("date_bin", "'15 minutes'::interval", _TSTZ, _INF_TSTZ): (
        "22008",
        "origin out of range",
    ),
    _call("date_mi", _INF_DATE, _DATE): ("22008", "cannot subtract infinite dates"),
    _call("date_mi", _DATE, "'-infinity'::date"): ("22008", "cannot subtract infinite dates"),
}


def _result(conn: Any, sql: str) -> Any:
    return conn.conn.pgconn.exec_params(f"{sql} LIMIT 1001".encode(), [], result_format=0)


def _field(res: Any, field: pq.DiagnosticField) -> str:
    return (res.error_field(field) or b"").decode()


@pytest.mark.parametrize(("sql", "rows"), list(ANSWERED.items()), ids=list(ANSWERED))
def test_an_infinity_answers_as_postgresql_answers_it(conn: Any, sql: str, rows: list[str | None]):
    res = _result(conn, sql)
    assert res.status == pq.ExecStatus.TUPLES_OK, (
        f"raised {_field(res, pq.DiagnosticField.MESSAGE_PRIMARY)}"
    )
    got = [res.get_value(r, 0) for r in range(res.ntuples)]
    assert [None if v is None else v.decode() for v in got] == rows


@pytest.mark.parametrize(("sql", "raised"), list(RAISED.items()), ids=list(RAISED))
def test_an_infinity_raises_as_postgresql_raises_it(conn: Any, sql: str, raised: tuple[str, str]):
    res = _result(conn, sql)
    assert res.status == pq.ExecStatus.FATAL_ERROR, "answered where PostgreSQL raises"
    said = (
        _field(res, pq.DiagnosticField.SQLSTATE),
        _field(res, pq.DiagnosticField.MESSAGE_PRIMARY),
    )
    assert said == raised
