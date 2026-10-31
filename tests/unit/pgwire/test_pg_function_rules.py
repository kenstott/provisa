# Copyright (c) 2026 Kenneth Stott
# Canary: 2f6b9d14-73ea-4c08-a1d7-5e80c3b6f92a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The two generated rules of pg_catalog's functions (REQ-2171, A REWRITE IS A SQLGLOT EXPANSION,
ALWAYS POSTGRESQL TO THE TARGET, AN ANSWER THAT DIFFERS IS REFUSED; REQ-2167): each statement run as
a client runs it, over pgwire on the default engine, and answered as PostgreSQL 17.11 answered it,
compared by answer and never by text."""

# Requirements: REQ-2167, REQ-2171
# ruff: noqa: F811 -- the fixtures are conn and pgwire of the module they are imported from

from __future__ import annotations

import os
from typing import Any

from psycopg import pq

from tests.unit.pgwire.pg17_function_rule_answers import RECORDED
from tests.unit.pgwire.test_type_conversions import conn, pgwire  # noqa: F401


def _run(conn: Any, sql: str) -> Any:
    return conn.conn.pgconn.exec_params(sql.encode(), [], result_format=0)


def _rows(res: Any) -> list[Any]:
    return [None if (v := res.get_value(i, 0)) is None else v.decode() for i in range(res.ntuples)]


def _problem(conn: Any, sql: str) -> str | None:
    res = _run(conn, f"{sql} AS v")
    kind, first, second = RECORDED[sql]
    if kind == "err":
        if res.status != pq.ExecStatus.FATAL_ERROR:
            return f"answered {_rows(res)}"
        said = (
            (res.error_field(pq.DiagnosticField.SQLSTATE) or b"").decode(),
            (res.error_field(pq.DiagnosticField.MESSAGE_PRIMARY) or b"").decode(),
        )
        return None if said == (first, second) else str(said)
    if res.status != pq.ExecStatus.TUPLES_OK:
        return (res.error_message or b"").decode()[:200]
    if res.ftype(0) != first:
        return f"type {res.ftype(0)}, rows {_rows(res)}"
    rows = _rows(res)
    return None if rows == second or _same_order(sql, rows, second) else str(rows)


def _same_order(sql: str, rows: list[Any], want: list[Any]) -> bool:
    """A cmp function promises its sign: PostgreSQL's magnitude is memcmp's and strcoll's."""
    if "cmp" not in sql or None in (*rows, *want):
        return False
    return [(int(v) > 0) - (int(v) < 0) for v in rows] == [
        (int(v) > 0) - (int(v) < 0) for v in want
    ]


def test_every_statement_answers_as_postgresql_17_answered(conn: Any):
    problems = {}
    dump = os.environ.get("J371_DUMP")
    for sql in RECORDED:
        problem = _problem(conn, sql)
        if problem is not None:
            problems[sql] = problem
            if conn.conn.pgconn.status != pq.ConnStatus.OK:
                problems["LOST"] = sql
                break
    if dump:
        import json

        json.dump(problems, open(dump, "w"), indent=0)
    assert not problems
