# Copyright (c) 2026 Kenneth Stott
# Canary: 7d2e5b40-1f8c-4a6e-9c3b-2a5e8f1d0c47
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A TEXT OR CHARACTER EDGE ON DUCKDB (REQ-2236, amended 2026-11-23): an edge value recognisable
from the input alone, where PostgreSQL's answer is fixed, answers as PostgreSQL answers it.
Each statement is one the functions measure runs (J7 of the DuckDB edge classification), each
answer PostgreSQL 17's."""

# Requirements: REQ-2236, REQ-2167
# ruff: noqa: F811 -- the fixtures are conn and pgwire of the module they are imported from

from __future__ import annotations

from typing import Any

import pytest
from psycopg import pq

from tests.unit.pgwire.test_type_conversions import conn, pgwire  # noqa: F401

_PADDED = "'  padded  '::character"
_EMPTY = "''::character"
_OBJECT = '\'{"a": 1, "b": [1, 2]}\''


def _call(name: str, *args: str) -> str:
    return f'SELECT pg_catalog."{name}"({", ".join(args)}) AS v'


def _of_rows(name: str, rows: str) -> str:
    return f'SELECT pg_catalog."{name}"(c1) AS v FROM (VALUES {rows}) AS t(c1)'


_TWO_EMPTY = f"({_EMPTY}), ({_EMPTY})"

#: statement -> the rows PostgreSQL 17 answers, each its text (None a NULL).
ANSWERED: dict[str, list[str | None]] = {
    # character with no length is character(1): its first character, padded to 1
    _call("bpchar", _EMPTY, "3::integer", "true::boolean"): [" "],
    _call("char_length", _PADDED): ["0"],
    _call("character_length", _PADDED): ["0"],
    _call("length", _PADDED): ["0"],
    _call("octet_length", _EMPTY): ["1"],
    _call("text", _PADDED): [""],
    _call("name", _PADDED): [""],
    _of_rows("max", _TWO_EMPTY): [" "],
    _of_rows("min", _TWO_EMPTY): [" "],
    # an object's member named ""
    _call("json_object_field", f"{_OBJECT}::json", "''::text"): [None],
    _call("json_object_field_text", f"{_OBJECT}::json", "''::text"): [None],
    _call("jsonb_object_field", f"{_OBJECT}::jsonb", "''::text"): [None],
    _call("jsonb_object_field_text", f"{_OBJECT}::jsonb", "''::text"): [None],
    # a NULL delimiter splits into characters
    _call("string_to_array", "'abc'::text", "NULL::text"): ["{a,b,c}"],
    _call("string_to_array", "'abc'::text", "NULL::text", "'abc'::text"): ["{a,b,c}"],
    # "char" is the first byte; one past 0x7F prints as \ooo
    _call("char", "'日本語テキスト😀'::text"): ["\\346"],
    # format is not strict: a NULL argument formats as ''
    _call("format", "'abc'::text", "NULL::integer"): ["abc"],
    # a NULL path element is skipped
    _call("jsonb_delete", f"{_OBJECT}::jsonb", "VARIADIC '{NULL}'::text[]"): [
        '{"a": 1, "b": [1, 2]}'
    ],
    # position(a, b) is b searched in a
    _call("position", "''::text", "'abc'::text"): ["0"],
    # reverse is by code point
    _call("reverse", "'été'::text"): ["́et́e"],
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
