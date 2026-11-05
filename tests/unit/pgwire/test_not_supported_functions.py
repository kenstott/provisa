# Copyright (c) 2026 Kenneth Stott
# Canary: 7c1d4e92-3a58-4b6f-9e20-b8d5f3a1c674
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What REQ-2167 (amended 2026-11-02) does not support, refused by name over pgwire whatever its
arguments: nextval, currval and setval, for Provisa does not model a source's sequences, and
regexp_replace with a start, an occurrence and flags. Each engine refuses them alike, PostgreSQL's
included; regexp_replace's other forms are served."""

# Requirements: REQ-2167
# ruff: noqa: F811 -- the fixtures are conn and pgwire of the module they are imported from

from __future__ import annotations

from typing import Any

import pytest
from psycopg import pq

from provisa.transpiler.function_macros import FunctionRefused
from provisa.transpiler.transpile import transpile
from tests.unit.pgwire.test_type_conversions import conn, pgwire  # noqa: F401

REFUSED: dict[str, str] = {
    'SELECT pg_catalog."nextval"(NULL::regclass) AS v': "nextval",
    "SELECT nextval(NULL::regclass) AS v": "nextval",
    "SELECT nextval('no_such_sequence') AS v": "nextval",
    'SELECT pg_catalog."currval"(NULL::regclass) AS v': "currval",
    "SELECT currval(NULL::regclass) AS v": "currval",
    'SELECT pg_catalog."setval"(NULL::regclass, NULL::bigint) AS v': "setval",
    'SELECT pg_catalog."setval"(NULL::regclass, NULL::bigint, NULL::boolean) AS v': "setval",
    "SELECT setval(NULL, 5) AS v": "setval",
    "SELECT setval('no_such_sequence', 5)": "setval",
    "SELECT setval('no_such_sequence'::regclass, 5, false)": "setval",
    "SELECT setval(2147483000, 5)": "setval",
    "SELECT setval('pg_class', 5)": "setval",
    'SELECT pg_catalog."regexp_replace"(NULL::text, NULL::text, NULL::text, NULL::integer, '
    "NULL::integer, NULL::text) AS v": "regexp_replace",
    "SELECT regexp_replace('aXbXc', 'X', '-', 1, 2, 'g') AS v": "regexp_replace",
    "SELECT regexp_replace('aXbXc', 'X', '-', 1, 0, '') AS v": "regexp_replace",
}


@pytest.mark.parametrize("sql", list(REFUSED))
def test_a_call_is_refused_by_name(conn: Any, sql: str):
    res = conn.conn.pgconn.exec_params(sql.encode(), [], result_format=0)
    assert res.status == pq.ExecStatus.FATAL_ERROR
    said = (res.error_field(pq.DiagnosticField.MESSAGE_PRIMARY) or b"").decode()
    assert said.startswith(f"{REFUSED[sql]}() has no form "), said


@pytest.mark.parametrize("dialect", ["duckdb", "postgres", "trino", "clickhouse"])
@pytest.mark.parametrize(
    "call",
    [
        "nextval('s')",
        "currval('s')",
        "setval('s', 1)",
        "setval('s', 1, true)",
        "regexp_replace(a, 'X', '-', 1, 2, 'g')",
        "pg_catalog.\"regexp_replace\"(a, 'X', '-', 1, 2, NULL::text)",
    ],
)
def test_every_engine_refuses_it(dialect: str, call: str):
    name = call.split("(")[0].split(".")[-1].strip('"')
    with pytest.raises(FunctionRefused, match=rf"^{name}\(\) has no form on {dialect}$"):
        transpile(f"SELECT {call} AS v FROM t", dialect)


@pytest.mark.parametrize(
    ("args", "want"),
    [
        ("'aXbXc', 'X', '-'", "a-bXc"),
        ("'aXbXc', 'X', '-', 'g'", "a-b-c"),
        ("'aXbXc', 'X', '-', 3", "aXb-c"),
        ("'aXbXc', 'X', '-', 1, 2", "aXb-c"),
    ],
)
def test_the_other_forms_of_regexp_replace_answer(conn: Any, args: str, want: str):
    res = conn.conn.pgconn.exec_params(
        f"SELECT regexp_replace({args}) AS v".encode(), [], result_format=0
    )
    assert res.status == pq.ExecStatus.TUPLES_OK, (res.error_message or b"").decode()
    assert res.get_value(0, 0) == want.encode()
