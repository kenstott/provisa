# Copyright (c) 2026 Kenneth Stott
# Canary: 6c1f8a3e-40d7-4e25-9b16-e7d2a05c3f94
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The json and jsonb functions DuckDB answers with UDFs, each statement run on a real PostgreSQL
and on DuckDB and the two answers compared as an answer is compared (REQ-2167, REQ-2171). A
statement's error on PostgreSQL has to be an error on DuckDB, with its SQLSTATE."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import json
from typing import Any

import duckdb
import psycopg
import pytest
from psycopg import pq

from provisa.core.pg_error import from_engine
from provisa.federation.duckdb_pg_functions import register
from provisa.pgwire.cast_types import result_casts
from provisa.transpiler.transpile import transpile_to_duckdb
from tests.unit.pgwire.pg17_json_matrix import SERVED, outcome, text_of

pytestmark = [pytest.mark.integration]

_DOC = """'{"a": {"b": [1, 2.50, {"c": null}], "d": "x"}, "e": [true, false], "f": 1e2}'"""

PATHS = (
    "$",
    "$.a",
    "$.a.b",
    "$.a.b[*]",
    "$.a.b[0]",
    "$.a.b[0 to 1]",
    "$.a.b[last]",
    "$.a.b[last - 1]",
    "$.a.b[0, 2]",
    "$.a.b[5]",
    "strict $.a.b[5]",
    "strict $.nope",
    "lax $.nope",
    "strict $.a.b.c",
    "$.a.*",
    "$.*",
    "$.**",
    "$.**{1}",
    "$.**{1 to 2}",
    "$.**.c",
    "$.a.b[*] ? (@ > 1)",
    "$.a.b[*] ? (@ == 2.5)",
    "$.a.b[*] ? (@ != 1)",
    "$.e[*] ? (@ == true)",
    '$.a.d ? (@ starts with "x")',
    '$.a.d ? (@ like_regex "^X$" flag "i")',
    "$.a.b ? (exists (@.c))",
    "$.a.b[*] ? (exists (@.c))",
    "$.a.b[*] ? ((@ > 1) is unknown)",
    "$.a.b[*] ? (@ > 1 && @ < 3)",
    "$.a.b[*] ? (@ < 2 || @ > 2)",
    "$.a.b[*] ? (!(@ > 1))",
    "$.f + 1",
    "$.f - 1",
    "$.f * 2",
    "$.f / 4",
    "$.f % 7",
    "-$.f",
    "+$.f",
    "$.f / 0",
    "$.a.d + 1",
    "$.a.b[*] + 1",
    "$.a.b.size()",
    "$.a.size()",
    "strict $.a.size()",
    "$.a.type()",
    "$.a.b[*].type()",
    "$.f.double()",
    "$.a.b[1].ceiling()",
    "$.a.b[1].floor()",
    "$.a.b[1].abs()",
    "$.keyvalue()",
    "$.f > 1",
    "$.f == 1",
    "$.a.d > 1",
    "$.nope > 1",
    "$.e[0]",
    "$ == null",
    '$.a.d == "x"',
    '"literal"',
    "1 + 2",
    '$.a.b[*] ? (@.type() == "number")',
    "null",
    "true",
)
PATH_FUNCTIONS = ("query_array", "query_first", "exists")
CALLS = (
    "jsonb_set('{\"a\":[1,2,3]}', '{a,-1}', '9')",
    "jsonb_set('{\"a\":[1,2,3]}', '{a,-9}', '9')",
    "jsonb_set('{\"a\":[1,2,3]}', '{a,-9}', '9', false)",
    "jsonb_set('{\"a\":[1,2,3]}', '{a,x}', '9')",
    "jsonb_set('[]', '{0}', '1')",
    "jsonb_set('{}', '{a}', '1')",
    "jsonb_set('{}', '{a,b}', '1')",
    "jsonb_set('{\"a\":1}', '{a,b}', '1')",
    "jsonb_set('{\"a\":1}', '{}', '1')",
    "jsonb_set('[1,[2,3]]', '{1,0}', '{\"z\":1}')",
    "jsonb_insert('[1,2]', '{0}', '9')",
    "jsonb_insert('[1,2]', '{0}', '9', true)",
    "jsonb_insert('[1,2]', '{-1}', '9')",
    "jsonb_insert('[1,2]', '{-1}', '9', true)",
    "jsonb_insert('[1,2]', '{9}', '9')",
    "jsonb_insert('[1,2]', '{-9}', '9')",
    "jsonb_insert('[]', '{0}', '9')",
    "jsonb_insert('1', '{0}', '9')",
    "jsonb_set_lax('{\"a\":1}', '{a}', NULL, true, 'return_target')",
    "jsonb_set_lax('{\"a\":1}', '{a}', NULL, true, 'raise_exception')",
    "jsonb_set_lax('{\"a\":1}', '{a}', NULL, true, 'nonsense')",
    "jsonb_set_lax('{\"a\":1}', '{a}', '2', true, 'nonsense')",
    "jsonb_set_lax(NULL, '{a}', NULL, true, NULL)",
    "jsonb_delete_path('[1,2,3]', '{1}')",
    "jsonb_delete_path('[1,2,3]', '{9}')",
    "jsonb_delete_path('[1,2,3]', '{x}')",
    "jsonb_delete_path('{\"a\":1}', '{}')",
    "jsonb_delete_path('{}', '{a}')",
    "jsonb_delete_path('1', '{a}')",
    "jsonb_concat('{}', '[1]')",
    "jsonb_concat('[]', '{\"a\":1}')",
    "jsonb_concat('{}', '1')",
    "jsonb_concat('{\"a\":1}', '1')",
    "jsonb_concat('1', '2')",
    "jsonb_concat('\"a\"', '{\"b\":1}')",
    "jsonb_concat('[]', '[]')",
    "jsonb_cmp('null', '1')",
    "jsonb_cmp('1', '\"a\"')",
    "jsonb_cmp('true', '1')",
    "jsonb_cmp('false', 'true')",
    "jsonb_cmp('[]', 'null')",
    "jsonb_cmp('[]', '1')",
    "jsonb_cmp('[1]', '1')",
    "jsonb_cmp('[1,2]', '1')",
    "jsonb_cmp('[1]', '[2]')",
    "jsonb_cmp('[1,2]', '[3]')",
    "jsonb_cmp('{}', '[1]')",
    "jsonb_cmp('{\"a\":1}', '{\"b\":1}')",
    "jsonb_cmp('{\"a\":1}', '{\"a\":2}')",
    'jsonb_cmp(\'{"a":1,"b":2}\', \'{"c":1}\')',
    "jsonb_cmp('[null]', '[1]')",
    "jsonb_cmp('[[1]]', '[1]')",
    "jsonb_cmp('[{}]', '[[]]')",
    "jsonb_cmp('1.0', '1')",
    "jsonb_contains('[1,2,[1,3]]', '[1,3]')",
    "jsonb_contains('[1,[1,3]]', '[[1,3]]')",
    "jsonb_contains('[{\"a\":1}]', '[{}]')",
    "jsonb_contains('{\"a\":[1,2]}', '{\"a\":[1]}')",
    "jsonb_contains('{\"a\":[1,2]}', '{\"a\":1}')",
    "jsonb_contains('\"a\"', '\"a\"')",
    "jsonb_contains('\"a\"', '\"b\"')",
    "jsonb_contains('1', '1.0')",
    "jsonb_contains('[1]', 'null')",
    "jsonb_contains('[null]', 'null')",
    "jsonb_exists('[\"a\",1]', '1')",
    "jsonb_exists('\"a\"', 'a')",
    "jsonb_exists_any('[\"a\"]', '{}')",
    "jsonb_exists_all('[\"a\"]', '{}')",
    "jsonb_exists_all('{\"a\":1}', '{a,NULL}')",
    "jsonb_exists_any('{\"a\":1}', '{NULL,a}')",
    "jsonb_pretty('1')",
    "jsonb_pretty('\"a\"')",
    "jsonb_pretty('[]')",
    "jsonb_pretty('{}')",
    "jsonb_pretty('[1,[2,{}],{\"a\":[]}]')",
    "jsonb_pretty('null')",
    "jsonb_strip_nulls('null')",
    'jsonb_strip_nulls(\'{"a":{"b":null}}\')',
    'json_strip_nulls(\'[{"a":null,"a":1}]\')',
    "jsonb_object('{a,b,c,d}')",
    "jsonb_object('{{a,1},{b,2}}')",
    "jsonb_object('{a,b}', '{1}')",
    "jsonb_object('{NULL,b}')",
    "jsonb_object('{}')",
    *(
        call
        for value in (
            "'{}'",
            "'[]'",
            "'null'",
            "'true'",
            "'false'",
            "'0'",
            "'1'",
            "'-1'",
            "'1.50'",
            "'1e100'",
            "'0.00001'",
            "'123456789.123456789'",
            "'\"\"'",
            "'\"a\"'",
            "'\"abcdefghijklmnopqrstuvwxyz\"'",
            "'[1,2]'",
            "'{\"a\":1}'",
            """'{"a":[1,{"b":null}],"cd":true}'""",
        )
        for call in (f"jsonb_hash({value})", f"jsonb_hash_extended({value}, 0)")
    ),
)
SEEDED = (
    *(f"jsonb_hash_extended({value}, 7)" for value in ("'{}'", "'null'", "'true'", "'0'", "'1.5'")),
    """jsonb_hash_extended('{"a":[1,{"b":null}],"cd":false}', 123456789012)""",
)


def _statements() -> list[str]:
    paths = [f"jsonb_path_{name}({_DOC}, '{path}')" for path in PATHS for name in PATH_FUNCTIONS]
    silent = [f"jsonb_path_query_array({_DOC}, '{path}', '{{}}', true)" for path in PATHS]
    rows = [f"* FROM jsonb_path_query({_DOC}, '{path}')" for path in PATHS]
    matches = [
        "jsonb_path_match('true', '$')",
        "jsonb_path_match('null', '$')",
        "jsonb_path_match('1', '$')",
        "jsonb_path_match('1', '$', '{}', true)",
        "jsonb_path_match('[true]', '$[0]')",
        """jsonb_path_exists('{"a":1}', '$.a ? (@ == $v)', '{"v":1}')""",
        """jsonb_path_exists('{"a":1}', '$.a ? (@ == $v)', '{"v":2}')""",
        """jsonb_path_exists('{"a":1}', '$.a ? (@ == $v)', '{}')""",
        """jsonb_path_exists('{"a":1}', '$.a', '[]')""",
        """jsonb_path_query_array('[1,2,3]', '$[*] ? (@ > $"m")', '{"m":1}')""",
    ]
    return [f"SELECT {item}" for item in (*CALLS, *SEEDED, *paths, *silent, *rows, *matches)]


STATEMENTS = _statements()


def _postgres(conn: psycopg.Connection, sql: str) -> tuple:
    res = conn.pgconn.exec_params(sql.encode(), [], result_format=0)
    if res.status != pq.ExecStatus.TUPLES_OK:
        return ("ERROR", (res.error_field(pq.DiagnosticField.SQLSTATE) or b"").decode())
    rows = [
        tuple(
            None if (v := res.get_value(i, j)) is None else bytes(v).decode()
            for j in range(res.nfields)
        )
        for i in range(res.ntuples)
    ]
    return ([res.ftype(j) for j in range(res.nfields)], rows)


def _on_duckdb(db: Any, sql: str) -> list[tuple] | str:
    """DuckDB's rows as PostgreSQL's text, or the SQLSTATE of its error."""
    casts = result_casts(sql) or ()

    def cell(index: int, value: Any) -> str | None:
        cast = casts[index] if index < len(casts) else None
        return text_of(cast.coerce(value) if cast is not None and cast.coerce else value)

    try:
        rows = db.execute(transpile_to_duckdb(sql)).fetchall()
    except Exception as refused:  # noqa: BLE001 -- the statement's outcome
        error = from_engine(refused)
        return error.sqlstate if error is not None else "engine"
    return [tuple(cell(i, value) for i, value in enumerate(row)) for row in rows]


def _sign(rows: list[tuple]) -> list[int]:
    return [(int(row[0]) > 0) - (int(row[0]) < 0) for row in rows]


def _differs(sql: str, want: tuple, got: list[tuple]) -> bool:
    """Whether ``got`` is not PostgreSQL's answer. jsonb_cmp of two strings is the raw result of
    the server's strcoll, whose size is libc's: only its sign is defined."""
    if "jsonb_cmp" in sql:
        return _sign(want[1]) != _sign(got)
    return outcome(sql, want, got, False) != SERVED


def test_the_udfs_answer_as_postgresql_does(pg_dsn: str):
    db = duckdb.connect()
    register(db)
    found: dict[str, Any] = {}
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        for sql in STATEMENTS:
            want = _postgres(conn, sql)
            got = _on_duckdb(db, sql)
            if want[0] == "ERROR":
                if got != want[1]:
                    found[sql] = {"postgres": want, "duckdb": got}
            elif isinstance(got, str) or _differs(sql, want, got):
                found[sql] = {"postgres": want, "duckdb": got}
    assert not found, json.dumps(found, indent=1, ensure_ascii=False)
