# Copyright (c) 2026 Kenneth Stott
# Canary: 5c1e9a72-3d4b-4f86-a0e2-7b9d6c3f1e58
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""FORMAT ON DUCKDB WRITES EACH VALUE AS ITS OUTPUT FUNCTION DOES (REQ-2236): format() reads every
argument by its type's output function, so %s of a boolean is t, of an array {1,2}, of an interval
'1 year 2 mons', of a bytea \\x0102 and of a character(3) its padding. DuckDB's own text of each
differs; each answer here is PostgreSQL 17's."""

# Requirements: REQ-2236
# ruff: noqa: F811 -- the fixtures are conn and pgwire of the module they are imported from

from __future__ import annotations

from typing import Any

import pytest
from psycopg import pq

from provisa.federation.duckdb_pg_format import array_out, elements_out, interval_out
from tests.unit.pgwire.test_type_conversions import conn, pgwire  # noqa: F401


def _format(*args: str) -> str:
    return f'SELECT pg_catalog."format"({", ".join(args)}) AS v'


_BOTH = "'%s|%L'::text"

#: statement -> the text PostgreSQL 17 answers.
ANSWERED: dict[str, str] = {
    # boolean: boolout's t and f, not the true of a cast to text
    _format(_BOTH, "true::boolean", "true::boolean"): "t|'t'",
    _format(_BOTH, "false", "false"): "f|'f'",
    _format("'%s'::text", "1 < 2"): "t",
    _format("'[%s]'::text", "NULL::boolean"): "[]",
    # arrays: array_out's braces, NULL and quoting
    _format(
        _BOTH, "ARRAY[1,NULL,2]::integer[]", "ARRAY[1,NULL,2]::integer[]"
    ): "{1,NULL,2}|'{1,NULL,2}'",
    _format("'%s'::text", "ARRAY['a b','c','',NULL,'null']::text[]"): '{"a b",c,"",NULL,"null"}',
    _format("'%s'::text", "ARRAY[true,false]::boolean[]"): "{t,f}",
    _format("'%s'::text", "ARRAY[true,false]"): "{t,f}",
    _format("'%s'::text", "ARRAY['1 day'::interval, '2 mons'::interval]"): '{"1 day","2 mons"}',
    _format("'%s %s'::text", "VARIADIC ARRAY[true,false]::boolean[]"): "t f",
    # interval: interval_out's mon(s), and the sign of each field
    _format(
        _BOTH, "'1 year 2 mons'::interval", "'1 year 2 mons'::interval"
    ): "1 year 2 mons|'1 year 2 mons'",
    _format("'%s'::text", "'-1 day 2 hours'::interval"): "-1 days +02:00:00",
    # bytea: byteaout's hexadecimal
    _format(_BOTH, "'\\x0102'::bytea", "'\\x0102'::bytea"): "\\x0102|E'\\\\x0102'",
    # character(n): bpcharout keeps the padding
    _format("'[%s]'::text", "'a'::character(3)"): "[a  ]",
    # already PostgreSQL's text: an instant in the session's zone, a numeric's scale
    _format("'%s'::text", "'2024-01-02 03:04:05+00'::timestamptz"): "2024-01-02 03:04:05+00",
    _format("'%s'::text", "1.50::numeric(10,3)"): "1.500",
}


def _result(conn: Any, sql: str) -> Any:
    return conn.conn.pgconn.exec_params(f"{sql} LIMIT 1001".encode(), [], result_format=0)


@pytest.mark.parametrize(("sql", "text"), list(ANSWERED.items()), ids=list(ANSWERED))
def test_format_writes_each_value_as_its_output_function(conn: Any, sql: str, text: str):
    res = _result(conn, sql)
    assert res.status == pq.ExecStatus.TUPLES_OK, (
        f"raised {(res.error_field(pq.DiagnosticField.MESSAGE_PRIMARY) or b'').decode()}"
    )
    assert res.get_value(0, 0).decode() == text


def test_an_array_no_form_writes_the_output_of_is_refused_by_name(conn: Any):
    res = _result(conn, _format("'%s'::text", "ARRAY['2024-01-01'::date]::date[]"))
    assert res.status == pq.ExecStatus.FATAL_ERROR
    said = (
        (res.error_field(pq.DiagnosticField.SQLSTATE) or b"").decode(),
        (res.error_field(pq.DiagnosticField.MESSAGE_PRIMARY) or b"").decode(),
    )
    assert said == ("0A000", "format() has no form on duckdb")


@pytest.mark.parametrize(
    ("duckdb_text", "written"),
    [
        ("1 year 2 months 3 days 04:05:06.5", "1 year 2 mons 3 days 04:05:06.5"),
        ("-1 day 02:00:00", "-1 days +02:00:00"),
        ("-00:00:00.5", "-00:00:00.5"),
        ("1 month", "1 mon"),
        ("00:00:00", "00:00:00"),
    ],
)
def test_interval_out_reads_duckdb_text_as_postgresql_writes_it(duckdb_text: str, written: str):
    assert interval_out(duckdb_text) == written


def test_array_out_quotes_as_array_out_does():
    assert array_out(['q"\\', "x"], "text") == '{"q\\"\\\\",x}'
    assert array_out([], "integer") == "{}"


def test_elements_out_keeps_a_null_element():
    assert elements_out(["true", None, "false"], "boolean") == ["t", None, "f"]
