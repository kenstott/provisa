# Copyright (c) 2026 Kenneth Stott
# Canary: 7c1f6a10-3b52-4e7d-9a44-2f0d8c6e5b31
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""CLIENT-CALLED SERVER FUNCTIONS ANSWER LIKE POSTGRESQL (REQ-2167, amended 2026-10-29): a server
function is answered as a PostgreSQL server with no such file, sequence or other object answers
it: strict NULL for a NULL argument, the answer that needs no object, and otherwise PostgreSQL's
own error and SQLSTATE. Each statement runs as the conformance probe runs it, through a real pgwire
server; each expected answer is PostgreSQL 17's (src/backend/utils/adt/genfile.c,
src/backend/access/transam/xlogfuncs.c, src/backend/commands/sequence.c)."""

# Requirements: REQ-2167
# ruff: noqa: F811 -- the fixtures are conn and pgwire of the module they are imported from

from __future__ import annotations

from typing import Any

import pytest
from psycopg import pq

from tests.unit.pgwire.test_type_conversions import conn, pgwire  # noqa: F401

BOOL, BYTEA, INT8, TEXT, RECORD, PG_LSN = 16, 17, 20, 25, 2249, 3220


def _run(conn: Any, sql: str, fmt: int = 0) -> Any:
    return conn.conn.pgconn.exec_params(sql.encode(), [], result_format=fmt)


def _rows(res: Any) -> list[Any]:
    return [None if (v := res.get_value(i, 0)) is None else v.decode() for i in range(res.ntuples)]


def _is_answered(conn: Any, sql: str, oid: int, rows: list[Any]) -> None:
    res = _run(conn, sql)
    assert res.status == pq.ExecStatus.TUPLES_OK, (res.error_message or b"").decode()
    assert (res.ftype(0), _rows(res)) == (oid, rows)


def _is_refused(conn: Any, sql: str, sqlstate: str, message: str) -> None:
    res = _run(conn, sql)
    assert res.status == pq.ExecStatus.FATAL_ERROR
    assert res.error_field(pq.DiagnosticField.SQLSTATE) == sqlstate.encode()
    assert res.error_field(pq.DiagnosticField.MESSAGE_PRIMARY) == message.encode()


ANSWERS: list[tuple[str, int, list[Any]]] = [
    ("SELECT pg_reload_conf() AS v", BOOL, ["t"]),
    ('SELECT pg_catalog."pg_reload_conf"() AS v', BOOL, ["t"]),
    ('SELECT pg_catalog."pg_create_restore_point"(NULL::text) AS v', PG_LSN, [None]),
    ('SELECT pg_catalog."pg_read_binary_file"(NULL::text) AS v', BYTEA, [None]),
    ('SELECT pg_catalog."pg_read_binary_file"(NULL::text, NULL::boolean) AS v', BYTEA, [None]),
    (
        'SELECT pg_catalog."pg_read_binary_file"(NULL::text, NULL::bigint, NULL::bigint) AS v',
        BYTEA,
        [None],
    ),
    (
        'SELECT pg_catalog."pg_read_binary_file"'
        "(NULL::text, NULL::bigint, NULL::bigint, NULL::boolean) AS v",
        BYTEA,
        [None],
    ),
    ("SELECT pg_read_binary_file('no_such_file', true) AS v", BYTEA, [None]),
    ("SELECT pg_read_binary_file('no_such_file', 0, 4, true) AS v", BYTEA, [None]),
    ('SELECT pg_catalog."pg_read_file"(NULL::text) AS v', TEXT, [None]),
    ("SELECT pg_read_file('no_such_file', true) AS v", TEXT, [None]),
    ('SELECT pg_catalog."pg_stat_file"(NULL::text) AS v', RECORD, [None]),
    ('SELECT pg_catalog."pg_stat_file"(NULL::text, NULL::boolean) AS v', RECORD, [None]),
    ("SELECT pg_stat_file('no_such_file', true) AS v", RECORD, [None]),
    ('SELECT pg_catalog."setval"(NULL::regclass, NULL::bigint) AS v', INT8, [None]),
    ('SELECT pg_catalog."setval"(NULL::regclass, NULL::bigint, NULL::boolean) AS v', INT8, [None]),
    ("SELECT setval(NULL, 5) AS v", INT8, [None]),
]


@pytest.mark.parametrize(("sql", "oid", "rows"), ANSWERS, ids=[a[0] for a in ANSWERS])
def test_a_server_function_answers_as_postgresql_answers_it(conn: Any, sql: str, oid: int, rows):
    _is_answered(conn, sql, oid, rows)


REFUSALS: list[tuple[str, str, str]] = [
    (
        "SELECT pg_create_restore_point('p')",
        "55000",
        "WAL level not sufficient for creating a restore point",
    ),
    (
        "SELECT pg_read_binary_file('no_such_file')",
        "58P01",
        'could not open file "no_such_file" for reading: No such file or directory',
    ),
    (
        "SELECT pg_read_binary_file('no_such_file', 0, 4)",
        "58P01",
        'could not open file "no_such_file" for reading: No such file or directory',
    ),
    (
        "SELECT pg_read_binary_file('no_such_file', false)",
        "58P01",
        'could not open file "no_such_file" for reading: No such file or directory',
    ),
    (
        "SELECT pg_read_binary_file('no_such_file', 0, -1)",
        "22023",
        "requested length cannot be negative",
    ),
    ("SELECT pg_read_binary_file('/etc/passwd')", "42501", "absolute path not allowed"),
    ("SELECT pg_read_binary_file('/etc/passwd', true)", "42501", "absolute path not allowed"),
    (
        "SELECT pg_read_binary_file('../postgresql.conf')",
        "42501",
        "path must be in or below the data directory",
    ),
    (
        "SELECT pg_read_file('a/../../b')",
        "42501",
        "path must be in or below the data directory",
    ),
    (
        "SELECT pg_read_file('postgresql.conf')",
        "58P01",
        'could not open file "postgresql.conf" for reading: No such file or directory',
    ),
    (
        "SELECT pg_stat_file('no_such_file')",
        "58P01",
        'could not stat file "no_such_file": No such file or directory',
    ),
    ("SELECT pg_stat_file('/etc/passwd', true)", "42501", "absolute path not allowed"),
    ("SELECT setval('no_such_sequence', 5)", "42P01", 'relation "no_such_sequence" does not exist'),
    (
        "SELECT setval('no_such_sequence'::regclass, 5, false)",
        "42P01",
        'relation "no_such_sequence" does not exist',
    ),
    ("SELECT setval(2147483000, 5)", "XX000", "could not open relation with OID 2147483000"),
    ("SELECT setval('pg_class', 5)", "42809", 'cannot open relation "pg_class"'),
]


@pytest.mark.parametrize(("sql", "sqlstate", "message"), REFUSALS, ids=[r[0] for r in REFUSALS])
def test_a_server_function_refuses_as_postgresql_refuses_it(
    conn: Any, sql: str, sqlstate: str, message: str
):
    _is_refused(conn, sql, sqlstate, message)


def test_a_missing_file_stats_as_a_row_of_nulls_in_from(conn: Any):
    res = _run(conn, "SELECT size, isdir FROM pg_stat_file('no_such_file', true)")
    assert (res.ntuples, res.nfields, res.get_value(0, 0), res.get_value(0, 1)) == (
        1,
        2,
        None,
        None,
    )
    assert res.ftype(0) == INT8 and res.ftype(1) == BOOL


def test_a_stat_of_a_missing_file_in_from_is_postgresqls_error(conn: Any):
    _is_refused(
        conn,
        "SELECT size FROM pg_stat_file('no_such_file')",
        "58P01",
        'could not stat file "no_such_file": No such file or directory',
    )


def test_the_answers_hold_in_binary(conn: Any):
    res = _run(conn, "SELECT pg_reload_conf() AS v", fmt=1)
    assert (res.ftype(0), res.get_value(0, 0)) == (BOOL, b"\x01")
    res = _run(conn, 'SELECT pg_catalog."pg_create_restore_point"(NULL::text) AS v', fmt=1)
    assert (res.ftype(0), res.get_value(0, 0)) == (PG_LSN, None)
