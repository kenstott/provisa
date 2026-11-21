# Copyright (c) 2026 Kenneth Stott
# Canary: 51906c50-c07d-4496-bec5-e368a56f7bba
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A statement whose error the engine meets only past its first Arrow batch (REQ-2167, REQ-2194):
the client is answered with the error, after the rows already sent -- PostgreSQL's own SQLSTATE and
message where the engine raised PostgreSQL's error, the SQLSTATE PostgreSQL reports for the engine's
error otherwise -- and the connection answers the next statement."""

# Requirements: REQ-2167, REQ-2194
# ruff: noqa: F811 -- the fixtures are conn and pgwire of the module they are imported from

from __future__ import annotations

from typing import Any

import pytest
from psycopg import pq

from tests.unit.pgwire.test_type_conversions import conn, pgwire  # noqa: F401

# int42pl overflows on the last row only, PostgreSQL's 22003 raised by its catalog macro: past what
# the engine computes when the statement is run (about 128K rows), so it is met as a batch is read.
# Far enough that the first batches are read before the error: DuckDB's threads run ahead of the
# reader, so at 400,000 rows about one read in thirteen under load met it before the first.
_ROWS = 2_000_000
_LATE_OVERFLOW = (
    f"SELECT pg_catalog.int42pl((2147483647 - {_ROWS} + g.i)::int4, 1::int2) AS v "
    f"FROM generate_series(1, {_ROWS}) AS g(i)"
)


# A zero divisor on row 1,900,000: the transpiler's division guard, met as a batch is read.
_LATE_ZERO_DIVISOR = f"SELECT 1/(g.i - 1900000) AS v FROM generate_series(1, {_ROWS}) AS g(i)"
# Engine errors no catalog macro raises, on the last row: DuckDB's own conversion and overflow.
_LATE_CONVERSION = (
    f"SELECT CASE WHEN g.i = {_ROWS} THEN 'x' ELSE g.i::text END::int AS v "
    f"FROM generate_series(1, {_ROWS}) AS g(i)"
)
_LATE_INT4_OVERFLOW = (
    f"SELECT (2147483647 - {_ROWS} + g.i)::int4 + 1 AS v FROM generate_series(1, {_ROWS}) AS g(i)"
)


def _said(res: Any) -> tuple[str, str]:
    return (
        res.error_field(pq.DiagnosticField.SQLSTATE).decode(),
        res.error_field(pq.DiagnosticField.MESSAGE_PRIMARY).decode(),
    )


@pytest.mark.parametrize("protocol", ["extended", "simple"])
def test_an_error_met_in_a_later_batch_is_postgresqls_and_the_connection_goes_on(
    conn: Any, protocol: str
):
    pgconn = conn.conn.pgconn
    run = (
        (lambda sql: pgconn.exec_params(sql.encode(), [], result_format=0))
        if protocol == "extended"
        else (lambda sql: pgconn.exec_(sql.encode()))
    )
    res = run(_LATE_OVERFLOW)
    assert res.status == pq.ExecStatus.FATAL_ERROR, res.status
    assert _said(res) == ("22003", "integer out of range")
    after = run("SELECT 42 AS v")
    assert after.status == pq.ExecStatus.TUPLES_OK, (after.error_message or b"").decode()
    assert after.get_value(0, 0) == b"42"


def _runner(conn: Any, protocol: str) -> Any:
    pgconn = conn.conn.pgconn
    if protocol == "extended":
        return lambda sql: pgconn.exec_params(sql.encode(), [], result_format=0)
    return lambda sql: pgconn.exec_(sql.encode())


@pytest.mark.parametrize("protocol", ["extended", "simple"])
@pytest.mark.parametrize(
    ("sql", "sqlstate"),
    [
        (_LATE_ZERO_DIVISOR, "22012"),  # division_by_zero
        (_LATE_CONVERSION, "22P02"),  # invalid_text_representation
        (_LATE_INT4_OVERFLOW, "22003"),  # numeric_value_out_of_range
    ],
    ids=["zero-divisor", "conversion", "int4-overflow"],
)
def test_an_engine_error_met_in_a_later_batch_is_answered_and_the_connection_goes_on(
    conn: Any, protocol: str, sql: str, sqlstate: str
):
    run = _runner(conn, protocol)
    res = run(sql)
    assert res.status == pq.ExecStatus.FATAL_ERROR, res.status
    assert res.error_field(pq.DiagnosticField.SQLSTATE).decode() == sqlstate, (
        res.error_message or b""
    ).decode()
    after = run("SELECT 42 AS v")
    assert after.status == pq.ExecStatus.TUPLES_OK, (after.error_message or b"").decode()
    assert after.get_value(0, 0) == b"42"


def test_the_zero_divisor_is_postgresqls_division_by_zero(conn: Any):
    res = _runner(conn, "extended")(_LATE_ZERO_DIVISOR)
    assert _said(res) == ("22012", "division by zero")


def test_a_time_at_the_end_of_the_day_is_answered_and_the_connection_goes_on(conn: Any):
    """24:00:00, which PostgreSQL holds and Python's time does not, as PostgreSQL answers it."""
    pgconn = conn.conn.pgconn
    for sql, want in [
        ("SELECT timetz('23:59:59.9+01'::timetz, 0) AS v", b"24:00:00+01"),
        ("SELECT '24:00:00'::time AS v", b"24:00:00"),
        ("SELECT 42 AS v", b"42"),
    ]:
        res = pgconn.exec_params(sql.encode(), [], result_format=0)
        assert res.status == pq.ExecStatus.TUPLES_OK, (sql, (res.error_message or b"").decode())
        assert res.get_value(0, 0) == want, sql
