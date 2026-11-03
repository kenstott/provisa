# Copyright (c) 2026 Kenneth Stott
# Canary: 51906c50-c07d-4496-bec5-e368a56f7bba
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A statement whose PostgreSQL error the engine meets only past its first Arrow batch (REQ-2167,
REQ-2194): the client is answered with PostgreSQL's own error, SQLSTATE and message, after the rows
already sent, and the connection answers the next statement."""

# Requirements: REQ-2167, REQ-2194
# ruff: noqa: F811 -- the fixtures are conn and pgwire of the module they are imported from

from __future__ import annotations

from typing import Any

import pytest
from psycopg import pq

from tests.unit.pgwire.test_type_conversions import conn, pgwire  # noqa: F401

# int42pl overflows on the last row only, PostgreSQL's 22003 raised by its catalog macro: past what
# the engine computes when the statement is run (about 128K rows), so it is met as a batch is read.
_ROWS = 400_000
_LATE_OVERFLOW = (
    f"SELECT pg_catalog.int42pl((2147483647 - {_ROWS} + g.i)::int4, 1::int2) AS v "
    f"FROM generate_series(1, {_ROWS}) AS g(i)"
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
