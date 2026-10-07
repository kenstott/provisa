# Copyright (c) 2026 Kenneth Stott
# Canary: f40db341-19a5-4e88-bf2d-acb7716387f0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A DIRECT PostgreSQL stream whose FETCH the request deadline cancels (REQ-1905).

The cancel aborts the stream's transaction. Closing the stream then ends that transaction and
gives the connection back, and the request ends with the deadline's own error: a CLOSE sent into
the aborted transaction raised "current transaction is aborted" from the close, and that error
replaced the deadline's (Flight, run 37608836037)."""

# Requirements: REQ-1905, REQ-1190

from __future__ import annotations

import tempfile

import pytest

pytestmark = [pytest.mark.integration]

pgserver = pytest.importorskip("pgserver")


@pytest.fixture(scope="module")
def pg_dsn():
    server = pgserver.get_server(tempfile.mkdtemp(prefix="provisa_direct_deadline_"))
    yield server.get_uri()


async def _driver(dsn: str):
    from urllib.parse import parse_qs, unquote, urlparse

    from provisa.executor.drivers.postgresql import PostgreSQLDriver

    u = urlparse(dsn)
    query = parse_qs(u.query)
    host = query["host"][0] if "host" in query else u.hostname
    assert host, dsn
    drv = PostgreSQLDriver()
    await drv.connect(
        host,
        u.port or 5432,
        (u.path or "/").lstrip("/"),
        unquote(u.username or ""),
        unquote(u.password or ""),
        1,
        1,  # one connection: the next read can only be served by the one the stream gave back
    )
    return drv


async def test_a_fetch_the_deadline_cancels_ends_with_the_deadline_and_frees_its_connection(
    pg_dsn,
):
    from provisa.core import request_deadline
    from provisa.executor.drivers.postgresql import PostgreSQLDriver

    drv = await _driver(pg_dsn)
    try:
        first = PostgreSQLDriver._FIRST_BATCH_ROWS
        # The first batch comes back at once; every row after it sleeps, so the next FETCH is
        # running on the server when the deadline cancels it.
        stream = await drv.open_stream(
            "SELECT g, CASE WHEN g > $1 THEN pg_sleep(30) END AS slept "
            "FROM generate_series(1, $2) g",
            [first, first + 10],
        )
        assert len(await stream.fetch(first)) == first

        deadline = request_deadline.Deadline(1.0, transport="flight", setting="limits.test")
        try:
            with request_deadline.bound(deadline):
                with pytest.raises(TimeoutError) as raised:
                    await stream.fetch(first)
                await stream.close()
        finally:
            deadline.stop()
        assert "current transaction is aborted" not in str(raised.value)

        # The connection went back idle: the pool's only connection serves the next read.
        result = await drv.execute("SELECT 1 AS one")
        assert result.rows == [(1,)]
    finally:
        await drv.close()
