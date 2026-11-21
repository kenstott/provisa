# Copyright (c) 2026 Kenneth Stott
# Canary: 0b9f4c4e-5f0e-4a7c-8f0b-7f3f6f0a9d12
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A statement's own error leaves its pooled connection usable by the raw passthrough (REQ-1863).

A function that itself raises SQLSTATE 0A000 was read as a stale cached plan: the statement was
forgotten here while PostgreSQL still held it, its name was given out again, and every statement
new to that pooled connection then failed with 42P05 `prepared statement "_provisa_pt_N" already
exists` (Conformance (functions), run 43861326336: 373 overloads on two connections). Real
PostgreSQL, the real driver's pool of one connection."""

# Requirements: REQ-1863, REQ-2167

from __future__ import annotations

import struct
import tempfile

import pytest

pytestmark = [pytest.mark.integration]

pgserver = pytest.importorskip("pixeltable_pgserver")


@pytest.fixture(scope="module")
def pg_dsn():
    server = pgserver.get_server(tempfile.mkdtemp(prefix="provisa_passthrough_stale_"))
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
        1,  # one connection: every statement here runs on the same pooled connection
    )
    return drv


def _text_values(data_row: bytes) -> list[str | None]:
    """A text-format DataRow message's column values."""
    (count,) = struct.unpack_from("!h", data_row, 5)
    pos = 7
    values: list[str | None] = []
    for _ in range(count):
        (length,) = struct.unpack_from("!i", data_row, pos)
        pos += 4
        if length < 0:
            values.append(None)
            continue
        values.append(data_row[pos : pos + length].decode())
        pos += length
    return values


def _run(drv, sql: str) -> list[list[str | None]]:
    from provisa.pgwire.pg_passthrough import open_passthrough

    result = open_passthrough(drv.borrow_raw, sql, [], [], None)
    return [_text_values(row) for row in result.cursor.fetch(10_000)]


async def test_a_function_s_own_0a000_leaves_the_pooled_connection_usable(pg_dsn):
    from provisa.pgwire.pg_passthrough import PassthroughFailure

    drv = await _driver(pg_dsn)
    try:
        backend = _run(drv, "SELECT pg_backend_pid()")
        with pytest.raises(PassthroughFailure) as raised:
            _run(drv, "SELECT array_position('{{1,2},{3,4}}'::integer[], 3)")
        assert (raised.value.sqlstate, str(raised.value)) == (
            "0A000",
            "searching for elements in multidimensional arrays is not supported",
        )
        for i in range(20):
            assert _run(drv, f"SELECT {i} AS n, 'v{i}'::text AS t") == [[str(i), f"v{i}"]]
        # The failing statement is still prepared and still answers as PostgreSQL does.
        with pytest.raises(PassthroughFailure) as again:
            _run(drv, "SELECT array_position('{{1,2},{3,4}}'::integer[], 3)")
        assert again.value.sqlstate == "0A000"
        assert _run(drv, "SELECT pg_backend_pid()") == backend  # the same connection throughout
    finally:
        await drv.close()


async def test_a_stale_cached_plan_is_prepared_again_on_the_same_connection(pg_dsn, monkeypatch):
    """PostgreSQL refuses a kept plan once its result type changed ("cached plan must not change
    result type") and keeps the statement prepared: it is closed, prepared again and run, and the
    client sees rows."""
    import psycopg

    from provisa.pgwire import pg_passthrough

    replaced: list[str] = []
    real_prepare = pg_passthrough._prepare

    def _prepare(exchange, borrowed, sql, *, replacing=None):
        if replacing is not None:
            replaced.append(sql)
        return real_prepare(exchange, borrowed, sql, replacing=replacing)

    monkeypatch.setattr(pg_passthrough, "_prepare", _prepare)
    sql = "SELECT label FROM passthrough_stale_plan ORDER BY label"
    drv = await _driver(pg_dsn)
    try:
        with psycopg.connect(pg_dsn, autocommit=True) as admin:
            admin.execute("DROP TABLE IF EXISTS passthrough_stale_plan")
            admin.execute("CREATE TABLE passthrough_stale_plan (label varchar(10))")
            admin.execute("INSERT INTO passthrough_stale_plan VALUES ('a'), ('b')")
            backend = _run(drv, "SELECT pg_backend_pid()")
            assert _run(drv, sql) == [["a"], ["b"]]
            # Same type OID, another type modifier: the kept plan's result type is now stale.
            admin.execute("ALTER TABLE passthrough_stale_plan ALTER COLUMN label TYPE varchar(20)")
            assert _run(drv, sql) == [["a"], ["b"]]
            assert replaced == [sql]  # PostgreSQL did refuse the kept plan
            assert _run(drv, sql) == [["a"], ["b"]]
            assert _run(drv, "SELECT 1 AS n") == [["1"]]
            assert _run(drv, "SELECT pg_backend_pid()") == backend
            admin.execute("DROP TABLE passthrough_stale_plan")
    finally:
        await drv.close()
