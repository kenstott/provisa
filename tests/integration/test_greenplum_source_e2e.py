# Copyright (c) 2026 Kenneth Stott
# Canary: bc218925-44b4-4701-b26a-75ce6523bcb0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: Greenplum as a named source read through the direct SourcePool (REQ-1097).

Greenplum is Postgres wire-compatible (REQ-950), so it reuses the asyncpg direct driver
(_make_pg) and the postgres dialect — the same code path cockroachdb/yugabytedb ride. The
harness auto-provisions the greenplum container (requires_greenplum marker ->
docker-compose.test.yml). Seeds a table, reads it back through the pool. The single-node
GPDB entrypoint builds its cluster on boot; the container's healthcheck already gates on a
live `select 1`, but seeding still retries briefly to ride out the master warming up.
"""

from __future__ import annotations

import asyncio
import os

import asyncpg
import pytest

from provisa.executor.pool import SourcePool

pytestmark = [pytest.mark.integration, pytest.mark.requires_greenplum, pytest.mark.asyncio]

_SID = "e2e-greenplum"


async def _connect() -> asyncpg.Connection:
    # Superuser gpadmin, trust auth (no password), default db postgres — how datagrip/greenplum:6.8
    # advertises its single-node master (see docker-compose.test.yml). Retry while the master warms.
    last: Exception | None = None
    for _ in range(30):
        try:
            return await asyncpg.connect(
                host="localhost",
                port=int(os.environ["GREENPLUM_PORT"]),
                database="postgres",
                user="gpadmin",
                password="",
            )
        except (asyncpg.PostgresError, OSError) as e:  # not-ready / connection-refused
            last = e
            await asyncio.sleep(2)
    raise RuntimeError(f"greenplum never accepted a connection: {last!r}")


async def _seed(sql_statements: list[str]) -> None:
    conn = await _connect()
    try:
        for stmt in sql_statements:
            await conn.execute(stmt)
    finally:
        await conn.close()


@pytest.fixture
async def pool():
    await _seed(
        [
            "DROP TABLE IF EXISTS widgets",
            "CREATE TABLE widgets (id int PRIMARY KEY, name text)",
            "INSERT INTO widgets VALUES (1,'a'),(2,'b'),(3,'c')",
        ]
    )
    p = SourcePool()
    await p.add(
        source_id=_SID,
        source_type="greenplum",
        host="localhost",
        port=int(os.environ["GREENPLUM_PORT"]),
        database="postgres",
        user="gpadmin",
        password="",
    )
    try:
        yield p
    finally:
        await p.close_all()
        await _seed(["DROP TABLE IF EXISTS widgets"])


async def test_greenplum_source_reads_through_pool(pool):
    assert pool.dialect_for(_SID) == "greenplum"
    result = await pool.execute(_SID, "SELECT id, name FROM widgets ORDER BY id")
    assert result.column_names == ["id", "name"]
    assert result.rows == [(1, "a"), (2, "b"), (3, "c")]
