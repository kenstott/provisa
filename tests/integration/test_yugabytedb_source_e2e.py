# Copyright (c) 2026 Kenneth Stott
# Canary: f73f87bb-887c-463c-8d45-d10b1846ac4e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: YugabyteDB as a named source read through the direct SourcePool (REQ-1097).

YugabyteDB YSQL is PostgreSQL wire-compatible, so it reuses the asyncpg direct
driver. The harness auto-provisions the yugabytedb container (requires_yugabytedb
marker → docker-compose.test.yml). Seeds a table, reads it back through the pool.
"""

from __future__ import annotations

import os

import asyncpg  # pyright: ignore[reportMissingImports]
import pytest

from provisa.executor.pool import SourcePool

pytestmark = [pytest.mark.integration, pytest.mark.requires_yugabytedb, pytest.mark.asyncio]

_SID = "e2e-yugabytedb"


async def _seed(sql_statements: list[str]) -> None:
    # Seed via a direct connection — asyncpg autocommits statements outside an
    # explicit transaction, and the read driver never commits (read path).
    conn = await asyncpg.connect(
        host="localhost",
        port=int(os.environ["YUGABYTEDB_PORT"]),
        database="yugabyte",
        user="yugabyte",
        password="yugabyte",
    )
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
            "CREATE TABLE widgets (id INT PRIMARY KEY, name TEXT)",
            "INSERT INTO widgets VALUES (1,'a'),(2,'b'),(3,'c')",
        ]
    )
    p = SourcePool()
    await p.add(
        source_id=_SID,
        source_type="yugabytedb",
        host="localhost",
        port=int(os.environ["YUGABYTEDB_PORT"]),
        database="yugabyte",
        user="yugabyte",
        password="yugabyte",
    )
    try:
        yield p
    finally:
        await p.close_all()
        await _seed(["DROP TABLE IF EXISTS widgets"])


async def test_yugabytedb_source_reads_through_pool(pool):
    assert pool.dialect_for(_SID) == "yugabytedb"
    result = await pool.execute(_SID, "SELECT id, name FROM widgets ORDER BY id")
    assert result.column_names == ["id", "name"]
    assert result.rows == [(1, "a"), (2, "b"), (3, "c")]
