# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: TiDB as a named source read through the direct SourcePool (REQ-1097).

TiDB is MySQL wire-compatible, so it reuses the aiomysql direct driver. The
harness auto-provisions the tidb container (requires_tidb marker →
docker-compose.test.yml). Seeds a table, reads it back through the pool.
"""

from __future__ import annotations

import os

import aiomysql  # pyright: ignore[reportMissingImports]
import pytest

from provisa.executor.pool import SourcePool

pytestmark = [pytest.mark.integration, pytest.mark.requires_tidb, pytest.mark.asyncio]

_SID = "e2e-tidb"


async def _seed(sql_statements: list[str]) -> None:
    # Seed via a direct autocommit connection — the read driver never commits (read path).
    conn = await aiomysql.connect(
        host="localhost",
        port=int(os.environ["TIDB_PORT"]),
        db="test",
        user="root",
        password="",
        autocommit=True,
    )
    try:
        async with conn.cursor() as cur:
            for stmt in sql_statements:
                await cur.execute(stmt)
    finally:
        conn.close()


@pytest.fixture
async def pool():
    await _seed(
        [
            "DROP TABLE IF EXISTS widgets",
            "CREATE TABLE widgets (id INT PRIMARY KEY, name VARCHAR(64))",
            "INSERT INTO widgets VALUES (1,'a'),(2,'b'),(3,'c')",
        ]
    )
    p = SourcePool()
    await p.add(
        source_id=_SID,
        source_type="tidb",
        host="localhost",
        port=int(os.environ["TIDB_PORT"]),
        database="test",
        user="root",
        password="",
    )
    try:
        yield p
    finally:
        await p.close_all()
        await _seed(["DROP TABLE IF EXISTS widgets"])


async def test_tidb_source_reads_through_pool(pool):
    assert pool.dialect_for(_SID) == "tidb"
    result = await pool.execute(_SID, "SELECT id, name FROM widgets ORDER BY id")
    assert result.column_names == ["id", "name"]
    assert result.rows == [(1, "a"), (2, "b"), (3, "c")]
