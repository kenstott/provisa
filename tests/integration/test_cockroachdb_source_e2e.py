# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: CockroachDB as a named source read through the direct SourcePool (REQ-1097).

CockroachDB is Postgres wire-compatible, so it reuses the asyncpg direct driver
(_make_pg). The harness auto-provisions the cockroachdb container (requires_cockroachdb
marker -> docker-compose.test.yml). Seeds a table, reads it back through the pool.
"""

from __future__ import annotations

import os

import asyncpg
import pytest

from provisa.executor.pool import SourcePool

pytestmark = [pytest.mark.integration, pytest.mark.requires_cockroachdb, pytest.mark.asyncio]

_SID = "e2e-cockroachdb"


async def _seed(sql_statements: list[str]) -> None:
    # Seed via a direct connection (asyncpg autocommits by default) — the read driver
    # never commits (read path).
    conn = await asyncpg.connect(
        host="localhost",
        port=int(os.environ["COCKROACHDB_PORT"]),
        database="defaultdb",
        user="root",
        password="",
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
            "CREATE TABLE widgets (id INT PRIMARY KEY, name STRING)",
            "INSERT INTO widgets VALUES (1,'a'),(2,'b'),(3,'c')",
        ]
    )
    p = SourcePool()
    await p.add(
        source_id=_SID,
        source_type="cockroachdb",
        host="localhost",
        port=int(os.environ["COCKROACHDB_PORT"]),
        database="defaultdb",
        user="root",
        password="",
    )
    try:
        yield p
    finally:
        await p.close_all()
        await _seed(["DROP TABLE IF EXISTS widgets"])


async def test_cockroachdb_source_reads_through_pool(pool):
    assert pool.dialect_for(_SID) == "cockroachdb"
    result = await pool.execute(_SID, "SELECT id, name FROM widgets ORDER BY id")
    assert result.column_names == ["id", "name"]
    assert result.rows == [(1, "a"), (2, "b"), (3, "c")]
