# Copyright (c) 2026 Kenneth Stott
# Canary: 65e938bc-f8b7-4f3e-9199-92377920079f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: SingleStore as a named source read through the direct SourcePool (REQ-1097).

SingleStore is MySQL wire-compatible, so it reuses the aiomysql direct driver. The
harness auto-provisions the singlestore container (requires_singlestore marker →
docker-compose.test.yml). Seeds a table, reads it back through the pool.

The singlestoredb-dev image requires a license key (SINGLESTORE_LICENSE) to start.
We don't have one in this environment, so the whole module is skipped when it's
absent — a documented, explicit skip, not a silent pass. Set SINGLESTORE_LICENSE
to actually exercise this test.
"""

from __future__ import annotations

import os

import aiomysql  # pyright: ignore[reportMissingImports]
import pytest

from provisa.executor.pool import SourcePool

pytestmark = [
    pytest.mark.integration,
    pytest.mark.requires_singlestore,
    pytest.mark.asyncio,
    pytest.mark.skipif(
        not os.environ.get("SINGLESTORE_LICENSE"),
        reason="SingleStore dev image requires SINGLESTORE_LICENSE; not present in this environment",
    ),
]

_SID = "e2e-singlestore"


async def _seed(sql_statements: list[str]) -> None:
    # Seed via a direct autocommit connection — the read driver never commits (read path).
    # No `db=` here: `provisa` may not exist yet on the first call (it's created by
    # the seed statements below). All statements use fully-qualified table names.
    conn = await aiomysql.connect(
        host="localhost",
        port=int(os.environ["SINGLESTORE_PORT"]),
        user="root",
        password="provisa",
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
            "CREATE DATABASE IF NOT EXISTS provisa",
            "DROP TABLE IF EXISTS provisa.widgets",
            "CREATE TABLE provisa.widgets (id INT PRIMARY KEY, name VARCHAR(64))",
            "INSERT INTO provisa.widgets VALUES (1,'a'),(2,'b'),(3,'c')",
        ]
    )
    p = SourcePool()
    await p.add(
        source_id=_SID,
        source_type="singlestore",
        host="localhost",
        port=int(os.environ["SINGLESTORE_PORT"]),
        database="provisa",
        user="root",
        password="provisa",
    )
    try:
        yield p
    finally:
        await p.close_all()
        await _seed(["DROP TABLE IF EXISTS provisa.widgets"])


async def test_singlestore_source_reads_through_pool(pool):
    assert pool.dialect_for(_SID) == "mysql"
    result = await pool.execute(_SID, "SELECT id, name FROM widgets ORDER BY id")
    assert result.column_names == ["id", "name"]
    assert result.rows == [(1, "a"), (2, "b"), (3, "c")]
