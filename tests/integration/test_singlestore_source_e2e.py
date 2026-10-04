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

SingleStore is MySQL wire-compatible, so it reuses the aiomysql direct driver. The test runs against
the live SingleStore instance the warehouse lane holds credentials for (SINGLESTORE_HOST, _PORT,
_USERNAME, _PASSWORD, _DATABASE): it seeds a table of its own, reads it back through the pool, and
drops it.
"""

from __future__ import annotations

import os
import ssl
import uuid

import aiomysql  # pyright: ignore[reportMissingImports]
import pytest

from provisa.executor.pool import SourcePool

pytestmark = [pytest.mark.integration, pytest.mark.requires_warehouse, pytest.mark.asyncio]

_SID = "e2e-singlestore"


def _conn_args() -> dict:
    return {
        "host": os.environ["SINGLESTORE_HOST"],
        "port": int(os.environ["SINGLESTORE_PORT"]),
        "user": os.environ["SINGLESTORE_USERNAME"],
        "password": os.environ["SINGLESTORE_PASSWORD"],
    }


async def _seed(sql_statements: list[str]) -> None:
    # Seed via a direct autocommit connection — the read driver never commits (read path). TLS,
    # as the source driver connects: SingleStore Cloud refuses a connection without it.
    conn = await aiomysql.connect(
        **_conn_args(),
        db=os.environ["SINGLESTORE_DATABASE"],
        ssl=ssl.create_default_context(),
        autocommit=True,
    )
    try:
        async with conn.cursor() as cur:
            for stmt in sql_statements:
                await cur.execute(stmt)
    finally:
        conn.close()


@pytest.fixture
async def table():
    # A table of this run's own: the instance is shared by every run of the lane.
    name = f"widgets_{uuid.uuid4().hex[:12]}"
    await _seed(
        [
            f"CREATE TABLE {name} (id INT PRIMARY KEY, name VARCHAR(64))",
            f"INSERT INTO {name} VALUES (1,'a'),(2,'b'),(3,'c')",
        ]
    )
    try:
        yield name
    finally:
        await _seed([f"DROP TABLE IF EXISTS {name}"])


@pytest.fixture
async def pool():
    args = _conn_args()
    p = SourcePool()
    await p.add(
        source_id=_SID,
        source_type="singlestore",
        host=args["host"],
        port=args["port"],
        database=os.environ["SINGLESTORE_DATABASE"],
        user=args["user"],
        password=args["password"],
    )
    try:
        yield p
    finally:
        await p.close_all()


async def test_singlestore_source_reads_through_pool(pool, table):
    assert (
        pool.dialect_for(_SID) == "singlestore"
    )  # SQLGlot's own SingleStore dialect (REQ-1755 amendment)
    result = await pool.execute(_SID, f"SELECT id, name FROM {table} ORDER BY id")
    assert result.column_names == ["id", "name"]
    assert result.rows == [(1, "a"), (2, "b"), (3, "c")]
