# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: SQL Server as a named source read through the direct SourcePool (REQ-1097).

The harness auto-provisions the sqlserver container (requires_sqlserver marker →
docker-compose.test.yml). Seeds a table, reads it back through the pool.
"""

from __future__ import annotations

import os

import pyodbc  # pyright: ignore[reportMissingImports]
import pytest

from provisa.executor.pool import SourcePool

pytestmark = [pytest.mark.integration, pytest.mark.requires_sqlserver, pytest.mark.asyncio]

_SID = "e2e-sqlserver"


def _seed(sql_statements: list[str]) -> None:
    # Seed via a direct autocommit connection — the read driver never commits (read path).
    conn = pyodbc.connect(
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER=localhost,{os.environ['SQLSERVER_PORT']};"
        "UID=sa;PWD=Provisa_2026!;Encrypt=yes;TrustServerCertificate=yes",
        autocommit=True,
    )
    try:
        cur = conn.cursor()
        for stmt in sql_statements:
            cur.execute(stmt)
    finally:
        conn.close()


@pytest.fixture
async def pool():
    _seed(
        [
            "IF OBJECT_ID('widgets', 'U') IS NOT NULL DROP TABLE widgets",
            "CREATE TABLE widgets (id INT PRIMARY KEY, name NVARCHAR(64))",
            "INSERT INTO widgets VALUES (1,'a'),(2,'b'),(3,'c')",
        ]
    )
    p = SourcePool()
    await p.add(
        source_id=_SID,
        source_type="sqlserver",
        host="localhost",
        port=int(os.environ["SQLSERVER_PORT"]),
        database="master",
        user="sa",
        password="Provisa_2026!",
    )
    try:
        yield p
    finally:
        await p.close_all()
        _seed(["IF OBJECT_ID('widgets', 'U') IS NOT NULL DROP TABLE widgets"])


async def test_sqlserver_source_reads_through_pool(pool):
    assert pool.dialect_for(_SID) == "tsql"
    result = await pool.execute(_SID, "SELECT id, name FROM widgets ORDER BY id")
    assert result.column_names == ["id", "name"]
    assert result.rows == [(1, "a"), (2, "b"), (3, "c")]
