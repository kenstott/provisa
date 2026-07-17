# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: Oracle as a named source read through the direct SourcePool (REQ-1097).

The harness auto-provisions the oracle container (requires_oracle marker →
docker-compose.test.yml, gvenzl/oracle-free:slim). Seeds a table, reads it
back through the pool via the oracledb-based direct driver.
"""

from __future__ import annotations

import os

import oracledb  # pyright: ignore[reportMissingImports]
import pytest

from provisa.executor.pool import SourcePool

pytestmark = [pytest.mark.integration, pytest.mark.requires_oracle, pytest.mark.asyncio]

_SID = "e2e-oracle"
_SERVICE_NAME = "FREEPDB1"


def _seed(sql_statements: list[str]) -> None:
    # Seed via a direct autocommit connection — the read driver never commits (read path).
    conn = oracledb.connect(
        user="system",
        password="provisa",
        dsn=oracledb.makedsn("localhost", int(os.environ["ORACLE_PORT"]), service_name=_SERVICE_NAME),
    )
    conn.autocommit = True
    try:
        cur = conn.cursor()
        try:
            for stmt in sql_statements:
                cur.execute(stmt)
        finally:
            cur.close()
    finally:
        conn.close()


def _drop_widgets() -> None:
    # Oracle has no DROP TABLE IF EXISTS — ignore ORA-00942 (table does not exist).
    conn = oracledb.connect(
        user="system",
        password="provisa",
        dsn=oracledb.makedsn("localhost", int(os.environ["ORACLE_PORT"]), service_name=_SERVICE_NAME),
    )
    conn.autocommit = True
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                BEGIN
                    EXECUTE IMMEDIATE 'DROP TABLE widgets';
                EXCEPTION
                    WHEN OTHERS THEN
                        IF SQLCODE != -942 THEN
                            RAISE;
                        END IF;
                END;
                """
            )
        finally:
            cur.close()
    finally:
        conn.close()


@pytest.fixture
async def pool():
    _drop_widgets()
    _seed(
        [
            "CREATE TABLE widgets (id NUMBER PRIMARY KEY, name VARCHAR2(64))",
            "INSERT INTO widgets VALUES (1,'a')",
            "INSERT INTO widgets VALUES (2,'b')",
            "INSERT INTO widgets VALUES (3,'c')",
        ]
    )
    p = SourcePool()
    await p.add(
        source_id=_SID,
        source_type="oracle",
        host="localhost",
        port=int(os.environ["ORACLE_PORT"]),
        database=_SERVICE_NAME,
        user="system",
        password="provisa",
    )
    try:
        yield p
    finally:
        await p.close_all()
        _drop_widgets()


async def test_oracle_source_reads_through_pool(pool):
    assert pool.dialect_for(_SID) == "oracle"
    result = await pool.execute(_SID, "SELECT id, name FROM widgets ORDER BY id")
    assert result.column_names == ["id", "name"]
    assert result.rows == [(1, "a"), (2, "b"), (3, "c")]
