# Copyright (c) 2026 Kenneth Stott
# Canary: 730a0934-e5a1-4bed-9a14-c8f6af625a18
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: every source family's zoned timestamp is read as the instant it is (REQ-2050).

A value written at a +02:00 offset (or, for MySQL's TIMESTAMP, written in a +02:00 session) is
read back through the direct SourcePool, the reader every landing and direct route uses, as the
same instant in UTC with its zone marked: Postgres timestamptz, Oracle TIMESTAMP WITH TIME ZONE
and WITH LOCAL TIME ZONE, SQL Server datetimeoffset, MariaDB (the MySQL family) TIMESTAMP. The
harness provisions each source's container from docker-compose.test.yml."""

# Requirements: REQ-2050

from __future__ import annotations

import os
from datetime import datetime, timezone

import pytest

from provisa.executor.pool import SourcePool

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_INSTANT = datetime(2024, 1, 2, 1, 4, 5, tzinfo=timezone.utc)  # 03:04:05 at +02:00


async def _read(source_type: str, port: int, database: str, user: str, password: str, sql: str):
    pool = SourcePool()
    await pool.add(
        source_id=f"zoned-{source_type}",
        source_type=source_type,
        host="localhost",
        port=port,
        database=database,
        user=user,
        password=password,
    )
    try:
        return (await pool.execute(f"zoned-{source_type}", sql)).rows
    finally:
        await pool.close_all()


async def test_postgres_timestamptz():
    rows = await _read(
        "postgresql",
        int(os.environ.get("PG_PORT", "5432")),
        "provisa",
        "provisa",
        os.environ.get("PG_PASSWORD", "provisa"),
        "SELECT TIMESTAMPTZ '2024-01-02 03:04:05+02:00'",
    )
    assert rows == [(_INSTANT,)]


@pytest.mark.requires_oracle
async def test_oracle_timestamp_with_time_zone_and_local_time_zone():
    rows = await _read(
        "oracle",
        int(os.environ["ORACLE_PORT"]),
        "FREEPDB1",
        "system",
        "provisa",
        "SELECT TIMESTAMP '2024-01-02 03:04:05 +02:00', "
        "CAST(TIMESTAMP '2024-01-02 03:04:05 +02:00' AS TIMESTAMP WITH LOCAL TIME ZONE) FROM dual",
    )
    assert rows == [(_INSTANT, _INSTANT)]


@pytest.mark.requires_sqlserver
async def test_sql_server_datetimeoffset():
    rows = await _read(
        "sqlserver",
        int(os.environ["SQLSERVER_PORT"]),
        "master",
        "sa",
        "Provisa_2026!",
        "SELECT CAST('2024-01-02 03:04:05 +02:00' AS DATETIMEOFFSET)",
    )
    assert rows == [(_INSTANT,)]


@pytest.mark.requires_mariadb
async def test_mysql_family_timestamp():
    import aiomysql

    conn = await aiomysql.connect(
        host="localhost",
        port=int(os.environ["MARIADB_PORT"]),
        user="root",
        password="provisa",
        db="provisa",
        autocommit=True,
    )
    try:
        async with conn.cursor() as cur:
            await cur.execute("DROP TABLE IF EXISTS zoned_ts")
            await cur.execute("CREATE TABLE zoned_ts (ts TIMESTAMP NULL)")
            # Written in a +02:00 session: MySQL stores the instant in UTC.
            await cur.execute("SET time_zone = '+02:00'")
            await cur.execute("INSERT INTO zoned_ts VALUES ('2024-01-02 03:04:05')")
    finally:
        conn.close()
    rows = await _read(
        "mariadb",
        int(os.environ["MARIADB_PORT"]),
        "provisa",
        "root",
        "provisa",
        "SELECT ts FROM zoned_ts",
    )
    # The reader's session is UTC: the instant comes back as UTC, with its zone.
    assert rows == [(_INSTANT,)]
