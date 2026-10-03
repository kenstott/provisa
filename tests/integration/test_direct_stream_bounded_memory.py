# Copyright (c) 2026 Kenneth Stott
# Canary: f3002191-799b-4c58-b02e-80f3e6e815cc
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: the MySQL, SQL Server and Oracle source drivers read a large result in bounded
batches (REQ-1190, REQ-1915).

Each driver, against its real server on the test stack (one heavy service per test), streams a
table of ``_ROWS`` rows through ``open_stream`` a batch at a time. The memory the read allocates
(``tracemalloc`` peak: the driver's own Python objects) stays at about one batch whatever the
table's size, where the materialized read (``execute``) holds the whole result — the control that
shows the measurement discriminates.
"""

# Requirements: REQ-1190, REQ-1915

from __future__ import annotations

import os
import tracemalloc

import pytest

pytestmark = [pytest.mark.integration]

_ROWS = 200_000
_BATCH = 5_000
_TABLE = "provisa_stream_probe"
_LABEL = "x" * 80


async def _peak_of(read) -> tuple[int, int]:
    """(rows read, peak bytes allocated while reading)."""
    tracemalloc.start()
    try:
        rows = await read()
        return rows, tracemalloc.get_traced_memory()[1]
    finally:
        tracemalloc.stop()


async def _streamed(driver, sql: str) -> int:
    stream = await driver.open_stream(sql)
    rows = 0
    try:
        while batch := await stream.fetch(_BATCH):
            rows += len(batch)
    finally:
        await stream.close()
    return rows


async def _materialized(driver, sql: str) -> int:
    return len((await driver.execute(sql)).rows)


async def _assert_bounded(driver, table: str) -> None:
    whole = f"SELECT id, label FROM {table}"
    small = f"SELECT id, label FROM {table} WHERE id <= {_ROWS // 10}"
    rows, streamed_peak = await _peak_of(lambda: _streamed(driver, whole))
    assert rows == _ROWS
    rows_small, streamed_small = await _peak_of(lambda: _streamed(driver, small))
    assert rows_small == _ROWS // 10
    rows_all, materialized_peak = await _peak_of(lambda: _materialized(driver, whole))
    assert rows_all == _ROWS
    # Ten times the rows, about the same working memory: one batch.
    assert streamed_peak < streamed_small * 2, (streamed_small, streamed_peak)
    # The materialized read holds the result; the stream holds a batch.
    assert streamed_peak * 5 < materialized_peak, (streamed_peak, materialized_peak)
    # A stream read to its end gave its connection back: the pool serves the next statement.
    assert (await driver.execute(f"SELECT COUNT(*) FROM {table}")).rows[0][0] == _ROWS


@pytest.mark.requires_mariadb
async def test_the_mysql_driver_streams_a_large_result_in_bounded_batches():
    import pymysql

    from provisa.executor.drivers.mysql import MySQLDriver

    port = int(os.environ["MARIADB_PORT"])
    seed = pymysql.connect(
        host="localhost",
        port=port,
        user="root",
        password="provisa",
        database="provisa",
        autocommit=True,
    )
    try:
        with seed.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {_TABLE}")
            cur.execute(f"CREATE TABLE {_TABLE} (id INT PRIMARY KEY, label VARCHAR(100))")
            cur.execute(f"INSERT INTO {_TABLE} SELECT seq, '{_LABEL}' FROM seq_1_to_{_ROWS}")
        driver = MySQLDriver()
        await driver.connect("localhost", port, "provisa", "root", "provisa", max_pool=2)
        try:
            await _assert_bounded(driver, _TABLE)
        finally:
            await driver.close()
    finally:
        with seed.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {_TABLE}")
        seed.close()


@pytest.mark.requires_sqlserver
async def test_the_sql_server_driver_streams_a_large_result_in_bounded_batches():
    import pyodbc

    from provisa.executor.drivers.sqlserver import SQLServerDriver

    port = int(os.environ["SQLSERVER_PORT"])
    seed = pyodbc.connect(
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER=localhost,{port};DATABASE=master;UID=sa;PWD=Provisa_2026!;"
        "TrustServerCertificate=yes",
        autocommit=True,
    )
    try:
        cur = seed.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {_TABLE}")
        cur.execute(f"CREATE TABLE {_TABLE} (id INT PRIMARY KEY, label VARCHAR(100))")
        cur.execute(
            f"INSERT INTO {_TABLE} SELECT TOP ({_ROWS}) "
            "ROW_NUMBER() OVER (ORDER BY (SELECT NULL)), ? "
            "FROM sys.all_objects a CROSS JOIN sys.all_objects b",
            _LABEL,
        )
        driver = SQLServerDriver()
        await driver.connect("localhost", port, "master", "sa", "Provisa_2026!", max_pool=2)
        try:
            await _assert_bounded(driver, _TABLE)
        finally:
            await driver.close()
    finally:
        seed.cursor().execute(f"DROP TABLE IF EXISTS {_TABLE}")
        seed.close()


@pytest.mark.requires_oracle
async def test_the_oracle_driver_streams_a_large_result_in_bounded_batches():
    import oracledb  # pyright: ignore[reportMissingImports]

    from provisa.executor.drivers.oracle import OracleDriver

    port = int(os.environ["ORACLE_PORT"])
    dsn = oracledb.makedsn("localhost", port, service_name="FREEPDB1")
    seed = oracledb.connect(user="system", password="provisa", dsn=dsn)
    seed.autocommit = True
    drop = (
        f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {_TABLE}'; EXCEPTION WHEN OTHERS THEN "
        "IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )
    try:
        cur = seed.cursor()
        cur.execute(drop)
        cur.execute(f"CREATE TABLE {_TABLE} (id NUMBER PRIMARY KEY, label VARCHAR2(100))")
        cur.execute(
            f"INSERT INTO {_TABLE} SELECT LEVEL, :label FROM dual CONNECT BY LEVEL <= {_ROWS}",
            label=_LABEL,
        )
        driver = OracleDriver()
        await driver.connect("localhost", port, "FREEPDB1", "system", "provisa", max_pool=2)
        try:
            await _assert_bounded(driver, _TABLE)
        finally:
            await driver.close()
    finally:
        seed.cursor().execute(drop)
        seed.close()
