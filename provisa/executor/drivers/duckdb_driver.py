# Copyright (c) 2025 Kenneth Stott
# Canary: 82037dbb-0db6-4ad0-92bf-59b01efe826c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""DuckDB direct driver. Sync but in-process and fast — no pool needed."""

from __future__ import annotations

import asyncio
from functools import partial

import duckdb

from provisa.executor.drivers.base import DirectDriver
from provisa.executor.trino import QueryResult


class DuckDBDriver(DirectDriver):
    def __init__(self) -> None:
        self._conn: duckdb.DuckDBPyConnection | None = None
        self._database: str = ":memory:"

    async def connect(
        self, host: str, port: int, database: str,
        user: str, password: str, min_pool: int = 1, max_pool: int = 5,
    ) -> None:
        # DuckDB: database is a file path or :memory:
        self._database = database
        self._conn = duckdb.connect(database)

    async def execute(self, sql: str, params: list | None = None) -> QueryResult:
        # DuckDB uses $1, $2 natively (like PG)
        loop = asyncio.get_event_loop()

        def _run():
            if params:
                result = self._conn.execute(sql, params)
            else:
                result = self._conn.execute(sql)
            columns = [desc[0] for desc in result.description]
            rows = result.fetchall()
            return QueryResult(rows=rows, column_names=columns)

        return await loop.run_in_executor(None, _run)

    async def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    @property
    def is_connected(self) -> bool:
        return self._conn is not None
