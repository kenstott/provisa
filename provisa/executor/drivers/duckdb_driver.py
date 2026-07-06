# Copyright (c) 2026 Kenneth Stott
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

import duckdb

from provisa.executor.drivers.base import DirectDriver
from provisa.executor.result import QueryResult

# Requirements: REQ-027, REQ-068


class DuckDBDriver(DirectDriver):  # REQ-027, REQ-068
    def __init__(self) -> None:
        self._conn: duckdb.DuckDBPyConnection | None = None
        self._database: str = ":memory:"

    async def connect(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        host: str,  # pyright: ignore[reportUnusedParameter]
        port: int,  # pyright: ignore[reportUnusedParameter]
        database: str,
        user: str,  # pyright: ignore[reportUnusedParameter]
        password: str,  # pyright: ignore[reportUnusedParameter]
        min_pool: int = 1,  # pyright: ignore[reportUnusedParameter]
        max_pool: int = 5,  # pyright: ignore[reportUnusedParameter]
    ) -> None:
        # DuckDB: database is a file path or :memory:
        self._database = database
        self._conn = duckdb.connect(database)

    async def execute(self, sql: str, params: list | None = None) -> QueryResult:
        # DuckDB uses $1, $2 natively (like PG)
        loop = asyncio.get_event_loop()

        def _run():
            assert self._conn is not None
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
