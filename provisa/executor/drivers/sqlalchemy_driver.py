# Copyright (c) 2026 Kenneth Stott
# Canary: 5b1d9e73-7c48-4d75-8e02-2c7a0d4f9e55
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Generic SQLAlchemy fallback driver (REQ-229, REQ-550).

The native drivers (asyncpg/aiomysql/oracledb/pyodbc) cover a fixed set of source types. This
driver is the FALLBACK for any type that has a SQLAlchemy dialect but no bespoke async driver —
it broadens the writable/readable set to the whole SQLAlchemy dialect universe (intersected with
SQLGlot's write dialects, since mutations are transpiled before they reach a driver — see
executor/writable.py).

SQLAlchemy Core is synchronous, so every call is offloaded to a thread. Writes run inside
``engine.begin()`` so they COMMIT on success (a mutation reaching the store must be durable, never
rolled back when the connection returns to the pool). The PG-canonical ``$N`` placeholders the
compiler emits are converted to SQLAlchemy named binds, which SQLAlchemy then renders in the
target dialect's own paramstyle.
"""

from __future__ import annotations

import asyncio
from typing import Any

from provisa.executor.drivers.base import DirectDriver
from provisa.executor.trino import QueryResult


def _to_named_params(sql: str, params: list | None) -> tuple[str, dict[str, Any]]:
    """Convert ``$N`` positional placeholders to SQLAlchemy ``:pN`` named binds.

    Replaces highest index first so ``$1`` does not corrupt ``$10`` (same guard the native MySQL
    driver uses for its ``%s`` conversion). Returns the rewritten SQL and the bind dict.
    """
    if not params:
        return sql, {}
    out = sql
    bind: dict[str, Any] = {}
    for i in range(len(params), 0, -1):
        out = out.replace(f"${i}", f":p{i}")
        bind[f"p{i}"] = params[i - 1]
    return out, bind


class SQLAlchemyDriver(DirectDriver):  # REQ-229, REQ-550
    """A DirectDriver backed by a synchronous SQLAlchemy engine, offloaded to threads.

    ``drivername`` is a SQLAlchemy URL drivername (e.g. ``sqlite``, ``postgresql+psycopg2``,
    ``mssql+pyodbc``, ``redshift+redshift_connector``). The corresponding DBAPI package must be
    installed for the engine to connect.
    """

    def __init__(self, drivername: str) -> None:
        self._drivername = drivername
        self._engine: Any = None  # sqlalchemy.Engine (sync)

    async def connect(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        min_pool: int = 1,
        max_pool: int = 5,
    ) -> None:
        from sqlalchemy import create_engine
        from sqlalchemy.engine import URL

        url = URL.create(
            self._drivername,
            username=user or None,
            password=password or None,
            host=host or None,
            port=port or None,
            database=database or None,
        )
        loop = asyncio.get_running_loop()
        # Pooling args are dialect-specific (sqlite pools differ), so keep create_engine minimal and
        # let each dialect apply its own default pool. Engine construction is cheap but blocking.
        self._engine = await loop.run_in_executor(None, lambda: create_engine(url))

    async def execute(self, sql: str, params: list | None = None) -> QueryResult:
        named_sql, bind = _to_named_params(sql, params)
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._run, named_sql, bind)

    def _run(self, sql: str, bind: dict[str, Any]) -> QueryResult:
        from sqlalchemy import text

        engine = self._engine
        assert engine is not None
        with engine.begin() as conn:  # begin() commits on success, rolls back on error
            result = conn.execute(text(sql), bind)
            if result.returns_rows:
                rows = [tuple(row) for row in result.fetchall()]
                columns = list(result.keys())
            else:
                rows, columns = [], []  # a write with no RETURNING has no result set
            return QueryResult(rows=rows, column_names=columns)

    async def close(self) -> None:
        engine = self._engine
        if engine is not None:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, engine.dispose)
            self._engine = None

    @property
    def is_connected(self) -> bool:
        return self._engine is not None
