# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Snowflake direct source driver (REQ-988).

Makes Snowflake a first-class NAMED SOURCE reachable on ANY engine: Provisa reads it directly then
lands a replica. Same snowflake-connector-python connection the Snowflake federation engine uses.
``account``/``warehouse``/``role`` (which host/port/user/password can't carry) come from
``Source.federation_hints`` via ``configure``. The driver imports lazily, so this module loads even
where snowflake-connector-python is not installed (REQ-988).
"""

from __future__ import annotations

import asyncio
from typing import Any

from provisa.executor.drivers.base import DirectDriver
from provisa.executor.result import QueryResult


class SnowflakeDriver(DirectDriver):  # REQ-988
    def __init__(self) -> None:
        self._conn: Any = None
        self._extra: dict[str, str] = {}

    def configure(self, extra: dict[str, str]) -> None:
        """``account`` is required; ``warehouse``/``role`` optional (from federation_hints)."""
        self._extra = dict(extra)

    async def connect(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        host: str,  # pyright: ignore[reportUnusedParameter]  # account comes via federation_hints
        port: int,  # pyright: ignore[reportUnusedParameter]
        database: str,
        user: str,
        password: str,
        min_pool: int = 1,  # pyright: ignore[reportUnusedParameter]
        max_pool: int = 5,  # pyright: ignore[reportUnusedParameter]
    ) -> None:
        account = self._extra.get("account") or host
        if not account:
            raise ValueError("snowflake source requires 'account' in federation_hints")
        import snowflake.connector as sf

        def _open() -> Any:
            return sf.connect(
                account=account,
                user=user,
                password=password,
                database=database or None,
                schema=self._extra.get("schema"),
                warehouse=self._extra.get("warehouse"),
                role=self._extra.get("role"),
            )

        self._conn = await asyncio.to_thread(_open)

    async def execute(self, sql: str, params: list | None = None) -> QueryResult:
        def _run() -> QueryResult:
            cur = self._conn.cursor()
            try:
                cur.execute(sql, params or None)
                cols = [d[0] for d in cur.description] if cur.description else []
                rows = cur.fetchall() if cur.description else []
                return QueryResult(rows=[tuple(r) for r in rows], column_names=cols)
            finally:
                cur.close()

        return await asyncio.to_thread(_run)

    async def close(self) -> None:
        if self._conn is not None:
            await asyncio.to_thread(self._conn.close)
            self._conn = None

    @property
    def is_connected(self) -> bool:
        return self._conn is not None
