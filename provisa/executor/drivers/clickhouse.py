# Copyright (c) 2026 Kenneth Stott
# Canary: 16f749bb-b448-49a1-ba7b-6f679817be81
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""ClickHouse direct source driver (REQ-986).

Makes a ClickHouse server a first-class NAMED SOURCE reachable on ANY engine: Provisa reads it
directly (HTTP via clickhouse-connect) then lands a replica. The same client family the ClickHouse
federation engine uses. Port defaults to the HTTP interface (8123); ``secure`` (TLS) is an optional
``federation_hints`` flag. Reads use ClickHouse's native columnar output (REQ-986).
"""

from __future__ import annotations

import asyncio
from typing import Any

from provisa.executor.drivers.base import DirectDriver
from provisa.executor.result import QueryResult


class ClickHouseDriver(DirectDriver):  # REQ-986
    def __init__(self) -> None:
        self._client: Any = None
        self._extra: dict[str, str] = {}

    def configure(self, extra: dict[str, str]) -> None:
        """Optional ``secure`` (TLS 'true'/'false') from federation_hints."""
        self._extra = dict(extra)

    async def connect(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        min_pool: int = 1,  # pyright: ignore[reportUnusedParameter]
        max_pool: int = 5,  # pyright: ignore[reportUnusedParameter]
    ) -> None:
        import clickhouse_connect

        secure = self._extra.get("secure", "").lower() in ("1", "true", "yes")

        def _open() -> Any:
            return clickhouse_connect.get_client(
                host=host,
                port=port or (8443 if secure else 8123),
                username=user or "default",
                password=password or "",
                database=database or "default",
                secure=secure,
            )

        self._client = await asyncio.to_thread(_open)

    async def execute(self, sql: str, params: list | None = None) -> QueryResult:
        # Confirmed live (federated_join via GraphQL against a real ClickHouse server): the
        # governed pipeline's `@N` positional placeholders reach here UNSUBSTITUTED for a
        # cross-engine push-down, the same shape trino.py/trino_flight.py already handle for
        # Trino — "arrives fully formed" only held for the single-source path this driver was
        # first written against. `@N` -> ClickHouse's own `%(pN)s` dict-substitution placeholder
        # (clickhouse_connect.driver.binding.finalize_query does real value escaping, not raw
        # string interpolation); see substitute_positional_placeholders for why the reverse-order
        # replacement is centralized but the placeholder spelling isn't.
        from provisa.compiler.params import (
            extract_params_comment,
            substitute_positional_placeholders,
        )

        exec_sql, embedded = extract_params_comment(sql)
        effective_params = params if params is not None else embedded
        ch_params: dict[str, Any] | None = None
        if effective_params:
            exec_sql = substitute_positional_placeholders(
                exec_sql, effective_params, lambda i: f"%(p{i})s"
            )
            ch_params = {f"p{i}": v for i, v in enumerate(effective_params, start=1)}

        def _run() -> QueryResult:
            res = self._client.query(exec_sql, parameters=ch_params)
            return QueryResult(
                rows=[tuple(r) for r in res.result_rows], column_names=list(res.column_names)
            )

        return await asyncio.to_thread(_run)

    async def close(self) -> None:
        if self._client is not None:
            await asyncio.to_thread(self._client.close)
            self._client = None

    @property
    def is_connected(self) -> bool:
        return self._client is not None
