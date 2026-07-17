# Copyright (c) 2026 Kenneth Stott
# Canary: 61356214-20aa-4ad2-a541-783335fa5233
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Shared helpers for the federation runtimes (duckdb / clickhouse / pg / sqlalchemy).

These are behaviorless — the four ``*FederationRuntime`` classes share no state or
lifecycle (each owns a different connection object), so their common logic lives here
as free functions the concretes call, not in a base class."""

from __future__ import annotations

import asyncio
from typing import Any, Callable

from provisa.executor.result import QueryResult


def result_from_dbapi(obj: Any) -> QueryResult:
    """Build a QueryResult from a DBAPI cursor or result object (anything exposing
    ``.description`` + ``.fetchall()``). A ``None`` description (non-SELECT) yields no
    columns and no rows. Used by the pg / sqlalchemy / duckdb runtimes; clickhouse
    delegates to its own ``_backend.query`` and does not use this."""
    cols = [d[0] for d in obj.description] if obj.description else []
    rows = obj.fetchall() if obj.description else []
    return QueryResult(rows=rows, column_names=cols)


def columns_from_describe(rows: Any) -> dict[str, str]:
    """Map a DESCRIBE result's ``(name, type, ...)`` rows to ``{name: type_lower}``,
    the engine-introspection shape shared by the duckdb and clickhouse runtimes."""
    return {row[0]: str(row[1]).lower() for row in rows}


async def run_async(
    run_sync: Callable[[str, list | None], QueryResult],
    sql: str,
    params: list | None = None,
) -> QueryResult:
    """Run a runtime's synchronous ``run_sync`` on the default executor. The shared
    async wrapper for runtimes whose driver is blocking (pg / sqlalchemy / clickhouse)."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, lambda: run_sync(sql, params))
