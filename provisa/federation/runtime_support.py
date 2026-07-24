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
from collections.abc import Iterator
from typing import Any, Callable

from provisa.executor.result import (
    QueryResult,
    ResultStream,
    StreamingQueryResult,
    StreamStats,
)

# Rows pulled per fetchmany when streaming a DBAPI cursor. Bounds the in-memory
# working set to one batch instead of the whole result (REQ-028).
_STREAM_BATCH_ROWS = 1000


def result_from_dbapi(obj: Any) -> QueryResult:
    """Build a QueryResult from a DBAPI cursor or result object (anything exposing
    ``.description`` + ``.fetchall()``). A ``None`` description (non-SELECT) yields no
    columns and no rows. Used by the pg / sqlalchemy / duckdb runtimes; clickhouse
    delegates to its own ``_backend.query`` and does not use this.

    Fully materializes. Use :func:`stream_from_dbapi` where the cursor's fetch state
    outlives this call and the caller is on a worker thread — but NOT for drivers that
    close the cursor before the result is drained, nor across the async ``run_async``
    boundary (a blocking ``fetchmany`` must not be pulled on the event-loop thread)."""
    cols = [d[0] for d in obj.description] if obj.description else []
    rows = obj.fetchall() if obj.description else []
    return QueryResult(rows=rows, column_names=cols)


def stream_from_dbapi(
    obj: Any,
    *,
    on_close: Callable[[StreamStats], None] | None = None,
) -> ResultStream:
    """Build a lazily-streamed result from a DBAPI cursor/result whose fetch state OUTLIVES
    this call. Rows are pulled in batches of ``_STREAM_BATCH_ROWS`` via ``fetchmany`` so a
    large result never fully materializes.

    Preconditions the caller MUST guarantee: the cursor stays open until the stream drains,
    and no other query runs on it meanwhile (hold a private cursor). ``on_close`` fires once
    at drain — the DuckDB terminal uses it to close the private cursor. Drivers that close
    the cursor in a ``finally`` or share one cursor across concurrent queries must use
    :func:`result_from_dbapi` instead. A ``None`` description (non-SELECT) yields an empty
    materialized result and fires ``on_close`` immediately."""
    if not obj.description:
        if on_close is not None:
            on_close(StreamStats(done=True))
        return QueryResult(rows=[], column_names=[])
    cols = [d[0] for d in obj.description]

    def _batches() -> Iterator[list[tuple]]:
        while True:
            chunk = obj.fetchmany(_STREAM_BATCH_ROWS)
            if not chunk:
                return
            yield chunk

    return StreamingQueryResult(_batches(), column_names=cols, on_close=on_close)


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
