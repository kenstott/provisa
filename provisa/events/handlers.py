# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Concrete ``handle`` bodies for the table-processor variants (REQ-941).

A processor's ``handle`` turns its claimed events into the node's work and reports the node's own
change (event_type, payload) so the base loop can re-post it. These factories build those callables
against the real collaborators — the write face (``store_writer``) plus an injected row source
(a source loader, or the engine running the MV's SQL) — so the variants stay thin and the wiring is
testable end-to-end.

- ``make_source_land`` → ``SourceTableProcessor.land``: fetch the source's rows and land them via the
  write face; the node's event is the landing shape (append / replace).
- ``make_mv_generate`` → ``MVTableProcessor.generate``: the engine runs the MV's SQL, the result is
  landed via the write face (an MV is a full ``replace`` unless it is incrementally maintained).

The engine is never the writer — landing always goes through ``store_writer``; the engine only reads
(for an MV, it computes the SELECT). A very large MV is the separate MPP-native materialization path.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from provisa.core.change_signal import APPEND, select_landing_shape
from provisa.federation import store_writer

_SHAPE_TO_EVENT = {APPEND: "append"}  # replace is the fallback below; delta is the push/CDC path


def make_source_land(
    store_dsn: str,
    *,
    schema: str,
    table: str,
    columns: list[tuple[str, str]],
    change_signal: str,
    watermark_column: str | None,
    pk_columns: list[str] | None,
    fetch: Callable[[list[dict]], Awaitable[list[dict]]],
) -> Callable[[list[dict]], Awaitable[tuple[str, dict] | None]]:
    """Build ``SourceTableProcessor.land``: from the claimed events, ``fetch`` the source's current
    rows and land them through the write face (shape from change_signal). Returns (event_type,
    payload) so the base re-posts the node's change, or None when nothing landed (token-gate)."""

    async def land(pending: list[dict]) -> tuple[str, dict] | None:
        rows = await fetch(pending)
        if not rows:
            return None  # no delta to land → no downstream ripple
        loc = await store_writer.land(
            store_dsn,
            schema=schema,
            table=table,
            columns=columns,
            rows=rows,
            change_signal=change_signal,
            watermark_column=watermark_column,
            pk_columns=pk_columns,
        )
        shape = select_landing_shape(change_signal, watermark_column)
        return _SHAPE_TO_EVENT.get(shape, "replace"), {"rows": len(rows), "landed": loc}

    return land


def make_mv_generate(
    store_dsn: str,
    *,
    schema: str,
    table: str,
    columns: list[tuple[str, str]],
    run_query: Callable[[], Awaitable[list[dict]]],
) -> Callable[[list[dict]], Awaitable[tuple[str, dict] | None]]:
    """Build ``MVTableProcessor.generate``: the engine runs the MV's SQL (``run_query``) and the
    result is landed through the write face — a full ``replace``. The engine computes; the write
    face writes. Returns (event_type, payload) for the base to re-post."""

    async def generate(pending: list[dict]) -> tuple[str, dict] | None:
        del pending  # an MV refresh recomputes to current state; the claimed events are the trigger
        rows = await run_query()
        loc = await store_writer.land(
            store_dsn, schema=schema, table=table, columns=columns, rows=rows, change_signal="ttl"
        )
        return "replace", {"rows": len(rows), "landed": loc}

    return generate
