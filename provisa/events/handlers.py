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

import inspect
from collections.abc import Awaitable, Callable
from typing import Any

from provisa.core.change_signal import APPEND, REPLACE, select_landing_shape
from provisa.events.content_hash import content_hash
from provisa.events.processor import NodeContext, PreprocessError
from provisa.federation import store_writer

_SHAPE_TO_EVENT = {APPEND: "append"}  # replace is the fallback below; delta is the push/CDC path


async def _apply_preprocess(
    preprocess: Callable[..., Any] | None,
    rows: list[dict],
    ctx: NodeContext | None,
    columns: list[tuple[str, str]],
) -> list[dict]:
    """REQ-957: run the node's ``preprocess(rows, ctx)`` hook between produce and land. None = identity.
    The hook may be sync or async. It feeds the content hash (must be deterministic), so it runs before
    the digest is computed. A raise is a FATAL data outcome → re-raised as PreprocessError (the loop
    turns it into an error event + poison fan-out); it is NOT a silent swallow."""
    if preprocess is None:
        return rows
    if ctx is not None:
        ctx.columns = columns  # enrich the envelope with the landing schema the closure knows
    try:
        out = preprocess(rows, ctx)
        if inspect.isawaitable(out):
            out = await out
    except PreprocessError:
        raise
    except Exception as exc:  # noqa: BLE001 — REQ-957: a user-hook raise is a fatal DATA outcome
        # (error event + poison fan-out), not an infra crash. Fail loud INTO the event vocabulary
        # via PreprocessError; the loop distinguishes it from a genuine crash (which must propagate).
        raise PreprocessError(str(exc)) from exc
    return out


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
) -> Callable[..., Awaitable[tuple[str, dict, str | None] | None]]:
    """Build ``SourceTableProcessor.land``: from the claimed events, ``fetch`` the source's current
    rows and land them through the write face (shape from change_signal). Returns (event_type, payload,
    content_hash) so the base re-posts the node's change + persists the hash, or None when nothing
    landed or (replace) the content matches ``prior_hash`` (REQ-981 output gate)."""

    async def land(
        pending: list[dict],
        *,
        prior_hash: str | None = None,
        ctx: NodeContext | None = None,
        preprocess: Callable[..., Any] | None = None,
    ) -> tuple[str, dict, str | None] | None:
        rows = await fetch(pending)
        if not rows:
            return None  # no delta to land → no downstream ripple
        # REQ-957: preprocess after produce, before land. []→ row-level no-op (no land, no re-post).
        rows = await _apply_preprocess(preprocess, rows, ctx, columns)
        if not rows:
            return None
        shape = select_landing_shape(change_signal, watermark_column)
        # REQ-981 output gate: a replace whose content is byte-identical to the prior land neither
        # lands nor ripples. Append/CDC deltas are new rows by definition → no content hash.
        digest: str | None = None
        if shape == REPLACE:
            digest = content_hash(rows, pk_columns)
            if digest == prior_hash:
                return None
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
        return _SHAPE_TO_EVENT.get(shape, "replace"), {"rows": len(rows), "landed": loc}, digest

    return land


def make_mv_generate(
    store_dsn: str,
    *,
    schema: str,
    table: str,
    columns: list[tuple[str, str]],
    run_query: Callable[[], Awaitable[list[dict]]],
) -> Callable[..., Awaitable[tuple[str, dict, str | None] | None]]:
    """Build ``MVTableProcessor.generate``: the engine runs the MV's SQL (``run_query``) and the
    result is landed through the write face — a full ``replace``. The engine computes; the write
    face writes. Returns (event_type, payload, content_hash) for the base to re-post + persist, or
    None when the recomputed output matches ``prior_hash`` (REQ-981 gate — an unchanged MV does not
    ripple its dependents)."""

    async def generate(
        pending: list[dict],
        *,
        prior_hash: str | None = None,
        ctx: NodeContext | None = None,
        preprocess: Callable[..., Any] | None = None,
    ) -> tuple[str, dict, str | None] | None:
        del pending  # an MV refresh recomputes to current state; the claimed events are the trigger
        rows = await run_query()
        # REQ-957: preprocess after the MV SQL, before land. []→ row-level no-op (no land, no
        # re-post) — but ONLY when the hook returned it; an empty MV recompute with no hook still
        # lands an empty replace (a legitimately-emptied MV clears its table).
        if preprocess is not None:
            rows = await _apply_preprocess(preprocess, rows, ctx, columns)
            if not rows:
                return None
        digest = content_hash(rows)  # MVs have no PK → hash over each row's canonical form
        if digest == prior_hash:
            return None  # recomputed output identical → no land, no downstream ripple
        loc = await store_writer.land(
            store_dsn, schema=schema, table=table, columns=columns, rows=rows, change_signal="ttl"
        )
        return "replace", {"rows": len(rows), "landed": loc}, digest

    return generate
