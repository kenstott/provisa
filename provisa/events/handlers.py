# Copyright (c) 2026 Kenneth Stott
# Canary: c186cb8f-4366-4eab-a930-c880aeb6cf09
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

from provisa.core.change_signal import APPEND, REPLACE
from provisa.events.content_hash import content_hash
from provisa.events.probes import WATERMARK, probe_shape
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
    probe_type: str = "none",
) -> Callable[..., Awaitable[tuple[str, dict, str | None] | None]]:
    """Build ``SourceTableProcessor.land``: from the claimed events, ``fetch`` the source's current
    rows and land them through the write face. REQ-982: the landing shape is the AUTHORITATIVE
    ``probe_shape(probe_type)`` (watermark → append, hash/count/none → replace), superseding the old
    ``watermark_column``-presence heuristic. Returns (event_type, payload, content_hash) so the base
    re-posts the node's change + persists the hash, or None when nothing landed or (replace) the
    content matches ``prior_hash`` (REQ-981 output gate)."""
    shape = probe_shape(probe_type)
    # REQ-982: watermark is the cursor probe → append requires the cursor column. Fail loud at build
    # time; never silently degrade an append node to a full replace because its cursor is missing.
    if probe_type == WATERMARK and watermark_column is None:
        raise ValueError(f"probe_type=watermark on {schema}.{table} requires a watermark_column")

    async def land(
        pending: list[dict],
        *,
        prior_hash: str | None = None,
        ctx: NodeContext | None = None,
        preprocess: Callable[..., Any] | None = None,
        forced: bool = False,
    ) -> tuple[str, dict, str | None] | None:
        rows = await fetch(pending)
        if not rows:
            return None  # no delta to land → no downstream ripple
        # REQ-957: preprocess after produce, before land. []→ row-level no-op (no land, no re-post).
        rows = await _apply_preprocess(preprocess, rows, ctx, columns)
        if not rows:
            return None
        # REQ-981 output gate: a replace whose content is byte-identical to the prior land neither
        # lands nor ripples. Append/CDC deltas are new rows by definition → no content hash. REQ-968:
        # a forced regen BYPASSES the gate — it re-lands and ripples regardless of an unchanged hash.
        digest: str | None = None
        if shape == REPLACE:
            digest = content_hash(rows, pk_columns)
            if not forced and digest == prior_hash:
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
            shape=shape,  # REQ-982: authoritative shape from probe_type
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
    persist: str = "replace",
    pk_columns: list[str] | None = None,
) -> Callable[..., Awaitable[tuple[str, dict, str | None] | None]]:
    """Build ``MVTableProcessor.generate``: the engine runs the MV's SQL (``run_query``) and the
    result is applied to the MV's OWN store table under the declared PERSISTENCE outcome (REQ-965:
    ``persist`` ∈ replace / append / upsert, via ``store_writer.persist_land``). The engine computes;
    the write face writes. Persistence is INDEPENDENT of the downstream emit set — this returns the
    primary ``replace`` change + content_hash for the base loop to gate (REQ-981) and, when an emit
    set is declared, re-shape into the per-consumer emit (REQ-965). Returns None when the recomputed
    output matches ``prior_hash`` (REQ-981 gate). ``persist`` is validated in ``persist_land``; an
    invalid outcome, or ``upsert`` without a PK, raises (never a silent replace)."""
    from provisa.events.outcomes import validate_persist

    validate_persist(persist)

    async def generate(
        pending: list[dict],
        *,
        prior_hash: str | None = None,
        ctx: NodeContext | None = None,
        preprocess: Callable[..., Any] | None = None,
        forced: bool = False,
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
        digest = content_hash(rows, pk_columns)  # PK-keyed hash when the derived table has identity
        # REQ-968: a forced regen recomputes + re-lands regardless of an unchanged hash (gate bypass).
        if not forced and digest == prior_hash:
            return None  # recomputed output identical → no land, no downstream ripple
        loc = await store_writer.persist_land(
            store_dsn,
            schema=schema,
            table=table,
            columns=columns,
            rows=rows,
            persist=persist,
            pk_columns=pk_columns,
        )
        return "replace", {"rows": len(rows), "landed": loc}, digest

    return generate


def _collect_delta_rows(pending: list[dict]) -> list[dict]:
    """REQ-969: the changed rows carried by the claimed delta/append events (``payload.delta`` — the
    demand-driven delta emission of REQ-965). Empty when no upstream carried a delta (e.g. a full
    ``replace`` input, or a probe token event) — the caller then documents a full recompute."""
    out: list[dict] = []
    for e in pending:
        if e["event_type"] in ("delta", "append"):
            rows = (e.get("payload") or {}).get("delta")
            if isinstance(rows, list):
                out.extend(rows)
    return out


def make_mv_incremental(
    store_dsn: str,
    *,
    schema: str,
    table: str,
    columns: list[tuple[str, str]],
    sql: str,
    run_query: Callable[[], Awaitable[list[dict]]],
    pk_columns: list[str],
    persist: str = "upsert",
) -> Callable[..., Awaitable[tuple[str, dict, str | None] | None]]:
    """REQ-969 (MAY): build an INCREMENTALLY-MAINTAINED ``MVTableProcessor.generate``. When an input
    arrives as a delta/append carrying its changed rows, apply ONLY those rows to the MV's prior
    landed state (``persist`` = upsert/append via ``apply_persistence``/``apply_cdc``) and emit the
    resulting delta — NO full SELECT. The cost axis that makes emit=delta (REQ-965) worth declaring.

    Feasibility is OPERATOR-DECLARED and checked at BUILD time (REQ-964: no silent downgrade): a PK is
    required for row identity, and the SQL must admit a safe incremental form (``is_incrementalizable``
    — a single-input bare-column projection). A declared-but-infeasible incremental MV is an EXPLICIT
    ERROR here, never a silent fall-back to full recompute (that would hide cost / emit a wrong delta).

    Per fire, when NO upstream delta is available (a full ``replace`` input, or a forced regen), the
    closure DOCUMENTS a full recompute (run the SELECT, re-land, emit the full set as a delta) — an
    explicit, commented fallback on the INPUT shape, distinct from the infeasible-declaration error."""
    from provisa.events.lineage import is_incrementalizable
    from provisa.events.outcomes import validate_persist

    validate_persist(persist)
    if not pk_columns:
        raise ValueError(
            "REQ-969: incremental maintenance requires a primary key for row identity — "
            "refusing a silent full-recompute downgrade"
        )
    if not is_incrementalizable(sql):
        raise ValueError(
            f"REQ-969: {schema}.{table} declares incremental but its SQL has no safe incremental "
            f"form (join/aggregate/filter/computed projection need delta-rule derivation, deferred) "
            f"— an infeasible incremental declaration is an explicit error, not a full recompute"
        )

    async def generate(
        pending: list[dict],
        *,
        prior_hash: str | None = None,
        ctx: NodeContext | None = None,
        preprocess: Callable[..., Any] | None = None,
        forced: bool = False,
    ) -> tuple[str, dict, str | None] | None:
        del ctx, preprocess
        delta_rows = _collect_delta_rows(pending)
        if delta_rows and not forced:
            loc = await store_writer.persist_land(
                store_dsn,
                schema=schema,
                table=table,
                columns=columns,
                rows=delta_rows,
                persist=persist,
                pk_columns=pk_columns,
            )
            # emit the applied delta; delta rows are new-by-definition → no content hash (REQ-965).
            return "delta", {"rows": len(delta_rows), "delta": delta_rows, "landed": loc}, None
        # DOCUMENTED full recompute (REQ-969): no upstream delta to apply → run the whole SELECT and
        # re-land. Explicit and gated (REQ-981), never a silent wrong delta.
        rows = await run_query()
        digest = content_hash(rows, pk_columns)
        if not forced and digest == prior_hash:
            return None
        loc = await store_writer.persist_land(
            store_dsn,
            schema=schema,
            table=table,
            columns=columns,
            rows=rows,
            persist=persist,
            pk_columns=pk_columns,
        )
        return "delta", {"rows": len(rows), "delta": rows, "landed": loc}, digest

    return generate
