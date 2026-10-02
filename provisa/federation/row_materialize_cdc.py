# Copyright (c) 2026 Kenneth Stott
# Canary: ef7c0e12-c744-420b-b297-89505507bd69
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""``apply_cdc_events``'s row-materialize branch (REQ-1865, design doc section 5).

For a ``row_materialize`` table, an incoming CDC batch is NOT applied via the ordinary
``materialize_exec.apply_cdc`` upsert-every-event path — that would eagerly insert rows nothing has
queried yet, which constraint 2 forbids. Instead: filter events to PKs ALREADY present in the row
cache (a bounded existence check), and for exactly those already-cached keys post one
``row_refresh`` event (payload ``{"keys": [...]}``) to the event-substrate outbox
(``provisa.events.queue``). The actual re-fetch never runs inline here — it runs later, out-of-band,
through ``provisa.events.row_materialize_lifecycle.process_row_refresh_events`` (section 6b: the
fetch is a real network round trip to the live source and must not sit on the CDC ingestion path).
An event for a key NOT already cached is dropped before any post — never triggers a fetch, never
inserted (constraint 2: never eager, never a background prefetch of a key nothing has asked for)."""

from __future__ import annotations

from typing import Any


def row_refresh_claim_node(node: str) -> str:
    """The ``event_status.dependent_table`` claim key a ``row_refresh`` event fans out to --
    DELIBERATELY NOT the bare physical node string.

    A row_materialize table's own ordinary whole-table federation strategy can independently
    resolve to MATERIALIZED (design doc section 3a: row_materialize does not suppress the
    is_stale / replica-build fallback), in which case ``provisa.events.boot.
    build_source_node_spec`` registers a plain ``SourceTableProcessor`` under the EXACT SAME
    node string this module uses (``f"{schema_name}.{table_name}"``, the same convention
    ``land_lock``/``ensure_resident`` key on). ``queue.claim`` claims ALL pending unclaimed work
    for a ``dependent_table`` regardless of ``event_type`` -- it has no event-type filter. If a
    ``row_refresh`` event were fanned to the bare node, whichever processor's tick fires first
    (the ordinary source processor or this mechanism's background drain) claims it indiscriminately:
    the ordinary processor's ``handle()`` has no notion of a ``{"keys": [...]}`` payload and would
    either drop it or misinterpret it as a plain refresh trigger, silently completing the claim
    without ever calling ``ensure_rows_resident`` -- and conversely this mechanism's own claim could
    steal an ordinary ``delta``/``replace`` event meant for the whole-table path, skipping a real
    refresh cycle. Fanning to a NAMESPACED pseudo-node (this function's output) instead of the bare
    node gives row_refresh its own claim lane that can never collide with the table's ordinary
    processor, while ``post_event``'s own ``source_table=node`` (the event's provenance, not its
    claim key) still names the real table for audit/debugging."""
    return f"{node}::row_refresh"


def _event_keys(events: list, pk_columns: list[str]) -> list[tuple[Any, ...]]:
    return [tuple(getattr(ev, "row", {}).get(pk) for pk in pk_columns) for ev in events]


async def handle_row_materialize_cdc(
    state: Any,
    *,
    schema: str,
    table: str,
    pk_columns: list[str],
    events: list,
    node: str,
) -> dict[str, int]:
    """Filter ``events`` to already-cached PKs and post one ``row_refresh`` event naming them.

    Returns ``{"cached": n_already_cached, "dropped": n_not_cached}`` — never ``{"upsert",
    "delete"}`` counts, because nothing is written to the row cache here; the write happens later,
    when the background processor calls ``ensure_rows_resident(force=True)``."""
    db = getattr(state, "tenant_db", None)
    engine = getattr(state, "federation_engine", None)
    if db is None or engine is None:
        return {"cached": 0, "dropped": len(events)}

    keys = _event_keys(events, pk_columns)
    if not keys:
        return {"cached": 0, "dropped": 0}

    from sqlalchemy import column
    from sqlalchemy import table as sa_table
    from sqlalchemy import select, tuple_

    from provisa.federation import store_writer

    # A lightweight (unreflected) table() construct binds these columns to a FROM clause without
    # requiring the row cache's full DDL shape here -- this is a read-only existence check, not a
    # DDL-aware caller (build_row_cache_table's real column types are irrelevant to it).
    cache_table = sa_table(table, *[column(c) for c in pk_columns], schema=schema or None)
    cache_cols = [cache_table.c[c] for c in pk_columns]
    pk_cond = (
        cache_cols[0].in_([k[0] for k in keys])
        if len(cache_cols) == 1
        else tuple_(*cache_cols).in_(keys)
    )
    async with store_writer.store_connection(engine.engine.materialize_store()) as conn:
        result = await conn.execute_core(select(*cache_cols).where(pk_cond))
        already_cached = {tuple(row) for row in result.fetchall()}

    if not already_cached:
        return {"cached": 0, "dropped": len(keys)}

    from provisa.events import queue

    async with db.acquire() as conn:
        event_id = await queue.post_event(
            conn,
            source_table=node,
            event_type="row_refresh",
            payload={"keys": [list(k) for k in already_cached]},
        )
        # Fan to the NAMESPACED pseudo-node (row_refresh_claim_node's own docstring explains why
        # the bare node would collide with this table's ordinary whole-table processor's claim).
        await queue.fan_out(conn, event_id, [row_refresh_claim_node(node)])

    return {"cached": len(already_cached), "dropped": len(keys) - len(already_cached)}
