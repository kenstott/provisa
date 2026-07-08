# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The event-substrate queue (REQ-940/941) — a transactional outbox + claim/lease work-queue over
the control-plane ``events`` + ``event_status`` tables.

Roles, all through this one API (portable SQLAlchemy Core — pg/sqlite; the fed engine is never
involved): INJECTORS ``post_event`` (in the SAME tx as the state change → atomic); a dispatcher
``fan_out`` to the dependents (from the SQLGlot lineage); TABLE PROCESSORS ``claim`` a target's
pending work, ``heartbeat`` the lease, ``complete`` it; a reaper ``reclaim_stale``; REPEATERS
``read_since`` by id cursor (fanout, never claim).

Claim granularity is the TARGET TABLE (not a lone event): one processor drains a table's pending
events in id order (coalesce at drain), so cross-table processors run in parallel while same-table
work is serialized — no concurrent write to one landed table, ordering preserved.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import insert, select, update

from provisa.core.schema_org import event_status, events

_VALID_EVENT = {"delta", "append", "replace", "warn", "error"}


async def post_event(
    conn: Any, *, source_table: str, event_type: str, payload: dict | None = None
) -> int:
    """Post one event about ``source_table`` and return its id. Called by an injector (or a table
    processor re-posting its own change) — enqueue in the SAME transaction as the state change so
    the outbox is atomic. ``event_type`` ∈ {delta, append, replace, warn, error}."""
    if event_type not in _VALID_EVENT:
        raise ValueError(
            f"invalid event_type {event_type!r}; expected one of {sorted(_VALID_EVENT)}"
        )
    result = await conn.execute_core(
        insert(events)
        .values(source_table=source_table, event_type=event_type, payload=payload or {})
        .returning(events.c.id)
    )
    return result.fetchone()[0]


async def fan_out(conn: Any, event_id: int, dependent_tables: list[str]) -> int:
    """Expand an event into one ``event_status`` work item per dependent table (the SQLGlot-derived
    lineage fanout). Idempotent — a retry does not double-insert (PK is (event_id, dependent_table);
    insert-if-absent via the dialect-agnostic upsert with no update columns). Returns the count."""
    for dep in dependent_tables:
        await conn.upsert(
            event_status,
            {"event_id": event_id, "dependent_table": dep, "claim_status": "unclaimed"},
            index_elements=["event_id", "dependent_table"],
            update_columns=[],  # DO NOTHING if the work item already exists
        )
    return len(dependent_tables)


async def claim(
    conn: Any, *, dependent_table: str, processor_name: str, now: datetime
) -> list[int]:
    """Claim ALL pending (unclaimed) work for ``dependent_table`` — the claim-by-target-table unit.
    Atomically flips them to ``claimed`` with the lease owner + heartbeat and returns the claimed
    event ids (in id order) for the processor to drain and coalesce. A concurrent claimant on the
    same table gets none (the row flip serializes)."""
    result = await conn.execute_core(
        update(event_status)
        .where(
            event_status.c.dependent_table == dependent_table,
            event_status.c.claim_status == "unclaimed",
        )
        .values(claim_status="claimed", processor_name=processor_name, heartbeat_at=now)
        .returning(event_status.c.event_id)
    )
    return sorted(r[0] for r in result.fetchall())


async def heartbeat(conn: Any, *, dependent_table: str, processor_name: str, now: datetime) -> None:
    """Refresh the lease on the claimed work this processor owns; a lapsed heartbeat lets the reaper
    reclaim it (so a crashed processor never orphans a table)."""
    await conn.execute_core(
        update(event_status)
        .where(
            event_status.c.dependent_table == dependent_table,
            event_status.c.processor_name == processor_name,
            event_status.c.claim_status == "claimed",
        )
        .values(heartbeat_at=now)
    )


async def complete(conn: Any, *, event_id: int, dependent_table: str, now: datetime) -> None:
    """Mark one work item completed (idempotent apply already happened). The processor completes the
    whole drained set, then re-posts its own change event for its dependents."""
    await conn.execute_core(
        update(event_status)
        .where(
            event_status.c.event_id == event_id,
            event_status.c.dependent_table == dependent_table,
        )
        .values(claim_status="completed", completed_at=now)
    )


async def reclaim_stale(conn: Any, *, older_than: datetime) -> int:
    """Revert claimed work whose lease lapsed (heartbeat < ``older_than``) back to unclaimed so any
    processor can re-claim it. At-least-once + idempotent landing = effectively-once. Returns count."""
    result = await conn.execute_core(
        update(event_status)
        .where(
            event_status.c.claim_status == "claimed",
            event_status.c.heartbeat_at < older_than,
        )
        .values(claim_status="unclaimed", processor_name=None, heartbeat_at=None)
        .returning(event_status.c.event_id)
    )
    return len(result.fetchall())


async def read_since(conn: Any, *, cursor: int, limit: int = 100) -> list[dict]:
    """Repeater fanout read: events with ``id > cursor`` in id order (each repeater tracks its own
    cursor and forwards to its SSE/Kafka subscribers — never a claim, so every repeater sees every
    event). Returns row dicts; the caller advances its cursor to the last id."""
    result = await conn.execute_core(
        select(events).where(events.c.id > cursor).order_by(events.c.id).limit(limit)
    )
    return [dict(r._mapping) for r in result.fetchall()]
