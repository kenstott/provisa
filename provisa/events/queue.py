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
pending work, ``heartbeat`` the lease, ``complete`` it; a reaper ``reclaim``; REPEATERS
``read_since`` by id cursor (fanout, never claim).

Claim granularity is the TARGET TABLE (not a lone event): one processor drains a table's pending
events in id order (coalesce at drain), so cross-table processors run in parallel while same-table
work is serialized — no concurrent write to one landed table, ordering preserved.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import and_, insert, or_, select, update

from provisa.core.schema_org import event_status, events, node_freshness_state

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
    conn: Any,
    *,
    dependent_table: str,
    processor_name: str,
    now: datetime,
    deadline: datetime | None = None,
) -> list[int]:
    """Claim ALL pending (unclaimed) work for ``dependent_table`` — the claim-by-target-table unit.
    Atomically flips them to ``claimed`` with the lease owner + heartbeat and returns the claimed
    event ids (in id order) for the processor to drain and coalesce. A concurrent claimant on the
    same table gets none (the row flip serializes). ``deadline`` (REQ-959) is the per-claim fire-by;
    a stuck-but-alive owner past deadline+grace is reclaimable even with a fresh heartbeat."""
    result = await conn.execute_core(
        update(event_status)
        .where(
            event_status.c.dependent_table == dependent_table,
            event_status.c.claim_status == "unclaimed",
        )
        .values(
            claim_status="claimed",
            processor_name=processor_name,
            heartbeat_at=now,
            deadline=deadline,
        )
        .returning(event_status.c.event_id)
    )
    return sorted(r[0] for r in result.fetchall())


async def resume_claims(conn: Any, *, dependent_table: str, processor_name: str) -> list[int]:
    """REQ-959 reassert-on-restart: the event ids this processor still owns (claim_status='claimed',
    processor_name = self) so a returning owner resumes its in-flight claim instead of waiting for a
    reaper. Zero rows = a peer already took over (reclaimed) → nothing to resume."""
    result = await conn.execute_core(
        select(event_status.c.event_id).where(
            event_status.c.dependent_table == dependent_table,
            event_status.c.processor_name == processor_name,
            event_status.c.claim_status == "claimed",
        )
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


async def complete(
    conn: Any, *, event_id: int, dependent_table: str, processor_name: str, now: datetime
) -> bool:
    """Mark one work item completed under the REQ-959 ownership CAS: the update matches only while
    THIS processor still owns the claim (processor_name = self, still 'claimed'). Returns True when it
    committed the completion, False when a peer had already taken the claim over (deadline/heartbeat
    reclaim) — the caller must then abort its ripple so no double effect commits.

    The processor completes the whole drained set; a False on ANY item means ownership was lost."""
    result = await conn.execute_core(
        update(event_status)
        .where(
            event_status.c.event_id == event_id,
            event_status.c.dependent_table == dependent_table,
            event_status.c.processor_name == processor_name,
            event_status.c.claim_status == "claimed",
        )
        .values(claim_status="completed", completed_at=now)
        .returning(event_status.c.event_id)
    )
    return result.fetchone() is not None


async def reclaim(
    conn: Any, *, now: datetime, heartbeat_cutoff: datetime, grace_seconds: float = 0.0
) -> int:
    """REQ-959 reclaim: revert claimed work to unclaimed when its owner is gone or stuck. Reclaimable =
    (heartbeat lapsed: heartbeat_at < ``heartbeat_cutoff``) OR (deadline+grace passed: deadline is set
    and deadline + grace < ``now``, catching a stuck-but-alive owner a heartbeat cannot). Any processor
    can then re-claim; at-least-once + idempotent land + the ownership CAS = effectively-once. Count."""
    from datetime import timedelta

    # deadline + grace < now  ⟺  deadline < now - grace (arithmetic in Python, not SQL, so it is
    # dialect-safe — column+interval does not translate uniformly across Postgres and SQLite).
    deadline_cutoff = now - timedelta(seconds=grace_seconds)
    result = await conn.execute_core(
        update(event_status)
        .where(
            event_status.c.claim_status == "claimed",
            or_(
                event_status.c.heartbeat_at < heartbeat_cutoff,
                and_(
                    event_status.c.deadline.is_not(None),
                    event_status.c.deadline < deadline_cutoff,
                ),
            ),
        )
        .values(claim_status="unclaimed", processor_name=None, heartbeat_at=None, deadline=None)
        .returning(event_status.c.event_id)
    )
    return len(result.fetchall())


async def get_events(conn: Any, event_ids: list[int]) -> list[dict]:
    """The event rows for ``event_ids`` in id order — a table processor fetches its claimed set to
    coalesce and hand to its handler (land / generate). Returns row dicts."""
    if not event_ids:
        return []
    result = await conn.execute_core(
        select(events).where(events.c.id.in_(event_ids)).order_by(events.c.id)
    )
    return [dict(r._mapping) for r in result.fetchall()]


async def get_node_state(conn: Any, node: str) -> dict | None:
    """The persisted freshness state for ``node`` — ``{content_hash, probe_token}`` — or None when the
    node has never landed. ``content_hash`` is the REQ-981 output-gate baseline; ``probe_token`` is the
    REQ-982 input-probe baseline. Both nullable independently (a node may have one and not the other)."""
    result = await conn.execute_core(
        select(node_freshness_state.c.content_hash, node_freshness_state.c.probe_token).where(
            node_freshness_state.c.node == node
        )
    )
    row = result.fetchone()
    if row is None:
        return None
    return {"content_hash": row[0], "probe_token": row[1]}


async def set_node_state(
    conn: Any, node: str, *, content_hash: str | None = None, probe_token: str | None = None
) -> None:
    """Upsert the freshness state for ``node``. Only the passed fields are written — an omitted
    (None-defaulted) field is left untouched on an existing row, so the content-hash gate (REQ-981) and
    the probe baseline (REQ-982) update independently without clobbering each other."""
    set_cols = {}
    if content_hash is not None:
        set_cols["content_hash"] = content_hash
    if probe_token is not None:
        set_cols["probe_token"] = probe_token
    if not set_cols:
        return
    await conn.upsert(
        node_freshness_state,
        {"node": node, **set_cols},
        index_elements=["node"],
        update_columns=list(set_cols),
    )


async def read_since(conn: Any, *, cursor: int, limit: int = 100) -> list[dict]:
    """Repeater fanout read: events with ``id > cursor`` in id order (each repeater tracks its own
    cursor and forwards to its SSE/Kafka subscribers — never a claim, so every repeater sees every
    event). Returns row dicts; the caller advances its cursor to the last id."""
    result = await conn.execute_core(
        select(events).where(events.c.id > cursor).order_by(events.c.id).limit(limit)
    )
    return [dict(r._mapping) for r in result.fetchall()]
