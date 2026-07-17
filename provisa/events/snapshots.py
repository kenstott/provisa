# Copyright (c) 2026 Kenneth Stott
# Canary: f03a4e99-2ece-4325-8b01-c83e539f7c01
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Preserved snapshots (REQ-983) — the one point-in-time form that departs from the NRT ideal.

A PRESERVED snapshot is a dataset MATERIALIZED-AND-SEALED because it is NOT reconstructible from
current state + retained event history — a genuine departure, unlike the reconstructible PIT of
REQ-958/965/967 (an accumulating append emit + a read-time as-of filter, which stays inside the
ideal). Because it cannot be rebuilt, it is FROZEN at seal time and never recomputed.

Two invariants, both fail-loud:

1. DECLARED + WHY-TAGGED — a snapshot is never inferred; it is declared with a ``reason`` (WHY the
   data cannot be reconstructed). Sealing without a reason is an explicit error (REQ-983).
2. IMMUTABLE — once sealed, a snapshot is point-in-time frozen. A second seal of the same name is
   refused (the row in ``preserved_snapshots`` IS the immutability record) — never a silent
   overwrite of a preserved dataset.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy import select

from provisa.core.schema_org import preserved_snapshots
from provisa.events.content_hash import content_hash
from provisa.federation import store_writer


@dataclass(frozen=True)
class SealedSnapshot:
    """A sealed preserved snapshot (REQ-983): its identity, mandatory why-tag, frozen store location,
    content digest, and the optional calendar-addressable period it froze."""

    name: str
    reason: str
    location: str
    content_hash: str
    window_id: str | None = None


async def get_snapshot(conn: Any, name: str) -> SealedSnapshot | None:
    """The sealed snapshot ``name``, or None when it has never been sealed. A present row means the
    snapshot is IMMUTABLE (REQ-983)."""
    result = await conn.execute_core(
        select(
            preserved_snapshots.c.name,
            preserved_snapshots.c.reason,
            preserved_snapshots.c.location,
            preserved_snapshots.c.content_hash,
            preserved_snapshots.c.window_id,
        ).where(preserved_snapshots.c.name == name)
    )
    row = result.fetchone()
    if row is None:
        return None
    return SealedSnapshot(
        name=row[0], reason=row[1], location=row[2], content_hash=row[3], window_id=row[4]
    )


async def seal_snapshot(
    conn: Any,
    store_dsn: str,
    *,
    name: str,
    reason: str,
    schema: str,
    table: str,
    columns: list[tuple[str, str]],
    rows: list[dict],
    pk_columns: list[str] | None = None,
    window_id: str | None = None,
) -> SealedSnapshot:
    """REQ-983: DECLARE and SEAL a preserved snapshot — materialize ``rows`` into a frozen store table
    and record the immutable seal in ``preserved_snapshots`` (in ``conn``'s transaction).

    Fails LOUD on either invariant:
    - a missing/blank ``reason`` (the mandatory why-tag: why the data cannot be reconstructed);
    - a re-seal of an already-sealed ``name`` (a preserved snapshot is materialized-once, immutable).

    ``conn`` is the CONTROL-PLANE connection (the seal record); ``store_dsn`` is the materialization
    store (the frozen data). Returns the :class:`SealedSnapshot`."""
    if not (reason and reason.strip()):
        raise ValueError(
            f"REQ-983: preserved snapshot {name!r} MUST be declared with a why-tag (the reason it "
            f"cannot be reconstructed) — refusing to seal a non-reconstructible dataset without one"
        )
    existing = await get_snapshot(conn, name)
    if existing is not None:
        raise ValueError(
            f"REQ-983: snapshot {name!r} is already sealed and immutable — a preserved snapshot is "
            f"materialized point-in-time ONCE, never overwritten (sealed at {existing.location})"
        )
    digest = content_hash(rows, pk_columns)
    location = await store_writer.persist_land(
        store_dsn,
        schema=schema,
        table=table,
        columns=columns,
        rows=rows,
        persist="replace",
        pk_columns=pk_columns,
    )
    await conn.upsert(
        preserved_snapshots,
        {
            "name": name,
            "reason": reason,
            "location": location,
            "content_hash": digest,
            "window_id": window_id,
        },
        index_elements=["name"],
        update_columns=[],  # DO NOTHING — a concurrent re-seal cannot overwrite the frozen record
    )
    return SealedSnapshot(
        name=name, reason=reason, location=location, content_hash=digest, window_id=window_id
    )
