# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Cross-instance MV refresh coordination (REQ-879).

The ``materialized_views`` control-plane catalog is the AUTHORITATIVE SHARED refresh state
for a load-balanced fleet against one materialization store. Every fleet instance drives its
refresh off this shared row, not its per-instance ``MVRegistry``, so exactly one instance
refreshes a given MV at a time.

Protocol (leases + fencing, NOT held pessimistic locks):

1. ATOMIC CLAIM — a single conditional UPDATE that BOTH dedups by version (skip when
   ``materialized_input_version`` already == the target REQ-862 input stamp) AND excludes
   concurrent writers (skip when ``status='refreshing'`` and the lease is still valid).
   0 rows updated = skip; 1 row = this instance owns the refresh. This is the per-MV
   election — decentralized, no global leader. A crashed writer's lease expires so the MV
   is reclaimable.

2. HEARTBEAT — ``renew_lease`` extends ``lease_until`` during a long refresh, WHERE the
   caller still owns a live lease.

3. FENCED COMMIT — ``commit_refresh`` finalizes only WHERE ``writer=me`` AND the lease is
   still valid. 0 rows means the lease was lost (slow / crashed-then-revived), so the result
   is DISCARDED — a stale writer can never clobber a newer refresh (fencing token).

Store errors are NOT swallowed — a silent skip would let two writers race, exactly the bug
this requirement fixes. Callers fail loud.
"""

from __future__ import annotations

import os
import socket
import uuid
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from sqlalchemy import and_, insert, or_, update
from sqlalchemy.exc import IntegrityError

from provisa.core.schema_org import materialized_views as _mvt

if TYPE_CHECKING:
    from provisa.core.database import Database
    from provisa.mv.models import MVDefinition


async def ensure_mv_row(store: "Database", mv: "MVDefinition") -> None:
    """Seed the shared coordination row for ``mv`` if absent (idempotent).

    The MV registry is in-memory; the control-plane ``materialized_views`` catalog row —
    the one ``claim_refresh`` runs its atomic election on — is created lazily here on the
    first coordinated refresh. Without it a ``shared``-tier MV has no row to claim, so the
    claim UPDATE matches 0 rows and the MV can never refresh (stays STALE forever).

    Dialect-agnostic (the control plane is PostgreSQL in production, SQLite in tests): a
    plain INSERT whose duplicate-key IntegrityError — the row already exists, or a concurrent
    fleet instance seeded it first — is the success case and is swallowed."""
    stmt = insert(_mvt).values(
        id=mv.id,
        source_tables=mv.source_tables,
        target_catalog=mv.target_catalog,
        target_schema=mv.target_schema,
        target_table=mv.target_table,
        refresh_interval=mv.refresh_interval,
        enabled=mv.enabled,
        custom_sql=mv.sql,
        status="stale",
    )
    try:
        async with store.acquire() as conn:
            await conn.execute_core(stmt)
    except IntegrityError:
        pass  # row already present (or seeded concurrently) — the desired end state


# One stable id per process: this instance's fencing/ownership token.
INSTANCE_WRITER: str = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"

# A lease long enough to outlive a single refresh step but short enough that a crashed
# writer's claim is reclaimed promptly. Renewed by the heartbeat during a long refresh.
DEFAULT_LEASE_SECONDS: int = 120


async def claim_refresh(
    store: Database,
    mv_id: str,
    writer: str,
    target_input_version: str | None,
    lease_seconds: int = DEFAULT_LEASE_SECONDS,
) -> bool:
    """Atomically claim the shared refresh of ``mv_id`` for ``writer``.

    Returns True iff this instance won the per-MV election (exactly one caller can). The
    single conditional UPDATE:
      - dedups: only when ``materialized_input_version`` differs from ``target_input_version``
        (skip a refresh that would re-materialize the already-written version). When the
        target version is unknown (None) the dedup clause is dropped — mutual exclusion still
        holds.
      - excludes concurrent writers: claimable only when not currently refreshing under a
        still-valid lease (``lease_until IS NULL`` or ``lease_until <= now`` counts as free,
        so a crashed writer's stale claim is reclaimed).
    """
    now = datetime.now(UTC)
    lease_until = now + timedelta(seconds=lease_seconds)
    conds = [
        _mvt.c.id == mv_id,
        # Free unless another writer holds a live lease.
        or_(
            _mvt.c.status != "refreshing",
            _mvt.c.lease_until.is_(None),
            _mvt.c.lease_until <= now,
        ),
    ]
    if target_input_version is not None:
        # Dedup: skip when the store already holds this exact input version.
        conds.append(_mvt.c.materialized_input_version.is_distinct_from(target_input_version))
    stmt = (
        update(_mvt)
        .where(and_(*conds))
        .values(status="refreshing", writer=writer, lease_until=lease_until)
    )
    async with store.acquire() as conn:
        result = await conn.execute_core(stmt)
    return result.rowcount == 1


async def renew_lease(
    store: Database,
    mv_id: str,
    writer: str,
    lease_seconds: int = DEFAULT_LEASE_SECONDS,
) -> bool:
    """Heartbeat: extend the lease during a long refresh. Returns False if the lease was
    already lost (a competing reclaim won), signalling the caller to abort and discard."""
    now = datetime.now(UTC)
    lease_until = now + timedelta(seconds=lease_seconds)
    stmt = (
        update(_mvt)
        .where(
            _mvt.c.id == mv_id,
            _mvt.c.writer == writer,
            _mvt.c.lease_until >= now,
        )
        .values(lease_until=lease_until)
    )
    async with store.acquire() as conn:
        result = await conn.execute_core(stmt)
    return result.rowcount == 1


async def commit_refresh(
    store: Database,
    mv_id: str,
    writer: str,
    *,
    row_count: int,
    input_version: str | None,
    definition_version: str | None,
    snapshot_id: str | None,
) -> bool:
    """Fenced commit: finalize the shared row ONLY while this instance still owns a live
    lease. Returns False when the lease was lost — the caller must DISCARD its result rather
    than let a stale writer clobber a newer refresh."""
    now = datetime.now(UTC)
    stmt = (
        update(_mvt)
        .where(
            _mvt.c.id == mv_id,
            _mvt.c.writer == writer,
            _mvt.c.lease_until >= now,
        )
        .values(
            status="fresh",
            last_refresh_at=now,
            row_count=row_count,
            last_error=None,
            materialized_input_version=input_version,
            materialized_definition_version=definition_version,
            snapshot_id=snapshot_id,
            writer=None,
            lease_until=None,
        )
    )
    async with store.acquire() as conn:
        result = await conn.execute_core(stmt)
    return result.rowcount == 1


async def release_refresh(
    store: Database,
    mv_id: str,
    writer: str,
    error: str | None,
) -> bool:
    """Release a claim without a successful commit (refresh failed). Clears the lease and
    marks the row stale so the next instance can reclaim immediately. Fenced on ``writer`` so
    a superseded writer never resets a row a newer claim already owns."""
    stmt = (
        update(_mvt)
        .where(_mvt.c.id == mv_id, _mvt.c.writer == writer)
        .values(status="stale", last_error=error, writer=None, lease_until=None)
    )
    async with store.acquire() as conn:
        result = await conn.execute_core(stmt)
    return result.rowcount == 1
