# Copyright (c) 2026 Kenneth Stott
# Canary: 3f8a1d62-7b04-4e95-a2c8-6d1e9b5f0c47
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The runs of a Syncing entry's direction: scheduled ones nobody watched, and Run now (REQ-2247).

One row per run: the entry, the direction, how it was started (``schedule`` or ``manual``), its outcome (``ok``
or ``failed``), what it applied and what it held, each a list of ``{kind, object, reason_code, params}``, and
the error of a run that failed. The entry's page shows its last run with what it applied and held.
"""

# Requirements: REQ-2247

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import insert, select

from provisa.core.repositories.catalog_exchange import DIRECTIONS
from provisa.core.schema_org import catalog_sync_runs

if TYPE_CHECKING:
    from provisa.core.database import Connection

OK = "ok"
FAILED = "failed"
OUTCOMES = (OK, FAILED)

#: How a run was started: by the direction's schedule, or by an administrator's Run now.
SCHEDULE = "schedule"
MANUAL = "manual"
TRIGGERS = (SCHEDULE, MANUAL)


@dataclass(frozen=True)
class SyncRun:  # REQ-2247
    """One recorded run."""

    entry: str
    direction: str
    trigger: str
    outcome: str
    applied: list[dict[str, Any]]
    held: list[dict[str, Any]]
    error: str | None
    started_at: datetime
    finished_at: datetime


async def record(
    conn: "Connection",
    entry: str,
    direction: str,
    *,
    trigger: str,
    outcome: str,
    applied: list[dict[str, Any]],
    held: list[dict[str, Any]],
    started_at: datetime,
    error: str | None = None,
) -> None:
    """Record one run of ``entry``'s ``direction``."""
    if direction not in DIRECTIONS:
        raise ValueError(f"unknown sync direction {direction!r}")
    if outcome not in OUTCOMES:
        raise ValueError(f"unknown run outcome {outcome!r}")
    if trigger not in TRIGGERS:
        raise ValueError(f"unknown run trigger {trigger!r}")
    await conn.execute_core(
        insert(catalog_sync_runs).values(
            entry=entry,
            direction=direction,
            trigger=trigger,
            outcome=outcome,
            applied=applied,
            held=held,
            error=error,
            started_at=started_at,
        )
    )


async def runs(conn: "Connection", entry: str, direction: str) -> list[SyncRun]:
    """``entry``'s recorded runs in ``direction``, latest first."""
    result = await conn.execute_core(
        select(catalog_sync_runs)
        .where(catalog_sync_runs.c.entry == entry)
        .where(catalog_sync_runs.c.direction == direction)
        .order_by(catalog_sync_runs.c.id.desc())
    )
    return [
        SyncRun(
            entry=row.entry,
            direction=row.direction,
            trigger=row.trigger,
            outcome=row.outcome,
            applied=list(row.applied),
            held=list(row.held),
            error=row.error,
            started_at=row.started_at,
            finished_at=row.finished_at,
        )
        for row in result.fetchall()
    ]
