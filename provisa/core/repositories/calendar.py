# Copyright (c) 2026 Kenneth Stott
# Canary: 4f8a2d61-9c3e-4b57-8a10-6d2f9e4c7b83
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Repository for named, versioned CALENDARS (REQ-962) — the periodic-snapshot boundary source.

A calendar is the shared, versioned definition an MV's snapshot schedule references: (base system,
timezone, fiscal/retail anchors, holidays, weekend). The holiday/business-day set is captured PER
VERSION and immutable, so a replay reproduces the same window existence. This is the control-plane
persistence the boot wiring loads into the in-memory CalendarRegistry (``_load_calendar_registry``).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from provisa.core.schema_org import calendars

if TYPE_CHECKING:
    from provisa.core.database import Connection


async def upsert(conn: "Connection", cal: dict[str, Any]) -> None:
    """Create or replace a calendar VERSION (REQ-962). Keyed by (name, version); a re-upsert of the
    same version overwrites its definition (a NEW version is the immutable-history mechanism, not an
    in-place edit of an existing one)."""
    row = {
        "name": cal["name"],
        "version": cal["version"],
        "base_system": cal.get("base_system", "gregorian"),
        "tz": cal.get("tz", "UTC"),
        "fiscal_anchor_month": cal.get("fiscal_anchor_month", 1),
        "fiscal_anchor_day": cal.get("fiscal_anchor_day", 1),
        "retail_anchor": cal.get("retail_anchor"),
        "week_start": cal.get("week_start", 0),
        "holidays": cal.get("holidays", []),
        "weekend": cal.get("weekend", [5, 6]),
    }
    await conn.upsert(
        calendars,
        row,
        index_elements=["name", "version"],
        update_columns=[
            "base_system",
            "tz",
            "fiscal_anchor_month",
            "fiscal_anchor_day",
            "retail_anchor",
            "week_start",
            "holidays",
            "weekend",
        ],
    )


async def list_all(conn: "Connection") -> list[dict]:
    """Every persisted calendar version, newest-created first (REQ-962)."""
    result = await conn.execute_core(select(calendars).order_by(calendars.c.created_at.desc()))
    return [dict(r._mapping) for r in result.fetchall()]


async def get_latest(conn: "Connection", name: str) -> dict | None:
    """The most-recently-created version of calendar ``name``, or None when it is unknown."""
    result = await conn.execute_core(
        select(calendars)
        .where(calendars.c.name == name)
        .order_by(calendars.c.created_at.desc())
        .limit(1)
    )
    row = result.fetchone()
    return dict(row._mapping) if row is not None else None
