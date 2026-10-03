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

from provisa.core import model_change

from typing import TYPE_CHECKING, Any

from sqlalchemy import delete as sa_delete, func, select

from provisa.core.repositories.integrity import Dependent, ObjectRef, guard
from provisa.core.schema_org import calendars, registered_tables

if TYPE_CHECKING:
    from provisa.core.database import Connection


async def upsert(conn: "Connection", cal: dict[str, Any]) -> None:
    """Create or replace a calendar VERSION (REQ-962). Keyed by (name, version); a re-upsert of the
    same version overwrites its definition (a NEW version is the immutable-history mechanism, not an
    in-place edit of an existing one)."""
    model_change.name("upsert", "calendar", cal["name"])  # REQ-1524
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


async def usage_count(conn: "Connection", name: str) -> int:
    """How many registered tables/MVs reference calendar ``name`` as their snapshot schedule
    (REQ-962). A calendar with usage MUST NOT be deleted — its snapshots would lose their boundary
    source. Counts across all versions (the binding is by name)."""
    result = await conn.execute_core(
        select(func.count())
        .select_from(registered_tables)
        .where(registered_tables.c.mv_calendar == name)
    )
    return int(result.fetchone()[0])


class CalendarDeleteRefused(Exception):
    """A calendar that may not be deleted because views take their snapshot schedule from it;
    ``dependents`` lists them."""

    def __init__(self, name: str, dependents: list[Dependent]) -> None:
        self.name = name
        self.dependents = dependents
        super().__init__(
            f"calendar {name!r} is in use by {len(dependents)} materialized view(s) — "
            "clear their snapshot schedule before deleting"
        )


async def delete(conn: "Connection", name: str) -> int:  # REQ-962, REQ-1918
    """Delete EVERY version of calendar ``name``: THE delete, for every surface. Returns the
    row count removed, 0 when there is no such calendar.

    A calendar in use MUST NOT be removed — its snapshots would lose their boundary source — so
    this is refused (:class:`CalendarDeleteRefused`), naming each, while a view or a
    materialized view takes its snapshot schedule from it. One transaction."""
    model_change.name("delete", "calendar", name)  # REQ-1524
    async with conn.transaction():
        found = await conn.execute_core(select(calendars.c.name).where(calendars.c.name == name))
        if found.fetchone() is None:
            return 0
        blocking = await guard(conn, ObjectRef("calendar", name))
        if blocking:
            raise CalendarDeleteRefused(name, blocking)
        result = await conn.execute_core(sa_delete(calendars).where(calendars.c.name == name))
    return int(result.rowcount or 0)
