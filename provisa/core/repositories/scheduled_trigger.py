# Copyright (c) 2026 Kenneth Stott
# Canary: 3d588ce8-b753-4dbc-8987-91929061380f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The org's scheduled triggers, in its model store (REQ-1003, REQ-1004, REQ-1919).

The connection is the org's model connection, so every read and write here is the org's own: a
trigger is never seen, replaced or removed through another org's connection.
"""

# Requirements: REQ-1003, REQ-1004, REQ-1919

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import delete, func, select, update

from provisa.core import model_change
from provisa.core.models import ScheduledTrigger
from provisa.core.repositories.origin import CONFIG
from provisa.core.repositories.origin import require as require_origin
from provisa.core.schema_org import scheduled_triggers

if TYPE_CHECKING:
    from provisa.core.database import Connection


def _values(trigger: ScheduledTrigger, *, webhook_name: str | None = None) -> dict[str, Any]:
    if trigger.sql is not None:
        kind = "sql"
    elif trigger.url is not None or (webhook_name or trigger.webhook_name) is not None:
        kind = "webhook"
    else:
        raise ValueError(
            f"trigger {trigger.id!r} names nothing to run: a scheduled trigger is a SQL statement "
            "or a webhook (internal functions are not supported)"
        )
    return {
        "id": trigger.id,
        "name": trigger.name or trigger.id,
        "cron": trigger.cron,
        "kind": kind,
        "url": trigger.url,
        "webhook_name": webhook_name or trigger.webhook_name,
        "args": dict(trigger.args),
        "sql": trigger.sql,
        "role": trigger.role,
        "enabled": trigger.enabled,
    }


async def list_all(conn: "Connection") -> list[dict[str, Any]]:
    """Every trigger the org's model holds, by id."""
    result = await conn.execute_core(select(scheduled_triggers).order_by(scheduled_triggers.c.id))
    return [dict(r._mapping) for r in result.fetchall()]


async def get(conn: "Connection", trigger_id: str) -> dict[str, Any] | None:
    result = await conn.execute_core(
        select(scheduled_triggers).where(scheduled_triggers.c.id == trigger_id)
    )
    row = result.fetchone()
    return dict(row._mapping) if row is not None else None


async def create(
    conn: "Connection",
    trigger: ScheduledTrigger,
    *,
    origin: str,
    webhook_name: str | None = None,
) -> None:
    """Add a trigger to the org's model. The caller has checked the id is free."""
    model_change.name("create", "scheduled trigger", trigger.id)  # REQ-1524
    require_origin(origin)
    await conn.execute_core(
        scheduled_triggers.insert().values(
            origin=origin, **_values(trigger, webhook_name=webhook_name)
        )
    )


async def delete_one(conn: "Connection", trigger_id: str) -> bool:
    """Remove a trigger from the org's model. False when the org holds no such trigger."""
    model_change.name("delete", "scheduled trigger", trigger_id)  # REQ-1524
    result = await conn.execute_core(
        delete(scheduled_triggers).where(scheduled_triggers.c.id == trigger_id)
    )
    return bool(result.rowcount)


async def set_enabled(conn: "Connection", trigger_id: str, enabled: bool) -> bool:
    """Enable or disable a trigger. False when the org holds no such trigger."""
    model_change.name("enable" if enabled else "disable", "scheduled trigger", trigger_id)
    result = await conn.execute_core(
        update(scheduled_triggers)
        .where(scheduled_triggers.c.id == trigger_id)
        .values(enabled=enabled, updated_at=func.now())
    )
    return bool(result.rowcount)


async def load_from_config(
    conn: "Connection", triggers: list[ScheduledTrigger], *, origin: str
) -> None:
    """A config's triggers, as the org loading it declares them: each is written with the load's
    ``origin``. When the config is the file (origin config), a config trigger the file no longer
    declares is removed; an admin-made trigger is the org's own and is left as it is."""
    require_origin(origin)
    if origin == CONFIG:
        declared = {t.id for t in triggers}
        for row in await list_all(conn):
            if row["origin"] == CONFIG and row["id"] not in declared:
                await delete_one(conn, row["id"])
    for trigger in triggers:
        model_change.name("upsert", "scheduled trigger", trigger.id)  # REQ-1524
        values = _values(trigger)
        await conn.upsert(
            scheduled_triggers,
            {"origin": origin, **values},
            index_elements=["id"],
            update_columns=[k for k in values if k != "id"] + ["origin"],
        )
