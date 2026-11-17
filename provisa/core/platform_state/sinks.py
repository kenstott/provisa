# Copyright (c) 2026 Kenneth Stott
# Canary: 5c1e9a47-2b83-4f6d-9e0a-7d4b3c8f1a26
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The client-started Kafka sinks now running (REQ-812).

A sink runs on the process that accepted its request, on whichever node, so how many a principal
holds is counted here, across the deployment. A sink is listed when it starts (:func:`claim`,
refused past the cap), beats while it runs (:func:`beat`) and leaves when it stops
(:func:`release`). A sink whose process died without a clean stop drops off when it stops beating,
after :data:`STALE_AFTER`."""

# Requirements: REQ-812

from __future__ import annotations

import hashlib
import uuid
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from sqlalchemy import delete, func, select, update

from provisa.core.schema_admin import running_sinks

if TYPE_CHECKING:
    from provisa.core.database import Database

TABLES = ("running_sinks",)

# How often a running sink beats, and how long a silent one still counts: three missed beats.
HEARTBEAT = timedelta(seconds=30)
STALE_AFTER = HEARTBEAT * 3

#: The operator setting that caps the sinks one principal holds.
CAP_SETTING = "subscriptions.max_sinks_per_principal"

_t = running_sinks.c


class SinkCapReached(PermissionError):
    """A principal already holds as many running sinks as the operator's cap allows."""

    code = "subscribe.sink_cap_reached"

    def __init__(self, cap: int) -> None:
        super().__init__(
            f"You already hold {cap} running Kafka sinks, the most the operator allows "
            f"({CAP_SETTING})"
        )
        self.params = {"cap": cap, "setting": CAP_SETTING}


def _now() -> datetime:
    return datetime.now(UTC)


def _lock_key(user_id: str) -> int:
    """The advisory-lock key serializing one principal's claims across processes: a signed bigint
    from the user id's digest."""
    return int.from_bytes(
        hashlib.sha256(f"sink:{user_id}".encode()).digest()[:8], "big", signed=True
    )


async def claim(
    db: "Database",
    *,
    user_id: str,
    org_id: str | None,
    roles: tuple[str, ...],
    topic: str,
    table_name: str,
    cap: int,
) -> str:
    """List a new sink for ``user_id`` and return its id, or raise :class:`SinkCapReached` when
    the principal already holds ``cap`` running sinks. The count and the insert are one transaction
    under the principal's advisory lock, so two processes cannot both take the last place."""
    now = _now()
    sink_id = uuid.uuid4().hex
    async with db.acquire() as conn:
        async with conn.transaction():
            await conn.advisory_xact_lock(_lock_key(user_id))
            await conn.execute_core(
                delete(running_sinks).where(_t.user_id == user_id, _t.last_seen < now - STALE_AFTER)
            )
            held = (
                await conn.execute_core(
                    select(func.count()).select_from(running_sinks).where(_t.user_id == user_id)
                )
            ).scalar_one()
            if held >= cap:
                raise SinkCapReached(cap)
            await conn.execute_core(
                running_sinks.insert().values(
                    sink_id=sink_id,
                    user_id=user_id,
                    org_id=org_id,
                    roles=list(roles),
                    topic=topic,
                    table_name=table_name,
                    started_at=now,
                    last_seen=now,
                )
            )
    return sink_id


async def beat(db: "Database", sink_id: str) -> None:
    """Say the sink is still running."""
    async with db.acquire() as conn:
        await conn.execute_core(
            update(running_sinks).where(_t.sink_id == sink_id).values(last_seen=_now())
        )


async def release(db: "Database", sink_id: str) -> None:
    """Take the sink off the list (it stopped)."""
    async with db.acquire() as conn:
        await conn.execute_core(delete(running_sinks).where(_t.sink_id == sink_id))


async def held_by(db: "Database", user_id: str) -> int:
    """How many running sinks ``user_id`` holds now."""
    since = _now() - STALE_AFTER
    async with db.acquire() as conn:
        return (
            await conn.execute_core(
                select(func.count())
                .select_from(running_sinks)
                .where(_t.user_id == user_id, _t.last_seen >= since)
            )
        ).scalar_one()
