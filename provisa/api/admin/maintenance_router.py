# Copyright (c) 2026 Kenneth Stott
# Canary: 6b2f80c1-49da-4f7b-8b0a-1d5a1f6d3c72
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The deployment-wide scheduled-maintenance notice (REQ-1466).

Some planned work takes the data plane down for minutes at a time and cannot be made rolling. The
engine-cluster topology switch is the case that forces this surface: ``var.engine_cluster_mode``
selects between the Autopilot and Standard shapes, both ``enable_autopilot`` and ``dns_config`` are
immutable, so switching REPLACES the cluster and every shard with it (REQ-1465). During that window
a query fails because the engine is gone, which is indistinguishable from the product being broken
unless the platform admin says otherwise.

Two endpoints and one row: a ``platform_settings`` holder turns the notice on before the work and
off after it, and every signed-in client reads it. Read is open to any authenticated caller — the
banner exists precisely for the people who are not administrators — while the write is gated, since
a false maintenance banner is itself an outage.

The wording is composed here, not in the browser, so the deployment says one thing on every surface;
a platform admin who has something specific to say overrides it with ``message``.
"""

# Requirements: REQ-1465, REQ-1466

from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Request
from pydantic import BaseModel
from sqlalchemy import select

from provisa.api.admin._platform_guard import require_platform_settings
from provisa.core.schema_admin import platform_notice

router = APIRouter()

# One deployment, one notice. A key rather than a bare single-row table because the row is upserted
# on every toggle and needs something to conflict on.
_NOTICE_ID = "current"

_DEFAULT_MESSAGE = (
    "Provisa is undergoing scheduled maintenance. Queries may fail or run slowly until it "
    "completes. No data or configuration is affected, and the service will be back shortly."
)


def _admin_pool():
    from provisa.api.app import state

    assert state.admin_db is not None
    return state.admin_db


class NoticeBody(BaseModel):
    active: bool
    # Omitted means the deployment's standard wording. Supplied means the platform admin has
    # something the standard wording does not cover.
    message: str | None = None
    # ISO-8601 instant the work is expected to end. Omitted means no estimate is being offered,
    # which the banner states rather than implying an imminent return.
    ends_at: datetime | None = None


def _serialize(row) -> dict:
    """The banner's whole input. ``message`` is resolved here so an unset one is not a client's
    problem to guess at."""
    if row is None:
        return {"active": False, "message": None, "ends_at": None, "started_at": None}
    return {
        "active": bool(row.active),
        "message": row.message or _DEFAULT_MESSAGE,
        "ends_at": row.ends_at.isoformat() if row.ends_at else None,
        "started_at": row.started_at.isoformat() if row.started_at else None,
    }


@router.get("/admin/platform/maintenance")
async def read_maintenance() -> dict:  # REQ-1466
    """The current notice, for every client that renders the banner.

    Deliberately ungated beyond authentication: an org member is exactly who the notice is for, and
    it carries nothing an administrator would withhold.
    """
    db = _admin_pool()
    async with db.acquire() as conn:
        result = await conn.execute_core(
            select(platform_notice).where(platform_notice.c.id == _NOTICE_ID)
        )
        return _serialize(result.first())


@router.put("/admin/platform/maintenance")
async def set_maintenance(request: Request, body: NoticeBody) -> dict:  # REQ-1466
    """Turn the notice on before planned work and off after it.

    ``started_at`` is stamped by the transition into ``active`` rather than being supplied, so the
    banner can say how long the window has been open without trusting a client's clock.
    """
    require_platform_settings(request)  # REQ-1337
    identity = getattr(request.state, "identity", None)
    db = _admin_pool()
    now = datetime.now(timezone.utc)
    async with db.acquire() as conn:
        result = await conn.execute_core(
            select(platform_notice).where(platform_notice.c.id == _NOTICE_ID)
        )
        existing = result.first()
        # Re-arming an already-active notice (a longer window, corrected wording) must not restart
        # the clock; turning it off clears the stamp so the next window starts its own.
        if not body.active:
            started_at = None
        elif existing is not None and existing.active and existing.started_at is not None:
            started_at = existing.started_at
        else:
            started_at = now
        await conn.upsert(
            platform_notice,
            {
                "id": _NOTICE_ID,
                "active": body.active,
                "message": body.message,
                "ends_at": body.ends_at,
                "started_at": started_at,
                "updated_at": now,
                "updated_by": getattr(identity, "user_id", None),
            },
            index_elements=["id"],
            update_columns=[
                "active",
                "message",
                "ends_at",
                "started_at",
                "updated_at",
                "updated_by",
            ],
        )
        result = await conn.execute_core(
            select(platform_notice).where(platform_notice.c.id == _NOTICE_ID)
        )
        return _serialize(result.first())
