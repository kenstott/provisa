# Copyright (c) 2026 Kenneth Stott
# Canary: 0d6f3b8e-2a41-4c7e-9b15-7e8c4f1a29d3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Requests to join an org, the inverse of an invitation (REQ-1250).

A signed-in person asks an org to admit them. What happens next depends on the org's
``open_to_requests`` flag, and the requester is never told which: an ``invite_only`` org (the
default) refuses the request silently, so asking cannot be used to learn which org names exist.
An ``open_to_requests`` org logs it ``pending`` for an org admin to decide (REQ-1251).

Every attempt writes one row (see ``schema_admin.org_join_requests``). The rate limit counts those
rows, refused ones included, so it answers the same whatever the org is.
"""

# Requirements: REQ-1250

from __future__ import annotations

import datetime
import hashlib
import uuid
from datetime import timezone
from typing import TYPE_CHECKING

from sqlalchemy import func, select, update

from provisa.core.schema_admin import (
    org_join_requests,
    orgs,
    user_org_memberships,
    user_profiles,
)

if TYPE_CHECKING:
    from provisa.core.database import Connection, Database

PENDING = "pending"
REJECTED = "rejected"
EXPIRED = "expired"

#: How many requests one person may make, to any orgs, within :data:`RATE_WINDOW`.
RATE_LIMIT = 10
RATE_WINDOW = datetime.timedelta(hours=1)

#: The identity provider a requester must have signed in with (REQ-1250).
REQUIRED_PROVIDER = "firebase"


class JoinRequestRateLimited(Exception):
    """The requester has made :data:`RATE_LIMIT` requests within :data:`RATE_WINDOW`."""

    def __init__(self, retry_after: datetime.timedelta) -> None:
        super().__init__(
            f"Too many join requests; try again in {int(retry_after.total_seconds())}s"
        )
        self.retry_after = retry_after


def _requester_lock_key(user_id: str) -> int:
    """A per-requester advisory-lock key, so one person's concurrent requests are counted in turn
    and the limit cannot be passed by sending them at once."""
    digest = hashlib.sha256(f"org_join_requests:{user_id}".encode()).digest()
    return int.from_bytes(digest[:8], "big") & 0x7FFF_FFFF_FFFF_FFFF


async def requester_provider(db: "Database", user_id: str) -> str | None:
    """The identity provider ``user_id`` last signed in with, as the auth middleware records it on
    every request (``user_profiles.provider``); None when no profile is recorded."""
    async with db.acquire() as conn:
        row = (
            await conn.execute_core(
                select(user_profiles.c.provider).where(user_profiles.c.user_id == user_id)
            )
        ).fetchone()
    return None if row is None else row[0]


async def _decides_pending(conn: "Connection", user_id: str, org_id: str) -> bool:
    """Whether a request from ``user_id`` to ``org_id`` is one for an org admin to decide: the org
    exists, is open to requests, the requester is not already a member, and has no live pending
    request there (expired ones were closed by the caller first)."""
    org = (
        await conn.execute_core(select(orgs.c.open_to_requests).where(orgs.c.id == org_id))
    ).fetchone()
    if org is None or not org[0]:
        return False
    member = (
        await conn.execute_core(
            select(user_org_memberships.c.user_id).where(
                user_org_memberships.c.user_id == user_id,
                user_org_memberships.c.org_id == org_id,
            )
        )
    ).fetchone()
    if member is not None:
        return False
    live = (
        await conn.execute_core(
            select(org_join_requests.c.id).where(
                org_join_requests.c.user_id == user_id,
                org_join_requests.c.org_id == org_id,
                org_join_requests.c.status == PENDING,
            )
        )
    ).fetchone()
    return live is None


async def submit_join_request(
    db: "Database",
    *,
    user_id: str,
    org_id: str,
    ttl: datetime.timedelta,
    now: datetime.datetime | None = None,
) -> None:
    """Record one request from ``user_id`` to join ``org_id``.

    Returns nothing on purpose: the caller answers the requester the same whether the request was
    logged or refused (REQ-1250). Raises :class:`JoinRequestRateLimited` when the requester is over
    the limit, which depends only on their own attempts. ``ttl`` is the invitation TTL, so a pending
    request expires as an unredeemed invitation would.
    """
    at = now if now is not None else datetime.datetime.now(tz=timezone.utc)
    async with db.acquire() as conn, conn.transaction():
        await conn.advisory_xact_lock(_requester_lock_key(user_id))
        window_start = at - RATE_WINDOW
        recent = (
            await conn.execute_core(
                select(func.count(), func.min(org_join_requests.c.requested_at)).where(
                    org_join_requests.c.user_id == user_id,
                    org_join_requests.c.requested_at > window_start,
                )
            )
        ).one()
        if recent[0] >= RATE_LIMIT:
            raise JoinRequestRateLimited(recent[1] + RATE_WINDOW - at)
        await conn.execute_core(
            update(org_join_requests)
            .where(
                org_join_requests.c.user_id == user_id,
                org_join_requests.c.org_id == org_id,
                org_join_requests.c.status == PENDING,
                org_join_requests.c.expires_at <= at,
            )
            .values(status=EXPIRED)
        )
        status = PENDING if await _decides_pending(conn, user_id, org_id) else REJECTED
        await conn.execute_core(
            org_join_requests.insert().values(
                id=str(uuid.uuid4()),
                user_id=user_id,
                org_id=org_id,
                status=status,
                requested_at=at,
                expires_at=at + ttl,
            )
        )


async def list_open_requests(
    db: "Database", org_id: str, now: datetime.datetime | None = None
) -> list[dict]:
    """The requests to join ``org_id`` awaiting an org admin's decision, oldest first: pending and
    not yet past their expiry. Refused attempts are the requester's record, not the org's, and are
    never listed."""
    at = now if now is not None else datetime.datetime.now(tz=timezone.utc)
    stmt = (
        select(
            org_join_requests.c.id,
            org_join_requests.c.user_id,
            user_profiles.c.email,
            user_profiles.c.display_name,
            org_join_requests.c.requested_at,
            org_join_requests.c.expires_at,
        )
        .select_from(
            org_join_requests.outerjoin(
                user_profiles, user_profiles.c.user_id == org_join_requests.c.user_id
            )
        )
        .where(
            org_join_requests.c.org_id == org_id,
            org_join_requests.c.status == PENDING,
            org_join_requests.c.expires_at > at,
        )
        .order_by(org_join_requests.c.requested_at)
    )
    async with db.acquire() as conn:
        rows = (await conn.execute_core(stmt)).fetchall()
    return [dict(r._mapping) for r in rows]
