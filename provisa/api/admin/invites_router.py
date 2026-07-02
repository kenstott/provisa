# Copyright (c) 2026 Kenneth Stott
# Canary: 3de609ff-6421-4f6e-9d77-5c7c93e20416
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Admin endpoints for org invite token management."""

# Requirements: REQ-120, REQ-125

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from provisa.core.database import Database

router = APIRouter(prefix="/admin/invites", tags=["admin"])


def _pool(_request: Request) -> Database:  # pyright: ignore[reportUnusedParameter]
    # org_invites/orgs live in the platform control plane.
    from provisa.api.app import state

    assert state.admin_db is not None
    return state.admin_db


class CreateInviteBody(BaseModel):
    org_id: str
    role_id: str | None = None
    expires_in_days: int = 7


@router.post("/")
async def create_invite(body: CreateInviteBody, request: Request):  # REQ-125
    import datetime
    import uuid
    from datetime import timezone

    pool = _pool(request)
    identity = getattr(request.state, "identity", None)
    created_by = identity.user_id if identity else "system"
    # token and expiry are computed app-side (portable) rather than via
    # PG-specific gen_random_uuid()/interval defaults — the platform control
    # plane may be any SQLAlchemy backend.
    token = str(uuid.uuid4())
    expires_at = datetime.datetime.now(tz=timezone.utc) + datetime.timedelta(
        days=body.expires_in_days
    )
    async with pool.acquire() as conn:
        org = await conn.fetchrow("SELECT id FROM orgs WHERE id = $1", body.org_id)
        if not org:
            raise HTTPException(status_code=404, detail="Org not found")
        row = await conn.fetchrow(
            """
            INSERT INTO org_invites (token, org_id, role_id, created_by, expires_at)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING token, org_id, role_id, created_by, expires_at
            """,
            token,
            body.org_id,
            body.role_id,
            created_by,
            expires_at,
        )
    return dict(row)


@router.get("/")
async def list_invites(request: Request):  # REQ-516
    pool = _pool(request)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT i.token, i.org_id, o.name as org_name, i.role_id,
                   i.created_by, i.expires_at, i.used_at, i.used_by
            FROM org_invites i
            JOIN orgs o ON o.id = i.org_id
            ORDER BY i.expires_at DESC
            """
        )
    return [dict(r) for r in rows]


@router.delete("/{token}")
async def revoke_invite(token: str, request: Request):  # REQ-516
    pool = _pool(request)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "DELETE FROM org_invites WHERE token = $1 AND used_at IS NULL RETURNING token",
            token,
        )
    if row is None:
        raise HTTPException(status_code=404, detail="Invite not found or already used")
    return {"revoked": token}
