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
from sqlalchemy import delete as _delete, select

from provisa.core.database import Database
from provisa.core.schema_admin import org_invites, orgs

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
    # Audit attribution must be a real user — never fall back to "system".
    if identity is None:
        raise HTTPException(status_code=401, detail="Authentication required")
    created_by = identity.user_id
    # token and expiry are computed app-side (portable) rather than via
    # PG-specific server-side UUID/interval defaults — the platform control
    # plane may be any SQLAlchemy backend.
    token = str(uuid.uuid4())
    expires_at = datetime.datetime.now(tz=timezone.utc) + datetime.timedelta(
        days=body.expires_in_days
    )
    async with pool.acquire() as conn:
        result = await conn.execute_core(select(orgs.c.id).where(orgs.c.id == body.org_id))
        if result.fetchone() is None:
            raise HTTPException(status_code=404, detail="Org not found")
        result = await conn.execute_core(
            org_invites.insert()
            .values(
                token=token,
                org_id=body.org_id,
                role_id=body.role_id,
                created_by=created_by,
                expires_at=expires_at,
            )
            .returning(
                org_invites.c.token,
                org_invites.c.org_id,
                org_invites.c.role_id,
                org_invites.c.created_by,
                org_invites.c.expires_at,
            )
        )
        row = result.fetchone()
    return dict(row._mapping)


@router.get("/")
async def list_invites(request: Request):  # REQ-516
    pool = _pool(request)
    async with pool.acquire() as conn:
        result = await conn.execute_core(
            select(
                org_invites.c.token,
                org_invites.c.org_id,
                orgs.c.name.label("org_name"),
                org_invites.c.role_id,
                org_invites.c.created_by,
                org_invites.c.expires_at,
                org_invites.c.used_at,
                org_invites.c.used_by,
            )
            .select_from(org_invites.join(orgs, orgs.c.id == org_invites.c.org_id))
            .order_by(org_invites.c.expires_at.desc())
        )
        rows = result.fetchall()
    return [dict(r._mapping) for r in rows]


@router.delete("/{token}")
async def revoke_invite(token: str, request: Request):  # REQ-516
    pool = _pool(request)
    async with pool.acquire() as conn:
        result = await conn.execute_core(
            _delete(org_invites)
            .where(org_invites.c.token == token, org_invites.c.used_at.is_(None))
            .returning(org_invites.c.token)
        )
        row = result.fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Invite not found or already used")
    return {"revoked": token}
