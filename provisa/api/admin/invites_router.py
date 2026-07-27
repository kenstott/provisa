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
from provisa.core.schema_admin import org_invites, orgs, user_org_memberships

router = APIRouter(prefix="/admin/invites", tags=["admin"])


def _pool(_request: Request) -> Database:  # pyright: ignore[reportUnusedParameter]
    # org_invites/orgs live in the platform control plane.
    from provisa.api.app import state

    assert state.admin_db is not None
    return state.admin_db


async def _require_org_admin(request: Request, org_id: str) -> None:  # REQ-1266
    """Raise 403 unless the caller may issue invites for ``org_id``: a platform admin
    (superadmin/admin — any org), or the org_admin of exactly this org. org_admin authority
    is confined to the org the caller is currently acting in (active_org_id) and backed by an
    admin-plane membership row. Dev/no-auth (anonymous) is allowed, matching _require_superadmin."""
    from provisa.api.app import state as _app_state
    from provisa.api.admin.capabilities import _resolved_capabilities

    identity = getattr(request.state, "identity", None)
    if identity is None or getattr(identity, "user_id", "anonymous") == "anonymous":
        return  # dev mode — no auth configured
    caps = _resolved_capabilities(identity, _app_state)
    if "superadmin" in caps or "admin" in caps:
        return  # platform admin acts in any org
    role_claims = {c.split(":")[0] for c in getattr(identity, "roles", [])}
    active_org = getattr(request.state, "active_org_id", None)
    if "org_admin" not in role_claims or active_org != org_id:
        raise HTTPException(status_code=403, detail=f"org_admin of {org_id} required")
    async with _pool(request).acquire() as conn:
        result = await conn.execute_core(
            select(user_org_memberships.c.org_id).where(
                user_org_memberships.c.user_id == identity.user_id,
                user_org_memberships.c.org_id == org_id,
            )
        )
        if result.fetchone() is None:
            raise HTTPException(status_code=403, detail=f"Not a member of {org_id}")


class CreateInviteBody(BaseModel):
    org_id: str
    role_id: str | None = None
    expires_in_days: int = 7
    # REQ-1287: address the invite to a person so GET /auth/my-invites can surface it to them on
    # first sign-in. Omit for a shareable link invite.
    email: str | None = None


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
    await _require_org_admin(request, body.org_id)
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
                email=body.email.strip().lower() if body.email else None,
                created_by=created_by,
                expires_at=expires_at,
            )
            .returning(
                org_invites.c.token,
                org_invites.c.org_id,
                org_invites.c.role_id,
                org_invites.c.email,
                org_invites.c.created_by,
                org_invites.c.expires_at,
            )
        )
        row = result.fetchone()
    return dict(row._mapping)


async def _administered_org_scope(request: Request) -> str | None:  # REQ-1266
    """Return None if the caller is a platform admin (sees all invites), or the single org_id
    the caller administers (org_admin, scoped to active_org_id). Raise 403 otherwise. Dev/no-auth
    returns None (sees all)."""
    from provisa.api.app import state as _app_state
    from provisa.api.admin.capabilities import _resolved_capabilities

    identity = getattr(request.state, "identity", None)
    if identity is None or getattr(identity, "user_id", "anonymous") == "anonymous":
        return None  # dev mode
    caps = _resolved_capabilities(identity, _app_state)
    if "superadmin" in caps or "admin" in caps:
        return None  # platform admin: all orgs
    role_claims = {c.split(":")[0] for c in getattr(identity, "roles", [])}
    active_org = getattr(request.state, "active_org_id", None)
    if "org_admin" not in role_claims or active_org is None:
        raise HTTPException(status_code=403, detail="org_admin required")
    return active_org


@router.get("/")
async def list_invites(request: Request):  # REQ-516
    scope_org = await _administered_org_scope(request)
    pool = _pool(request)
    stmt = (
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
    if scope_org is not None:
        stmt = stmt.where(org_invites.c.org_id == scope_org)
    async with pool.acquire() as conn:
        result = await conn.execute_core(stmt)
        rows = result.fetchall()
    return [dict(r._mapping) for r in rows]


@router.delete("/{token}")
async def revoke_invite(token: str, request: Request):  # REQ-516
    scope_org = await _administered_org_scope(request)
    pool = _pool(request)
    stmt = _delete(org_invites).where(org_invites.c.token == token, org_invites.c.used_at.is_(None))
    if scope_org is not None:
        stmt = stmt.where(org_invites.c.org_id == scope_org)
    async with pool.acquire() as conn:
        result = await conn.execute_core(stmt.returning(org_invites.c.token))
        row = result.fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Invite not found or already used")
    return {"revoked": token}
