# Copyright (c) 2026 Kenneth Stott
# Canary: 3de609ff-6421-4f6e-9d77-5c7c93e20416
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Auth introspection endpoint."""

# Requirements: REQ-120, REQ-121, REQ-122, REQ-123, REQ-124, REQ-125

from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import APIRouter, Request
from pydantic import BaseModel
from sqlalchemy import func, insert, select, update

from provisa.core.schema_admin import local_users, org_invites, orgs, user_org_memberships
from provisa.core.schema_org import roles

if TYPE_CHECKING:
    pass

router = APIRouter(prefix="/auth", tags=["auth"])


@router.get("/me")  # REQ-120, REQ-125
async def me(request: Request):
    from provisa.api.app import state
    from provisa.auth.role_mapping import resolve_assignments

    identity = getattr(request.state, "identity", None)

    tenant_db = state.tenant_db
    assert tenant_db is not None
    async with tenant_db.acquire() as conn:
        result = await conn.execute_core(select(roles.c.id))
        role_rows = result.fetchall()
    all_role_ids = {r[0] for r in role_rows}

    # Dev/no-auth mode is keyed on "no auth provider configured" (auth_config is None) — NOT on the
    # username. Unsecured mode honors X-Provisa-Role, so the username IS the selected role (not
    # "anonymous"); every configured role is exposed with wildcard domain access so all are selectable.
    unsecured = getattr(state, "auth_config", None) is None
    if unsecured or identity is None:
        uid = identity.user_id if identity is not None else "anonymous"
        return {
            "user_id": uid,
            "email": None,
            "display_name": uid,
            "dev_mode": True,
            "active_org_id": "root",
            "org_memberships": [{"org_id": "root", "org_name": "Enterprise"}],
            "assignments": [{"role_id": rid, "domain_id": "*"} for rid in sorted(all_role_ids)],
        }

    raw = resolve_assignments(identity)
    assignments = [
        {"role_id": a.role_id, "domain_id": a.domain_id} for a in raw if a.role_id in all_role_ids
    ]

    # user_org_memberships/orgs live in the platform control plane.
    admin_db = state.admin_db
    assert admin_db is not None
    async with admin_db.acquire() as conn:
        result = await conn.execute_core(
            select(user_org_memberships.c.org_id, orgs.c.name.label("org_name"))
            .select_from(
                user_org_memberships.join(orgs, orgs.c.id == user_org_memberships.c.org_id)
            )
            .where(user_org_memberships.c.user_id == identity.user_id)
        )
        org_rows = [dict(r._mapping) for r in result.fetchall()]

    # The auth middleware always resolves active_org_id for an authenticated identity;
    # a missing value is a wiring bug, not a reason to silently grant the 'root' org.
    active_org_id = getattr(request.state, "active_org_id", None)
    if active_org_id is None:
        from fastapi import HTTPException

        raise HTTPException(
            status_code=500, detail="active_org_id not resolved for authenticated user"
        )
    return {
        "user_id": identity.user_id,
        "email": identity.email,
        "display_name": identity.display_name,
        "dev_mode": False,
        "active_org_id": active_org_id,
        "org_memberships": [{"org_id": r["org_id"], "org_name": r["org_name"]} for r in org_rows],
        "assignments": assignments,
    }


@router.get("/provider-type")  # REQ-120
async def provider_type():
    from provisa.api.app import state

    cfg = getattr(state, "config", None)
    auth_cfg = getattr(cfg, "auth", None) if cfg else None
    if auth_cfg is None:
        return {"provider": None}
    provider = (
        auth_cfg.get("provider")
        if isinstance(auth_cfg, dict)
        else getattr(auth_cfg, "provider", None)
    )
    return {"provider": provider}


@router.get("/invite/{token}")
async def get_invite(token: str):  # REQ-516
    from provisa.api.app import state

    # org_invites/orgs live in the platform control plane.
    admin_db = state.admin_db
    assert admin_db is not None
    async with admin_db.acquire() as conn:
        result = await conn.execute_core(
            select(
                org_invites.c.token,
                org_invites.c.org_id,
                orgs.c.name.label("org_name"),
                org_invites.c.role_id,
                org_invites.c.expires_at,
                org_invites.c.used_at,
            )
            .select_from(org_invites.join(orgs, orgs.c.id == org_invites.c.org_id))
            .where(org_invites.c.token == token)
        )
        fetched = result.fetchone()
    row = dict(fetched._mapping) if fetched is not None else None
    if row is None:
        from fastapi import HTTPException

        raise HTTPException(status_code=404, detail="Invite not found")
    import datetime
    from datetime import timezone

    now = datetime.datetime.now(tz=timezone.utc)
    if row["used_at"] is not None:
        from fastapi import HTTPException

        raise HTTPException(status_code=410, detail="Invite already used")
    if row["expires_at"] < now:
        from fastapi import HTTPException

        raise HTTPException(status_code=410, detail="Invite expired")
    return {
        "token": row["token"],
        "org_id": row["org_id"],
        "org_name": row["org_name"],
        "role_id": row["role_id"],
        "valid": True,
    }


class RegisterRequest(BaseModel):
    username: str
    password: str
    email: str | None = None
    display_name: str | None = None
    invite_token: str | None = None


@router.post("/register")  # REQ-124
async def register(body: RegisterRequest):
    from provisa.api.app import state

    cfg = getattr(state, "config", None)
    auth_cfg = getattr(cfg, "auth", None) if cfg else None
    if auth_cfg is None:
        return {"detail": "Auth not configured"}, 400
    provider = (
        auth_cfg.get("provider")
        if isinstance(auth_cfg, dict)
        else getattr(auth_cfg, "provider", None)
    )
    if provider != "basic":
        from fastapi import HTTPException

        raise HTTPException(
            status_code=400, detail="Registration only available with basic auth provider"
        )

    import bcrypt
    import uuid

    password_hash = bcrypt.hashpw(body.password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
    user_id = str(uuid.uuid4())

    # local_users/org_invites/user_org_memberships live in the platform control plane.
    admin_db = state.admin_db
    assert admin_db is not None
    async with admin_db.acquire() as conn:
        result = await conn.execute_core(
            select(local_users.c.id).where(local_users.c.username == body.username)
        )
        existing = result.fetchone()
        if existing:
            from fastapi import HTTPException

            raise HTTPException(status_code=409, detail="Username already exists")
        await conn.execute_core(
            insert(local_users).values(
                id=user_id,
                username=body.username,
                password_hash=password_hash,
                email=body.email,
                display_name=body.display_name,
                is_active=True,
            )
        )
        if body.invite_token:
            import datetime
            from datetime import timezone
            from fastapi import HTTPException

            now = datetime.datetime.now(tz=timezone.utc)
            result = await conn.execute_core(
                select(
                    org_invites.c.org_id,
                    org_invites.c.role_id,
                    org_invites.c.expires_at,
                    org_invites.c.used_at,
                ).where(org_invites.c.token == body.invite_token)
            )
            fetched = result.fetchone()
            invite = dict(fetched._mapping) if fetched is not None else None
            if invite is None or invite["used_at"] is not None or invite["expires_at"] < now:
                raise HTTPException(status_code=400, detail="Invalid or expired invite token")
            await conn.upsert(
                user_org_memberships,
                {"user_id": user_id, "org_id": invite["org_id"]},
                index_elements=["user_id", "org_id"],
                update_columns=[],
            )
            await conn.execute_core(
                update(org_invites)
                .where(org_invites.c.token == body.invite_token)
                .values(used_at=func.now(), used_by=user_id)
            )
    return {"user_id": user_id, "username": body.username}
