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

from fastapi import APIRouter, Request
from pydantic import BaseModel

router = APIRouter(prefix="/auth", tags=["auth"])


@router.get("/me")  # REQ-120, REQ-125
async def me(request: Request):
    from provisa.api.app import state
    from provisa.auth.role_mapping import resolve_assignments

    identity = getattr(request.state, "identity", None)

    pg_pool = state.pg_pool
    assert pg_pool is not None
    async with pg_pool.acquire() as conn:
        role_rows = await conn.fetch("SELECT id FROM roles")
    all_role_ids = {r["id"] for r in role_rows}

    if identity is None or identity.user_id == "anonymous":
        # Dev mode: expose every configured role with wildcard domain access
        return {
            "user_id": "anonymous",
            "dev_mode": True,
            "active_org_id": "root",
            "org_memberships": [{"org_id": "root", "org_name": "Enterprise"}],
            "assignments": [{"role_id": rid, "domain_id": "*"} for rid in sorted(all_role_ids)],
        }

    raw = resolve_assignments(identity)
    assignments = [
        {"role_id": a.role_id, "domain_id": a.domain_id} for a in raw if a.role_id in all_role_ids
    ]

    pg_pool2 = state.pg_pool
    assert pg_pool2 is not None
    async with pg_pool2.acquire() as conn:
        org_rows = await conn.fetch(
            "SELECT m.org_id, o.name as org_name FROM user_org_memberships m "
            "JOIN orgs o ON o.id = m.org_id WHERE m.user_id = $1",
            identity.user_id,
        )

    active_org_id = getattr(request.state, "active_org_id", "root")
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

    pg_pool = state.pg_pool
    assert pg_pool is not None
    async with pg_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT i.token, i.org_id, o.name as org_name, i.role_id, i.expires_at, i.used_at
            FROM org_invites i
            JOIN orgs o ON o.id = i.org_id
            WHERE i.token = $1
            """,
            token,
        )
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

    pg_pool = state.pg_pool
    assert pg_pool is not None
    async with pg_pool.acquire() as conn:
        existing = await conn.fetchrow(
            "SELECT id FROM local_users WHERE username = $1", body.username
        )
        if existing:
            from fastapi import HTTPException

            raise HTTPException(status_code=409, detail="Username already exists")
        await conn.execute(
            "INSERT INTO local_users (id, username, password_hash, email, display_name, is_active) "
            "VALUES ($1, $2, $3, $4, $5, true)",
            user_id,
            body.username,
            password_hash,
            body.email,
            body.display_name,
        )
        if body.invite_token:
            import datetime
            from datetime import timezone
            from fastapi import HTTPException

            now = datetime.datetime.now(tz=timezone.utc)
            invite = await conn.fetchrow(
                "SELECT org_id, role_id, expires_at, used_at FROM org_invites WHERE token = $1",
                body.invite_token,
            )
            if invite is None or invite["used_at"] is not None or invite["expires_at"] < now:
                raise HTTPException(status_code=400, detail="Invalid or expired invite token")
            await conn.execute(
                "INSERT INTO user_org_memberships (user_id, org_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
                user_id,
                invite["org_id"],
            )
            await conn.execute(
                "UPDATE org_invites SET used_at = NOW(), used_by = $1 WHERE token = $2",
                user_id,
                body.invite_token,
            )
    return {"user_id": user_id, "username": body.username}
