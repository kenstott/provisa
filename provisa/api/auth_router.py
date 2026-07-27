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

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy import func, insert, select, update

from provisa.core.schema_admin import (
    local_users,
    org_invites,
    orgs,
    superadmin_bootstrap,
    user_org_memberships,
    user_profiles,
)
from provisa.core.org_membership import email_matches_rule
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
            # REQ-1286: the dev principal's org is the control plane's resolved org_id — the same
            # value that names the org_<id> tenant schema. A literal here names an org whose
            # schema was never created, and every runtime resolution for it fails.
            "active_org_id": state.org_id,
            "org_memberships": [{"org_id": state.org_id, "org_name": "Enterprise"}],
            "assignments": [{"role_id": rid, "domain_id": "*"} for rid in sorted(all_role_ids)],
        }

    # admin/superadmin are platform bypass keywords, not tenant `roles` rows, so they are absent
    # from all_role_ids — but a bootstrap superadmin's ONLY assignment is `admin`. Keep the bypass
    # roles alongside real tenant roles so the platform superadmin surfaces to the UI (otherwise
    # /me returns []: the onboarding gate then traps the superadmin, who has 0 org memberships).
    _PLATFORM_BYPASS = {"admin", "superadmin"}
    raw = resolve_assignments(identity)
    assignments = [
        {"role_id": a.role_id, "domain_id": a.domain_id}
        for a in raw
        if a.role_id in all_role_ids or a.role_id in _PLATFORM_BYPASS
    ]

    # user_org_memberships/orgs/user_profiles live in the platform control plane.
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
        # given_name/family_name are user-owned (PATCH /auth/profile), not carried by the IdP token.
        prof_result = await conn.execute_core(
            select(user_profiles.c.given_name, user_profiles.c.family_name).where(
                user_profiles.c.user_id == identity.user_id
            )
        )
        prof = prof_result.fetchone()

    # active_org_id is None only for a just-authenticated user with no org membership yet (mid-
    # onboarding, before redeem-invite). That is a legitimate state on this platform-plane endpoint —
    # /me reports null active org + empty memberships so the UI can route to invite redemption.
    active_org_id = getattr(request.state, "active_org_id", None)
    return {
        "user_id": identity.user_id,
        "email": identity.email,
        "display_name": identity.display_name,
        "dev_mode": False,
        "active_org_id": active_org_id,
        "given_name": prof.given_name if prof is not None else None,
        "family_name": prof.family_name if prof is not None else None,
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


@router.get("/bootstrap-status")
async def bootstrap_status():  # REQ-1288
    """Whether the sole platform-admin slot is still unclaimed.

    The bootstrap grant (REQ-1266) is silent: the first identity to authenticate becomes the
    platform admin as a side effect of signing in, with no warning and no way to see it coming.
    The login page reads this BEFORE any credential exists so it can say so up front. Public for
    the same reason /auth/provider-type is — it reveals only that a deployment is uninitialized.
    """
    from provisa.api.app import state

    # state.auth_config is the same dict the middleware resolves its own bootstrap flag from
    # (provisa/auth/wiring.py:156) — read it there, not off state.config, so this endpoint and the
    # grant it warns about can never disagree. None means no auth section at all: unsecured, so
    # nobody is promoted by signing in and there is nothing to warn about.
    auth_cfg = getattr(state, "auth_config", None)
    if auth_cfg is None:
        return {"unclaimed": False}
    if not auth_cfg.get("bootstrap_superadmin", False):
        return {"unclaimed": False}

    # Bootstrap mode needs the platform plane for its singleton lock — the middleware asserts the
    # same. A missing admin_db here is a wiring fault, not a state to paper over.
    admin_db = state.admin_db
    assert admin_db is not None
    async with admin_db.acquire() as conn:
        result = await conn.execute_core(select(superadmin_bootstrap.c.user_id).limit(1))
        claimed = result.fetchone() is not None
    return {"unclaimed": not claimed}


@router.get("/my-invites")
async def my_invites(request: Request):  # REQ-1287
    """Pending invitations addressed to the caller's email.

    Onboarding asks three separate questions — do you have an account, do you have an invitation,
    do you have a membership — and before this endpoint the middle one was unanswerable: an invited
    user who arrived without their token looked identical to a stranger, so the UI could only offer
    "create an org". Returns [] for a link-only invite (email is null) or a dev principal with no
    email; the caller then falls through to org creation or pasting a token.
    """
    import datetime
    from datetime import timezone

    from provisa.api.app import state

    identity = getattr(request.state, "identity", None)
    email = getattr(identity, "email", None) if identity is not None else None
    if not email:
        return {"invites": []}

    admin_db = state.admin_db
    assert admin_db is not None
    now = datetime.datetime.now(tz=timezone.utc)
    async with admin_db.acquire() as conn:
        result = await conn.execute_core(
            select(
                org_invites.c.token,
                org_invites.c.org_id,
                orgs.c.name.label("org_name"),
                org_invites.c.role_id,
                org_invites.c.expires_at,
            )
            .select_from(org_invites.join(orgs, orgs.c.id == org_invites.c.org_id))
            .where(
                func.lower(org_invites.c.email) == email.strip().lower(),
                org_invites.c.used_at.is_(None),
                org_invites.c.expires_at > now,
            )
            .order_by(org_invites.c.expires_at)
        )
        rows = [dict(r._mapping) for r in result.fetchall()]
    return {
        "invites": [
            {
                "token": r["token"],
                "org_id": r["org_id"],
                "org_name": r["org_name"],
                "role_id": r["role_id"],
                "expires_at": r["expires_at"].isoformat(),
            }
            for r in rows
        ]
    }


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
            # The invite's granted role lives in the tenant control plane (user_role_assignments),
            # not the platform plane above. Without this the invitee gets org MEMBERSHIP but no role,
            # so the capability layer sees an identity with zero assignments — an org with an admin
            # who cannot administer it. The invite always carries role_id (schema_admin.org_invites).
            # It MUST land in the INVITED org's schema: this platform-plane request leaves current_org
            # unset, so state.tenant_db would resolve the DEFAULT org. Bind the invited org's runtime
            # (its tenant_db is search_path-scoped to org_<id>).
            from provisa.api.app import ensure_org_runtime
            from provisa.core.org_membership import grant_org_role

            rt = await ensure_org_runtime(invite["org_id"])
            assert rt.tenant_db is not None
            await grant_org_role(rt.tenant_db, user_id, invite["role_id"])
    return {"user_id": user_id, "username": body.username}


class RedeemInviteRequest(BaseModel):
    token: str


@router.post("/redeem-invite")  # REQ-516
async def redeem_invite(body: RedeemInviteRequest, request: Request):
    """Redeem an org invite for an ALREADY-authenticated bearer user (Firebase/OIDC).

    /register consumes invites for the basic provider (username+password created in the same call).
    A bearer identity has no /register step — it arrives with a valid token from the IdP and a
    ?invite= link — so this is the redemption path for it: add org membership (platform plane) and
    the invite's role assignment (tenant plane), then burn the invite. Idempotent per (user, org).
    """
    from provisa.api.app import state

    identity = getattr(request.state, "identity", None)
    if identity is None or getattr(identity, "user_id", "anonymous") == "anonymous":
        raise HTTPException(status_code=401, detail="Authentication required to redeem an invite")
    user_id = identity.user_id

    import datetime
    from datetime import timezone

    now = datetime.datetime.now(tz=timezone.utc)

    admin_db = state.admin_db
    assert admin_db is not None
    async with admin_db.acquire() as conn:
        result = await conn.execute_core(
            select(
                org_invites.c.org_id,
                org_invites.c.role_id,
                org_invites.c.expires_at,
                org_invites.c.used_at,
                orgs.c.email_rule,
            )
            .select_from(org_invites.join(orgs, orgs.c.id == org_invites.c.org_id))
            .where(org_invites.c.token == body.token)
        )
        fetched = result.fetchone()
        invite = dict(fetched._mapping) if fetched is not None else None
        if invite is None or invite["used_at"] is not None or invite["expires_at"] < now:
            raise HTTPException(status_code=400, detail="Invalid or expired invite token")
        # REQ-1268: an org email rule gates who may join, even with a valid invite — the invited
        # address and the authenticated address can differ. Reject a mismatch (or a missing email
        # when a rule is set) rather than granting membership the org's policy forbids.
        if not email_matches_rule(identity.email, invite["email_rule"]):
            raise HTTPException(
                status_code=403,
                detail="Your email address is not permitted to join this organization",
            )
        await conn.upsert(
            user_org_memberships,
            {"user_id": user_id, "org_id": invite["org_id"]},
            index_elements=["user_id", "org_id"],
            update_columns=[],
        )
        await conn.execute_core(
            update(org_invites)
            .where(org_invites.c.token == body.token)
            .values(used_at=func.now(), used_by=user_id)
        )

    # Role assignment is tenant-plane (see /register for the same split) and must land in the
    # INVITED org's schema. This platform-plane request leaves current_org unset, so state.tenant_db
    # would resolve the DEFAULT org — bind the invited org's runtime (tenant_db scoped to org_<id>).
    from provisa.api.app import ensure_org_runtime
    from provisa.core.org_membership import grant_org_role

    rt = await ensure_org_runtime(invite["org_id"])
    assert rt.tenant_db is not None
    await grant_org_role(rt.tenant_db, user_id, invite["role_id"])
    return {"user_id": user_id, "org_id": invite["org_id"], "role_id": invite["role_id"]}


class ProfileUpdate(BaseModel):
    given_name: str | None = None
    family_name: str | None = None


@router.patch("/profile")  # REQ-1266
async def update_profile(body: ProfileUpdate, request: Request):
    """Update the authenticated user's own first/last name (user_profiles, platform plane).

    display_name/email mirror the IdP token and are read-only here; given_name/family_name have
    no IdP source (Firebase/OIDC tokens carry no first/last split) so the user supplies them. The
    profile row already exists — _upsert_profile writes it on every authenticated request — so this
    updates in place. An empty string clears the field (stored as NULL); whitespace is trimmed.
    """
    from provisa.api.app import state

    identity = getattr(request.state, "identity", None)
    if identity is None or getattr(identity, "user_id", "anonymous") == "anonymous":
        raise HTTPException(status_code=401, detail="Authentication required")

    def _norm(v: str | None) -> str | None:
        if v is None:
            return None
        s = v.strip()
        return s or None

    admin_db = state.admin_db
    assert admin_db is not None
    async with admin_db.acquire() as conn:
        await conn.execute_core(
            update(user_profiles)
            .where(user_profiles.c.user_id == identity.user_id)
            .values(given_name=_norm(body.given_name), family_name=_norm(body.family_name))
        )
    return {
        "user_id": identity.user_id,
        "given_name": _norm(body.given_name),
        "family_name": _norm(body.family_name),
    }
