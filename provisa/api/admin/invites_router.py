# Copyright (c) 2026 Kenneth Stott
# Canary: 15fd6612-3d0c-4cb4-aa4a-a4d598752fa8
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

import logging

from fastapi import APIRouter, Request
from pydantic import BaseModel
from sqlalchemy import delete as _delete, select

from provisa.api.errors import ApiError
from provisa.core.database import Database
from provisa.core.schema_admin import org_invites, orgs, user_org_memberships
from provisa.security.rights import Capability, can_act_cross_org

log = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/invites", tags=["admin"])


def _pool(_request: Request) -> Database:  # pyright: ignore[reportUnusedParameter]
    # org_invites/orgs live in the platform control plane.
    from provisa.api.app import state

    assert state.admin_db is not None
    return state.admin_db


async def _require_org_admin(request: Request, org_id: str) -> None:  # REQ-1266
    """Raise 403 unless the caller may issue invites for ``org_id``: a platform_admin (any org),
    or the org_admin of exactly this org. org_admin authority is confined to the org the caller is
    currently acting in (active_org_id) and backed by an admin-plane membership row. Dev/no-auth
    (anonymous) is allowed, matching _require_platform_admin."""
    from provisa.api.app import state as _app_state
    from provisa.api.admin.capabilities import _resolved_capabilities

    identity = getattr(request.state, "identity", None)
    if identity is None or getattr(identity, "user_id", "anonymous") == "anonymous":
        return  # dev mode — no auth configured
    caps = _resolved_capabilities(identity, _app_state)
    if can_act_cross_org(caps):
        return  # holds cross_org — acts in any org
    # REQ-1337: RIGHTS, not role names. Issuing an invitation is user management, and the right is
    # confined to the org being acted in because the caller does not hold cross_org.
    active_org = getattr(request.state, "active_org_id", None)
    if Capability.USER_MANAGEMENT.value not in caps or active_org != org_id:
        raise ApiError(
            403,
            "invites.user_management_in_org_required",
            f"user_management in {org_id} required",
            org_id=org_id,
        )
    async with _pool(request).acquire() as conn:
        result = await conn.execute_core(
            select(user_org_memberships.c.org_id).where(
                user_org_memberships.c.user_id == identity.user_id,
                user_org_memberships.c.org_id == org_id,
            )
        )
        if result.fetchone() is None:
            raise ApiError(403, "invites.not_org_member", f"Not a member of {org_id}", org_id=org_id)


class CreateInviteBody(BaseModel):
    org_id: str
    role_id: str | None = None
    expires_in_days: int = 7
    # REQ-1287: address the invite to a person so GET /auth/my-invites can surface it to them on
    # first sign-in. Omit for a shareable link invite.
    email: str | None = None


# REQ-1314: the role a link invitation confers when it names none. analyst is the least-privileged
# of the four default roles (REQ-1297); defaulting upward would hand a shareable link more authority
# than its creator chose to grant.
DEFAULT_INVITE_ROLE = "analyst"


async def resolve_invite_role(org_id: str, role_id: str | None) -> str:
    """The role an invitation into ``org_id`` confers, validated against that org's roles table.

    REQ-1313: ``org_invites.role_id`` is a plain Text column with no foreign key (it references the
    per-org ``roles`` table, which lives in another model), and redemption feeds it straight into
    ``grant_org_role``. Without this check an org_admin can name any string at all — including
    ``platform_admin``, whose capabilities resolve deployment-wide regardless of which org schema
    the assignment sits in. Raises 403/422 rather than returning a substitute: silently downgrading
    an unconferrable role would grant access the inviter did not intend to describe.
    """
    from provisa.api.app import ensure_org_runtime, state
    from provisa.core.schema_org import roles as org_roles

    resolved = role_id if role_id is not None else DEFAULT_INVITE_ROLE
    # platform_admin confers deployment-wide administration, so it may only be conferred into the
    # root org — REQ-1298 makes an invitation into root followed by that assignment the sole path
    # to a backup platform administrator, and this is what stops an org_admin minting one at home.
    if resolved == "platform_admin" and org_id != state.org_id:
        raise ApiError(
            403,
            "invites.platform_admin_root_only",
            "platform_admin may only be conferred by an invitation into the root org",
        )
    rt = await ensure_org_runtime(org_id)
    assert rt.tenant_db is not None
    async with rt.tenant_db.acquire() as conn:
        found = await conn.execute_core(select(org_roles.c.id).where(org_roles.c.id == resolved))
        if found.fetchone() is None:
            raise ApiError(
                422,
                "invites.role_not_in_org",
                f"Role '{resolved}' does not exist in org '{org_id}'",
                role_id=resolved,
                org_id=org_id,
            )
    return resolved


@router.post("/")
async def create_invite(body: CreateInviteBody, request: Request):  # REQ-125
    import datetime
    import uuid
    from datetime import timezone

    pool = _pool(request)
    identity = getattr(request.state, "identity", None)
    # Audit attribution must be a real user — never fall back to "system".
    if identity is None:
        raise ApiError(401, "auth.authentication_required", "Authentication required")
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
        result = await conn.execute_core(
            select(orgs.c.id, orgs.c.name).where(orgs.c.id == body.org_id)
        )
        org_row = result.fetchone()
        if org_row is None:
            raise ApiError(404, "invites.org_not_found", "Org not found")
        org_name = org_row._mapping["name"]
    # REQ-1313/REQ-1314: resolve the default and validate against the target org's roles BEFORE the
    # insert, so a refused role leaves no invitation row behind and the inviter sees the refusal
    # rather than the invitee hitting it at redemption.
    role_id = await resolve_invite_role(body.org_id, body.role_id)
    async with pool.acquire() as conn:
        result = await conn.execute_core(
            org_invites.insert()
            .values(
                token=token,
                org_id=body.org_id,
                role_id=role_id,
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
    invite = dict(row._mapping)
    # REQ-1310: an addressed invitation is delivered. A link invitation (no email) is distributed by
    # the org_admin, so there is nothing to send.
    invite["delivery"] = await _deliver_invite(
        email=invite["email"],
        org_id=body.org_id,
        org_name=org_name,
        inviter=created_by,
        role_id=role_id,
        expires_at=expires_at,
        token=token,
    )
    return invite


async def _deliver_invite(  # REQ-1310
    *,
    email: str | None,
    org_id: str,
    org_name: str,
    inviter: str,
    role_id: str,
    expires_at,
    token: str,
) -> str:
    """Send the invitation and report what happened, as a value in the creation response.

    Returns "not_addressed" for a link invitation, "saas_only" when this deployment does not send
    email (REQ-1330: sending exists only under multitenancy — a self-hosted org_admin distributes
    the link themselves), "sent", or "failed: <reason>". A send failure is reported rather than
    raised: the invitation row is valid and its link still works, so refusing the whole request
    would destroy a usable invitation over a mail-server problem. Reporting it inline is what
    tells the org_admin to distribute the link themselves.
    """
    if not email:
        return "not_addressed"
    from starlette.concurrency import run_in_threadpool

    from provisa.api.app import state as _app_state
    from provisa.core.mail import compose_invite_message, email_sender

    cfg = getattr(_app_state, "config", None)
    if cfg is None:
        raise ApiError(503, "invites.config_not_loaded", "Server configuration is not loaded")
    if not getattr(cfg, "multitenancy", False):  # REQ-1330
        return "saas_only"
    message = compose_invite_message(
        to=email,
        org_name=org_name,
        org_id=org_id,
        # The user id, not a display name: display_name is optional on an identity and this line
        # must name someone in every message.
        inviter=inviter,
        role_id=role_id,
        expires_at=expires_at,
        base_url=cfg.mail.base_url,
        token=token,
    )
    try:
        sender = email_sender(cfg.mail)  # REQ-1330: the port; the provider is config, not code
        await run_in_threadpool(sender.send, message)
    except Exception as exc:  # reported to the caller, never swallowed
        log.error("invitation to %s for org %s could not be delivered: %s", email, org_id, exc)
        return f"failed: {exc}"
    return "sent"


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
    if can_act_cross_org(caps):
        return None  # holds cross_org: all orgs
    active_org = getattr(request.state, "active_org_id", None)
    if Capability.USER_MANAGEMENT.value not in caps or active_org is None:
        raise ApiError(403, "invites.user_management_required", "user_management required")
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
        raise ApiError(
            404, "invites.invite_not_found_or_used", "Invite not found or already used"
        )
    return {"revoked": token}
