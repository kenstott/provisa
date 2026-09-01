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
from provisa.core.org_invite import (
    ENV_POLICIES,
    ENV_POLICY_NONE,
    ENV_POLICY_PER_VISITOR,
    ENV_POLICY_SHARED,
    SANDBOX_ROLE,
    unspent,
)
from provisa.core.schema_admin import org_invites, orgs, user_org_memberships
from provisa.security.rights import Capability, can_act_cross_org

log = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/invites", tags=["admin"])


def _pool(_request: Request) -> Database:  # pyright: ignore[reportUnusedParameter]
    # org_invites/orgs live in the platform control plane.
    from provisa.api.app import state

    assert state.admin_db is not None
    return state.admin_db


async def _require_org_admin(
    request: Request, org_id: str, *, allow_cross_org: bool = True
) -> None:  # REQ-1266
    """Raise 403 unless the caller may issue invites for ``org_id``: a platform_admin (any org),
    or the org_admin of exactly this org. org_admin authority is confined to the org the caller is
    currently acting in (active_org_id) and backed by an admin-plane membership row. Dev/no-auth
    (anonymous) is allowed, matching _require_platform_admin.

    REQ-1605: ``allow_cross_org=False`` withholds the cross_org bypass. Issuing an invite or
    changing membership is an ACTION platform_admin may still take in any org for support/recovery
    (REQ-1266, REQ-1303's own grant path). Reading who is in an org, its settings, its branding, or
    its config is that org's DATA AT REST — platform_admin sees it only where it also holds an
    actual org_admin assignment (seeded, REQ-1599's sandbox, or a REQ-1303 grant taken for that
    org), the same gate a non-cross_org caller already passes below.
    """
    from provisa.api.app import state as _app_state
    from provisa.api.admin.capabilities import _resolved_capabilities

    identity = getattr(request.state, "identity", None)
    if identity is None or getattr(identity, "user_id", "anonymous") == "anonymous":
        return  # dev mode — no auth configured
    caps = _resolved_capabilities(identity, _app_state)
    if allow_cross_org and can_act_cross_org(caps):
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
            raise ApiError(
                403, "invites.not_org_member", f"Not a member of {org_id}", org_id=org_id
            )


class CreateInviteBody(BaseModel):
    org_id: str
    role_id: str | None = None
    expires_in_days: int = 7
    # REQ-1287: address the invite to a person so GET /auth/my-invites can surface it to them on
    # first sign-in. Omit for a shareable link invite.
    email: str | None = None
    # REQ-1594: how many redemptions this link admits. 1 is the addressed invitation every caller
    # minted before open invites existed, so an omitted value changes nothing; None is unlimited,
    # which is the only answer a "Try it Out" link on a public page can give.
    max_uses: int | None = 1
    # REQ-1595: what the redeemer is given to work in — see provisa.core.org_invite.
    env_policy: str = ENV_POLICY_NONE
    env_ttl_seconds: int | None = None
    env_name: str | None = None


# REQ-1314: the role a link invitation confers when it names none. analyst is the least-privileged
# of the four default roles (REQ-1297); defaulting upward would hand a shareable link more authority
# than its creator chose to grant.
DEFAULT_INVITE_ROLE = "analyst"


def _check_env_policy(body: CreateInviteBody) -> None:
    """Refuse an env policy the schema would refuse, with an error the inviter can act on (REQ-1595).

    The same rule the CheckConstraints carry, applied here so the inviter gets a 400 naming the
    missing field instead of the 500 an integrity error becomes. Not a second authority: the
    constraint remains the one that decides, and this exists so nobody meets it.
    """
    if body.env_policy not in ENV_POLICIES:
        raise ApiError(
            400,
            "invites.invalid_env_policy",
            f"env_policy must be one of {', '.join(ENV_POLICIES)}",
        )
    if body.env_policy == ENV_POLICY_PER_VISITOR and body.env_ttl_seconds is None:
        raise ApiError(
            400,
            "invites.env_ttl_required",
            "A per_visitor invite needs env_ttl_seconds: without one, every redemption leaves an "
            "environment nothing will ever reap.",
        )
    if body.env_policy == ENV_POLICY_SHARED and not body.env_name:
        raise ApiError(
            400, "invites.env_name_required", "A shared invite must name the environment it seats"
        )
    # REQ-1597: the sandbox role is defined by a subtraction that happens when the ephemeral
    # environment is created (``create_environment``'s ``define_role_from``). Conferred by any other
    # policy there is no such environment and therefore no subtraction -- the invitation would seat
    # someone under a name whose meaning was never applied.
    if body.role_id == SANDBOX_ROLE and body.env_policy != ENV_POLICY_PER_VISITOR:
        raise ApiError(
            400,
            "invites.sandbox_role_needs_per_visitor",
            f"The '{SANDBOX_ROLE}' role is only conferrable by a {ENV_POLICY_PER_VISITOR} "
            "invitation: it is defined inside the environment that redemption mints.",
        )
    if body.max_uses is not None and body.max_uses < 1:
        raise ApiError(400, "invites.invalid_max_uses", "max_uses must be at least 1, or null")


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
    _check_env_policy(body)
    created_by = identity.user_id
    # token and expiry are computed app-side (portable) rather than via
    # PG-specific server-side UUID/interval defaults — the platform control
    # plane may be any SQLAlchemy backend.
    token = str(uuid.uuid4())
    expires_at = datetime.datetime.now(tz=timezone.utc) + datetime.timedelta(
        days=body.expires_in_days
    )
    env_policy = body.env_policy
    env_ttl_seconds = body.env_ttl_seconds
    env_name = body.env_name
    # REQ-1602: a per_visitor invite is addressed to whatever the inviter was looking at, not to
    # whatever string a client happens to pass — active_env() is the request's own resolved
    # environment (REQ-1487), so this is the same answer the inviter's own screen was showing.
    if env_policy == ENV_POLICY_PER_VISITOR:
        from provisa.api.org_runtime import active_env

        env_name = active_env()
    # REQ-1602: sandbox org invites use 1-day idle TTL if not specified (redeem_env forces PER_VISITOR policy)
    if body.org_id == "sandbox" and env_ttl_seconds is None:
        env_ttl_seconds = 86400  # 1 day of inactivity before deletion
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
                max_uses=body.max_uses,
                env_policy=env_policy,
                env_ttl_seconds=env_ttl_seconds,
                env_name=env_name,
            )
            .returning(
                org_invites.c.token,
                org_invites.c.org_id,
                org_invites.c.role_id,
                org_invites.c.email,
                org_invites.c.created_by,
                org_invites.c.expires_at,
                org_invites.c.uses,
                org_invites.c.max_uses,
                org_invites.c.env_policy,
                org_invites.c.env_ttl_seconds,
                org_invites.c.env_name,
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
        inviter_email=identity.email,  # REQ-1577
        role_id=role_id,
        expires_at=expires_at,
        token=token,
    )
    return invite


async def _org_branding(org_id: str) -> dict[str, str]:  # REQ-1486
    """The org's branding document, for the invitation. Empty when the org set none."""
    from provisa.api.app import state
    from provisa.core.org_branding import parse_branding
    from provisa.core.schema_admin import orgs

    assert state.admin_db is not None
    async with state.admin_db.acquire() as conn:
        result = await conn.execute_core(select(orgs.c.branding).where(orgs.c.id == org_id))
        row = result.fetchone()
    if row is None:
        raise ApiError(404, "invites.org_not_found", f"Org {org_id} not found", org_id=org_id)
    return parse_branding(row._mapping["branding"])


async def _deliver_invite(  # REQ-1310
    *,
    email: str | None,
    org_id: str,
    org_name: str,
    inviter: str,
    inviter_email: str | None,
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
    branding = await _org_branding(org_id)  # REQ-1486
    from provisa.core.mail_stats import MailAttempt, record  # REQ-1576

    # Composition sits inside the reporting boundary with the send. Building the message can fail
    # on configuration too -- an empty mail.base_url has no link to put in it -- and that failure
    # belongs in the response and the delivery record like any other, not as a 500 that destroys a
    # valid invitation row.
    try:
        message = compose_invite_message(
            branding=branding,
            to=email,
            org_name=org_name,
            org_id=org_id,
            # The user id, not a display name: display_name is optional on an identity and this
            # line must name someone in every message.
            inviter=inviter,
            # REQ-1577: where a reply lands. None when the identity carries no email address,
            # which leaves the message without a Reply-To rather than pointing it at an unread
            # mailbox.
            inviter_email=inviter_email,
            role_id=role_id,
            expires_at=expires_at,
            base_url=cfg.mail.base_url,
            token=token,
        )
        sender = email_sender(cfg.mail)  # REQ-1330: the port; the provider is config, not code
        await run_in_threadpool(sender.send, message)
    except Exception as exc:  # reported to the caller, never swallowed
        log.error("invitation to %s for org %s could not be delivered: %s", email, org_id, exc)
        # REQ-1576: recorded BEFORE returning, and failures especially — an invitation nobody
        # receives is indistinguishable from one nobody sent unless the attempt is on file.
        await record(
            _pool_for_stats(),
            MailAttempt(
                provider=cfg.mail.provider,
                kind="invite",
                recipient=email,
                succeeded=False,
                org_id=org_id,
                error=str(exc),
                requested_by=inviter,
            ),
        )
        return f"failed: {exc}"
    await record(
        _pool_for_stats(),
        MailAttempt(
            provider=cfg.mail.provider,
            kind="invite",
            recipient=email,
            succeeded=True,
            org_id=org_id,
            requested_by=inviter,
        ),
    )
    return "sent"


def _pool_for_stats() -> Database:
    """The control plane, where the mail record lives (REQ-1576)."""
    from provisa.api.app import state

    assert state.admin_db is not None
    return state.admin_db


async def _administered_org_scope(request: Request) -> str | None:  # REQ-1266
    """Return the org_id whose invites this request may see, or None for no scope at all.

    REQ-1604: the scope is the ACTIVE org, whoever is asking. cross_org is the right to act in any
    org (REQ-1318), one at a time — the org named by the Host or the header — not the right to see
    every org's invitations at once. Listing them all put another org's live invite tokens on the
    page of the org the operator had selected, and no invite listing is a cross-org report.

    None means the request bound no org at all: the dev/no-auth path, and the cross_org caller on
    the platform plane before an org is selected. An org_admin without an active org is a 403 --
    their right is over their own org, so an unbound request has nothing for them to administer."""
    from provisa.api.app import state as _app_state
    from provisa.api.admin.capabilities import _resolved_capabilities

    identity = getattr(request.state, "identity", None)
    if identity is None or getattr(identity, "user_id", "anonymous") == "anonymous":
        return None  # dev mode
    caps = _resolved_capabilities(identity, _app_state)
    active_org = getattr(request.state, "active_org_id", None)
    if can_act_cross_org(caps):
        return active_org
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
            org_invites.c.email,
            org_invites.c.created_by,
            org_invites.c.expires_at,
            org_invites.c.used_at,
            org_invites.c.used_by,
            # REQ-1594/REQ-1595: an open invite's row says nothing without these — "used_at is set"
            # no longer means spent, and the org_admin listing their links needs to see how many
            # redemptions are left and what each one hands out.
            org_invites.c.uses,
            org_invites.c.max_uses,
            org_invites.c.env_policy,
            org_invites.c.env_ttl_seconds,
            org_invites.c.env_name,
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
    # REQ-1594: revocable while it still has a redemption left in it. An open link is used and
    # still open, so gating on used_at would make the first redemption un-revoke it.
    stmt = _delete(org_invites).where(org_invites.c.token == token, unspent())
    if scope_org is not None:
        stmt = stmt.where(org_invites.c.org_id == scope_org)
    async with pool.acquire() as conn:
        result = await conn.execute_core(stmt.returning(org_invites.c.token))
        row = result.fetchone()
    if row is None:
        raise ApiError(404, "invites.invite_not_found_or_used", "Invite not found or already used")
    return {"revoked": token}
