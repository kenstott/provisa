# Copyright (c) 2026 Kenneth Stott
# Canary: c7013bcd-1a8c-4116-8615-c74adff26143
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Auth introspection endpoint."""

# Requirements: REQ-120, REQ-121, REQ-122, REQ-123, REQ-124, REQ-125, REQ-1568

from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import APIRouter, Request
from pydantic import BaseModel
from sqlalchemy import delete, func, insert, select, update

from provisa.api.errors import ApiError
from provisa.auth.scram_store import delete_verifier, write_verifier
from provisa.core.schema_admin import (
    local_users,
    org_invites,
    orgs,
    superadmin_bootstrap,
    user_org_memberships,
    user_profiles,
)
from provisa.core.org_membership import (
    JOINED_VIA_INVITE,
    membership_values,
)
from provisa.core.secrets import resolve_secrets
from provisa.core.schema_org import roles
from provisa.security.rights import PLATFORM_ADMIN_ROLE

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
    # REQ-1469: whether /billing exists at all. The routes are mounted by the commercial plugin, so
    # a self-hosted deployment has no billing surface and the UI must not offer one.
    from provisa.core.commerce import enabled as billing_enabled

    billing = billing_enabled()
    if unsecured or identity is None:
        uid = identity.user_id if identity is not None else "anonymous"
        return {
            "user_id": uid,
            "email": None,
            "display_name": uid,
            "dev_mode": True,
            "billing": billing,
            # REQ-1286: the dev principal's org is the control plane's resolved org_id — the same
            # value that names the org_<id> tenant schema. A literal here names an org whose
            # schema was never created, and every runtime resolution for it fails.
            "active_org_id": state.org_id,
            "org_memberships": [
                # REQ-1478: the dev principal's membership is a property of the deployment, not
                # something that happened to a person, so there is nothing to announce.
                {
                    "org_id": state.org_id,
                    "org_name": "Enterprise",
                    "joined_via": None,
                    "acknowledged": True,
                }
            ],
            "assignments": [{"role_id": rid, "domain_id": "*"} for rid in sorted(all_role_ids)],
        }

    # REQ-1297: platform_admin is seeded into every org schema, so it normally IS in all_role_ids —
    # but the bootstrap claimant's assignment is synthesized in the middleware and a process whose
    # state.roles predates the seed would drop it. Keep it alongside real tenant roles so the
    # platform administrator surfaces to the UI (otherwise /me returns []: the onboarding gate then
    # traps them, since they may hold 0 org memberships).
    _PLATFORM_BYPASS = {PLATFORM_ADMIN_ROLE}
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
        from provisa.core.org_membership import bindable_memberships

        result = await conn.execute_core(bindable_memberships(identity.user_id))
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
    # REQ-1471: the IdP owns login, so this is the first authenticated call of a session and the
    # earliest point the platform knows which shard that session will query. Starting the shard's
    # cold start here — not blocking on it — spends the operator's read-and-compose time on the
    # ~2-4min node provision the first query would otherwise pay for inside the request.
    from provisa.federation.engine_wake import prewarm_engine

    prewarm_engine(state, active_org_id)
    return {
        "user_id": identity.user_id,
        "email": identity.email,
        "display_name": identity.display_name,
        "dev_mode": False,
        "billing": billing,
        "active_org_id": active_org_id,
        "given_name": prof.given_name if prof is not None else None,
        "family_name": prof.family_name if prof is not None else None,
        # REQ-1478: joined_via/acknowledged travel with each membership so the UI can tell a person
        # they were joined to an org by their email address, or by an invitation they accepted,
        # rather than leaving an unexplained org in their switcher.
        "org_memberships": [
            {
                "org_id": r["org_id"],
                "org_name": r["org_name"],
                "joined_via": r["joined_via"],
                "acknowledged": r["acknowledged_at"] is not None,
            }
            for r in org_rows
        ],
        "assignments": assignments,
    }


class AcknowledgeJoinRequest(BaseModel):
    org_id: str


@router.post("/acknowledge-join")  # REQ-1478
async def acknowledge_join(body: AcknowledgeJoinRequest, request: Request):
    """Record that the caller has seen how they came to belong to ``org_id``.

    Only the member can acknowledge their own membership, so the row is addressed by the
    authenticated user id — the org id in the body selects which of their memberships it is.
    """
    from provisa.api.app import state
    from provisa.core.org_membership import acknowledge_membership

    identity = getattr(request.state, "identity", None)
    if identity is None:
        raise ApiError(401, "auth.required", "Authentication required")
    assert state.admin_db is not None
    await acknowledge_membership(state.admin_db, identity.user_id, body.org_id)
    return {"org_id": body.org_id, "acknowledged": True}


class SuperuserLoginRequest(BaseModel):
    username: str
    password: str


@router.post("/superuser-login")  # REQ-125, REQ-1472
async def superuser_login(body: SuperuserLoginRequest):
    """Exchange the break-glass credentials for a browser session token.

    Mounted for every provider, unlike ``/auth/login`` (basic only): the operator account is
    the deployment's own credential, not an IdP account, so on a Firebase or OIDC deployment
    this is the only sign-in it has. The same throttle guards it as every other password check.
    """
    from provisa.api.app import state
    from provisa.auth.superuser import issue_superuser_session, resolve_superuser_config
    from provisa.auth.throttle import LockedOut, login_attempt

    auth_cfg = getattr(state, "auth_config", None)
    if not auth_cfg:
        raise ApiError(404, "auth.superuser_not_configured", "No superuser is configured")
    su_config = resolve_superuser_config(auth_cfg.get("superuser"))
    if su_config is None:
        raise ApiError(404, "auth.superuser_not_configured", "No superuser is configured")
    raw_secret = auth_cfg.get("jwt_secret")
    secret = resolve_secrets(raw_secret) if raw_secret else None
    try:
        with login_attempt(body.username, body.password):
            token = issue_superuser_session(body.username, body.password, su_config, secret)
    except LockedOut as locked:
        raise ApiError(429, "auth.too_many_attempts", str(locked))
    except ValueError as exc:
        raise ApiError(401, "auth.invalid_credentials", str(exc))
    return {"access_token": token, "token_type": "bearer"}


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


@router.post("/claim-bootstrap")
async def claim_bootstrap(request: Request):  # REQ-1290
    """Claim the sole platform-admin slot for the caller. Deliberate, never a side effect.

    The middleware used to claim this while validating any bearer token, so a browser that still
    held a valid credential took platform admin on a page refresh — the first-login disclosure
    (REQ-1288) never got a chance to render. Only the first-login page calls this, and only after
    the user picks a provider on the page that says what claiming means.

    First writer wins on the fixed id=1 row: concurrent callers race the INSERT, one lands, and
    everybody reads back whoever won.
    """
    from provisa.api.app import state

    auth_cfg = getattr(state, "auth_config", None)
    if auth_cfg is None or not auth_cfg.get("bootstrap_superadmin", False):
        raise ApiError(404, "auth.bootstrap_claiming_disabled", "Bootstrap claiming is not enabled")

    identity = getattr(request.state, "identity", None)
    if identity is None:
        raise ApiError(401, "auth.authentication_required", "Authentication required")

    # Bootstrap mode needs the platform plane for its singleton lock — the middleware asserts the
    # same. A missing admin_db here is a wiring fault, not a state to paper over.
    admin_db = state.admin_db
    assert admin_db is not None
    async with admin_db.acquire() as conn:
        claimed_user_id = await conn.upsert_returning(
            superadmin_bootstrap,
            {"id": 1, "user_id": identity.user_id},
            index_elements=["id"],
            update_columns=[],
            returning="user_id",
        )
    claimed = claimed_user_id == identity.user_id
    if claimed:
        await _seat_claimant_in_root(identity.user_id)
    return {
        "claimed": claimed,
        "claimed_by": claimed_user_id,
        # REQ-1296: the org the claimant lands in. Naming it in the response is what lets the login
        # page send the next request into a populated org instead of nowhere.
        "org_id": state.org_id if claimed else None,
    }


async def _seat_claimant_in_root(user_id: str) -> None:  # REQ-1296
    """Seat the platform-admin claimant in the bootstrap org as a real member holding platform_admin.

    Claiming the slot used to leave the claimant holding an in-memory grant and nothing else: no
    membership row, no tenant-plane assignment, so the first screen after the welcome modal showed
    "No roles configured" and "You do not have permission to view this page". The claim now writes
    both planes, exactly as joining any other org does — the bootstrap org is an org, and its
    administrator is a member of it.

    The bootstrap org id is ``state.org_id``, the control plane's resolved value (REQ-1286), so the
    membership row and the ``org_<id>`` schema the assignment lands in can never name different orgs.
    """
    from provisa.api.app import state
    from provisa.core.org_membership import (
        JOINED_VIA_CREATED,
        grant_membership,
        grant_org_role,
    )
    from provisa.security.rights import ORG_ADMIN_ROLE, PLATFORM_ADMIN_ROLE

    assert state.admin_db is not None
    # Claiming the bootstrap slot is the claimant's own act, so the membership needs no explaining.
    await grant_membership(state.admin_db, user_id, state.org_id, joined_via=JOINED_VIA_CREATED)
    # current_org is unbound on this request, so the tenant_db shim resolves the default (bootstrap)
    # org's runtime — the same org the membership names.
    tenant_db = state.tenant_db
    assert tenant_db is not None, "the bootstrap org's tenant plane must be up before a claim"
    await grant_org_role(tenant_db, user_id, PLATFORM_ADMIN_ROLE)
    # REQ-1297: platform_admin carries only the control-plane bypass — no column grants name it and it
    # holds no data capabilities. The claimant is also the bootstrap org's data-plane administrator, so
    # seat them as its org_admin too. Without this the claim lands on "No roles configured" again: the
    # welcome modal would hand them a deployment whose own org they cannot query.
    await grant_org_role(tenant_db, user_id, ORG_ADMIN_ROLE)


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


async def _auto_join_offers(request: Request) -> list[dict]:
    """The auto-join orgs claiming the caller's address that they are not already a member of.

    REQ-1568. A single claim never reaches here — the sign-in path joined it, so the membership
    filter below removes it. What is left is the set nobody could choose between on the caller's
    behalf.
    """
    from provisa.api.app import state
    from provisa.core.org_membership import resolve_auto_join_orgs

    identity = getattr(request.state, "identity", None)
    if identity is None or identity.user_id == "anonymous":
        return []

    admin_db = state.admin_db
    assert admin_db is not None
    matches = await resolve_auto_join_orgs(admin_db, identity.email, identity.user_id)
    if not matches:
        return []
    roles_by_org = dict(matches)
    async with admin_db.acquire() as conn:
        result = await conn.execute_core(
            select(orgs.c.id, orgs.c.name).where(orgs.c.id.in_(list(roles_by_org)))
        )
        names = {r[0]: r[1] for r in result.fetchall()}
        member_result = await conn.execute_core(
            select(user_org_memberships.c.org_id).where(
                user_org_memberships.c.user_id == identity.user_id
            )
        )
        already = {r[0] for r in member_result.fetchall()}
    return [
        {"org_id": org_id, "org_name": names[org_id], "role_id": role_id}
        for org_id, role_id in matches
        if org_id not in already
    ]


@router.get("/auto-join-offers")
async def auto_join_offers(request: Request):  # REQ-1568
    """The orgs the caller may join on the strength of their email address alone.

    Non-empty only when more than one org claimed the address, because a lone claim is joined at
    sign-in. The page shows the list and the person picks; nothing is joined by reading this.
    """
    return {"offers": await _auto_join_offers(request)}


class AcceptAutoJoinBody(BaseModel):
    org_id: str


@router.post("/auto-join")
async def accept_auto_join(request: Request, body: AcceptAutoJoinBody):  # REQ-1568
    """Join the one org the caller picked out of the offers, and decline the rest.

    The offer list is recomputed here rather than trusted from the client: the org id arriving in
    the body is only a choice among what the rules currently admit, never a claim of eligibility.
    Every other claimant records the REQ-1306 opt-out — the person has said which org they belong
    to, so the others must not ask again at the next sign-in.
    """
    from provisa.api.app import state
    from provisa.api.auto_join import join_org_automatically
    from provisa.core.org_membership import suppress_auto_join

    offers = await _auto_join_offers(request)
    chosen = next((o for o in offers if o["org_id"] == body.org_id), None)
    if chosen is None:
        raise ApiError(404, "auth.auto_join_not_offered", "That org is not offering to admit you.")

    identity = request.state.identity
    admin_db = state.admin_db
    assert admin_db is not None
    await join_org_automatically(
        admin_db, identity.user_id, identity.email, chosen["org_id"], chosen["role_id"]
    )
    for other in offers:
        if other["org_id"] != chosen["org_id"]:
            await suppress_auto_join(admin_db, identity.user_id, other["org_id"])
    return {"org_id": chosen["org_id"], "role_id": chosen["role_id"]}


@router.post("/auto-join/decline")
async def decline_auto_join(request: Request):  # REQ-1568
    """Turn down every org claiming the caller's address, so they can go on and create their own.

    Recorded as the REQ-1306 opt-out per org, which is what stops the same question being put at
    every sign-in.
    """
    from provisa.api.app import state
    from provisa.core.org_membership import suppress_auto_join

    offers = await _auto_join_offers(request)
    identity = request.state.identity
    admin_db = state.admin_db
    assert admin_db is not None
    for offer in offers:
        await suppress_auto_join(admin_db, identity.user_id, offer["org_id"])
    return {"declined": [o["org_id"] for o in offers]}


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
        raise ApiError(404, "auth.invite_not_found", "Invite not found")
    import datetime
    from datetime import timezone

    now = datetime.datetime.now(tz=timezone.utc)
    if row["used_at"] is not None:
        raise ApiError(410, "auth.invite_already_used", "Invite already used")
    if row["expires_at"] < now:
        raise ApiError(410, "auth.invite_expired", "Invite expired")
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
        raise ApiError(
            400,
            "auth.registration_basic_only",
            "Registration only available with basic auth provider",
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
            raise ApiError(409, "auth.username_exists", "Username already exists")
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
                raise ApiError(400, "auth.invalid_invite_token", "Invalid or expired invite token")
            joined_org_id = invite["org_id"]
            await conn.upsert(
                user_org_memberships,
                membership_values(user_id, invite["org_id"], JOINED_VIA_INVITE),
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
    if body.invite_token:
        # REQ-1474: the invitee works under the org's trial if one is running, so the free
        # evaluation is spent for them as well as for the buyer — otherwise an org could mint an
        # endless supply of trials by inviting accounts that later go and create orgs of their own.
        # Outside the transaction above, which holds the only admin-plane connection this request
        # has: the seam opens its own.
        from provisa.core.commerce import bind_member_to_org_trial

        await bind_member_to_org_trial(admin_db, joined_org_id, body.email)
    # REQ-1394: registration is the third moment a plaintext password exists, so the account can
    # negotiate SCRAM over pgwire from the start rather than after a password change.
    await write_verifier(admin_db, user_id, body.username, body.password)
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
        raise ApiError(
            401, "auth.redeem_auth_required", "Authentication required to redeem an invite"
        )
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
            ).where(org_invites.c.token == body.token)
        )
        fetched = result.fetchone()
        invite = dict(fetched._mapping) if fetched is not None else None
        if invite is None or invite["used_at"] is not None or invite["expires_at"] < now:
            raise ApiError(400, "auth.invalid_invite_token", "Invalid or expired invite token")
        # REQ-1572: the org's email rule does NOT gate this. The rule decides who may join on their
        # own initiative; an invitation is an org admin naming a person, which is that decision
        # already made, and it is single-use and expiring. Gating it on the rule refused the very
        # people an admin deliberately reached outside their own domain — a contractor, an auditor,
        # someone whose work address is not the one their IdP account carries — with an error the
        # invitee could do nothing about. /register (basic provider) has never applied the rule to
        # an invite either, so this also ends a split where the same invitation was admitted or
        # refused depending on which identity provider the deployment ran.
        # REQ-1313: revalidate rather than trusting the stored value — a role can be removed from
        # the org between the invitation being written and this redemption, and assigning a role
        # that no longer exists would leave a user_role_assignments row pointing at nothing. This
        # runs BEFORE the membership upsert and the burn: a refused role must leave the invitation
        # intact and re-redeemable once the org restores the role, not spend it on a failure.
        from provisa.api.admin.invites_router import resolve_invite_role

        role_id = await resolve_invite_role(invite["org_id"], invite["role_id"])
        await conn.upsert(
            user_org_memberships,
            membership_values(user_id, invite["org_id"], JOINED_VIA_INVITE),
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
    await grant_org_role(rt.tenant_db, user_id, role_id)
    # REQ-1474: see /register — redeeming an invite into a trialling org spends this account's own
    # free evaluation, because from here on they are working inside one.
    from provisa.core.commerce import bind_member_to_org_trial

    await bind_member_to_org_trial(admin_db, invite["org_id"], identity.email)
    return {"user_id": user_id, "org_id": invite["org_id"], "role_id": role_id}


class ProfileUpdate(BaseModel):
    given_name: str | None = None
    family_name: str | None = None


@router.patch("/profile")  # REQ-1266
async def update_profile(body: ProfileUpdate, request: Request):
    """Update the authenticated user's own first/last name (user_profiles, platform plane).

    display_name/email mirror the IdP token and are read-only here; given_name/family_name have
    no IdP source (Firebase/OIDC tokens carry no first/last split) so the user supplies them. An
    empty string clears the field (stored as NULL); whitespace is trimmed.

    Written as an upsert, not an in-place UPDATE: the IdP mirror that creates the row
    (_upsert_profile) is dispatched fire-and-forget from the auth middleware, so on a user's first
    authenticated request it may not have committed by the time this handler runs — an UPDATE
    would match zero rows and silently discard the name. The insert names only user_id and the two
    user-owned columns; email/display_name/provider stay the mirror's to write, whichever lands
    first.
    """
    from provisa.api.app import state

    identity = getattr(request.state, "identity", None)
    if identity is None or getattr(identity, "user_id", "anonymous") == "anonymous":
        raise ApiError(401, "auth.authentication_required", "Authentication required")

    def _norm(v: str | None) -> str | None:
        if v is None:
            return None
        s = v.strip()
        return s or None

    admin_db = state.admin_db
    assert admin_db is not None
    async with admin_db.acquire() as conn:
        await conn.upsert(
            user_profiles,
            {
                "user_id": identity.user_id,
                "given_name": _norm(body.given_name),
                "family_name": _norm(body.family_name),
            },
            index_elements=["user_id"],
            update_columns=["given_name", "family_name"],
        )
    return {
        "user_id": identity.user_id,
        "given_name": _norm(body.given_name),
        "family_name": _norm(body.family_name),
    }


@router.delete("/account")  # REQ-1307, REQ-1312
async def delete_account(request: Request, confirm: str | None = None):
    """Delete the authenticated user's own Provisa account.

    Leaves every org they belong to (the same two-plane removal as REQ-1306, minus the auto-join
    opt-out — the account is going away, so there is nothing left to suppress), removes their
    ``user_profiles`` row and any ``local_users`` credential row, and tombstones every remaining
    reference to their id (REQ-1312).

    Refused while they are the last org_admin of any org, or the last platform_admin of the
    deployment: the response names each blocking org so they know exactly what to hand off first.
    The orgs themselves and everything registered in them survive — those belong to the org, not to
    the person.

    ``confirm`` must repeat the user id, the same typed ceremony org deletion carries (REQ-1300).

    REQ-1263 personal access tokens are not revoked here because no PAT table exists yet; when one
    lands, its rows for this user are deleted alongside the profile.
    """
    from provisa.api.admin.orgs_router import _admin_pool, _org_tenant_db
    from provisa.core.org_membership import org_admin_user_ids, remove_from_org, tombstone_id
    from provisa.core.schema_org import admin_audit_log, query_audit_log, user_role_assignments

    identity = getattr(request.state, "identity", None)
    user_id = getattr(identity, "user_id", None) if identity is not None else None
    if user_id in (None, "anonymous"):
        raise ApiError(401, "auth.authentication_required", "Authentication required")
    if confirm != user_id:
        raise ApiError(
            400,
            "auth.delete_account_confirm_required",
            (
                "Deleting your account is irreversible: your profile and every org membership are "
                "removed and cannot be restored. Repeat your user id in the 'confirm' parameter to "
                "proceed."
            ),
        )
    admin_db = _admin_pool()
    async with admin_db.acquire() as conn:
        result = await conn.execute_core(
            select(user_org_memberships.c.org_id).where(user_org_memberships.c.user_id == user_id)
        )
        member_org_ids = sorted(r[0] for r in result.fetchall())

    # Name EVERY org that blocks the deletion rather than refusing on the first one — one refusal
    # per handoff is a queue the user has to discover by repetition.
    tenant_dbs = {}
    blocking: list[str] = []
    for org_id in member_org_ids:
        tenant_dbs[org_id] = await _org_tenant_db(org_id)
        admins = await org_admin_user_ids(tenant_dbs[org_id])
        if user_id in admins and len(admins) == 1:
            blocking.append(org_id)
    if blocking:
        raise ApiError(
            409,
            "auth.last_org_admin",
            (
                f"You are the last org_admin of: {', '.join(blocking)}. Promote another org_admin "
                f"in each, or delete the organization, before deleting your account."
            ),
            orgs=", ".join(blocking),
        )

    # The deployment must not be left without a platform administrator either. platform_admin is
    # held either by a role assignment in the platform-plane schema or by the bootstrap claimant.
    from provisa.api.app import state

    platform_admins: set[str] = set()
    assert state.tenant_db is not None
    async with state.tenant_db.acquire() as conn:
        result = await conn.execute_core(
            select(user_role_assignments.c.user_id).where(
                user_role_assignments.c.role_id == PLATFORM_ADMIN_ROLE
            )
        )
        platform_admins = {r[0] for r in result.fetchall()}
    async with admin_db.acquire() as conn:
        result = await conn.execute_core(
            select(superadmin_bootstrap.c.user_id).where(superadmin_bootstrap.c.id == 1)
        )
        claimant = result.scalar()
    if claimant is not None:
        platform_admins.add(claimant)
    if user_id in platform_admins and len(platform_admins) == 1:
        raise ApiError(
            409,
            "auth.last_platform_admin",
            (
                "You are the last platform_admin of this deployment. Grant platform_admin to "
                "another user before deleting your account."
            ),
        )

    for org_id in member_org_ids:
        await remove_from_org(admin_db, tenant_dbs[org_id], user_id, org_id)

    tombstone = tombstone_id(user_id)
    async with admin_db.acquire() as conn:
        # Tombstone rather than NULL: org_invites.created_by is NOT NULL, and a dangling id is
        # worse than an explicit one. The row stays referentially intact and stops naming anyone.
        await conn.execute_core(
            update(orgs).where(orgs.c.created_by == user_id).values(created_by=tombstone)
        )
        await conn.execute_core(
            update(org_invites)
            .where(org_invites.c.created_by == user_id)
            .values(created_by=tombstone)
        )
        await conn.execute_core(
            update(org_invites).where(org_invites.c.used_by == user_id).values(used_by=tombstone)
        )
        await conn.execute_core(delete(user_profiles).where(user_profiles.c.user_id == user_id))
        await conn.execute_core(delete(local_users).where(local_users.c.id == user_id))
    # REQ-1394: the verifier outlives no user. Left behind it would keep a deleted name negotiable
    # over pgwire and would collide with the next user given that username.
    await delete_verifier(admin_db, user_id)
    # Audit attributions carry the tombstone too. Audit entries are NEVER deleted (REQ-1312) — a
    # trail that erases on request is not a trail.
    for org_id in member_org_ids:
        async with tenant_dbs[org_id].acquire() as conn:
            await conn.execute_core(
                update(query_audit_log)
                .where(query_audit_log.c.user_id == user_id)
                .values(user_id=tombstone)
            )
            await conn.execute_core(
                update(admin_audit_log)
                .where(admin_audit_log.c.actor_id == user_id)
                .values(actor_id=tombstone)
            )
            await conn.execute_core(
                update(admin_audit_log)
                .where(admin_audit_log.c.subject_id == user_id)
                .values(subject_id=tombstone)
            )
    return {"deleted": user_id, "tombstone": tombstone, "left_orgs": member_org_ids}
