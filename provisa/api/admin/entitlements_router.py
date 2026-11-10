# Copyright (c) 2026 Kenneth Stott
# Canary: 9c03076d-7fa4-407f-9128-bc328ca0c328
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The one write API for a principal's entitlement lists (REQ-2219, ONE API, THREE WAYS TO USE IT).

Sets, lists and ends the lists Provisa holds per role grant and kind, each with a reason, an
optional expiry, who set it and an entry in the org's administrative trail; and reads effective
access with where each list came from. The UI is a client of this.

Setting needs ``assign_entitlements``, scoped by the caller's ``roles`` list. Everyone reads their
own; ``user_management`` or ``assign_entitlements`` reads others', within the roles it administers.
Where the identity provider sends a list for a role and kind, that list wins and a write there is
refused by name.

Principals, roles and kinds travel in the body and the query string: a principal is whatever the
sign-in token carries, and a slash in one must not change the route.

The picker finds people through one search by name or email over the org's local accounts and
known members (``/people``). A write names a principal, or an email: one that matches a person of
the org is that person's write, and one that matches no one is a pending grant, attached when a
sign-in token's email matches (``provisa.entitlements.pending``).
"""

# Requirements: REQ-2219

from __future__ import annotations

from dataclasses import asdict
from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Request
from pydantic import AwareDatetime, BaseModel

from provisa.api.admin._guards import require_active_org_id
from provisa.api.admin.capabilities import _resolved_capabilities, require_capability_request
from provisa.api.errors import ApiError
from provisa.core.org_membership import record_admin_action
from provisa.entitlements import pending, people, store
from provisa.entitlements.effective import access_view
from provisa.entitlements.index import KINDS, SEMANTIC_LISTS
from provisa.entitlements.lists import HeldEntitlements
from provisa.entitlements.runtime import reaches_role
from provisa.security.rights import Capability

router = APIRouter(prefix="/admin/entitlements", tags=["admin"])

_ASSIGN = Capability.ASSIGN_ENTITLEMENTS.value
_READ_OTHERS = frozenset({_ASSIGN, Capability.USER_MANAGEMENT.value})
_ANONYMOUS = "anonymous"
_NOTHING_HELD = HeldEntitlements()


class _ListKey(BaseModel):
    """Names a principal, or an email for a person who may not have been seen yet."""

    principal: str | None = None
    email: str | None = None
    role_id: str
    kind: str
    reason: str


class SetListBody(_ListKey):
    identifiers: list[str]
    expires_at: AwareDatetime | None = None


class EndListBody(_ListKey):
    pass


def _db(state: Any) -> Any:
    if state.model_db is None:
        raise ApiError(503, "entitlements.database_not_connected", "Database not connected")
    return state.model_db


def _caller(request: Request) -> str | None:
    """The signed-in caller's user id; None in dev/no-auth mode, where every gate is open."""
    user_id = getattr(getattr(request.state, "identity", None), "user_id", _ANONYMOUS)
    return None if user_id == _ANONYMOUS else user_id


def _refuse_out_of_reach(role_id: str) -> ApiError:
    return ApiError(
        403,
        "entitlements.role_out_of_reach",
        f"Role {role_id!r} is outside the roles this caller administers",
        role_id=role_id,
    )


def _require_role(state: Any, role_id: str) -> None:
    if role_id not in state.roles:
        raise ApiError(
            404, "entitlements.role_not_found", f"Role {role_id!r} not found", role_id=role_id
        )


def _admin_db(state: Any) -> Any:
    if state.admin_db is None:
        raise ApiError(503, "entitlements.database_not_connected", "Database not connected")
    return state.admin_db


async def _resolve(request: Request, state: Any, body: _ListKey) -> str | None:
    """The principal a write names: its own, or the one person of the org with its email. None
    when the email matches no one: the write is a pending grant."""
    if (body.principal is None) == (body.email is None):
        raise ApiError(
            422, "entitlements.principal_or_email", "Name a principal or an email, not both"
        )
    if body.principal is not None:
        if not body.principal.strip():
            raise ApiError(422, "entitlements.principal_required", "A principal is required")
        return body.principal
    assert body.email is not None
    if not body.email.strip():
        raise ApiError(422, "entitlements.email_required", "An email is required")
    matched = await people.with_email(
        _admin_db(state),
        _db(state),
        require_active_org_id(request),
        body.email,
        multitenancy=state.multitenancy,
    )
    if len(matched) > 1:
        raise ApiError(
            409,
            "entitlements.email_ambiguous",
            f"{body.email!r} is the email of more than one person: name the principal",
            email=body.email,
            principals=[p.principal for p in matched],
        )
    return matched[0].principal if matched else None


async def _require_write(request: Request, state: Any, body: _ListKey) -> str | None:
    """Gate and check what every write shares, the identity provider's precedence last. Returns
    the principal written, or None for a pending grant to ``body.email``."""
    require_capability_request(request, _ASSIGN)
    _require_role(state, body.role_id)
    if _caller(request) is not None and not reaches_role(state, body.role_id, {_ASSIGN}):
        raise _refuse_out_of_reach(body.role_id)
    principal = await _resolve(request, state, body)
    if not body.reason.strip():
        raise ApiError(422, "entitlements.reason_required", "A reason is required")
    if body.kind not in KINDS:
        raise ApiError(
            422,
            "entitlements.unknown_kind",
            f"{body.kind!r} is not a kind of entitlement list",
            kind=body.kind,
        )
    if principal is not None and store.idp_supersedes(
        state.idp_entitlements, principal, body.role_id, body.kind
    ):
        raise ApiError(
            409,
            "entitlements.idp_supersedes",
            f"The identity provider sends {principal!r}'s {body.kind!r} list for role "
            f"{body.role_id!r}: that list wins, and it cannot be changed here",
            principal=principal,
            role_id=body.role_id,
            kind=body.kind,
        )
    return principal


def _check_set(body: SetListBody) -> None:
    if any(not identifier.strip() for identifier in body.identifiers):
        raise ApiError(422, "entitlements.blank_identifier", "An identifier cannot be blank")
    if body.expires_at is None:
        return
    if body.expires_at <= datetime.now(UTC):
        raise ApiError(
            422,
            "entitlements.expiry_in_past",
            "An expiry must be in the future",
            expires_at=body.expires_at.isoformat(),
        )
    # An expiry ends objects in the mask: with no identifiers, or for a kind the mask does not
    # hold, the rule would remove nothing, and saving it would promise an end that never comes.
    if not body.identifiers or body.kind not in SEMANTIC_LISTS:
        raise ApiError(
            422,
            "entitlements.expiry_removes_nothing",
            f"An expiry on this {body.kind!r} list would remove nothing",
            kind=body.kind,
        )


def _view(state: Any, principal: str, role_id: str) -> dict:
    return asdict(access_view(state, principal, role_id, datetime.now(UTC)))


def _pending_view(grant: pending.PendingGrant) -> dict:
    return {
        "pending": True,
        "email": grant.email,
        "role_id": grant.role_id,
        "kind": grant.kind,
        "identifiers": list(grant.identifiers),
        "expires_at": grant.expires_at.isoformat() if grant.expires_at else None,
        "set_by": grant.set_by,
        "reason": grant.reason,
    }


async def _set_pending(state: Any, body: SetListBody, actor: str) -> dict:
    assert body.email is not None
    db = _db(state)
    async with db.acquire() as conn:
        grant = await pending.set_pending(
            conn,
            body.email,
            body.role_id,
            body.kind,
            body.identifiers,
            expires_at=body.expires_at,
            set_by=actor,
            reason=body.reason,
        )
    await record_admin_action(
        db,
        action="entitlements.set_pending",
        actor_id=actor,
        subject_id=grant.email,
        detail={
            "role_id": body.role_id,
            "kind": body.kind,
            "identifiers": body.identifiers,
            "reason": body.reason,
            "expires_at": body.expires_at.isoformat() if body.expires_at else None,
        },
    )
    return _pending_view(grant)


@router.put("/lists")
async def set_list(body: SetListBody, request: Request) -> dict:
    """Set a principal's list for a role grant and kind, replacing what they held. The expiry, if
    any, replaces the one the list was last set with. Takes effect on the next request."""
    from provisa.api.app import state

    principal = await _require_write(request, state, body)
    _check_set(body)
    actor = _caller(request) or _ANONYMOUS
    if principal is None:
        return await _set_pending(state, body, actor)
    db = _db(state)
    async with db.acquire() as conn:
        async with conn.transaction():
            await store.write_list(
                conn,
                principal,
                body.role_id,
                body.kind,
                body.identifiers,
                expires_at=body.expires_at,
                set_by=actor,
                reason=body.reason,
            )
        # This worker now; the write advances the org's config stamp, which reloads the others.
        state.held_entitlements = await store.load_held(conn)
    await record_admin_action(
        db,
        action="entitlements.set",
        actor_id=actor,
        subject_id=principal,
        detail={
            "role_id": body.role_id,
            "kind": body.kind,
            "identifiers": body.identifiers,
            "reason": body.reason,
            "expires_at": body.expires_at.isoformat() if body.expires_at else None,
        },
    )
    return _view(state, principal, body.role_id)


@router.post("/lists/end")
async def end_list(body: EndListBody, request: Request) -> dict:
    """End a principal's list for a role grant and kind, and the expiry it was set with. Takes
    effect on the next request."""
    from provisa.api.app import state

    principal = await _require_write(request, state, body)
    actor = _caller(request) or _ANONYMOUS
    db = _db(state)
    if principal is None:
        assert body.email is not None
        async with db.acquire() as conn:
            grant = await pending.end_pending(
                conn, body.email, body.role_id, body.kind, set_by=actor, reason=body.reason
            )
        if grant is None:
            raise ApiError(
                404,
                "entitlements.pending_not_found",
                f"No grant waits on {body.email!r} for role {body.role_id!r} and {body.kind!r}",
                email=body.email,
                role_id=body.role_id,
                kind=body.kind,
            )
        await record_admin_action(
            db,
            action="entitlements.end_pending",
            actor_id=actor,
            subject_id=grant.email,
            detail={"role_id": body.role_id, "kind": body.kind, "reason": body.reason},
        )
        return {**_pending_view(grant), "pending": False}
    async with db.acquire() as conn:
        async with conn.transaction():
            await store.withdraw_list_expiries(
                conn, principal, body.role_id, body.kind, set_by=actor, reason=body.reason
            )
            await store.remove_list(
                conn, principal, body.role_id, body.kind, set_by=actor, reason=body.reason
            )
        state.held_entitlements = await store.load_held(conn)
    await record_admin_action(
        db,
        action="entitlements.end",
        actor_id=actor,
        subject_id=principal,
        detail={"role_id": body.role_id, "kind": body.kind, "reason": body.reason},
    )
    return _view(state, principal, body.role_id)


@router.get("/people")
async def find_people(q: str, request: Request) -> list[dict]:
    """The picker's search: the org's local accounts and known members whose name or email
    contains ``q``, each as the principal a sign-in token carries, with name and email for
    display only. Gated as a write: it finds whom to grant to."""
    from provisa.api.app import state

    require_capability_request(request, _ASSIGN)
    found = await people.search(
        _admin_db(state),
        _db(state),
        require_active_org_id(request),
        q,
        multitenancy=state.multitenancy,
    )
    return [asdict(person) for person in found]


@router.get("/pending")
async def list_pending(request: Request) -> list[dict]:
    """The grants waiting on a typed email, within the roles the caller administers."""
    from provisa.api.app import state

    require_capability_request(request, _ASSIGN)
    async with _db(state).acquire() as conn:
        waiting = await pending.load_pending(conn)
    if _caller(request) is not None:
        waiting = [g for g in waiting if reaches_role(state, g.role_id, {_ASSIGN})]
    return [_pending_view(grant) for grant in waiting]


def _visible_roles(request: Request, state: Any, principal: str, roles: set[str]) -> set[str]:
    """Which of ``roles`` the caller may read ``principal``'s lists for: all of a person's own,
    and within the roles it administers for anyone else's. Refuses a caller with neither right."""
    caller = _caller(request)
    if caller is None or caller == principal:
        return roles
    held = _resolved_capabilities(request.state.identity, state)
    if not _READ_OTHERS & held:
        raise ApiError(
            403,
            "auth.missing_capability",
            f"Missing capability: {_ASSIGN!r} or {Capability.USER_MANAGEMENT.value!r}",
        )
    return {role for role in roles if reaches_role(state, role, _READ_OTHERS)}


@router.get("/lists")
async def list_lists(principal: str, request: Request) -> dict:
    """The lists Provisa holds for ``principal``, with who set each and why, and their expiries.
    What the identity provider sends is read through ``/effective``."""
    from provisa.api.app import state

    held = state.held_entitlements.get(principal, _NOTHING_HELD)
    named = {r.role_id for r in held.lists} | {r.role_id for r in held.expiries}
    visible = _visible_roles(request, state, principal, named)
    return {
        "principal": principal,
        "lists": [
            {
                "role_id": r.role_id,
                "kind": r.kind,
                "identifiers": list(r.identifiers),
                "capability": r.capability,
                "set_by": r.set_by,
                "reason": r.reason,
            }
            for r in held.lists
            if r.role_id in visible
        ],
        "expiries": [
            {
                "rule_key": r.rule_key,
                "role_id": r.role_id,
                "ends_at": r.ends_at,
                "ends": {capability: list(ids) for capability, ids in r.ends.items()},
                "list_kind": r.list_kind,
                "reason": r.reason,
            }
            for r in held.expiries
            if r.role_id in visible
        ],
    }


@router.get("/effective")
async def effective(principal: str, role_id: str, request: Request) -> dict:
    """``principal``'s effective lists for ``role_id``: per kind what applies, where it came from
    (the identity provider, Provisa or the role), what it superseded and what an expiry ended."""
    from provisa.api.app import state

    _require_role(state, role_id)
    if not _visible_roles(request, state, principal, {role_id}):
        raise _refuse_out_of_reach(role_id)
    return _view(state, principal, role_id)
