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
"""

# Requirements: REQ-2219

from __future__ import annotations

from dataclasses import asdict
from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Request
from pydantic import AwareDatetime, BaseModel

from provisa.api.admin.capabilities import _resolved_capabilities, require_capability_request
from provisa.api.errors import ApiError
from provisa.core.org_membership import record_admin_action
from provisa.entitlements import store
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
    principal: str
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


def _require_write(request: Request, state: Any, body: _ListKey) -> None:
    """Gate and check what every write shares, the identity provider's precedence last."""
    require_capability_request(request, _ASSIGN)
    _require_role(state, body.role_id)
    if _caller(request) is not None and not reaches_role(state, body.role_id, {_ASSIGN}):
        raise _refuse_out_of_reach(body.role_id)
    if not body.principal.strip():
        raise ApiError(422, "entitlements.principal_required", "A principal is required")
    if not body.reason.strip():
        raise ApiError(422, "entitlements.reason_required", "A reason is required")
    if body.kind not in KINDS:
        raise ApiError(
            422,
            "entitlements.unknown_kind",
            f"{body.kind!r} is not a kind of entitlement list",
            kind=body.kind,
        )
    if body.kind in state.idp_entitlements.get(body.principal, {}).get(body.role_id, {}):
        raise ApiError(
            409,
            "entitlements.idp_supersedes",
            f"The identity provider sends {body.principal!r}'s {body.kind!r} list for role "
            f"{body.role_id!r}: that list wins, and it cannot be changed here",
            principal=body.principal,
            role_id=body.role_id,
            kind=body.kind,
        )


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


@router.put("/lists")
async def set_list(body: SetListBody, request: Request) -> dict:
    """Set a principal's list for a role grant and kind, replacing what they held. The expiry, if
    any, replaces the one the list was last set with. Takes effect on the next request."""
    from provisa.api.app import state

    _require_write(request, state, body)
    _check_set(body)
    actor = _caller(request) or _ANONYMOUS
    db = _db(state)
    async with db.acquire() as conn:
        async with conn.transaction():
            await store.withdraw_list_expiries(
                conn, body.principal, body.role_id, body.kind, set_by=actor, reason=body.reason
            )
            await store.set_list(
                conn,
                body.principal,
                body.role_id,
                body.kind,
                body.identifiers,
                set_by=actor,
                reason=body.reason,
            )
            if body.expires_at is not None:
                await store.add_expiry(
                    conn,
                    body.principal,
                    body.role_id,
                    body.expires_at,
                    {"*": body.identifiers},
                    set_by=actor,
                    reason=body.reason,
                    list_kind=body.kind,
                )
        # This worker now; the write advances the org's config stamp, which reloads the others.
        state.held_entitlements = await store.load_held(conn)
    await record_admin_action(
        db,
        action="entitlements.set",
        actor_id=actor,
        subject_id=body.principal,
        detail={
            "role_id": body.role_id,
            "kind": body.kind,
            "identifiers": body.identifiers,
            "reason": body.reason,
            "expires_at": body.expires_at.isoformat() if body.expires_at else None,
        },
    )
    return _view(state, body.principal, body.role_id)


@router.post("/lists/end")
async def end_list(body: EndListBody, request: Request) -> dict:
    """End a principal's list for a role grant and kind, and the expiry it was set with. Takes
    effect on the next request."""
    from provisa.api.app import state

    _require_write(request, state, body)
    actor = _caller(request) or _ANONYMOUS
    db = _db(state)
    async with db.acquire() as conn:
        async with conn.transaction():
            await store.withdraw_list_expiries(
                conn, body.principal, body.role_id, body.kind, set_by=actor, reason=body.reason
            )
            await store.remove_list(
                conn, body.principal, body.role_id, body.kind, set_by=actor, reason=body.reason
            )
        state.held_entitlements = await store.load_held(conn)
    await record_admin_action(
        db,
        action="entitlements.end",
        actor_id=actor,
        subject_id=body.principal,
        detail={"role_id": body.role_id, "kind": body.kind, "reason": body.reason},
    )
    return _view(state, body.principal, body.role_id)


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
