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

Two reads serve the picker, gated as a write: the roles a person holds, with where each came from
(``/roles``), and the kinds the write accepts with the names it picks from (``/kinds``).
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
from provisa.entitlements import pending, people, roles, store
from provisa.entitlements.effective import (
    ExpiryPreview,
    access_view,
    capability_views,
    model_index,
    preview_expiry,
)
from provisa.entitlements.index import (
    KIND_ORDER,
    KINDS,
    SEMANTIC_LISTS,
    TREE_DEPTH,
    WILDCARD,
    capability_scopes,
    names_tree,
)
from provisa.entitlements.lists import ExpiryRule, HeldEntitlements
from provisa.entitlements.runtime import reaches_role, role_defaults
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


class AddAccessBody(BaseModel):
    """Add access: only what is new, appended to the person's list (REQ-2219)."""

    principal: str
    role_id: str
    kind: str
    identifiers: list[str]
    # The capabilities the until date ends the added identifiers for; None: every one.
    capabilities: list[str] | None = None
    until: AwareDatetime | None = None
    reason: str


class _RuleBody(BaseModel):
    """What an expiry rule says: at ``ends_at``, ``ends`` ends ({capability or *: [identifier]};
    {} ends the whole grant)."""

    ends_at: AwareDatetime
    ends: dict[str, list[str]]


class ExpiryBody(_RuleBody):
    principal: str
    role_id: str
    reason: str


class ChangeExpiryBody(_RuleBody):
    reason: str


class EndExpiryBody(BaseModel):
    reason: str


class PreviewBody(_RuleBody):
    principal: str
    role_id: str
    # The rule being edited, left out of the comparison; None for a new rule.
    rule_key: str | None = None


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
    _require_grant_write(request, state, body.role_id, body.reason)
    principal = await _resolve(request, state, body)
    _require_list_write(state, principal, body.role_id, body.kind)
    return principal


def _require_grant_write(request: Request, state: Any, role_id: str, reason: str) -> None:
    """Gate a write to a person's grant of ``role_id``: the right, the role, its reach, a reason."""
    require_capability_request(request, _ASSIGN)
    _require_role(state, role_id)
    if _caller(request) is not None and not reaches_role(state, role_id, {_ASSIGN}):
        raise _refuse_out_of_reach(role_id)
    if not reason.strip():
        raise ApiError(422, "entitlements.reason_required", "A reason is required")


def _require_list_write(state: Any, principal: str | None, role_id: str, kind: str) -> None:
    """Refuse a list write for a kind that is none, or one the identity provider sends."""
    if kind not in KINDS:
        raise ApiError(
            422,
            "entitlements.unknown_kind",
            f"{kind!r} is not a kind of entitlement list",
            kind=kind,
        )
    if principal is not None and store.idp_supersedes(
        state.idp_entitlements, principal, role_id, kind
    ):
        raise ApiError(
            409,
            "entitlements.idp_supersedes",
            f"The identity provider sends {principal!r}'s {kind!r} list for role "
            f"{role_id!r}: that list wins, and it cannot be changed here",
            principal=principal,
            role_id=role_id,
            kind=kind,
        )


def _check_list_expiry(kind: str, identifiers: list[str], expires_at: datetime | None) -> None:
    if any(not identifier.strip() for identifier in identifiers):
        raise ApiError(422, "entitlements.blank_identifier", "An identifier cannot be blank")
    if expires_at is None:
        return
    if expires_at <= datetime.now(UTC):
        raise ApiError(
            422,
            "entitlements.expiry_in_past",
            "An expiry must be in the future",
            expires_at=expires_at.isoformat(),
        )
    # An expiry ends objects in the mask: with no identifiers, or for a kind the mask does not
    # hold, the rule would remove nothing, and saving it would promise an end that never comes.
    if not identifiers or kind not in SEMANTIC_LISTS:
        raise ApiError(
            422,
            "entitlements.expiry_removes_nothing",
            f"An expiry on this {kind!r} list would remove nothing",
            kind=kind,
        )


def _check_set(body: SetListBody) -> None:
    _check_list_expiry(body.kind, body.identifiers, body.expires_at)


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


@router.get("/roles")
async def person_roles(principal: str, request: Request) -> dict:
    """The roles ``principal`` holds in the caller's org, within the roles the caller administers,
    each with where it came from: Provisa's assignments, the deployment's default assignments, or
    the identity provider (a role it names that the org does not define is left out: no list can
    be set on it). Gated as a write: it offers the role grants a list is set on."""
    from provisa.api.app import state

    require_capability_request(request, _ASSIGN)
    require_active_org_id(request)
    async with _db(state).acquire() as conn:
        held = await roles.held_roles(
            conn,
            principal,
            state.idp_entitlements.get(principal, {}),
            state.auth_config or {},
            state.roles,
        )
    if _caller(request) is not None:
        held = [r for r in held if reaches_role(state, r.role_id, {_ASSIGN})]
    return {"principal": principal, "roles": [asdict(r) for r in held]}


def _node(node: Any) -> dict:
    return {"id": node.id, "label": node.label, "children": [_node(c) for c in node.children]}


@router.get("/kinds")
async def list_kinds(request: Request) -> dict:
    """The kinds of list the write accepts, in the order a client lists them, each with whether an
    expiry can end it and how deep the names tree goes for it; and that tree, of the model's
    domains, tables and columns as the identifiers that select them (null before a model is
    built); and each capability with the kind of list that scopes it. Gated as a write: it is what a list is picked from."""
    from provisa.api.app import state

    require_capability_request(request, _ASSIGN)
    require_active_org_id(request)
    index = model_index(state)
    return {
        "kinds": [
            {"kind": kind, "semantic": kind in SEMANTIC_LISTS, "tree_depth": TREE_DEPTH.get(kind)}
            for kind in KIND_ORDER
        ],
        "names": None if index is None else [_node(n) for n in names_tree(index)],
        # Each capability and the kind that scopes it; null: unscoped, held wherever the role is.
        "capability_scopes": capability_scopes(),
    }


@router.post("/lists/add")
async def add_access(body: AddAccessBody, request: Request) -> dict:
    """Add access: append only what is new to the principal's list for a role grant and kind,
    without restating the rest; with ``until``, an expiry rule ending what was added, for
    ``capabilities`` (every one when left out), at that date. Where Provisa holds no list the
    person has the role's default, and the additions are appended to that."""
    from provisa.api.app import state

    _require_grant_write(request, state, body.role_id, body.reason)
    if not body.principal.strip():
        raise ApiError(422, "entitlements.principal_required", "A principal is required")
    _require_list_write(state, body.principal, body.role_id, body.kind)
    _check_list_expiry(body.kind, body.identifiers, body.until)
    actor = _caller(request) or _ANONYMOUS
    base = role_defaults(state.roles[body.role_id]).get(body.kind, [WILDCARD])
    db = _db(state)
    async with db.acquire() as conn:
        async with conn.transaction():
            added = await store.add_to_list(
                conn,
                body.principal,
                body.role_id,
                body.kind,
                body.identifiers,
                base=base,
                capabilities=body.capabilities,
                until=body.until,
                set_by=actor,
                reason=body.reason,
            )
        state.held_entitlements = await store.load_held(conn)
    await record_admin_action(
        db,
        action="entitlements.add",
        actor_id=actor,
        subject_id=body.principal,
        detail={
            "role_id": body.role_id,
            "kind": body.kind,
            "identifiers": list(added),
            "capabilities": body.capabilities,
            "reason": body.reason,
            "until": body.until.isoformat() if body.until else None,
        },
    )
    return _view(state, body.principal, body.role_id)


@router.get("/lists/history")
async def list_history(principal: str, role_id: str, request: Request) -> list[dict]:
    """What was added to ``principal``'s lists for ``role_id``, oldest first: per change, the kind,
    the identifiers it added, who set it and why, when, and until when."""
    from provisa.api.app import state

    _require_role(state, role_id)
    if not _visible_roles(request, state, principal, {role_id}):
        raise _refuse_out_of_reach(role_id)
    async with _db(state).acquire() as conn:
        history = await store.list_history(conn, principal, role_id)
    return [asdict(a) for a in history]


def _no_model() -> ApiError:
    return ApiError(
        409,
        "entitlements.no_model",
        "There is no model yet to expand entitlement lists against",
    )


@router.get("/effective/capabilities")
async def effective_capabilities(principal: str, role_ids: str, request: Request) -> list[dict]:
    """``principal``'s effective access per capability acting as ``role_ids`` (comma-separated):
    the union over those roles of where each capability applies. Per capability: the kind that
    scopes it (null: unscoped), the identifiers it covers (null: unscoped), per role the origin of
    each list that applied, and the model version the expansion belongs to."""
    from provisa.api.app import state

    named = list(dict.fromkeys(r for r in (s.strip() for s in role_ids.split(",")) if r))
    for role_id in named:
        _require_role(state, role_id)
    visible = _visible_roles(request, state, principal, set(named))
    for role_id in named:
        if role_id not in visible:
            raise _refuse_out_of_reach(role_id)
    if state.names_table is None:
        raise _no_model()
    held = state.held_entitlements.get(principal, _NOTHING_HELD)
    return [
        asdict(view) for view in capability_views(state, principal, named, held, datetime.now(UTC))
    ]


def _preview(
    state: Any,
    principal: str,
    role_id: str,
    body: _RuleBody,
    replacing: str | None = None,
) -> ExpiryPreview:
    if state.names_table is None:
        raise _no_model()
    return preview_expiry(state, principal, role_id, body.ends_at, body.ends, replacing=replacing)


def _refuse_removes_nothing(principal: str, role_id: str) -> ApiError:
    # REQ-2219: a rule that would remove nothing is flagged instead of saved.
    return ApiError(
        422,
        "entitlements.expiry_removes_nothing",
        f"This rule would remove nothing from {principal!r}'s grant of {role_id!r} on its date",
        principal=principal,
        role_id=role_id,
    )


def _rule(state: Any, rule_key: str) -> tuple[str, ExpiryRule]:
    """The rule ``rule_key`` names and whose grant it is on."""
    for principal, held in state.held_entitlements.items():
        for rule in held.expiries:
            if rule.rule_key == rule_key:
                return principal, rule
    raise ApiError(
        404,
        "entitlements.expiry_not_found",
        f"No expiry rule {rule_key!r}",
        rule_key=rule_key,
    )


@router.post("/expiries/preview")
async def preview(body: PreviewBody, request: Request) -> dict:
    """A dry run of an expiry rule: per capability, the identifiers it would end on its date of
    what the person would reach then, and whether it would remove nothing. ``rule_key`` names the
    rule being edited, which the comparison leaves out."""
    from provisa.api.app import state

    require_capability_request(request, _ASSIGN)
    _require_role(state, body.role_id)
    if _caller(request) is not None and not reaches_role(state, body.role_id, {_ASSIGN}):
        raise _refuse_out_of_reach(body.role_id)
    return asdict(_preview(state, body.principal, body.role_id, body, body.rule_key))


@router.post("/expiries")
async def add_expiry(body: ExpiryBody, request: Request) -> dict:
    """Add an expiry rule to a principal's grant of a role. A rule that would remove nothing on
    its date is refused by name."""
    from provisa.api.app import state

    _require_grant_write(request, state, body.role_id, body.reason)
    if not body.principal.strip():
        raise ApiError(422, "entitlements.principal_required", "A principal is required")
    if _preview(state, body.principal, body.role_id, body).removes_nothing:
        raise _refuse_removes_nothing(body.principal, body.role_id)
    actor = _caller(request) or _ANONYMOUS
    db = _db(state)
    async with db.acquire() as conn:
        rule_key = await store.add_expiry(
            conn,
            body.principal,
            body.role_id,
            body.ends_at,
            body.ends,
            set_by=actor,
            reason=body.reason,
        )
        state.held_entitlements = await store.load_held(conn)
    await record_admin_action(
        db,
        action="entitlements.expiry_added",
        actor_id=actor,
        subject_id=body.principal,
        detail={
            "rule_key": rule_key,
            "role_id": body.role_id,
            "ends_at": body.ends_at.isoformat(),
            "ends": body.ends,
            "reason": body.reason,
        },
    )
    return _view(state, body.principal, body.role_id)


@router.put("/expiries/{rule_key}")
async def change_expiry(rule_key: str, body: ChangeExpiryBody, request: Request) -> dict:
    """Change an expiry rule: its date (extend), what it ends, or both. A change that would remove
    nothing on its date is refused by name."""
    from provisa.api.app import state

    require_capability_request(request, _ASSIGN)
    principal, rule = _rule(state, rule_key)
    _require_grant_write(request, state, rule.role_id, body.reason)
    if _preview(state, principal, rule.role_id, body, rule_key).removes_nothing:
        raise _refuse_removes_nothing(principal, rule.role_id)
    actor = _caller(request) or _ANONYMOUS
    db = _db(state)
    async with db.acquire() as conn:
        await store.change_expiry(
            conn, rule, principal, body.ends_at, body.ends, set_by=actor, reason=body.reason
        )
        state.held_entitlements = await store.load_held(conn)
    await record_admin_action(
        db,
        action="entitlements.expiry_changed",
        actor_id=actor,
        subject_id=principal,
        detail={
            "rule_key": rule_key,
            "role_id": rule.role_id,
            "ends_at": body.ends_at.isoformat(),
            "ends": body.ends,
            "reason": body.reason,
        },
    )
    return _view(state, principal, rule.role_id)


@router.delete("/expiries/{rule_key}")
async def withdraw_expiry(rule_key: str, body: EndExpiryBody, request: Request) -> dict:
    """Withdraw an expiry rule from a principal's grant."""
    from provisa.api.app import state

    require_capability_request(request, _ASSIGN)
    principal, rule = _rule(state, rule_key)
    _require_grant_write(request, state, rule.role_id, body.reason)
    actor = _caller(request) or _ANONYMOUS
    db = _db(state)
    async with db.acquire() as conn:
        await store.withdraw_expiry(conn, rule, principal, set_by=actor, reason=body.reason)
        state.held_entitlements = await store.load_held(conn)
    await record_admin_action(
        db,
        action="entitlements.expiry_withdrawn",
        actor_id=actor,
        subject_id=principal,
        detail={"rule_key": rule_key, "role_id": rule.role_id, "reason": body.reason},
    )
    return _view(state, principal, rule.role_id)
