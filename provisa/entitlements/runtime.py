# Copyright (c) 2026 Kenneth Stott
# Canary: b1f92ec3-2c80-42f3-b547-8a49ee6aa3a5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A request's mask: what every surface calls (REQ-2219).

A transport that authenticates a person binds them once per call
(:func:`bind_entitlements`, or :func:`entitlements_scope`), beside its session variables. Then:

* :func:`request_mask` is the person's mask over a role's schema, or None for work no person is
  doing (a model build, a scheduled job), which is not masked;
* :func:`current_mask_hash` is what a cache key includes, empty when nothing is narrowed;
* :func:`govern_for_request` narrows a role's governance context to the request's mask;
* :func:`warnings` is what a sign-in or the names report shows of a person's lists.
"""

# Requirements: REQ-2219

from __future__ import annotations

import logging
from collections.abc import Collection, Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from provisa.entitlements.index import (
    WILDCARD,
    Finding,
    build_index,
    check_lists,
    covers,
    matches,
)
from provisa.entitlements.lists import (
    IDP,
    ExpiryRule,
    HeldEntitlements,
    IdpLists,
    effective_lists,
    held_version,
    idp_lists_hash,
    parse_idp_entitlements,
    rule_ends,
)
from provisa.entitlements.mask import EntitlementMask, MemberInput, compute_mask
from provisa.security.rights import role_ids_from_claims

log = logging.getLogger(__name__)

_NOTHING_HELD = HeldEntitlements()


@dataclass(frozen=True)
class EntitlementRequest:
    """One person's identity-provider lists, as bound for a call. What Provisa holds is read
    where the mask is, in the org the request is bound to."""

    user_id: str
    idp: IdpLists
    idp_hash: str
    acting_roles: tuple[str, ...] = ()


_current: ContextVar[EntitlementRequest | None] = ContextVar("provisa_entitlements", default=None)


def bind_entitlements(identity: Any) -> Token:
    """Bind ``identity``'s lists for the call being served in this context. A token whose lists
    cannot be read raises :class:`~provisa.entitlements.lists.EntitlementClaimError`."""
    idp = parse_idp_entitlements(identity.raw_claims, identity.entitlements_claim)
    acting = tuple(role_ids_from_claims(getattr(identity, "acting_roles", None) or ()))
    return _current.set(EntitlementRequest(identity.user_id, idp, idp_lists_hash(idp), acting))


def reset_entitlements(token: Token) -> None:
    _current.reset(token)


@contextmanager
def entitlements_scope(identity: Any) -> Iterator[None]:
    """:func:`bind_entitlements` for the block."""
    token = bind_entitlements(identity)
    try:
        yield
    finally:
        reset_entitlements(token)


def captured_entitlements() -> dict | None:
    """The bound person's lists as JSON, to bind again where the call's context is not (a minted
    pgwire session, REQ-2226): without them that session would see the whole role."""
    request = _current.get()
    if request is None:
        return None
    return {"user_id": request.user_id, "idp": request.idp, "idp_hash": request.idp_hash}


@contextmanager
def restored_entitlements(captured: dict | None) -> Iterator[None]:
    """Bind what :func:`captured_entitlements` captured, for the block."""
    if captured is None:
        yield
        return
    idp = {
        kind: {name: tuple(values) for name, values in lists.items()}
        for kind, lists in captured["idp"].items()
    }
    token = _current.set(EntitlementRequest(captured["user_id"], idp, captured["idp_hash"]))
    try:
        yield
    finally:
        _current.reset(token)


def _role_context(state: Any, role_id: str) -> Any:
    """The compilation context of ``role_id``'s schema, or None for a role with no schema (a
    qualifier a meta-role carries, such as a region's derived role)."""
    from provisa.api.role_surface import role_surface  # noqa: PLC0415

    surface = role_surface(state, role_id)
    return None if surface is None else surface.context


def role_defaults(role: Mapping[str, Any]) -> Mapping[str, list[str]]:
    """The role's default lists. Defaults naming exactly the domains the role is offered to are
    the role itself, so a plain role grant is unchanged; they narrow nothing."""
    defaults = dict(role.get("default_entitlements") or {})
    domains = defaults.get("domains")
    offered = role.get("domain_access")
    if domains is not None and offered is not None and set(domains) == set(offered):
        defaults.pop("domains")
    return defaults


def _members(role_id: str) -> tuple[str, ...]:
    from provisa.security.meta_role import members_of  # noqa: PLC0415

    return members_of(role_id)


def _compute(
    state: Any,
    role_id: str,
    request: EntitlementRequest,
    held: HeldEntitlements,
    version: tuple,
    now: datetime,
) -> EntitlementMask:
    cache = state.entitlement_masks
    names = state.names_table
    products = tuple(state.data_product_refs)

    def index_of(role: str, context: Any) -> Any:
        return cache.index((version, role), lambda: build_index(names, context, products))

    context = _role_context(state, role_id)
    if context is None:
        raise RuntimeError(f"role {role_id!r} has no schema to compute entitlements against")
    model_index = cache.index((version, "*"), lambda: build_index(names, None, products))
    members: list[MemberInput] = []
    findings: list[Finding] = []
    for member in _members(role_id):
        member_context = _role_context(state, member)
        role = state.roles.get(member)
        if member_context is None or role is None:
            continue  # a qualifier with no schema of its own reaches nothing
        lists = effective_lists(member, request.idp, held, role_defaults(role))
        findings.extend(check_lists(member, role, lists, model_index))
        expiries = tuple(r for r in held.expiries if r.role_id == member)
        members.append(MemberInput(member, lists, index_of(member, member_context), expiries))
    return compute_mask(role_id, index_of(role_id, context), members, version, now, tuple(findings))


def principal_mask(
    state: Any, principal: str, role_id: str, held: HeldEntitlements, now: datetime
) -> EntitlementMask | None:
    """``principal``'s mask over ``role_id``'s schema at ``now``, from the identity provider's
    lists as the store mirrors them and ``held``: what the admin API reads for a person who need
    not be the caller. None for a role with no schema. Not cached: ``held`` may be a preview."""
    if _role_context(state, role_id) is None:
        return None
    if state.names_table is None:
        raise RuntimeError("there is no model yet to expand entitlement lists against")
    idp = state.idp_entitlements.get(principal, {})
    request = EntitlementRequest(principal, idp, idp_lists_hash(idp))
    version = (state.schema_boot_id, state.schema_version)
    return _compute(state, role_id, request, held, version, now)


def request_mask(state: Any, role_id: str) -> EntitlementMask | None:
    """The bound person's mask over ``role_id``'s schema; None when no person is bound, or when
    there is no model yet and they carry no lists. Lists with no names table to expand them
    against raise: they must not widen to the whole role."""
    request = _current.get()
    if request is None:
        return None
    held = state.held_entitlements.get(request.user_id, _NOTHING_HELD)
    if state.names_table is None:
        if request.idp or held.lists:
            raise RuntimeError(
                "entitlement lists are bound but this model has no names table to expand them"
            )
        return None
    if (
        not request.idp
        and not held.lists
        and not held.expiries
        and not any(role_defaults(state.roles.get(member, {})) for member in _members(role_id))
    ):
        return None  # a plain role grant: nothing narrows it and nothing needs computing
    version = (state.schema_boot_id, state.schema_version)
    key = (role_id, request.idp_hash, held_version(held), version)
    now = datetime.now(UTC)

    def build() -> EntitlementMask:
        mask = _compute(state, role_id, request, held, version, now)
        for finding in mask.findings:
            log.warning("entitlements: %s (%s)", finding.message, finding.code)
        return mask

    return state.entitlement_masks.get_or_build(key, build, now=now)


@dataclass(frozen=True)
class ListReach:
    """Where a list-scoped capability reaches through one role grant at a moment: its list, less
    what the expiry rules passed by then end of it (REQ-2219, EXPIRY RULES END EVERY CAPABILITY
    THEY NAME). A grant that ended is no longer held, so its reach is never asked."""

    identifiers: tuple[str, ...]
    ended: tuple[str, ...]

    def reaches(self, paths: tuple[tuple[str, ...], ...]) -> bool:
        return any(matches(i, paths) for i in self.identifiers) and not any(
            matches(e, paths) for e in self.ended
        )

    def kept(self) -> tuple[str, ...]:
        """The listed identifiers no ended one covers whole."""
        return tuple(i for i in self.identifiers if not any(covers(e, i) for e in self.ended))


def list_reach(
    identifiers: tuple[str, ...] | None,
    origin: str | None,
    rules: Collection[ExpiryRule],
    capability: str,
    kind: str,
    at: datetime,
) -> ListReach:
    """``capability``'s reach through a role grant whose ``kind`` list is ``identifiers`` (None:
    no list, every one) from ``origin``, with ``rules`` (that grant's) applied as of ``at``. The
    identity provider's own expiry counts for its lists: Provisa's rules never touch them."""
    listed = (WILDCARD,) if identifiers is None else identifiers
    if origin == IDP:
        return ListReach(listed, ())
    ended = [i for rule in rules if rule.ends_at <= at for i in rule_ends(rule, capability, kind)]
    return ListReach(listed, tuple(dict.fromkeys(ended)))


def reaches_role(state: Any, target_role: str, capabilities: Collection[str]) -> bool:
    """Whether the bound person's ``roles`` lists reach ``target_role``, through a role they act as
    that holds one of ``capabilities`` (REQ-2219). A list naming a role covers it and every role
    with it as an ancestor; a role with no ``roles`` list reaches every role. An expiry rule passed
    on the grant ends what it names of that reach. Raises when no
    person is bound: this answers an authorization question, and no person cannot be read as
    anyone."""
    request = _current.get()
    if request is None:
        raise RuntimeError("no person is bound to this call: who may reach a role cannot be read")
    chain = state.role_chains.get(target_role, [target_role])
    paths = tuple((role.lower(),) for role in chain)
    held = state.held_entitlements.get(request.user_id, _NOTHING_HELD)
    wanted = set(capabilities)
    now = datetime.now(UTC)
    for role_id in request.acting_roles:
        for member in _members(role_id):
            role = state.roles.get(member)
            if role is None:
                continue
            listed = effective_lists(member, request.idp, held, role_defaults(role)).get("roles")
            rules = tuple(r for r in held.expiries if r.role_id == member)
            for capability in wanted & set(role.get("capabilities") or ()):
                reach = list_reach(
                    None if listed is None else listed.identifiers,
                    None if listed is None else listed.origin,
                    rules,
                    capability,
                    "roles",
                    now,
                )
                if reach.reaches(paths):
                    return True
    return False


def warnings(state: Any, role_id: str) -> tuple[Finding, ...]:
    """The flags on the bound person's lists for ``role_id``, for a sign-in or the names report."""
    mask = request_mask(state, role_id)
    return () if mask is None else mask.findings


def current_mask_hash(state: Any, role_id: str) -> str:
    """The hash a cache key includes: empty when no person is bound or the mask narrows nothing."""
    mask = request_mask(state, role_id)
    return "" if mask is None else mask.hash


def govern_for_request(governance: Any, state: Any, role_id: str) -> Any:
    """``governance`` (a per-read copy) narrowed to the bound person's mask over ``role_id``."""
    from provisa.compiler.entitlement_rule import narrow_governance  # noqa: PLC0415

    return narrow_governance(governance, request_mask(state, role_id))
