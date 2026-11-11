# Copyright (c) 2026 Kenneth Stott
# Canary: d792e273-6e9a-4bbd-9024-b9e7d94b674c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a person can reach through one role grant, and where each list came from (REQ-2219).

What the admin API answers with, for a person who need not be the caller: so the identity
provider's lists are the store's mirror of their last sign-in, where enforcement reads the token.
"""

# Requirements: REQ-2219

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any

from provisa.entitlements.index import (
    DATA_PLANE_CAPABILITIES,
    WILDCARD,
    Finding,
    ObjectIndex,
    build_index,
    capability_scopes,
    check_lists,
    covers,
    matches,
    overlaps,
)
from provisa.entitlements.lists import (
    IDP,
    PROVISA,
    EffectiveList,
    ExpiryRule,
    HeldEntitlements,
    effective_lists,
)
from provisa.entitlements.mask import EntitlementMask
from provisa.entitlements.runtime import ListReach, list_reach, principal_mask, role_defaults

_NOTHING_HELD = HeldEntitlements()
_EVERY_CAPABILITY = "*"


@dataclass(frozen=True)
class ListView:
    """One kind's list: what applies, where it came from, and what a passed expiry ended of it."""

    kind: str
    identifiers: tuple[str, ...]
    origin: str
    superseded: tuple[str, ...] | None
    ended: tuple[str, ...]
    set_by: str | None
    reason: str | None


@dataclass(frozen=True)
class RuleView:
    rule_key: str | None
    ends_at: datetime
    ends: dict[str, tuple[str, ...]]
    list_kind: str | None
    reason: str | None
    passed: bool


@dataclass(frozen=True)
class AccessView:
    principal: str
    role_id: str
    lists: tuple[ListView, ...]
    expiries: tuple[RuleView, ...]
    # A passed rule that ends the whole grant: nothing below is reached, whatever a list says.
    grant_ended: bool
    findings: tuple[Finding, ...]


def _ended_by(rule: ExpiryRule, identifiers: tuple[str, ...]) -> set[str]:
    """Which of ``identifiers`` a passed ``rule`` ends for every capability. A rule on one
    capability leaves the others, so it ends no identifier outright; it is listed with the rules."""
    if not rule.ends:
        return set(identifiers)
    ended = rule.ends.get(_EVERY_CAPABILITY, ())
    return {
        identifier
        for identifier in identifiers
        if any(matches(e, (tuple(identifier.lower().split(".")),)) for e in ended)
    }


def model_index(state: Any) -> ObjectIndex | None:
    """The whole model's objects, which lists are checked against and the picker offers; None
    when there is no model yet."""
    if state.names_table is None:
        return None
    version = (state.schema_boot_id, state.schema_version)
    return state.entitlement_masks.index(
        (version, "*"),
        lambda: build_index(state.names_table, None, tuple(state.data_product_refs)),
    )


def _findings(state: Any, role_id: str, role: dict, lists: dict) -> tuple[Finding, ...]:
    """The flags on these lists, or none when there is no model yet to check them against."""
    index = model_index(state)
    if index is None:
        return ()
    return tuple(check_lists(role_id, role, lists, index))


def access_view(state: Any, principal: str, role_id: str, now: datetime) -> AccessView:
    """``principal``'s access through ``role_id`` at ``now``."""
    role = state.roles[role_id]
    held = state.held_entitlements.get(principal, _NOTHING_HELD)
    idp = state.idp_entitlements.get(principal, {})
    lists = effective_lists(role_id, idp, held, role_defaults(role))
    rules = tuple(r for r in held.expiries if r.role_id == role_id)
    passed = tuple(r for r in rules if r.ends_at <= now)
    records = {r.kind: r for r in held.lists if r.role_id == role_id and r.capability is None}

    views = []
    for kind in sorted(lists):
        listed = lists[kind]
        record = records.get(kind) if listed.origin == PROVISA else None
        # The provider's own expiry counts for its lists: Provisa's rules never touch them.
        ended = (
            set()
            if listed.origin == IDP
            else {i for rule in passed for i in _ended_by(rule, listed.identifiers)}
        )
        views.append(
            ListView(
                kind,
                tuple(i for i in listed.identifiers if i not in ended),
                listed.origin,
                listed.superseded,
                tuple(i for i in listed.identifiers if i in ended),
                record.set_by if record else None,
                record.reason if record else None,
            )
        )
    return AccessView(
        principal,
        role_id,
        tuple(views),
        tuple(
            RuleView(r.rule_key, r.ends_at, dict(r.ends), r.list_kind, r.reason, r.ends_at <= now)
            for r in rules
        ),
        any(not r.ends for r in passed),
        _findings(state, role_id, role, lists),
    )


#: The lists a data-plane capability's reach passes through (``provisa.entitlements.mask``).
_DATA_PLANE_CHAIN: dict[str, tuple[str, ...]] = {
    "read": ("domains", "tables", "columns", "metrics"),
    "traverse": ("domains", "tables", "relationships"),
    "execute": ("domains", "commands"),
    "consume": ("domains", "data_products"),
}


@dataclass(frozen=True)
class RoleOrigin:
    """Where one role's part of a capability came from: per kind of list that applied, its
    origin (idp, provisa or role); empty, no list applied, so the role's whole reach.
    ``from_role`` is the role the capability comes from: ``role_id`` itself, or the parent role it
    inherits it from. ``ended``: what a passed expiry rule ended within that role's list."""

    role_id: str
    lists: dict[str, str]
    from_role: str
    ended: tuple[str, ...] = ()


@dataclass(frozen=True)
class CapabilityView:
    """One capability over the roles a request acts as: the union of each role's reach.
    ``identifiers`` is the readable expansion, None for an unscoped capability."""

    capability: str
    kind: str | None
    identifiers: tuple[str, ...] | None
    origins: tuple[RoleOrigin, ...]
    model_version: tuple


def _mask_reach(mask: EntitlementMask, capability: str) -> set[str]:
    """The identifiers of the objects ``mask`` reaches for a data-plane ``capability``."""
    kinds = DATA_PLANE_CAPABILITIES[capability][1]
    return {
        ".".join(o.spelled)
        for o in mask.index.objects
        if o.kind in kinds and mask.bits >> o.bit & 1
    }


def _listed(
    held: HeldEntitlements, role_id: str, capability: str, kind: str, lists: Mapping
) -> tuple[tuple[str, ...], str | None]:
    """A list-scoped capability's identifiers for ``role_id`` and the origin that decided them:
    the rows Provisa holds for the capability alone, else the kind's list, else every one."""
    own = [r for r in held.lists if r.role_id == role_id and r.capability == capability]
    if own:
        return tuple(i for r in own for i in r.identifiers), PROVISA
    listed: EffectiveList | None = lists.get(kind)
    if listed is None:
        return (WILDCARD,), None
    return listed.identifiers, listed.origin


def _reach(
    held: HeldEntitlements,
    role_id: str,
    capability: str,
    kind: str,
    lists: Mapping,
    rules: Iterable[ExpiryRule],
    at: datetime,
) -> tuple[ListReach, str | None]:
    """A list-scoped capability's reach through ``role_id`` at ``at``, ``rules`` applied, and the
    origin of the list it reads."""
    identifiers, origin = _listed(held, role_id, capability, kind, lists)
    mine = tuple(r for r in rules if r.role_id == role_id)
    return list_reach(identifiers, origin, mine, capability, kind, at), origin


def inherited_from(state: Any, role_id: str, capability: str) -> str:
    """The role ``role_id`` holds ``capability`` from: the furthest role up its chain holding it
    (REQ-2155: a role's capabilities include what it inherits), else ``role_id`` itself."""
    chain = state.role_chains.get(role_id, [role_id])
    holding = [
        r for r in chain if capability in ((state.roles.get(r) or {}).get("capabilities") or ())
    ]
    return holding[-1] if holding else role_id


def capability_views(
    state: Any, principal: str, role_ids: Iterable[str], held: HeldEntitlements, now: datetime
) -> list[CapabilityView]:
    """``principal``'s capabilities acting as ``role_ids`` at ``now``, each with where it applies:
    per capability, the union over the roles that grant it (REQ-2219: several roles combine by
    union per capability). The data plane's capabilities are a role's whenever it has a schema,
    and their expansion is its mask; another scoped capability's is its kind's list, less what a
    passed expiry rule ended of it. A role's capabilities include those it inherits from its
    parent roles, each origin naming the role it comes from."""
    scopes = capability_scopes()
    version = (state.schema_boot_id, state.schema_version)
    scoped: dict[str, set[str]] = {}
    unscoped: set[str] = set()
    origins: dict[str, list[RoleOrigin]] = {}
    idp = state.idp_entitlements.get(principal, {})
    for role_id in role_ids:
        role = state.roles[role_id]
        lists = effective_lists(role_id, idp, held, role_defaults(role))
        mask = principal_mask(state, principal, role_id, held, now)
        if mask is not None:
            for capability, chain in _DATA_PLANE_CHAIN.items():
                scoped.setdefault(capability, set()).update(_mask_reach(mask, capability))
                origins.setdefault(capability, []).append(
                    RoleOrigin(role_id, {k: lists[k].origin for k in chain if k in lists}, role_id)
                )
        for capability in role.get("capabilities") or ():
            kind = scopes.get(capability)
            source = inherited_from(state, role_id, capability)
            if kind is None:
                unscoped.add(capability)
                origins.setdefault(capability, []).append(RoleOrigin(role_id, {}, source))
                continue
            reach, origin = _reach(held, role_id, capability, kind, lists, held.expiries, now)
            if reach.ended_all:
                continue  # a grant-wide rule passed: the role grant reaches nothing
            scoped.setdefault(capability, set()).update(reach.kept())
            origins.setdefault(capability, []).append(
                RoleOrigin(role_id, {} if origin is None else {kind: origin}, source, reach.ended)
            )
    return [
        CapabilityView(
            capability,
            scopes.get(capability),
            tuple(sorted(scoped[capability])) if capability in scoped else None,
            tuple(origins[capability]),
            version,
        )
        for capability in sorted(origins)
    ]


@dataclass(frozen=True)
class ExpiryPreview:
    """What a rule would end on its date, per capability, computed from the person's reach then."""

    removes: dict[str, tuple[str, ...]]
    removes_nothing: bool


def preview_expiry(
    state: Any,
    principal: str,
    role_id: str,
    ends_at: datetime,
    ends: Mapping[str, Iterable[str]],
    *,
    replacing: str | None = None,
) -> ExpiryPreview:
    """What a rule on ``principal``'s grant of ``role_id`` ending ``ends`` at ``ends_at`` removes
    from what they would reach then, every other rule in force applied; ``replacing`` names a rule
    the new one replaces, left out of both sides. An expiry only removes bits from the mask: what
    is outside it, and what the identity provider's lists speak for, it cannot end."""
    held = state.held_entitlements.get(principal, _NOTHING_HELD)
    rules = tuple(r for r in held.expiries if r.rule_key is None or r.rule_key != replacing)
    rule = ExpiryRule(role_id, ends_at, {c: tuple(ids) for c, ids in ends.items()})
    before = principal_mask(state, principal, role_id, replace(held, expiries=rules), ends_at)
    after = principal_mask(
        state, principal, role_id, replace(held, expiries=(*rules, rule)), ends_at
    )
    removes: dict[str, tuple[str, ...]] = {}
    if before is not None and after is not None:
        for capability in DATA_PLANE_CAPABILITIES:
            gone = _mask_reach(before, capability) - _mask_reach(after, capability)
            if gone:
                removes[capability] = tuple(sorted(gone))
    removes.update(_list_scoped_removes(state, principal, role_id, held, rules, rule, ends_at))
    return ExpiryPreview(removes, not removes)


def _list_scoped_removes(
    state: Any,
    principal: str,
    role_id: str,
    held: HeldEntitlements,
    rules: tuple[ExpiryRule, ...],
    rule: ExpiryRule,
    at: datetime,
) -> dict[str, tuple[str, ...]]:
    """Per list-scoped capability of ``role_id``, what ``rule`` ends at ``at`` of what its list
    reaches then, ``rules`` already applied: the list's identifiers, for a grant-wide rule; else
    the identifiers it names that the list reaches and no rule before it ended."""
    role = state.roles[role_id]
    lists = effective_lists(
        role_id, state.idp_entitlements.get(principal, {}), held, role_defaults(role)
    )
    scopes = capability_scopes()
    out: dict[str, tuple[str, ...]] = {}
    for capability in role.get("capabilities") or ():
        kind = scopes.get(capability)
        if kind is None or capability in DATA_PLANE_CAPABILITIES:
            continue
        before, _origin = _reach(held, role_id, capability, kind, lists, rules, at)
        after, _origin = _reach(held, role_id, capability, kind, lists, (*rules, rule), at)
        if before.ended_all:
            continue
        if after.ended_all:
            gone = set(before.kept())
        else:
            gone = {
                e
                for e in after.ended
                if any(overlaps(e, i) for i in before.identifiers)
                and not any(covers(b, e) for b in before.ended)
            }
        if gone:
            out[capability] = tuple(sorted(gone))
    return out
