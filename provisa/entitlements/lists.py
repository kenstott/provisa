# Copyright (c) 2026 Kenneth Stott
# Canary: 89cad7d8-93d0-485d-bb2b-52494132fdb9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The entitlement lists: per role and kind, the identifiers a person is entitled to (REQ-2219).

An identity provider sends them on the token, in either of two forms, and Provisa may hold them
too. For one person, role and kind the provider's list wins outright; Provisa's applies only
where the provider is silent; the role's default applies where neither speaks.

Nothing here reads the model: an identifier is a string until the index expands it.
"""

# Requirements: REQ-2219

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime

#: The identity provider setting naming the claim that carries the nested form,
#: {role: {kind: [identifier, ...]}}. There is no default name: a provider with none set sends no
#: nested lists.
NESTED_CLAIM_SETTING = "entitlements_claim"

#: The flat form: ``provisa:<role>:<kind>:<identifier>``, an entry of any list claim, for an
#: identity provider that can emit only flat values such as groups. An empty identifier
#: (``provisa:viewer:tables:``) says the list is present and empty: it means nothing of that kind.
FLAT_PREFIX = "provisa:"

IDP = "idp"
PROVISA = "provisa"
ROLE = "role"

#: per role, per kind, the identifiers.
IdpLists = dict[str, dict[str, tuple[str, ...]]]


class EntitlementClaimError(ValueError):
    """The token's entitlement claim cannot be read. A list that cannot be read must not be
    taken for no list, which means the role's whole default."""


def nested_claim_name(provider_settings: Mapping) -> str | None:
    """The claim ``provider_settings`` names for the nested form; None when it names none."""
    name = provider_settings.get(NESTED_CLAIM_SETTING)
    if name is None:
        return None
    if not isinstance(name, str):
        raise ValueError(f"{NESTED_CLAIM_SETTING} must be the name of a claim, got {name!r}")
    return name.strip() or None


def _nested(claim_name: str, claim: object) -> IdpLists:
    if not isinstance(claim, Mapping):
        raise EntitlementClaimError(f"{claim_name} must be a map of roles to kinds")
    out: IdpLists = {}
    for role, kinds in claim.items():
        if not isinstance(kinds, Mapping):
            raise EntitlementClaimError(f"{claim_name}[{role!r}] must be a map of kinds")
        for kind, identifiers in kinds.items():
            if not isinstance(identifiers, list) or not all(
                isinstance(i, str) for i in identifiers
            ):
                raise EntitlementClaimError(
                    f"{claim_name}[{role!r}][{kind!r}] must be a list of identifiers"
                )
            out.setdefault(str(role), {})[str(kind)] = tuple(identifiers)
    return out


def _flat(entry: str, out: IdpLists) -> None:
    parts = entry.split(":", 3)
    if len(parts) != 4 or not parts[1] or not parts[2]:
        raise EntitlementClaimError(
            f"{entry!r}: the flat form is provisa:<role>:<kind>:<identifier>"
        )
    _, role, kind, identifier = parts
    kinds = out.setdefault(role, {})
    held = kinds.get(kind, ())
    kinds[kind] = (*held, identifier) if identifier else held


def parse_idp_entitlements(claims: Mapping | None, nested_claim: str | None) -> IdpLists:
    """The lists a token carries, per role and kind: the claim its provider names for the nested
    form (``nested_claim``; None reads no nested lists) and every flat entry of its list claims,
    together. A claim present but unreadable raises :class:`EntitlementClaimError`."""
    claims = claims or {}
    out: IdpLists = (
        _nested(nested_claim, claims[nested_claim])
        if nested_claim is not None and nested_claim in claims
        else {}
    )
    for name, value in claims.items():
        if name == nested_claim or not isinstance(value, list):
            continue
        for entry in value:
            if isinstance(entry, str) and entry.startswith(FLAT_PREFIX):
                _flat(entry, out)
    return out


def _digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, default=str, separators=(",", ":")).encode()
    ).hexdigest()


def idp_lists_hash(lists: IdpLists) -> str:
    """A hash of the token's lists, the same however the token ordered them."""
    return _digest({r: {k: sorted(ids) for k, ids in ks.items()} for r, ks in lists.items()})


@dataclass(frozen=True)
class HeldList:
    """A list Provisa holds for a person's role grant, set by a transaction."""

    role_id: str
    kind: str
    identifiers: tuple[str, ...]
    # Set when the row scopes one capability alone (a product's creator holds data_product_rw on
    # it); the capability reads it, and it is no list for the kind.
    capability: str | None = None
    set_by: str | None = None
    reason: str | None = None


@dataclass(frozen=True)
class ExpiryRule:
    """One rule on a person's grant: at ``ends_at``, what ``ends`` names ends. ``ends`` maps a
    capability, or ``*``, to identifiers; empty, it ends the whole grant. It only removes."""

    role_id: str
    ends_at: datetime
    ends: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    reason: str | None = None
    # What the API reads it by, and the list it was set with. Neither is the rule, so neither is
    # in held_version.
    rule_key: str | None = None
    list_kind: str | None = None


def rule_ends(rule: ExpiryRule, capability: str, kind: str) -> tuple[str, ...] | None:
    """What a passed ``rule`` ends of ``capability``, a capability scoped by the list ``kind``
    outside the mask: None for everything (a grant-wide rule), else the identifiers named under
    the capability, and those under ``*`` when the rule was set with the ``kind`` list itself.
    Otherwise ``*`` names the mask's objects, which are no list-scoped capability's."""
    if not rule.ends:
        return None
    named = tuple(rule.ends.get(capability, ()))
    if rule.list_kind == kind:
        named = (*named, *rule.ends.get("*", ()))
    return named


@dataclass(frozen=True)
class HeldEntitlements:
    """What Provisa holds for one person."""

    lists: tuple[HeldList, ...] = ()
    expiries: tuple[ExpiryRule, ...] = ()


def held_version(held: HeldEntitlements) -> str:
    """The version of what Provisa holds for a person: a hash of its content, so it changes when
    the content does and two workers agree on it. The empty holding's version is the empty string."""
    if not held.lists and not held.expiries:
        return ""
    return _digest(
        {
            "lists": sorted(
                (x.role_id, x.kind, sorted(x.identifiers), x.capability or "") for x in held.lists
            ),
            "expiries": sorted(
                (
                    x.role_id,
                    x.ends_at.isoformat(),
                    sorted((c, sorted(i)) for c, i in x.ends.items()),
                )
                for x in held.expiries
            ),
        }
    )


@dataclass(frozen=True)
class EffectiveList:
    """The list that applies for one kind, with its origin and any list it superseded."""

    kind: str
    identifiers: tuple[str, ...]
    origin: str
    superseded: tuple[str, ...] | None = None


def effective_lists(
    role_id: str,
    idp: IdpLists,
    held: HeldEntitlements,
    defaults: Mapping[str, list[str] | tuple[str, ...]],
) -> dict[str, EffectiveList]:
    """Per kind, the list for this role: the provider's, else Provisa's, else the role's default.
    A kind none of them speaks to is absent, and absent means the role's whole schema."""
    from_idp = idp.get(role_id, {})
    from_held: dict[str, tuple[str, ...]] = {}
    for row in held.lists:
        if row.role_id != role_id or row.capability is not None:
            continue
        from_held[row.kind] = (*from_held.get(row.kind, ()), *row.identifiers)
    out: dict[str, EffectiveList] = {}
    for kind in {*from_idp, *from_held, *defaults}:
        if kind in from_idp:
            out[kind] = EffectiveList(kind, from_idp[kind], IDP, from_held.get(kind))
        elif kind in from_held:
            out[kind] = EffectiveList(kind, from_held[kind], PROVISA)
        else:
            out[kind] = EffectiveList(kind, tuple(defaults[kind]), ROLE)
    return out
