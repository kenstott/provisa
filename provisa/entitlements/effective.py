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

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from provisa.entitlements.index import Finding, build_index, check_lists, matches
from provisa.entitlements.lists import IDP, PROVISA, ExpiryRule, HeldEntitlements, effective_lists
from provisa.entitlements.runtime import role_defaults

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


def _findings(state: Any, role_id: str, role: dict, lists: dict) -> tuple[Finding, ...]:
    """The flags on these lists, or none when there is no model yet to check them against."""
    if state.names_table is None:
        return ()
    version = (state.schema_boot_id, state.schema_version)
    index = state.entitlement_masks.index(
        (version, "*"),
        lambda: build_index(state.names_table, None, tuple(state.data_product_refs)),
    )
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
