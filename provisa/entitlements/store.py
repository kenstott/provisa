# Copyright (c) 2026 Kenneth Stott
# Canary: 5a2e15a8-f05c-43cf-b529-9994d9d1647f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Provisa-held rows of the entitlement store (REQ-2219).

Append-only: setting a list appends a row whose ``valid_from`` ends the row before it for the
same person, role, kind and capability, and ``removed`` ends one with nothing after it. What a
person holds now is the last row per key (:func:`load_held`). Publishing the store (the ops
tables) and the role's own rows are written elsewhere. The identity provider's rows mirror what a
sign-in's token carried (:func:`mirror_idp`): enforcement reads the token, and a write for someone
else can only be refused against the mirror (:func:`load_idp`).
"""

# Requirements: REQ-2219

from __future__ import annotations

import uuid
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from sqlalchemy import insert, select

from provisa.compiler.naming import domain_to_sql_name
from provisa.core.schema_org import entitlement_expiries, entitlements
from provisa.entitlements.lists import (
    IDP,
    PROVISA,
    ExpiryRule,
    HeldEntitlements,
    HeldList,
    IdpLists,
)

if TYPE_CHECKING:
    from datetime import datetime

    from provisa.core.database import Connection

#: The capability a product's creator holds on it (REQ-2219).
DATA_PRODUCT_RW = "data_product_rw"
DATA_PRODUCTS_KIND = "data_products"


async def set_list(
    conn: "Connection",
    principal: str,
    role_id: str,
    kind: str,
    identifiers: Iterable[str],
    *,
    set_by: str | None,
    reason: str | None,
    capability: str | None = None,
) -> None:
    """Set ``principal``'s list for ``role_id`` and ``kind``, replacing what they held."""
    await conn.execute_core(
        insert(entitlements).values(
            source=PROVISA,
            principal=principal,
            role_id=role_id,
            kind=kind,
            capability=capability,
            identifiers=list(identifiers),
            set_by=set_by,
            reason=reason,
        )
    )


async def remove_list(
    conn: "Connection",
    principal: str,
    role_id: str,
    kind: str,
    *,
    set_by: str | None,
    reason: str | None,
    capability: str | None = None,
) -> None:
    """End ``principal``'s list for ``role_id`` and ``kind``."""
    await conn.execute_core(
        insert(entitlements).values(
            source=PROVISA,
            principal=principal,
            role_id=role_id,
            kind=kind,
            capability=capability,
            identifiers=[],
            removed=True,
            set_by=set_by,
            reason=reason,
        )
    )


def _entry_ends(ends: Mapping[str, Iterable[str]]) -> dict[str, list[str]]:
    """``ends`` as stored. An entry's expiry names what it ends; one naming nothing would end the
    whole grant, which is the grant's own expiry (``provisa.auth.role_grants.end_grant``)."""
    out = {capability: list(ids) for capability, ids in ends.items()}
    if not any(out.values()):
        raise ValueError("an entry's expiry names what it ends; the whole grant's is its own")
    return out


async def add_expiry(
    conn: "Connection",
    principal: str,
    role_id: str,
    ends_at: "datetime",
    ends: Mapping[str, Iterable[str]],
    *,
    set_by: str | None,
    reason: str | None,
    list_kind: str | None = None,
) -> str:
    """Add an expiry rule to ``principal``'s grant of ``role_id``; returns its key. ``list_kind``
    names the list it was set with, which :func:`withdraw_list_expiries` withdraws it with."""
    stored = _entry_ends(ends)
    rule_key = uuid.uuid4().hex
    await conn.execute_core(
        insert(entitlement_expiries).values(
            rule_key=rule_key,
            principal=principal,
            role_id=role_id,
            ends_at=ends_at,
            ends=stored,
            list_kind=list_kind,
            set_by=set_by,
            reason=reason,
        )
    )
    return rule_key


async def withdraw_list_expiries(
    conn: "Connection",
    principal: str,
    role_id: str,
    kind: str,
    *,
    set_by: str | None,
    reason: str | None,
) -> None:
    """Withdraw the rules in force that ``principal``'s list for ``role_id`` and ``kind`` was set
    with: a rule outliving its list would end what a later list selects. Appends, as every change
    to the store does."""
    for rule in (await _live_rules(conn, principal, role_id)).values():
        if rule.list_kind != kind:
            continue
        await conn.execute_core(
            insert(entitlement_expiries).values(
                rule_key=rule.rule_key,
                principal=principal,
                role_id=role_id,
                ends_at=rule.ends_at,
                ends=rule.ends,
                list_kind=kind,
                removed=True,
                set_by=set_by,
                reason=reason,
            )
        )


async def _live_rules(conn: "Connection", principal: str, role_id: str) -> dict[str, Any]:
    """The rules in force on ``principal``'s grant of ``role_id``, by key: each key's last row,
    unless it withdrew the rule."""
    rows = await conn.execute_core(
        select(entitlement_expiries)
        .where(
            entitlement_expiries.c.principal == principal,
            entitlement_expiries.c.role_id == role_id,
        )
        .order_by(entitlement_expiries.c.valid_from, entitlement_expiries.c.id)
    )
    live: dict[str, Any] = {}
    for row in rows.fetchall():
        if row.removed:
            live.pop(row.rule_key, None)
        else:
            live[row.rule_key] = row
    return live


async def write_list(
    conn: "Connection",
    principal: str,
    role_id: str,
    kind: str,
    identifiers: Iterable[str],
    *,
    expires_at: "datetime | None",
    set_by: str,
    reason: str,
) -> None:
    """The one write of a principal's list, as the write API and an attached pending grant make
    it: withdraw the expiry the list was last set with, set the list, and add its expiry if any.
    The caller holds the transaction."""
    ids = list(identifiers)
    await withdraw_list_expiries(conn, principal, role_id, kind, set_by=set_by, reason=reason)
    await set_list(conn, principal, role_id, kind, ids, set_by=set_by, reason=reason)
    if expires_at is not None:
        await add_expiry(
            conn,
            principal,
            role_id,
            expires_at,
            {"*": ids},
            set_by=set_by,
            reason=reason,
            list_kind=kind,
        )


async def _held_list(
    conn: "Connection", principal: str, role_id: str, kind: str
) -> tuple[str, ...] | None:
    """``principal``'s Provisa-held list for ``role_id`` and ``kind`` now; None when none is held."""
    rows = await conn.execute_core(
        select(entitlements)
        .where(
            entitlements.c.source == PROVISA,
            entitlements.c.principal == principal,
            entitlements.c.role_id == role_id,
            entitlements.c.kind == kind,
            entitlements.c.capability.is_(None),
        )
        .order_by(entitlements.c.valid_from.desc(), entitlements.c.id.desc())
        .limit(1)
    )
    row = rows.fetchone()
    return None if row is None or row.removed else tuple(row.identifiers)


async def add_to_list(
    conn: "Connection",
    principal: str,
    role_id: str,
    kind: str,
    identifiers: Iterable[str],
    *,
    base: Iterable[str],
    capabilities: Iterable[str] | None,
    until: "datetime | None",
    set_by: str,
    reason: str,
) -> tuple[str, ...]:
    """Add access: append ``identifiers`` to ``principal``'s list for ``role_id`` and ``kind``,
    without restating the rest, and with ``until`` an expiry rule ending what was added, for
    ``capabilities`` (None: every one) at that date. ``base`` is the list the person has when
    Provisa holds none (the role's default). Returns what was added: an identifier already listed
    is not added again. A person holds an entry once: one already listed keeps the latest of its
    expiries, and no expiry is later than any date, so a rule ending it sooner for these
    capabilities lets it go (:func:`_extend`). The caller holds the transaction."""
    held = await _held_list(conn, principal, role_id, kind)
    current = list(base if held is None else held)
    wanted = tuple(dict.fromkeys(identifiers))
    added = tuple(i for i in wanted if i not in current)
    again = tuple(i for i in wanted if i in current)
    extended = await _extend(
        conn, principal, role_id, kind, again, capabilities, until, set_by=set_by, reason=reason
    )
    if added:
        await set_list(
            conn, principal, role_id, kind, [*current, *added], set_by=set_by, reason=reason
        )
    ending = (*added, *extended)
    if until is not None and ending:
        await add_expiry(
            conn,
            principal,
            role_id,
            until,
            {capability: ending for capability in (capabilities or ("*",))},
            set_by=set_by,
            reason=reason,
            list_kind=kind,
        )
    return added


def _later(until: "datetime | None", ends_at: "datetime") -> bool:
    return until is None or until > ends_at


async def _extend(
    conn: "Connection",
    principal: str,
    role_id: str,
    kind: str,
    again: tuple[str, ...],
    capabilities: Iterable[str] | None,
    until: "datetime | None",
    *,
    set_by: str,
    reason: str,
) -> tuple[str, ...]:
    """Grant ``again`` (identifiers already listed) once more, until ``until``: each rule set with
    the ``kind`` list that ends one of them sooner, for a capability this grant covers
    (``capabilities``, None: every one), lets it go. Returns those let go: they end at ``until``
    now. A rule under ``*`` is let go only by a grant for every capability; a grant for some names
    them, and ``*`` covers others it does not."""
    if not again:
        return ()
    covered = None if capabilities is None else set(capabilities)
    extended: list[str] = []
    for rule in (await _live_rules(conn, principal, role_id)).values():
        if rule.list_kind != kind or not _later(until, rule.ends_at):
            continue
        let_go = [
            (capability, i)
            for capability, ids in rule.ends.items()
            if covered is None or capability in covered
            for i in again
            if i in ids
        ]
        if not let_go:
            continue
        extended.extend(i for _capability, i in let_go)
        kept = {
            capability: [i for i in ids if (capability, i) not in let_go]
            for capability, ids in rule.ends.items()
        }
        kept = {capability: ids for capability, ids in kept.items() if ids}
        held = ExpiryRule(
            rule.role_id,
            rule.ends_at,
            {capability: tuple(ids) for capability, ids in rule.ends.items()},
            rule_key=rule.rule_key,
            list_kind=rule.list_kind,
        )
        if kept:
            await change_expiry(
                conn, held, principal, rule.ends_at, kept, set_by=set_by, reason=reason
            )
        else:
            await withdraw_expiry(conn, held, principal, set_by=set_by, reason=reason)
    return tuple(dict.fromkeys(extended))


async def change_expiry(
    conn: "Connection",
    rule: ExpiryRule,
    principal: str,
    ends_at: "datetime",
    ends: Mapping[str, Iterable[str]],
    *,
    set_by: str,
    reason: str,
) -> None:
    """Change ``rule`` on ``principal``'s grant: a row with its key, which is the rule from now on.
    It keeps the list it was set with."""
    assert rule.rule_key is not None
    stored = _entry_ends(ends)
    await conn.execute_core(
        insert(entitlement_expiries).values(
            rule_key=rule.rule_key,
            principal=principal,
            role_id=rule.role_id,
            ends_at=ends_at,
            ends=stored,
            list_kind=rule.list_kind,
            set_by=set_by,
            reason=reason,
        )
    )


async def withdraw_expiry(
    conn: "Connection", rule: ExpiryRule, principal: str, *, set_by: str, reason: str
) -> None:
    """Withdraw ``rule`` from ``principal``'s grant."""
    assert rule.rule_key is not None
    await conn.execute_core(
        insert(entitlement_expiries).values(
            rule_key=rule.rule_key,
            principal=principal,
            role_id=rule.role_id,
            ends_at=rule.ends_at,
            ends={capability: list(ids) for capability, ids in rule.ends.items()},
            list_kind=rule.list_kind,
            removed=True,
            set_by=set_by,
            reason=reason,
        )
    )


@dataclass(frozen=True)
class Addition:
    """One change to a person's list that added identifiers: what, by whom, why, when, and until
    when (the rule set with it, as it stands now; None when there is none or it was withdrawn)."""

    kind: str
    added: tuple[str, ...]
    set_by: str | None
    reason: str | None
    valid_from: "datetime"
    until: "datetime | None"


async def list_history(conn: "Connection", principal: str, role_id: str) -> list[Addition]:
    """The timeline of what was added to ``principal``'s lists for ``role_id``, oldest first. A
    rule belongs to the change it was set with: set in the same transaction, for the same list."""
    rows = (
        await conn.execute_core(
            select(entitlements)
            .where(
                entitlements.c.source == PROVISA,
                entitlements.c.principal == principal,
                entitlements.c.role_id == role_id,
                entitlements.c.capability.is_(None),
            )
            .order_by(entitlements.c.valid_from, entitlements.c.id)
        )
    ).fetchall()
    rules = (
        await conn.execute_core(
            select(entitlement_expiries)
            .where(
                entitlement_expiries.c.principal == principal,
                entitlement_expiries.c.role_id == role_id,
            )
            .order_by(entitlement_expiries.c.valid_from, entitlement_expiries.c.id)
        )
    ).fetchall()
    first: dict[str, Any] = {}
    last: dict[str, Any] = {}
    for rule in rules:
        first.setdefault(rule.rule_key, rule)
        last[rule.rule_key] = rule
    set_with = {
        (r.list_kind, r.valid_from): last[key]
        for key, r in first.items()
        if r.list_kind is not None
    }
    out: list[Addition] = []
    previous: dict[str, tuple[str, ...]] = {}
    for row in rows:
        before = previous.get(row.kind, ())
        now_held = () if row.removed else tuple(row.identifiers)
        previous[row.kind] = now_held
        added = tuple(i for i in now_held if i not in before)
        if not added:
            continue
        rule = set_with.get((row.kind, row.valid_from))
        out.append(
            Addition(
                row.kind,
                added,
                row.set_by,
                row.reason,
                row.valid_from,
                None if rule is None or rule.removed else rule.ends_at,
            )
        )
    return out


def idp_supersedes(idp: Mapping[str, IdpLists], principal: str, role_id: str, kind: str) -> bool:
    """Whether the identity provider sends ``principal``'s ``kind`` list for ``role_id``: that
    list wins, and no write reaches it (REQ-2219)."""
    return kind in idp.get(principal, {}).get(role_id, {})


async def mirror_idp(conn: "Connection", principal: str, lists: IdpLists) -> None:
    """Record what ``principal``'s token carried at sign-in: a row per list that changed, and a
    ``removed`` row per list it no longer carries. Nothing changed appends nothing, so a sign-in
    with the same lists does not advance the config stamp."""
    current = (await load_idp(conn, principal)).get(principal, {})
    wanted = {(role, kind): ids for role, kinds in lists.items() for kind, ids in kinds.items()}
    held = {(role, kind): ids for role, kinds in current.items() for kind, ids in kinds.items()}
    for (role_id, kind), identifiers in wanted.items():
        if held.get((role_id, kind)) != identifiers:
            await conn.execute_core(
                insert(entitlements).values(
                    source=IDP,
                    principal=principal,
                    role_id=role_id,
                    kind=kind,
                    identifiers=list(identifiers),
                )
            )
    for role_id, kind in held.keys() - wanted.keys():
        await conn.execute_core(
            insert(entitlements).values(
                source=IDP,
                principal=principal,
                role_id=role_id,
                kind=kind,
                identifiers=[],
                removed=True,
            )
        )


async def load_idp(conn: "Connection", principal: str | None = None) -> dict[str, IdpLists]:
    """user id -> the lists their identity provider last sent, per role and kind: the last row of
    each key, unless it ended the key. ``principal`` reads one person's."""
    query = select(entitlements).where(entitlements.c.source == IDP)
    if principal is not None:
        query = query.where(entitlements.c.principal == principal)
    rows = await conn.execute_core(query.order_by(entitlements.c.valid_from, entitlements.c.id))
    last: dict[tuple[str, str, str], tuple[str, ...] | None] = {}
    for row in rows.fetchall():
        key = (row.principal, row.role_id, row.kind)
        last[key] = None if row.removed else tuple(row.identifiers)
    out: dict[str, IdpLists] = {}
    for (who, role_id, kind), identifiers in last.items():
        if identifiers is not None:
            out.setdefault(who, {}).setdefault(role_id, {})[kind] = identifiers
    return out


async def record_product_creator(
    conn: "Connection", creator: str, role_id: str, domain_id: str, product_id: str
) -> None:
    """A product's creator holds read-write on it (REQ-2219): one Provisa-held row scoped to the
    data_product_rw capability, granted, changed and expired like any other."""
    await set_list(
        conn,
        creator,
        role_id,
        DATA_PRODUCTS_KIND,
        [f"{domain_to_sql_name(domain_id)}.{product_id}"],
        set_by=creator,
        reason="created this data product",
        capability=DATA_PRODUCT_RW,
    )


@dataclass
class _Held:
    lists: dict[tuple[str, str, str | None], HeldList]
    expiries: dict[str, ExpiryRule]


async def load_held(conn: "Connection") -> dict[str, HeldEntitlements]:
    """user id -> what Provisa holds for them now: the last row of each key, unless it ended the
    key. What the runtime build holds, so a request's mask reads nothing."""
    held: dict[str, _Held] = {}
    rows = await conn.execute_core(
        select(entitlements)
        .where(entitlements.c.source == PROVISA)
        .order_by(entitlements.c.valid_from, entitlements.c.id)
    )
    for row in rows.fetchall():
        slot = held.setdefault(row.principal, _Held({}, {}))
        key = (row.role_id, row.kind, row.capability)
        if row.removed:
            slot.lists.pop(key, None)
        else:
            slot.lists[key] = HeldList(
                row.role_id,
                row.kind,
                tuple(row.identifiers),
                capability=row.capability,
                set_by=row.set_by,
                reason=row.reason,
            )
    rules = await conn.execute_core(
        select(entitlement_expiries).order_by(
            entitlement_expiries.c.valid_from, entitlement_expiries.c.id
        )
    )
    for row in rules.fetchall():
        slot = held.setdefault(row.principal, _Held({}, {}))
        if row.removed:
            slot.expiries.pop(row.rule_key, None)
        else:
            slot.expiries[row.rule_key] = ExpiryRule(
                row.role_id,
                row.ends_at,
                {capability: tuple(ids) for capability, ids in row.ends.items()},
                reason=row.reason,
                rule_key=row.rule_key,
                list_kind=row.list_kind,
            )
    return {
        principal: HeldEntitlements(tuple(h.lists.values()), tuple(h.expiries.values()))
        for principal, h in held.items()
        if h.lists or h.expiries
    }
