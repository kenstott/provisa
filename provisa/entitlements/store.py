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
    rule_key = uuid.uuid4().hex
    await conn.execute_core(
        insert(entitlement_expiries).values(
            rule_key=rule_key,
            principal=principal,
            role_id=role_id,
            ends_at=ends_at,
            ends={capability: list(ids) for capability, ids in ends.items()},
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
    for rule in live.values():
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
