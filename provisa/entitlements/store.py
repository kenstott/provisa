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
tables), the identity provider's mirror rows and the role's own rows are written elsewhere.
"""

# Requirements: REQ-2219

from __future__ import annotations

import uuid
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy import insert, select

from provisa.compiler.naming import domain_to_sql_name
from provisa.core.schema_org import entitlement_expiries, entitlements
from provisa.entitlements.lists import PROVISA, ExpiryRule, HeldEntitlements, HeldList

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
) -> str:
    """Add an expiry rule to ``principal``'s grant of ``role_id``; returns its key."""
    rule_key = uuid.uuid4().hex
    await conn.execute_core(
        insert(entitlement_expiries).values(
            rule_key=rule_key,
            principal=principal,
            role_id=role_id,
            ends_at=ends_at,
            ends={capability: list(ids) for capability, ids in ends.items()},
            set_by=set_by,
            reason=reason,
        )
    )
    return rule_key


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
            )
    return {
        principal: HeldEntitlements(tuple(h.lists.values()), tuple(h.expiries.values()))
        for principal, h in held.items()
        if h.lists or h.expiries
    }
