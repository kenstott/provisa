# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What the model store says of a derived role (REQ-2079): the values its attribute has been seen
with, and what names it.

A derived role is not a row of ``roles``: it is an attribute the org declares (REQ-2057), a parent
role ``<attribute>`` and a value role ``<attribute>__<value>`` per value a member's attributes
give it. The values seen are those the org records for its members and those their identity
providers supplied at sign-in. What names it is every grant list, mask list and row rule naming the
parent or one of its value roles, alone or in a composite entry (``analyst+region__eu``)."""

# Requirements: REQ-2079, REQ-2057

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import select, union

from provisa.auth.derived_roles import SEPARATOR
from provisa.core.repositories.integrity import Dependent, ObjectRef, _dependents
from provisa.core.schema_org import (
    metrics,
    rls_rules,
    table_columns,
    tracked_functions,
    tracked_webhooks,
    user_attributes,
    user_idp_attributes,
)
from provisa.security.inheritance import COMPOSITE

if TYPE_CHECKING:
    from provisa.core.database import Connection


async def values_seen(conn: "Connection", attribute: str) -> list[str]:
    """The values ``attribute`` has been seen with, from the org's records and the providers'."""
    seen = union(
        select(user_attributes.c.value).where(user_attributes.c.name == attribute),
        select(user_idp_attributes.c.value).where(user_idp_attributes.c.name == attribute),
    )
    rows = (await conn.execute_core(select(seen.subquery().c.value))).fetchall()
    return sorted({r[0] for r in rows if r[0]})


def names_it(entry: str, attribute: str) -> bool:
    """True when a grant entry names the derived role of ``attribute`` or one of its values."""
    return any(
        part == attribute or part.startswith(attribute + SEPARATOR)
        for part in entry.split(COMPOSITE)
    )


async def uses(conn: "Connection", attribute: str) -> list[Dependent]:
    """Every row rule, mask and grant list that names the derived role of ``attribute``."""
    found: dict[ObjectRef, set[str]] = {}

    def note(kind: str, key: Any, via: str) -> None:
        found.setdefault(ObjectRef(kind, key), set()).add(via)

    for key, role_id in (
        await conn.execute_core(select(rls_rules.c.id, rls_rules.c.role_id))
    ).fetchall():
        if names_it(role_id, attribute):
            note("row_filter", key, "rls_rules.role_id")
    for key, visible_to, masks in (
        await conn.execute_core(
            select(table_columns.c.id, table_columns.c.visible_to, table_columns.c.masks)
        )
    ).fetchall():
        if any(names_it(e, attribute) for e in visible_to or []):
            note("column", key, "table_columns.visible_to")
        if any(names_it(e, attribute) for m in masks or [] for e in m["to"]):
            note("column", key, "table_columns.masks")
    for table, kind in (
        (metrics, "metric"),
        (tracked_functions, "command"),
        (tracked_webhooks, "webhook"),
    ):
        for name, visible_to in (
            await conn.execute_core(select(table.c.name, table.c.visible_to))
        ).fetchall():
            if any(names_it(e, attribute) for e in visible_to or []):
                note(kind, name, f"{table.name}.visible_to")
    return await _dependents(conn, found)
