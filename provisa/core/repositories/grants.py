# Copyright (c) 2026 Kenneth Stott
# Canary: 2991f980-fc4a-4241-935b-edd9b27f3204
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Taking a role off an object's grants (REQ-1918).

A role cannot be deleted while a grant names it. The grants are lists of role ids on the object:
a table's columns (``visible_to``, each mask entry's ``to``) and its write grants (REQ-2074), a metric's ``visible_to``,
a command's and a webhook's assigned roles (``visible_to``). Removing the role from them is an
edit of that object, written here through the model store, one object at a time.
"""

# Requirements: REQ-1918, REQ-1524

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import delete, select, update

from provisa.core import model_change
from provisa.core.schema_org import (
    metrics,
    table_columns,
    table_write_grants,
    tracked_functions,
    tracked_webhooks,
)

if TYPE_CHECKING:
    from sqlalchemy import Table as SaTable

    from provisa.core.database import Connection


async def _revoke(
    conn: "Connection", table: "SaTable", key: str, where: Any, lists: tuple[str, ...], role_id: str
) -> bool | None:
    """Remove ``role_id`` from ``lists`` of every row ``where`` selects. None when no row is
    selected (the object does not exist); else whether anything changed."""
    rows = (
        await conn.execute_core(select(table.c[key], *(table.c[c] for c in lists)).where(where))
    ).fetchall()
    if not rows:
        return None
    changed = False
    for row in rows:
        values = {c: list(getattr(row, c) or []) for c in lists}
        kept = {
            c: [_without(r, role_id) for r in v] if c == "masks" else [r for r in v if r != role_id]
            for c, v in values.items()
        }
        if kept != values:
            await conn.execute_core(
                update(table).where(table.c[key] == getattr(row, key)).values(**kept)
            )
            changed = True
    return changed


def _without(entry: dict, role_id: str) -> dict:
    """A mask entry (REQ-2059) with ``role_id`` taken off its ``to``; the entry keeps its place."""
    return {**entry, "to": [r for r in entry["to"] if r != role_id]}


async def revoke_from_table(conn: "Connection", table_id: int, role_id: str) -> bool | None:
    """Take ``role_id`` off every grant of the table: its columns' read grants and mask entries,
    and its write grants (REQ-2074)."""
    model_change.name("revoke", "table grants", f"{role_id} on table {table_id}")
    columns = await _revoke(
        conn,
        table_columns,
        "id",
        table_columns.c.table_id == table_id,
        ("visible_to", "masks"),
        role_id,
    )
    removed = await conn.execute_core(
        delete(table_write_grants).where(
            table_write_grants.c.table_id == table_id, table_write_grants.c.role_id == role_id
        )
    )
    if columns is None:
        return None
    return columns or bool(removed.rowcount)


_OBJECT_TABLES: dict[str, "SaTable"] = {
    "metric": metrics,
    "command": tracked_functions,
    "webhook": tracked_webhooks,
}


async def revoke_from_object(conn: "Connection", kind: str, name: str, role_id: str) -> bool | None:
    """Take ``role_id`` off a metric's, command's or webhook's assigned roles (``visible_to``)."""
    table = _OBJECT_TABLES[kind]
    model_change.name("revoke", f"{kind} grants", f"{role_id} on {name}")
    return await _revoke(conn, table, "name", table.c.name == name, ("visible_to",), role_id)
