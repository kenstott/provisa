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
a table's columns (``visible_to``, ``writable_by``, ``unmasked_to``), a metric's ``visible_to``,
a command's and a webhook's assigned roles (``visible_to``). Removing the role from them is an
edit of that object, written here through the model store, one object at a time.
"""

# Requirements: REQ-1918, REQ-1524

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import select, update

from provisa.core import model_change
from provisa.core.schema_org import metrics, table_columns, tracked_functions, tracked_webhooks

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
        kept = {c: [r for r in v if r != role_id] for c, v in values.items()}
        if kept != values:
            await conn.execute_core(
                update(table).where(table.c[key] == getattr(row, key)).values(**kept)
            )
            changed = True
    return changed


async def revoke_from_table(conn: "Connection", table_id: int, role_id: str) -> bool | None:
    """Take ``role_id`` off every column grant of the table: read, write and unmasked."""
    model_change.name("revoke", "table grants", f"{role_id} on table {table_id}")
    return await _revoke(
        conn,
        table_columns,
        "id",
        table_columns.c.table_id == table_id,
        ("visible_to", "writable_by", "unmasked_to"),
        role_id,
    )


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
