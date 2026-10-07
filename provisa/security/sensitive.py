# Copyright (c) 2026 Kenneth Stott
# Canary: 3d995870-9de3-4f90-9b71-c0a39c311204
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Sensitive data (REQ-1943).

A tag definition carries a Sensitive data option; a column carrying any tag with it set is a
sensitive column. The built-in pii tag has it set, and it cannot be cleared; an organisation sets
it on tags of its own, such as mnpi. Sensitivity is never propagated: a column derived from a
sensitive one is sensitive only if it carries a sensitive tag itself.

The sensitive_data right governs every way a sensitive column's values are revealed or hidden, in
every environment, prod included: adding or removing a sensitive tag on a column, setting or
clearing the option on a tag definition, and changing a sensitive column's role masks, fake,
synthetic rule or column grants.
"""

# Requirements: REQ-1943

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import select

if TYPE_CHECKING:
    from provisa.core.database import Connection

#: The right (REQ-1943).
SENSITIVE_DATA = "sensitive_data"

#: What hides or reveals a column's values: its read grants, its role masks, its fake and its
#: synthetic rule.
HIDING_FIELDS = (
    "visible_to",
    "unmasked_to",
    "mask_type",
    "mask_pattern",
    "mask_replace",
    "mask_value",
    "mask_precision",
    "fake",
    "fake_stable",
    "synthetic_rule",
)


def system_sensitive_tag_ids() -> frozenset[str]:
    """The built-in tags with the Sensitive data option set: pii."""
    from provisa.core.models import SYSTEM_TAGS

    return frozenset(t.id for t in SYSTEM_TAGS if t.sensitive)


async def sensitive_tag_ids(conn: "Connection", tags_table: Any = None) -> frozenset[str]:
    """Every tag with the Sensitive data option set: the built-in ones and the organisation's
    own. ``tags_table`` addresses another environment's tags (default: the connection's)."""
    from provisa.core.schema_org import tags

    table = tags if tags_table is None else tags_table
    rows = (
        await conn.execute_core(select(table.c.id).where(table.c.sensitive.is_(True)))
    ).fetchall()
    return system_sensitive_tag_ids() | {r[0] for r in rows}


async def sensitive_columns(conn: "Connection", table_id: int) -> frozenset[str]:
    """The sensitive columns of table ``table_id``: those carrying a sensitive tag."""
    from provisa.core.schema_org import tag_assignments as ta

    ids = await sensitive_tag_ids(conn)
    rows = (
        await conn.execute_core(
            select(ta.c.column_name).where(
                ta.c.object_type == "column",
                ta.c.table_id == table_id,
                ta.c.base_tag_id.in_(sorted(ids)),
            )
        )
    ).fetchall()
    return frozenset(r[0] for r in rows)


def _value(column: Any, field: str) -> Any:
    raw = column[field] if isinstance(column, dict) else getattr(column, field)
    if field in ("visible_to", "unmasked_to"):
        return sorted(raw or [])
    if field == "fake_stable":
        return bool(raw)
    return raw


def hiding_changes(
    stored: dict[str, dict], columns: list[Any], sensitive: frozenset[str]
) -> list[str]:
    """``column (field)`` for each change ``columns`` -- a table's columns as saved -- makes to how
    a sensitive column is hidden or revealed, against ``stored`` (its columns as stored, by name)."""
    out = []
    for column in columns:
        if column.name not in sensitive or column.name not in stored:
            continue
        for field in HIDING_FIELDS:
            if _value(stored[column.name], field) != _value(column, field):
                out.append(f"{column.name} ({field})")
    return out


def refusal(changes: list[str]) -> str:
    return "changing how a sensitive column is hidden needs the sensitive_data right: " + ", ".join(
        changes
    )


async def refuse_unpermitted_change(conn: "Connection", table: Any, *, holds: bool) -> str | None:
    """The refusal of a save of ``table`` that changes how a sensitive column is hidden, by a
    caller not holding the sensitive_data right; None when it may be saved."""
    if holds:
        return None
    from provisa.core.repositories.table import load_columns
    from provisa.core.schema_org import registered_tables as rt

    row = (
        await conn.execute_core(
            select(rt.c.id).where(
                rt.c.source_id == table.source_id,
                rt.c.schema_name == table.schema_name,
                rt.c.table_name == table.table_name,
            )
        )
    ).fetchone()
    if row is None:
        return None  # a new table: no column of it carries a tag yet
    sensitive = await sensitive_columns(conn, row[0])
    if not sensitive:
        return None
    stored = {c["column_name"]: c for c in await load_columns(conn, row[0])}
    changes = hiding_changes(stored, list(table.columns), sensitive)
    return refusal(changes) if changes else None


async def config_changes(conn: "Connection", config: Any) -> list[str]:
    """What applying ``config`` (a configuration, which adds and updates and removes nothing,
    REQ-1919) would change about how sensitive columns are hidden: a tag whose Sensitive data
    option it sets or clears, a sensitive tag it adds to a column, and a sensitive column whose
    hiding it changes."""
    from provisa.core.models import base_tag_id
    from provisa.core.repositories import tag as tag_repo
    from provisa.core.repositories.table import load_columns
    from provisa.core.schema_org import registered_tables as rt
    from provisa.core.schema_org import tag_assignments as ta

    out: list[str] = []
    ids = set(await sensitive_tag_ids(conn))
    for tag in config.tags:
        stored = await tag_repo.get(conn, tag.id)
        if tag.sensitive != bool(stored is not None and stored["sensitive"]):
            out.append(f"tag {tag.id} (sensitive)")
        if tag.sensitive:
            ids.add(tag.id)

    async def table_id(source_id: str, schema_name: str, table_name: str) -> int | None:
        row = (
            await conn.execute_core(
                select(rt.c.id).where(
                    rt.c.source_id == source_id,
                    rt.c.schema_name == schema_name,
                    rt.c.table_name == table_name,
                )
            )
        ).fetchone()
        return None if row is None else int(row[0])

    for a in config.tag_assignments:
        if a.object_type != "column" or base_tag_id(a.tag_id) not in ids:
            continue
        tid = a.table_id
        if tid is None and a.table_ref is not None:
            source_id, schema_name, table_name = a.table_ref.split(".", 2)
            tid = await table_id(source_id, schema_name, table_name)
        held = (
            None
            if tid is None
            else (
                await conn.execute_core(
                    select(ta.c.id).where(
                        ta.c.table_id == tid,
                        ta.c.column_name == a.column_name,
                        ta.c.base_tag_id == base_tag_id(a.tag_id),
                    )
                )
            ).fetchone()
        )
        if held is None:
            out.append(f"{a.table_ref or tid}.{a.column_name} (tag {a.tag_id})")
    for table in config.tables:
        tid = await table_id(table.source_id, table.schema_name, table.table_name)
        if tid is None:
            continue
        sensitive = await sensitive_columns(conn, tid)
        if not sensitive:
            continue
        stored = {c["column_name"]: c for c in await load_columns(conn, tid)}
        out += [f"{table.table_name}.{c}" for c in hiding_changes(stored, table.columns, sensitive)]
    return out
