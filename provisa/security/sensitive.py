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

from sqlalchemy import or_, select

from provisa.core.column_scope import SCOPE_DOMAIN

if TYPE_CHECKING:
    from provisa.core.database import Connection

#: The right (REQ-1943).
SENSITIVE_DATA = "sensitive_data"

#: What hides or reveals a column's values: its read grants and the scope they are read with
#: (REQ-1959: ``restricted`` with no grant is nobody, ``domain`` with none is every role in
#: reach, ``public`` reaches past the domain), its role masks, its fake and its synthetic rule.
HIDING_FIELDS = (
    "visible_to",
    "scope",
    "masks",
    "fake",
    "fake_stable",
    "synthetic_rule",
)


def system_sensitive_tag_ids() -> frozenset[str]:
    """The built-in tags with the Sensitive data option set: pii."""
    from provisa.core.models import SYSTEM_TAGS

    return frozenset(t.id for t in SYSTEM_TAGS if t.sensitive)


def sensitive_assignments(
    tag_assignments: Any,
    registered_tables: Any,
    tags_table: Any,
    term_refs: Any,
    glossary_terms: Any,
) -> Any:
    """Each (table_id, table_name, column_name, base_tag_id, term_id) a sensitive tag is on a
    column at: a built-in sensitive tag, or one of the organisation's own with the option set,
    assigned to the column (term_id NULL) or to the defined, in-service glossary term its ref is on
    (REQ-2145). One
    statement, so the environment's data choice (provisa.core.env_data) and the
    sensitive_coverage meta view read sensitivity alike; the tables are passed so another
    environment's schema can be addressed."""
    from sqlalchemy import literal, union_all

    from provisa.core.term_tags import inherited_column_tags

    ta, rt = tag_assignments, registered_tables
    sensitive = or_(
        ta.c.base_tag_id.in_(sorted(system_sensitive_tag_ids())),
        ta.c.base_tag_id.in_(select(tags_table.c.id).where(tags_table.c.sensitive.is_(True))),
    )
    direct = (
        select(
            rt.c.id.label("table_id"),
            rt.c.table_name,
            ta.c.column_name,
            ta.c.base_tag_id,
            literal(None).label("term_id"),
        )
        .select_from(ta.join(rt, rt.c.id == ta.c.table_id))
        .where(ta.c.object_type == "column", sensitive)
    )
    inherited = inherited_column_tags(ta, term_refs, glossary_terms).where(sensitive).subquery()
    through = select(
        rt.c.id.label("table_id"),
        rt.c.table_name,
        inherited.c.column_name,
        inherited.c.base_tag_id,
        inherited.c.term_id,
    ).select_from(inherited.join(rt, rt.c.id == inherited.c.table_id))
    return union_all(direct, through)


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
    """The sensitive columns of table ``table_id``: those carrying a sensitive tag, on the column
    or on its term (REQ-2145)."""
    from provisa.core.term_tags import column_tag_ids

    ids = await sensitive_tag_ids(conn)
    return frozenset(c for c, tags in (await column_tag_ids(conn, table_id)).items() if tags & ids)


def mask_entry_dict(entry: Any) -> dict:
    """A mask entry (REQ-2059) as stored, whichever surface holds it: the stored dict, the config
    model's entry, or the admin API's input."""
    import dataclasses

    from pydantic import BaseModel

    if isinstance(entry, dict):
        raw = entry
    elif isinstance(entry, BaseModel):
        raw = entry.model_dump()
    else:
        raw = dataclasses.asdict(entry)
    # REQ-2094: an entry with no why is stored without the key, so a why absent and a why empty
    # are the same entry.
    return {k: v for k, v in raw.items() if k != "why" or v}


def _value(column: Any, field: str) -> Any:
    raw = column[field] if isinstance(column, dict) else getattr(column, field)
    if field == "visible_to":
        return sorted(raw or [])
    if field == "masks":  # REQ-2059: the list in its order, each entry as stored
        # REQ-2094: an entry's why says why it hides as it does; it is not how it hides.
        return [{k: v for k, v in mask_entry_dict(e).items() if k != "why"} for e in raw]
    if field == "fake_stable":
        return bool(raw)
    if field == "scope":
        return raw or SCOPE_DOMAIN  # a column that states none is ``domain``
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
        if a.object_type == "term" and base_tag_id(a.tag_id) in ids:  # REQ-2145
            if not await _term_holds(conn, a.term, base_tag_id(a.tag_id)):
                out.append(f"term {a.term} (tag {a.tag_id})")
            continue
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


async def _term_holds(conn: "Connection", term: str | None, base: str) -> bool:
    """Whether the glossary term named ``term`` already carries tag ``base`` (REQ-2145)."""
    from provisa.core.schema_org import glossary_terms
    from provisa.core.schema_org import tag_assignments as ta

    row = (
        await conn.execute_core(
            select(ta.c.id)
            .join(glossary_terms, glossary_terms.c.id == ta.c.term_id)
            .where(
                ta.c.object_type == "term",
                ta.c.base_tag_id == base,
                glossary_terms.c.name == (term or "").strip().lower(),
            )
        )
    ).fetchone()
    return row is not None
