# Copyright (c) 2026 Kenneth Stott
# Canary: 8e41b7c2-3d5a-4f90-b6e1-2a9c7d0f5b36
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Column rules keyed on a tag or a classification (REQ-2278): their store, and the keys each
column carries -- its tags (its term's pii included, REQ-2145) and its accepted classifications --
which the runtime build reads them through (:mod:`provisa.security.column_rules`)."""

# Requirements: REQ-2278

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import delete as _delete, insert, select, update

from provisa.core.models import ColumnRuleModel
from provisa.core.schema_org import column_rules

if TYPE_CHECKING:
    from provisa.core.database import Connection
    from provisa.security.column_rules import ColumnRule, Key, Target


def _row(r: Any) -> dict:
    return dict(r._mapping)


async def list_all(conn: "Connection") -> list[dict]:
    result = await conn.execute_core(select(column_rules).order_by(column_rules.c.id))
    return [_row(r) for r in result.fetchall()]


async def get(conn: "Connection", rule_id: int) -> dict | None:
    found = (
        await conn.execute_core(select(column_rules).where(column_rules.c.id == rule_id))
    ).fetchone()
    return None if found is None else _row(found)


async def upsert(conn: "Connection", rule: ColumnRuleModel) -> dict:
    """Save the rule of its key and role: a role has at most one rule per tag or classification,
    so a save for a key and role that has one replaces it."""
    key_column = "tag_id" if rule.tag_id is not None else "classification"
    key_value = rule.tag_id if rule.tag_id is not None else rule.classification
    values = {
        "tag_id": rule.tag_id,
        "classification": rule.classification,
        "role_id": rule.role_id,
        "action": rule.action,
        "mask": None if rule.mask is None else rule.mask.model_dump(exclude_none=True),
    }
    where = (column_rules.c[key_column] == key_value) & (column_rules.c.role_id == rule.role_id)
    async with conn.transaction():
        held = (await conn.execute_core(select(column_rules.c.id).where(where))).fetchone()
        if held is None:
            await conn.execute_core(insert(column_rules).values(**values))
        else:
            await conn.execute_core(
                update(column_rules).where(column_rules.c.id == held[0]).values(**values)
            )
        saved = (await conn.execute_core(select(column_rules).where(where))).fetchone()
    return _row(saved)


async def delete(conn: "Connection", rule_id: int) -> bool:
    result = await conn.execute_core(_delete(column_rules).where(column_rules.c.id == rule_id))
    return bool(result.rowcount)


async def keyed(conn: "Connection") -> tuple[set[str], set[str]]:
    """The tag ids and the classification kinds some rule is keyed on: what a change of a
    column's tags or classifications rebuilds the runtime for."""
    rows = (
        await conn.execute_core(select(column_rules.c.tag_id, column_rules.c.classification))
    ).fetchall()
    return (
        {r.tag_id for r in rows if r.tag_id is not None},
        {r.classification for r in rows if r.classification is not None},
    )


async def column_keys(conn: "Connection") -> dict["Target", set["Key"]]:
    """Each column's keys: the tags it carries -- a table column's own and its term's (REQ-2145),
    a command output column's own and its term's -- and its accepted classifications."""
    from provisa.core.schema_org import (
        glossary_term_refs as refs,
        glossary_terms as terms,
        profiler_classifications as pc,
        tag_assignments as ta,
    )
    from provisa.core.term_tags import FLOWING_TAGS, effective_column_tags, governs
    from provisa.security.column_rules import CLASSIFICATION, tag_keys

    out: dict[Target, set[Key]] = {}

    def add(target: "Target", keys: "Any") -> None:
        out.setdefault(target, set()).update(keys)

    for (table_id, column), tags in (await effective_column_tags(conn)).items():
        for tag in tags:
            add(("column", table_id, column), tag_keys(tag["tag_id"]))
    own = select(ta.c.command_name, ta.c.column_name, ta.c.tag_id).where(
        ta.c.object_type == "command_column"
    )
    for r in (await conn.execute_core(own)).fetchall():
        add(("command_column", r.command_name, r.column_name), tag_keys(r.tag_id))
    through = (
        select(refs.c.command_name, refs.c.column_name, ta.c.tag_id)
        .select_from(
            ta.join(refs, refs.c.term_id == ta.c.term_id).join(terms, terms.c.id == ta.c.term_id)
        )
        .where(
            refs.c.command_name.is_not(None),
            ta.c.object_type == "term",
            ta.c.base_tag_id.in_(FLOWING_TAGS),
            governs(terms),
        )
    )
    for r in (await conn.execute_core(through)).fetchall():
        add(("command_column", r.command_name, r.column_name), tag_keys(r.tag_id))
    accepted = select(pc.c.table_id, pc.c.command_name, pc.c.physical_column, pc.c.kind).where(
        pc.c.status == "accepted"
    )
    for r in (await conn.execute_core(accepted)).fetchall():
        target: Target = (
            ("column", r.table_id, r.physical_column)
            if r.table_id is not None
            else ("command_column", r.command_name, r.physical_column)
        )
        add(target, ((CLASSIFICATION, r.kind),))
    return out


async def ruled_columns(conn: "Connection") -> dict["Target", tuple["ColumnRule", ...]]:
    """Each column some rule reaches, with those rules: what the runtime build governs by."""
    from provisa.security.column_rules import ColumnRule, ruled_columns as _ruled

    rules = [ColumnRule.from_row(r) for r in await list_all(conn)]
    if not rules:
        return {}
    return _ruled(await column_keys(conn), rules)
