# Copyright (c) 2026 Kenneth Stott
# Canary: 4c7e2a9d-6b13-4f58-9e0a-1d3b5c8f7a26
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The stored names table (REQ-194): the last model build's names, its refused clashes, and the
aliases the disambiguate policy froze. The build is the only writer of the first two; the third
is a model change of its own."""

# Requirements: REQ-194

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from sqlalchemy import and_, delete, insert, update

from provisa.core.schema_org import (
    model_name_clashes,
    model_names,
    registered_tables,
    table_columns,
)


# Every worker builds the model it is told changed (REQ-1072), so two workers' builds write these
# tables at once: a build holds this while it replaces them.
_NAMES_WRITE_LOCK = 7261748191


async def write_names_table(conn: Any, rows: Iterable[Any]) -> None:
    """Replace the stored names table with ``rows`` (names_table.NameRow) and clear the refused
    clashes: a build that stored a names table refused none. One transaction under the writers'
    lock, so a concurrent build's rows are never met half replaced."""
    values = [
        {
            "object_kind": r.object_kind,
            "object_id": r.object_id,
            "surface": r.surface,
            "name": r.name,
            "physical_name": r.physical_name,
            "label": r.label,
            "domain_id": r.domain_id,
            "table_id": r.table_id,
            "is_alias": r.alias,
            "warnings": [{"code": w.code, "message": w.message} for w in r.warnings],
        }
        for r in rows
    ]
    async with conn.transaction():
        await conn.advisory_xact_lock(_NAMES_WRITE_LOCK)
        await conn.execute_core(delete(model_names))
        await conn.execute_core(delete(model_name_clashes))
        # One statement however many names: a multi-row VALUES binds ten parameters a name, and
        # PostgreSQL refuses a statement of more than 65535.
        await conn.execute_core_many(insert(model_names), values)


async def write_refused_clashes(conn: Any, clashes: Iterable[Any]) -> None:
    """Replace the stored refused clashes with ``clashes`` (names_table.NameClash). The names
    table stays as the last version built stored it."""
    values = {
        (c.surface, c.name, c.newcomer): {
            "surface": c.surface,
            "name": c.name,
            "holder": c.holder,
            "newcomer": c.newcomer,
            "message": c.message(),
        }
        for c in clashes
    }
    async with conn.transaction():
        await conn.advisory_xact_lock(_NAMES_WRITE_LOCK)
        await conn.execute_core(delete(model_name_clashes))
        await conn.execute_core_many(insert(model_name_clashes), list(values.values()))


async def freeze_aliases(conn: Any, frozen: Iterable[Any]) -> None:
    """Keep each name the disambiguate policy derived (names_table.FrozenAlias) as its object's
    alias, so later builds never rename it (REQ-194, CLASH POLICY). An object that has an alias
    keeps it: only a steward changes one. A table's or view's is ``registered_tables.alias``, a
    column's ``table_columns.alias`` (the column's SQL name)."""
    frozen = list(frozen)
    if not frozen:
        return
    from provisa.core import model_change

    async with model_change.scope("the names table froze disambiguated names as aliases"):
        for f in frozen:
            if f.object_kind in ("table", "view"):
                await conn.execute_core(
                    update(registered_tables)
                    .where(
                        and_(
                            registered_tables.c.id == int(f.object_id),
                            registered_tables.c.alias.is_(None),
                        )
                    )
                    .values(alias=f.alias)
                )
            elif f.object_kind == "column":
                table_id, column = f.object_id.split(":", 1)
                await conn.execute_core(
                    update(table_columns)
                    .where(
                        and_(
                            table_columns.c.table_id == int(table_id),
                            table_columns.c.column_name == column,
                            table_columns.c.alias.is_(None),
                        )
                    )
                    .values(alias=f.alias)
                )
            else:
                raise ValueError(f"a {f.object_kind} has no stored alias to freeze {f.alias!r} in")
