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

# Requirements: REQ-194, REQ-1914

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from sqlalchemy import and_, bindparam, delete, insert, select, update

from provisa.core.repositories.build_output import replacing_build_output
from provisa.core.schema_org import (
    model_name_clashes,
    model_names,
    registered_tables,
    table_columns,
)


_NAME_KEY = ("object_kind", "object_id", "surface")


def _stored_row(r: Any) -> dict[str, Any]:
    """A names-table row (names_table.NameRow) as ``model_names`` stores it."""
    return {
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


def names_table_changes(
    stored: Iterable[Mapping[str, Any]], rows: Iterable[Any]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """What turns the ``stored`` names table into the one of ``rows`` (names_table.NameRow): the
    keys of the stored rows to delete (each gone or changed) and the rows to insert (each new or
    changed). A row the build left as it was is in neither, so a registration writes the names
    of the tables it changed, not the whole model's."""
    wanted = {tuple(v[k] for k in _NAME_KEY): v for v in map(_stored_row, rows)}
    held = {tuple(s[k] for k in _NAME_KEY): dict(s) for s in stored}
    gone = [
        {f"key_{k}": part for k, part in zip(_NAME_KEY, key, strict=True)}
        for key, s in held.items()
        if wanted.get(key) != s
    ]
    new = [v for key, v in wanted.items() if held.get(key) != v]
    return gone, new


async def write_names_table(conn: Any, rows: Iterable[Any]) -> None:
    """Make the stored names table the one of ``rows`` (names_table.NameRow) and clear the refused
    clashes: a build that stored a names table refused none. Only the rows that differ from the
    stored ones are written (:func:`names_table_changes`)."""
    async with replacing_build_output(conn, model_names):
        # Read under the lock: the change is made to what the last build committed.
        stored = (await conn.execute_core(select(*model_names.c))).mappings().all()
        gone, new = names_table_changes(stored, rows)
        # One statement each however many names: the keys and rows travel beside it (a multi-row
        # VALUES binds ten parameters a name, and PostgreSQL refuses a statement of more than
        # 65535).
        await conn.execute_core_many(
            delete(model_names).where(
                and_(*(model_names.c[k] == bindparam(f"key_{k}") for k in _NAME_KEY))
            ),
            gone,
        )
        await conn.execute_core(delete(model_name_clashes))
        await conn.execute_core_many(insert(model_names), new)


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
    async with replacing_build_output(conn, model_names):
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
