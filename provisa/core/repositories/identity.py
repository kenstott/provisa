# Copyright (c) 2026 Kenneth Stott
# Canary: f195d59e-33d4-4e5b-b3a5-5632893f4a7a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2141 (identity across variants): a relationship marked same entity says the rows it joins
in two tables are one real-world thing. It holds only between two tables the same class term
grounds, joined one-to-one on a key of each -- the primary key or a declared unique constraint --
so each row is the same entity as at most one row of the other. One rule, here, for every path
that saves such a relationship or changes the grounding it stands on."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from provisa.compiler.sql_types import key_list
from provisa.core.schema_org import (
    registered_tables,
    relationships,
    table_columns,
)

if TYPE_CHECKING:
    from provisa.core.database import Connection


@dataclass(frozen=True)
class IdentityRefused:
    """Why a relationship cannot be marked same entity: a stable code, its parameters, and the
    English text."""

    code: str
    message: str
    params: dict[str, Any] = field(default_factory=dict)


class TableInIdentity(ValueError):
    """A table's class term cannot change while a same-entity relationship stands on it."""

    def __init__(self, table: str, relationship: str) -> None:
        super().__init__(
            f"table {table} is joined as the same entity by relationship {relationship!r}: "
            "unmark it before changing the table's term"
        )
        self.table = table
        self.relationship = relationship


async def _address(conn: "Connection", table_id: int) -> str:
    row = (
        await conn.execute_core(
            select(
                registered_tables.c.source_id,
                registered_tables.c.schema_name,
                registered_tables.c.table_name,
            ).where(registered_tables.c.id == table_id)
        )
    ).one()
    return f"{row.source_id}.{row.schema_name}.{row.table_name}"


async def _class_term(conn: "Connection", table_id: int) -> int | None:
    row = (
        await conn.execute_core(
            select(registered_tables.c.class_term_id).where(registered_tables.c.id == table_id)
        )
    ).first()
    return None if row is None else row.class_term_id


async def _keys(conn: "Connection", table_id: int) -> list[frozenset[str]]:
    """The column sets that identify one row of ``table_id``: its primary key, and each unique
    constraint its source declares (REQ-1093)."""
    primary = frozenset(
        r.column_name
        for r in (
            await conn.execute_core(
                select(table_columns.c.column_name).where(
                    (table_columns.c.table_id == table_id) & table_columns.c.is_primary_key
                )
            )
        ).fetchall()
    )
    declared = (
        await conn.execute_core(
            select(registered_tables.c.unique_constraints).where(registered_tables.c.id == table_id)
        )
    ).scalar_one()
    keys = [frozenset(u["columns"]) for u in declared]
    return ([primary] if primary else []) + keys


async def same_entity_refusal(
    conn: "Connection",
    *,
    relationship: str,
    source_table_id: int,
    target_table_id: int | None,
    source_column: str,
    target_column: str,
    cardinality: str,
    junction: bool,
) -> IdentityRefused | None:
    """Why ``relationship`` cannot be marked same entity, as declared; None when it can."""
    if junction or target_table_id is None:
        return IdentityRefused(
            "schema.relationship_same_entity_direct",
            f"relationship {relationship!r} cannot be marked same entity: it joins through a "
            "junction or a function, not one row to one row",
            {"relationship": relationship},
        )
    if cardinality != "one-to-one":
        return IdentityRefused(
            "schema.relationship_same_entity_cardinality",
            f"relationship {relationship!r} cannot be marked same entity: it is {cardinality}, "
            "and the same entity is one row to one row",
            {"relationship": relationship, "cardinality": cardinality},
        )
    source, target = await _address(conn, source_table_id), await _address(conn, target_table_id)
    term = await _class_term(conn, source_table_id)
    if term is None or term != await _class_term(conn, target_table_id):
        return IdentityRefused(
            "schema.relationship_same_entity_not_grounded",
            f"relationship {relationship!r} cannot be marked same entity: {source} and {target} "
            "are not grounded in the same term",
            {"relationship": relationship, "source": source, "target": target},
        )
    for table_id, table, columns in (
        (source_table_id, source, source_column),
        (target_table_id, target, target_column),
    ):
        if frozenset(key_list(columns)) not in await _keys(conn, table_id):
            return IdentityRefused(
                "schema.relationship_same_entity_not_keys",
                f"relationship {relationship!r} cannot be marked same entity: {columns} is not "
                f"the primary key or a unique key of {table}",
                {"relationship": relationship, "table": table, "columns": columns},
            )
    return None


async def stored_refusal(conn: "Connection", relationship: str) -> IdentityRefused | None:
    """``same_entity_refusal`` for the relationship as stored."""
    row = (
        await conn.execute_core(select(relationships).where(relationships.c.id == relationship))
    ).one()
    return await same_entity_refusal(
        conn,
        relationship=relationship,
        source_table_id=row.source_table_id,
        target_table_id=row.target_table_id,
        source_column=row.source_column,
        target_column=row.target_column or "",
        cardinality=row.cardinality,
        junction=row.via_table_id is not None or row.target_function_name is not None,
    )


async def identity_on(conn: "Connection", table_id: int) -> str | None:
    """A same-entity relationship joining ``table_id``, which its class term stands under; None
    when there is none."""
    row = (
        await conn.execute_core(
            select(relationships.c.id)
            .where(
                relationships.c.same_entity
                & (
                    (relationships.c.source_table_id == table_id)
                    | (relationships.c.target_table_id == table_id)
                )
            )
            .order_by(relationships.c.id)
        )
    ).first()
    return None if row is None else row.id


async def refuse_term_change(conn: "Connection", table_id: int) -> None:
    """Raise :class:`TableInIdentity` when a same-entity relationship stands on ``table_id``'s
    class term: ungrounding the table, or grounding it in another term, would leave it joining
    two different kinds of thing as one."""
    held = await identity_on(conn, table_id)
    if held is not None:
        raise TableInIdentity(await _address(conn, table_id), held)


async def identity_gap_ids(conn: "Connection") -> set[int]:
    """The class terms grounded in several tables that same-entity relationships do not all join
    -- a curation gap (REQ-2130). glossary_terms_meta's identity_gap is the same rule."""
    tables: dict[int, set[int]] = {}
    for r in (
        await conn.execute_core(
            select(registered_tables.c.class_term_id, registered_tables.c.id).where(
                registered_tables.c.class_term_id.is_not(None)
            )
        )
    ).fetchall():
        tables.setdefault(r.class_term_id, set()).add(r.id)
    joined: dict[int, set[int]] = {}
    for r in (
        await conn.execute_core(
            select(relationships.c.source_table_id, relationships.c.target_table_id).where(
                relationships.c.same_entity
            )
        )
    ).fetchall():
        joined.setdefault(r.source_table_id, set()).add(r.target_table_id)
        joined.setdefault(r.target_table_id, set()).add(r.source_table_id)
    gaps: set[int] = set()
    for term, held in tables.items():
        reached = {min(held)}
        frontier = list(reached)
        while frontier:
            for other in joined.get(frontier.pop(), set()) & held:
                if other not in reached:
                    reached.add(other)
                    frontier.append(other)
        if reached != held:
            gaps.add(term)
    return gaps
