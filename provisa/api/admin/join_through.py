# Copyright (c) 2026 Kenneth Stott
# Canary: 4a8d2f6e-1c3b-4e79-9b05-d7e2a6c8f134
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Joining two relationships through the table they share (REQ-1586).

:func:`plan` says what two relationships of the list make through their middle table
(:func:`provisa.core.join_through.qualify`), named as generated names are (REQ-415).
:func:`create` is the one way a junction-backed relationship between two ends is made: the
Relationships list's Join through, the shared dimension a discovery run's pair is accepted as, and
a discovery run's junction suggestion all call it. It saves through createRelationship's own body,
so the caller's rights, the domain gate and the registry's checks apply as to any relationship.
"""

# Requirements: REQ-1586, REQ-415, REQ-413

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from provisa.core import relationship_naming
from provisa.core.join_through import (
    VIA_CARDINALITY,
    Hop,
    Through,
    cypher_label,
    existing,
    qualify,
)
from provisa.core.join_through import rel_id as through_rel_id
from provisa.core.schema_org import registered_tables, relationships, table_columns

if TYPE_CHECKING:
    from provisa.api.admin.types import MutationResult
    from provisa.core.database import Connection

#: The nomination a joined relationship's Cypher type and GraphQL field are named from: its alias,
#: which is its middle table's name told apart from the names already taken.
LABEL_SOURCE = "fixed"


@dataclass(frozen=True)
class _Table:
    name: str
    alias: str | None


@dataclass(frozen=True)
class Plan:
    """What joining two relationships makes: ``through`` between tables named ``source_table`` and
    ``target_table`` through ``via_table``, saved under ``named``. ``attributes``: the middle
    table's columns that are no key of the join, the relationship's attributes."""

    through: Through
    source_table: str
    target_table: str
    via_table: str
    named: relationship_naming.Named
    attributes: tuple[str, ...]


async def _hops(conn: "Connection", ids: tuple[str, str]) -> list[Hop]:
    r = relationships
    rows = (
        await conn.execute_core(
            select(
                r.c.id,
                r.c.source_table_id,
                r.c.source_column,
                r.c.target_table_id,
                r.c.target_column,
                r.c.cardinality,
                r.c.via_table_id,
            ).where(r.c.id.in_(ids), r.c.retired.is_(False))
        )
    ).fetchall()
    by_id = {
        row.id: Hop(
            rel_id=row.id,
            source_table_id=int(row.source_table_id),
            source_column=row.source_column or "",
            target_table_id=None if row.target_table_id is None else int(row.target_table_id),
            target_column=row.target_column or "",
            cardinality=row.cardinality or "",
            via_table_id=None if row.via_table_id is None else int(row.via_table_id),
        )
        for row in rows
    }
    return [by_id[i] for i in ids if i in by_id]


async def _tables(conn: "Connection", ids: set[int]) -> dict[int, _Table]:
    t = registered_tables
    rows = (
        await conn.execute_core(select(t.c.id, t.c.table_name, t.c.alias).where(t.c.id.in_(ids)))
    ).fetchall()
    return {int(row.id): _Table(row.table_name, row.alias) for row in rows}


async def _attributes(conn: "Connection", through: Through) -> tuple[str, ...]:
    keys = {through.via_source_column, through.via_target_column}
    rows = (
        await conn.execute_core(
            select(table_columns.c.column_name)
            .where(table_columns.c.table_id == through.via_table_id)
            .order_by(table_columns.c.id)
        )
    ).fetchall()
    return tuple(row.column_name for row in rows if row.column_name not in keys)


async def _named(
    conn: "Connection", through: Through, tables: dict[int, _Table], rel_id: str | None
) -> relationship_naming.Named:
    """REQ-415: the names the joined relationship is saved under. A name the person typed is kept
    as typed and refused when taken; the generated one is told apart."""
    source = tables[through.source_table_id]
    target = tables[through.target_table_id]
    via = tables[through.via_table_id]
    (named,) = await relationship_naming.unique_names(
        conn,
        [
            relationship_naming.Generated(
                rel_id=rel_id or through_rel_id(source.name, target.name, via.name),
                id_typed=rel_id is not None,
                source_table_id=through.source_table_id,
                target_table_id=through.target_table_id,
                source_column=through.source_column,
                target_column=through.target_column,
                cardinality=VIA_CARDINALITY,
                target_name=target.alias or target.name,
                target_table=target.name,
                alias=cypher_label(via.name),
                via_label_source=LABEL_SOURCE,
                via_table_name=via.name,
            )
        ],
    )
    return named


async def plan(conn: "Connection", first_id: str, second_id: str) -> Plan | None:
    """What joining the relationships ``first_id`` and ``second_id`` through the table they share
    makes, or None: they share no middle table, or the two ends are already joined through it."""
    hops = await _hops(conn, (first_id, second_id))
    if len(hops) != 2:
        return None
    through = qualify(hops[0], hops[1])
    if through is None or await existing(conn, through) is not None:
        return None
    tables = await _tables(
        conn, {through.source_table_id, through.target_table_id, through.via_table_id}
    )
    return Plan(
        through=through,
        source_table=tables[through.source_table_id].name,
        target_table=tables[through.target_table_id].name,
        via_table=tables[through.via_table_id].name,
        named=await _named(conn, through, tables, None),
        attributes=await _attributes(conn, through),
    )


async def create(
    info: Any, conn: "Connection", through: Through, rel_id: str | None
) -> tuple["MutationResult", str]:
    """Create the junction-backed relationship ``through`` describes, under ``rel_id`` as typed or
    a generated id (REQ-415), through createRelationship's body. The result and the id it is
    saved under."""
    from provisa.api.admin.schema_mutation import _save_relationship_as_caller
    from provisa.api.admin.types import RelationshipInput

    tables = await _tables(
        conn, {through.source_table_id, through.target_table_id, through.via_table_id}
    )
    named = await _named(conn, through, tables, rel_id)
    result = await _save_relationship_as_caller(
        info,
        RelationshipInput(
            id=named.rel_id,
            source_table_id=tables[through.source_table_id].name,
            target_table_id=tables[through.target_table_id].name,
            source_column=through.source_column,
            target_column=through.target_column,
            cardinality=VIA_CARDINALITY,
            alias=named.alias,
            graphql_alias=named.graphql_alias,
            via_table=tables[through.via_table_id].name,
            via_source_column=through.via_source_column,
            via_target_column=through.via_target_column,
            via_label_source=LABEL_SOURCE,
        ),
        creating=True,
    )
    return result, named.rel_id


class JoinRefused(ValueError):
    """Accepting a junction suggestion was refused, and why."""


#: What createRelationship answers when the caller may not create the relationship and a request
#: for it is queued instead: nothing was made.
_QUEUED = "schema.relationship_request_queued"


async def accept_proposal(
    request: Any, conn: "Connection", run_id: int, proposal_id: int, name: str | None
) -> str:
    """Accept a discovery run's junction suggestion (REQ-1586): create the relationship it joins
    through its junction, under the reviewer's ``name`` or a generated one, and record the
    proposal accepted. The relationship's id is returned."""
    import types

    from provisa.discovery import proposals as proposals_repo

    await proposals_repo.undecided(conn, run_id, proposal_id)
    through = await proposals_repo.junction_of(conn, run_id, proposal_id)
    assert through is not None  # the caller asked junction_of first
    info = types.SimpleNamespace(context={"request": request})
    result, made = await create(info, conn, through, name or None)
    if not result.success:
        raise JoinRefused(f"The relationship {made}: {result.message}")
    if result.code == _QUEUED:
        raise JoinRefused(f"The relationship {made} was not made: {result.message}")
    try:
        await proposals_repo.accepted_as(conn, run_id, proposal_id, made)
    except Exception:
        # Nothing is half made: the relationship this accept made a moment ago goes with it.
        from provisa.api.admin.schema_helpers import _rebuild_schemas
        from provisa.core.repositories import relationship as rel_repo

        async with conn.transaction():
            await rel_repo.remove_where(conn, relationships.c.id == made)
        await _rebuild_schemas()
        raise
    return made
