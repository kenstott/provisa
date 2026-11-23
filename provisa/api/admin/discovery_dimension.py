# Copyright (c) 2026 Kenneth Stott
# Canary: 8d1f4b63-2a7e-4c95-b0d8-6e3a9c5f1b27
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Accepting a many-to-many proposal of a discovery run as a shared dimension (REQ-413).

What :func:`provisa.discovery.dimension.plan` says accepting makes is made by the mutations a
steward's own forms call: the dimension's view by registerTable, each side's relationship to it
by createRelationship, and the two sides' relationship to each other through it by
:func:`provisa.api.admin.join_through.create` (REQ-1586). Nothing is half made: a relationship refused removes what this accept
made before it, and the proposal stays undecided.
"""

# Requirements: REQ-413, REQ-1586

from __future__ import annotations

import types
from typing import TYPE_CHECKING, Any, cast

from sqlalchemy import select

from provisa.api.admin import join_through
from provisa.core import relationship_naming
from provisa.core.schema_org import registered_tables
from provisa.discovery import dimension
from provisa.discovery import proposals as proposals_repo

if TYPE_CHECKING:
    from provisa.api.admin.types import MutationResult
    from provisa.core.database import Connection

#: What registerTable or createRelationship answers when the caller may not create the object
#: and a request for it is queued instead: nothing was made.
_QUEUED = frozenset({"schema.creation_request_queued", "schema.relationship_request_queued"})


class DimensionRefused(ValueError):
    """Accepting the proposal as a shared dimension was refused, and why."""


def _made(result: "MutationResult", what: str) -> None:
    if not result.success:
        raise DimensionRefused(f"{what}: {result.message}")
    if result.code in _QUEUED:
        raise DimensionRefused(f"{what} was not made: {result.message}")


async def readers(conn: "Connection", sides: tuple[dimension.Side, ...]) -> list[str]:
    """The roles served every side's column: those the dimension's one column is visible to,
    since it holds the values of them all (the rule a code resolver's bridge view keeps)."""
    from provisa.api.app import state
    from provisa.core.repositories import table as table_repo
    from provisa.security.rights import served_roles

    seen: frozenset[str] | None = None
    for side in sides:
        table = await table_repo.get(conn, side.table_id)
        assert table is not None  # planned from its registration
        column = next(c for c in table["columns"] if c["column_name"] == side.column)
        served = served_roles(state.roles, state.role_chains, side.domain_id, column)
        seen = served if seen is None else seen & served
    return sorted(seen or ())


def _view_input(planned: dimension.Plan, visible_to: list[str]) -> Any:
    from provisa.api.admin.types import ColumnInput, TableInput
    from provisa.core.models import DERIVED_SOURCE_ID

    sides = ", ".join(f"{s.table_name}.{s.column}" for s in planned.joining)
    return TableInput(
        source_id=DERIVED_SOURCE_ID,
        domain_id=planned.domain_id,
        schema_name=dimension.VIEW_SCHEMA,
        table_name=planned.name,
        description=f"Each {planned.column} value of {sides}, once: the dimension they share.",
        columns=[
            ColumnInput(
                name=planned.column,
                visible_to=visible_to,
                data_type=planned.data_type,
                is_primary_key=True,
            )
        ],
        view_sql=planned.view_sql,
        # REQ-1586 (amended 2026-11-22): the shared dimension is marked a dimension (REQ-1320).
        modeling_role="dimension",
    )


def _edge_input(edge: dimension.Edge, named: relationship_naming.Named) -> Any:
    from provisa.api.admin.types import RelationshipInput

    return RelationshipInput(
        id=named.rel_id,
        source_table_id=edge.source_table,
        target_table_id=edge.target_table,
        source_column=edge.source_column,
        target_column=edge.target_column,
        cardinality=edge.cardinality,
        alias=named.alias,
        graphql_alias=named.graphql_alias,
    )


async def _edge_names(
    conn: "Connection", planned: dimension.Plan, dimension_id: int, edges: list[dimension.Edge]
) -> list[relationship_naming.Named]:
    """REQ-415: the names each side's edge is saved under, told apart from the relationships
    registered and from each other (two columns of one table joining one dimension)."""
    return await relationship_naming.unique_names(
        conn,
        [
            relationship_naming.Generated(
                rel_id=edge.id,
                id_typed=False,
                source_table_id=side.table_id,
                target_table_id=dimension_id,
                source_column=edge.source_column,
                target_column=edge.target_column,
                cardinality=edge.cardinality,
                target_name=planned.name,
                target_table=planned.name,
                graphql_alias=edge.graphql_alias,
            )
            for side, edge in zip(planned.joining, edges, strict=True)
        ],
    )


async def _registered(conn: "Connection", planned: dimension.Plan) -> int:
    found = (
        await conn.execute_core(
            select(registered_tables.c.id).where(
                registered_tables.c.domain_id == planned.domain_id,
                registered_tables.c.schema_name == dimension.VIEW_SCHEMA,
                registered_tables.c.table_name == planned.name,
            )
        )
    ).scalar_one()
    return int(found)


async def _undo(conn: "Connection", edge_ids: list[str], view_id: int | None) -> None:
    """Remove what an accept that did not complete made. None of it was decided by anyone: the
    view and its relationships were made a moment ago by this accept."""
    from provisa.api.admin.schema_helpers import _rebuild_schemas
    from provisa.core.repositories import relationship as rel_repo
    from provisa.core.repositories import table as table_repo
    from provisa.core.schema_org import relationships

    async with conn.transaction():
        if edge_ids:
            await rel_repo.remove_where(conn, relationships.c.id.in_(edge_ids))
        if view_id is not None:
            await table_repo.discard(conn, view_id)
    await _rebuild_schemas()


async def accept(
    request: Any, conn: "Connection", run_id: int, proposal_id: int, name: str | None
) -> str:
    """Accept a many-to-many proposal as a shared dimension: make the dimension (or reuse the
    one a side already joins) and the relationships of the sides not yet joined to it, then
    record the proposal accepted. The dimension's name is returned."""
    from provisa.api.admin import schema_mutation_ops as ops
    from provisa.api.admin.schema_mutation import _save_relationship_as_caller

    row = await proposals_repo.undecided(conn, run_id, proposal_id)
    try:
        planned = await dimension.plan(
            conn,
            (int(row.source_table_id), str(row.source_column)),
            (int(row.target_table_id), str(row.target_column)),
            suggested_name=row.suggested_name,
            name=name,
        )
    except dimension.TwoDimensions as two:
        raise DimensionRefused(f"Proposal {proposal_id} cannot be accepted: {two}") from two
    info = cast(Any, types.SimpleNamespace(context={"request": request}))
    view_id: int | None = None
    dimension_id = planned.table_id
    if dimension_id is None:
        visible_to = await readers(conn, planned.joining)
        if not visible_to:
            raise DimensionRefused(
                f"Proposal {proposal_id} cannot be accepted: no role sees both "
                + " and ".join(f"{s.table_name}.{s.column}" for s in planned.joining)
            )
        _made(
            await ops.register_table(info, _view_input(planned, visible_to)),
            f"The dimension {planned.name}",
        )
        view_id = dimension_id = await _registered(conn, planned)
    made: list[str] = []
    try:
        edges = [dimension.edge(planned, side) for side in planned.joining]
        for edge, named in zip(
            edges, await _edge_names(conn, planned, dimension_id, edges), strict=True
        ):
            _made(
                await _save_relationship_as_caller(info, _edge_input(edge, named), creating=True),
                f"The relationship {named.rel_id}",
            )
            made.append(named.rel_id)
        # REQ-1586: the two sides joined to each other through the dimension, made as every
        # junction-backed relationship between two ends is.
        joined = dimension.through(planned, dimension_id)
        if await join_through.existing(conn, joined) is None:
            result, joined_id = await join_through.create(info, conn, joined, None)
            _made(result, f"The relationship {joined_id}")
            made.append(joined_id)
        await proposals_repo.accepted_as(conn, run_id, proposal_id, planned.name)
    except Exception:
        await _undo(conn, made, view_id)
        raise
    return planned.name
