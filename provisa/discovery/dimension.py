# Copyright (c) 2026 Kenneth Stott
# Canary: 3e9a6c18-7b2d-4f50-9d41-c8e5a2b7f063
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A many-to-many pair accepted as a shared dimension (REQ-413).

Neither column of the pair is a key, so no relationship starts at one and ends at the other.
Accepting the pair makes a derived table (a view) holding each value of the two columns once,
named for the concept the proposal names, and a many-to-one relationship from each side to it:
both sides then join through it on every front door, and the two sides are joined to each other
through it by one junction-backed relationship (REQ-1586). The dimension is an ordinary view, which
can be renamed or extended.

A second pair on the same values reuses the dimension: a side already joined many-to-one to a
derived table, or to a registered table marked a dimension (a reference table carrying the
dimension's own details), is joined to that dimension, and only the side not yet joined gains a
relationship. This module plans what accepting makes; the admin API makes it through the
mutations that register a view and create a relationship.
"""

# Requirements: REQ-413, REQ-1586

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy import and_, or_, select, tuple_

from provisa.core.join_through import Through
from provisa.core.models import DERIVED_SOURCE_ID
from provisa.core.schema_org import registered_tables, relationships, table_columns

if TYPE_CHECKING:
    from provisa.core.database import Connection

#: The schema a dimension's view is registered under, as a code resolver's bridge view is.
VIEW_SCHEMA = "views"

#: A column: (registered table id, column name).
ColumnRef = tuple[int, str]


class TwoDimensions(ValueError):
    """The pair's two sides already join two different dimensions."""


@dataclass(frozen=True)
class Side:
    """One column of the pair, as its table is registered."""

    table_id: int
    table_name: str
    domain_id: str
    column: str
    data_type: str


@dataclass(frozen=True)
class Plan:
    """What accepting the pair makes. ``table_id`` None: the dimension is made, as ``view_sql``;
    else it is the registered dimension one side already joins, and ``view_sql`` is None.
    ``joining``: the sides not yet joined to it, each gaining a many-to-one relationship.
    ``sides``: the pair's two columns, which the dimension joins to each other
    (REQ-1586)."""

    name: str
    column: str
    domain_id: str
    data_type: str
    table_id: int | None
    view_sql: str | None
    joining: tuple[Side, ...]
    sides: tuple[Side, Side]


@dataclass(frozen=True)
class Edge:
    """A side's many-to-one relationship to the dimension."""

    id: str
    source_table: str
    source_column: str
    target_table: str
    target_column: str
    cardinality: str
    graphql_alias: str


def concept(suggested_name: str | None, column: str) -> str:
    """The dimension's name: the business concept the proposal names (a kebab-case phrase), as an
    identifier. A proposal naming none takes the name its values are held under, the column's
    (REQ-413: the dimension is named for the concept its values are)."""
    words = re.findall(r"[a-z0-9]+", (suggested_name or column).lower())
    return "_".join(words)


async def _sides(conn: "Connection", refs: tuple[ColumnRef, ColumnRef]) -> tuple[Side, Side]:
    found = (
        await conn.execute_core(
            select(
                registered_tables.c.id,
                registered_tables.c.table_name,
                registered_tables.c.domain_id,
                table_columns.c.column_name,
                table_columns.c.data_type,
            )
            .select_from(
                registered_tables.join(
                    table_columns, table_columns.c.table_id == registered_tables.c.id
                )
            )
            .where(tuple_(table_columns.c.table_id, table_columns.c.column_name).in_(list(refs)))
        )
    ).fetchall()
    by_ref = {(int(r.id), str(r.column_name)): r for r in found}
    out: list[Side] = []
    for ref in refs:
        row = by_ref.get(ref)
        if row is None:
            raise ValueError(f"column {ref[1]!r} of table {ref[0]} is not registered")
        out.append(Side(int(row.id), row.table_name, row.domain_id, ref[1], str(row.data_type)))
    return out[0], out[1]


@dataclass(frozen=True)
class _Joined:
    table_id: int
    name: str
    column: str


async def _joined(conn: "Connection", refs: tuple[ColumnRef, ...]) -> dict[ColumnRef, _Joined]:
    """The dimension each column of ``refs`` already joins: the derived table (a registered view)
    or the registered table marked a dimension (REQ-1320) its many-to-one relationship ends at,
    whose key holds the values (REQ-1586, amended 2026-11-22)."""
    rel, tbl = relationships, registered_tables
    found = (
        await conn.execute_core(
            select(
                rel.c.source_table_id,
                rel.c.source_column,
                tbl.c.id,
                tbl.c.table_name,
                rel.c.target_column,
            )
            .select_from(rel.join(tbl, tbl.c.id == rel.c.target_table_id))
            .where(
                tuple_(rel.c.source_table_id, rel.c.source_column).in_(list(refs)),
                rel.c.cardinality == "many-to-one",
                rel.c.retired.is_(False),
                or_(
                    and_(tbl.c.source_id == DERIVED_SOURCE_ID, tbl.c.view_sql.is_not(None)),
                    tbl.c.modeling_role == "dimension",
                ),
                tbl.c.retired.is_(False),
            )
            .order_by(rel.c.id)
        )
    ).fetchall()
    joined: dict[ColumnRef, _Joined] = {}
    for r in found:
        joined.setdefault(
            (int(r.source_table_id), str(r.source_column)),
            _Joined(int(r.id), str(r.table_name), str(r.target_column)),
        )
    return joined


def _held(joined: dict[ColumnRef, _Joined], refs: tuple[ColumnRef, ColumnRef]) -> _Joined | None:
    dims = {joined[r].table_id: joined[r] for r in refs if r in joined}
    if len(dims) > 1:
        a, b = (joined[r] for r in refs)
        raise TwoDimensions(
            f"its columns already join two different dimensions, {a.name} and {b.name}"
        )
    return next(iter(dims.values()), None)


def _statement_name(side: Side) -> str:
    """A table as a view's statement names it: ``domain.table``."""
    from provisa.compiler.naming import domain_to_sql_name

    return f"{domain_to_sql_name(side.domain_id)}.{side.table_name}"


def _quoted(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def view_sql(column: str, sides: tuple[Side, ...]) -> str:
    """Each value the sides' columns hold, once (UNION), under ``column``."""
    return " UNION ".join(
        f"SELECT {_quoted(s.column)} AS {_quoted(column)} FROM {_statement_name(s)} "
        f"WHERE {_quoted(s.column)} IS NOT NULL"
        for s in sides
    )


async def named(
    conn: "Connection", source: ColumnRef, target: ColumnRef, suggested_name: str | None
) -> tuple[str, bool]:
    """The dimension accepting the pair joins, before it is accepted: its name, and whether it is
    one already registered."""
    held = _held(await _joined(conn, (source, target)), (source, target))
    if held is not None:
        return held.name, True
    return concept(suggested_name, target[1]), False


async def plan(
    conn: "Connection",
    source: ColumnRef,
    target: ColumnRef,
    *,
    suggested_name: str | None,
    name: str | None,
) -> Plan:
    """What accepting the pair makes: a new dimension named ``name`` (the reviewer's) or for the
    proposal's concept, or the dimension one side already joins, and the relationships of the
    sides not yet joined to it."""
    refs = (source, target)
    sides = await _sides(conn, refs)
    joined = await _joined(conn, refs)
    held = _held(joined, refs)
    if held is not None:
        return Plan(
            name=held.name,
            column=held.column,
            domain_id=sides[0].domain_id,
            data_type=sides[0].data_type,
            table_id=held.table_id,
            view_sql=None,
            joining=tuple(s for s in sides if (s.table_id, s.column) not in joined),
            sides=sides,
        )
    column = concept(suggested_name, target[1])
    return Plan(
        name=name or column,
        column=column,
        domain_id=sides[0].domain_id,
        data_type=sides[0].data_type,
        table_id=None,
        view_sql=view_sql(column, sides),
        joining=sides,
        sides=sides,
    )


def through(planned: Plan, dimension_id: int) -> Through:
    """The relationship between the pair's two sides through the dimension (REQ-1586): each side
    joins the dimension's one column."""
    first, second = planned.sides
    return Through(
        shape="shared_dimension",
        via_table_id=dimension_id,
        source_table_id=first.table_id,
        source_column=first.column,
        target_table_id=second.table_id,
        target_column=second.column,
        via_source_column=planned.column,
        via_target_column=planned.column,
    )


def edge(planned: Plan, side: Side) -> Edge:
    """``side``'s many-to-one relationship to the dimension. Its GraphQL field is named for the
    dimension: the side's own column commonly holds the concept's name, and a field named for the
    target alone would take that column's place."""
    return Edge(
        id=f"{side.table_name}-{side.column}-to-{planned.name}",
        source_table=side.table_name,
        source_column=side.column,
        target_table=planned.name,
        target_column=planned.column,
        cardinality="many-to-one",
        graphql_alias=f"{planned.name}_dimension",
    )
