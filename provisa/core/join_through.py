# Copyright (c) 2026 Kenneth Stott
# Canary: 7c2e9a41-5d8b-4f13-b6a0-3e9d1c7f5b24
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Two relationships that share a middle table, joined through it (REQ-1586).

Each relationship is a hop from a referring column to the column it refers to. Two hops share a
middle table when:

- they refer from the same table, a junction J (J.a -> A, J.b -> B): the joined relationship runs
  A -> J -> B, and J's other columns are its attributes; or
- they refer to the same table, a shared dimension D (A.x -> D, B.y -> D): the joined relationship
  runs A -> D -> B.

Either way the joined relationship is one junction-backed (via_table) relationship between the two
other ends, many-to-many, and the two hops it is made of stay.
"""

# Requirements: REQ-1586

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from provisa.core.database import Connection

#: How the two hops share their middle table.
Shape = Literal["junction", "shared_dimension"]

#: The cardinality a junction-backed relationship is stored with: each source row reaches a list
#: of target rows through the middle table (models.Cardinality has no many-to-many member).
VIA_CARDINALITY = "one-to-many"


@dataclass(frozen=True)
class Hop:
    """One relationship as registered: its ends by registered table id. ``via_table_id`` set: it is
    junction-backed already, and joins nothing further."""

    rel_id: str
    source_table_id: int
    source_column: str
    target_table_id: int | None
    target_column: str
    cardinality: str
    via_table_id: int | None = None


@dataclass(frozen=True)
class _Reference:
    """A hop read as a reference: ``referring`` column refers to ``referred`` column."""

    referring_table: int
    referring_column: str
    referred_table: int
    referred_column: str


@dataclass(frozen=True)
class Through:
    """The relationship two hops make through their middle table: from ``source`` to ``target``
    through ``via_table_id``, joined source_column = via_source_column and via_target_column =
    target_column."""

    shape: Shape
    via_table_id: int
    source_table_id: int
    source_column: str
    target_table_id: int
    target_column: str
    via_source_column: str
    via_target_column: str


def _reference(hop: Hop) -> _Reference | None:
    """The hop as a reference, or None for one that is no single column pair between two tables:
    a computed relationship (no target table) or one junction-backed already."""
    if hop.target_table_id is None or hop.via_table_id is not None or not hop.target_column:
        return None
    if hop.cardinality == "one-to-many":
        return _Reference(
            hop.target_table_id, hop.target_column, hop.source_table_id, hop.source_column
        )
    if hop.cardinality in ("many-to-one", "one-to-one"):
        return _Reference(
            hop.source_table_id, hop.source_column, hop.target_table_id, hop.target_column
        )
    return None


def qualify(first: Hop, second: Hop) -> Through | None:
    """The relationship ``first`` and ``second`` make through the table they share, or None when
    they share none. The middle table is neither end: a hop between a table and itself, or one
    whose end is the other's middle, shares no middle table. When both shapes fit (two hops from
    one junction to one table), the junction is the middle. A junction's two legs refer from two
    of its columns: one column referring to two tables is no junction."""
    a, b = _reference(first), _reference(second)
    if a is None or b is None or a == b:
        return None
    if (
        a.referring_table == b.referring_table
        and a.referring_column != b.referring_column
        and a.referring_table not in (a.referred_table, b.referred_table)
    ):
        return Through(
            shape="junction",
            via_table_id=a.referring_table,
            source_table_id=a.referred_table,
            source_column=a.referred_column,
            target_table_id=b.referred_table,
            target_column=b.referred_column,
            via_source_column=a.referring_column,
            via_target_column=b.referring_column,
        )
    if a.referred_table == b.referred_table and a.referred_table not in (
        a.referring_table,
        b.referring_table,
    ):
        return Through(
            shape="shared_dimension",
            via_table_id=a.referred_table,
            source_table_id=a.referring_table,
            source_column=a.referring_column,
            target_table_id=b.referring_table,
            target_column=b.referring_column,
            via_source_column=a.referred_column,
            via_target_column=b.referred_column,
        )
    return None


def rel_id(source_table: str, target_table: str, via_table: str) -> str:
    """The generated id of the joined relationship, before it is told apart (REQ-415)."""
    return f"{source_table}-{target_table}-via-{via_table}"


def cypher_label(via_table: str) -> str:
    """The Cypher type the joined relationship is served under, before it is told apart: its
    middle table's name, as a junction nominating its table is."""
    from provisa.gql.label_map import _upper_snake

    return _upper_snake(via_table)


async def existing(conn: "Connection", through: Through) -> str | None:
    """The relationship already joining the two ends through the middle table on the same columns,
    either way round, or None."""
    from sqlalchemy import select

    from provisa.core.schema_org import relationships as r

    found = (
        await conn.execute_core(
            select(
                r.c.id,
                r.c.source_table_id,
                r.c.source_column,
                r.c.target_table_id,
                r.c.target_column,
                r.c.via_source_column,
                r.c.via_target_column,
            ).where(r.c.via_table_id == through.via_table_id, r.c.retired.is_(False))
        )
    ).fetchall()
    forward = (
        through.source_table_id,
        through.source_column,
        through.via_source_column,
        through.via_target_column,
        through.target_column,
        through.target_table_id,
    )
    for row in found:
        held = (
            int(row.source_table_id),
            row.source_column,
            row.via_source_column,
            row.via_target_column,
            row.target_column,
            None if row.target_table_id is None else int(row.target_table_id),
        )
        if held in (forward, forward[::-1]):
            return str(row.id)
    return None
