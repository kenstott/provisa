# Copyright (c) 2026 Kenneth Stott
# Canary: 3f9a1d7e-2c64-4b85-a0e3-7d5b9c1e4f26
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unique names for generated relationships (REQ-415)."""

# Requirements: REQ-415

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from provisa.core.database import Connection


class RelationshipNameTaken(ValueError):
    """A relationship name a person typed is one another relationship already has (``kind``: the
    id, alias or GraphQL field), or a generated name that neither its column nor its table tells
    apart. Refused by name."""

    code = "relationship.name_taken"

    def __init__(self, kind: str, name: str, held_by: str) -> None:
        self.kind = kind
        self.name = name
        self.held_by = held_by
        self.params = {"kind": kind, "name": name, "held_by": held_by}
        super().__init__(
            f"The name {name!r} is taken by relationship {held_by!r}; name this one otherwise"
        )


def disambiguated(base: str, column: str, table: str, taken: Iterable[str]) -> str:
    """``base`` where no name in ``taken`` is it, else ``<base>_by_<column>``, else
    ``<base>_by_<table>_<column>``. Names are compared without case."""
    held = {t.casefold() for t in taken}
    names = (base, f"{base}_by_{column}", f"{base}_by_{table}_{column}")
    for name in names:
        if name.casefold() not in held:
            return name
    raise RelationshipNameTaken("generated", names[-1], names[-1])


@dataclass(frozen=True)
class Generated:
    """A relationship about to be saved under generated names. ``rel_id`` is its id, made unique
    unless ``id_typed`` (a person named it, or it is unique by construction): a typed id that is
    taken is refused. ``alias`` is a generated alias (a foreign key's), made unique on its source
    table. ``target_name`` is the target's alias or table name, the noun its derived GraphQL field
    is named for; ``target_table`` its table name, the second thing names are told apart by.
    ``graphql_alias`` is a generated field it is pinned to (a shared dimension's edge), made unique
    on its source table as the derived one is. ``via_label_source`` and ``via_table_name``: a
    junction-backed relationship's nomination and middle table (REQ-1586), which its field and
    Cypher type are named from."""

    rel_id: str
    id_typed: bool
    source_table_id: int
    target_table_id: int
    source_column: str
    target_column: str
    cardinality: str
    target_name: str
    target_table: str
    alias: str | None = None
    graphql_alias: str | None = None
    via_label_source: str | None = None
    via_table_name: str | None = None


@dataclass(frozen=True)
class Named:
    """The names a generated relationship is saved under: ``alias`` and ``graphql_alias`` are
    None where the derived names are already unique."""

    rel_id: str
    alias: str | None
    graphql_alias: str | None


@dataclass
class _Held:
    """One relationship's names as the namespaces see them."""

    rel_id: str
    source_table_id: int
    target_table_id: int | None
    alias: str | None
    field: str | None  # its GraphQL field, snake-cased
    cypher_type: str | None
    served: bool


def _field_of(
    graphql_alias: str | None,
    target_name: str,
    cardinality: str,
    alias: str | None = None,
    via_label_source: str | None = None,
    via_type_value: str | None = None,
    via_table_name: str | None = None,
) -> str | None:
    """A relationship's GraphQL field, snake-cased so the naming convention does not separate two
    spellings of one field: its pinned field, else the one derived for its target (REQ-194)."""
    from provisa.compiler.naming import apply_convention
    from provisa.core.relationship_names import derive_graphql_alias

    if graphql_alias:
        return apply_convention(graphql_alias, "snake")
    return derive_graphql_alias(
        target_name,
        cardinality,
        "snake",
        via_label_source=via_label_source,
        via_type_value=via_type_value,
        via_table_name=via_table_name,
        cypher_alias=alias,
    )


async def _registered(conn: "Connection") -> list[_Held]:
    """Every registered relationship's names. A retired one keeps its id and alias (REQ-2248) and
    is not served, so its field and Cypher type are free."""
    from sqlalchemy import select

    from provisa.core.relationship_names import effective_cypher_type
    from provisa.core.schema_org import registered_tables, relationships

    target = registered_tables.alias("target")
    via = registered_tables.alias("via")
    r = relationships
    rows = (
        await conn.execute_core(
            select(
                r.c.id,
                r.c.source_table_id,
                r.c.target_table_id,
                r.c.alias,
                r.c.graphql_alias,
                r.c.source_column,
                r.c.cardinality,
                r.c.via_label_source,
                r.c.via_type_value,
                r.c.retired,
                target.c.alias.label("target_alias"),
                target.c.table_name.label("target_table_name"),
                via.c.table_name.label("via_table_name"),
            ).select_from(
                r.outerjoin(target, target.c.id == r.c.target_table_id).outerjoin(
                    via, via.c.id == r.c.via_table_id
                )
            )
        )
    ).fetchall()
    return [
        _Held(
            rel_id=row.id,
            source_table_id=int(row.source_table_id),
            target_table_id=None if row.target_table_id is None else int(row.target_table_id),
            alias=row.alias,
            field=_field_of(
                row.graphql_alias,
                row.target_alias or row.target_table_name or "",
                row.cardinality,
                row.alias,
                row.via_label_source,
                row.via_type_value,
                row.via_table_name,
            ),
            cypher_type=effective_cypher_type(
                alias=row.alias,
                source_column=row.source_column or "",
                cardinality=row.cardinality or "",
                via_label_source=row.via_label_source,
                via_type_value=row.via_type_value,
                via_table_name=row.via_table_name,
            ),
            served=not row.retired,
        )
        for row in rows
    ]


def _named(g: Generated, held: list[_Held], convention: str) -> tuple[Named, _Held]:
    from provisa.compiler.naming import apply_convention
    from provisa.core.relationship_names import effective_cypher_type

    # A one-to-many edge is told apart by the column on its far side, the referring one, as a
    # foreign key's two directions both are by the foreign-key column.
    column = g.target_column if g.cardinality == "one-to-many" else g.source_column
    same_source = [h for h in held if h.source_table_id == g.source_table_id]

    if g.id_typed:
        if any(h.rel_id == g.rel_id for h in held):
            raise RelationshipNameTaken("id", g.rel_id, g.rel_id)
        rel_id = g.rel_id
    else:
        rel_id = disambiguated(g.rel_id, column, g.target_table, (h.rel_id for h in held))

    aliases = [h.alias for h in same_source if h.alias]
    alias = None if g.alias is None else disambiguated(g.alias, column, g.target_table, aliases)

    def cypher(of_alias: str | None) -> str | None:
        return effective_cypher_type(
            alias=of_alias,
            source_column=g.source_column,
            cardinality=g.cardinality,
            via_label_source=g.via_label_source,
            via_type_value=None,
            via_table_name=g.via_table_name,
        )

    cypher_type = cypher(alias)
    pair = [
        h.cypher_type
        for h in same_source
        if h.served and h.cypher_type and h.target_table_id == g.target_table_id
    ]
    if cypher_type is not None and cypher_type.casefold() in {c.casefold() for c in pair}:
        # The Cypher type is pinned as the alias, which the type is served as (upper-cased); the
        # alias is one of its source table's, so it is unique there too.
        alias = disambiguated(cypher_type, column, g.target_table, [*pair, *aliases]).upper()
        cypher_type = cypher(alias)

    # Named after the Cypher type is settled: a junction nominating a fixed label takes its field
    # from its alias (REQ-1586).
    graphql_alias = g.graphql_alias
    field = _field_of(
        g.graphql_alias,
        g.target_name,
        g.cardinality,
        alias,
        g.via_label_source,
        None,
        g.via_table_name,
    )
    fields = [h.field for h in same_source if h.served and h.field]
    if field is not None and field.casefold() in {f.casefold() for f in fields}:
        field = disambiguated(field, column, g.target_table, fields)
        # A generated pinned field keeps its own spelling (snake case); a derived one takes the
        # naming convention's, as the field it replaces would have.
        graphql_alias = field if g.graphql_alias else apply_convention(field, convention)

    named = Named(rel_id, alias, graphql_alias)
    return named, _Held(
        rel_id, g.source_table_id, g.target_table_id, alias, field, cypher_type, served=True
    )


async def unique_names(conn: "Connection", generated: Sequence[Generated]) -> list[Named]:
    """The names each of ``generated`` is saved under, in order: unique against the relationships
    registered and those before it in ``generated``, in each namespace those names must be unique
    in -- the id across the model, the alias and the GraphQL field on the source table, the Cypher
    type between the source and the target table. A generated name that is taken is told apart
    (:func:`disambiguated`); a name already registered is never changed, and a typed id that is
    taken is refused (:class:`RelationshipNameTaken`)."""
    from provisa.compiler.naming import active_gql_convention

    convention = active_gql_convention()
    held = await _registered(conn)
    out: list[Named] = []
    for g in generated:
        named, now = _named(g, held, convention)
        held.append(now)
        out.append(named)
    return out
