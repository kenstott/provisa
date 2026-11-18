# Copyright (c) 2026 Kenneth Stott
# Canary: fb7d2648-3004-4088-b46a-0c4d79dbd119
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A relationship's effective names, stored on its row: its Cypher type and its GraphQL field.

The names a relationship is reached by are derived when no steward pinned them -- the Cypher type
from its foreign-key column and cardinality, or a junction's nominated label (REQ-1586); the
GraphQL field from its target table and cardinality, in the deployment's naming convention. They
are derived here, once, and stored, so every reader -- the schema build, the admin API, the meta
domain (a product's panel, the knowledge graph, SPARQL) -- reads the same names rather than each
deriving its own (REQ-351)."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from inflect import Word as InflectWord
    from inflect import engine as _Engine

    from provisa.core.database import Connection

_inflect: "_Engine | None" = None


def _engine() -> "_Engine":
    global _inflect
    if _inflect is None:
        import inflect

        _inflect = inflect.engine()
    return _inflect


def _to_singular(camel: str) -> str:
    """Singularize camelCase word using inflect with round-trip validation."""
    eng = _engine()
    candidate = eng.singular_noun(cast("InflectWord", camel))
    if candidate is False:
        return camel  # already singular
    # Validate: if round-trip doesn't reconstruct original, inflect got it wrong
    if eng.plural(cast("InflectWord", candidate)) == camel:
        return candidate
    return camel


def target_field_name(  # REQ-155, REQ-194
    table_name: str,
    alias: str | None,
    naming_rules: list[dict],
    convention: str | None,
) -> str:
    """The GraphQL field name of the table a relationship reaches, as the schema build names it
    (``schema_gen._assign_names``): its alias, else its name under the naming rules, in its own
    convention (``convention``: the table's, else its source's) or the one in force."""
    from provisa.compiler.naming import active_gql_convention, generate_name

    return generate_name(
        table_name, naming_rules, alias=alias, convention=convention or active_gql_convention()
    )


def derive_graphql_alias(  # REQ-155, REQ-194, REQ-351, REQ-1586
    target_field: str,
    cardinality: str,
    convention: str = "apollo_graphql",
    *,
    via_label_source: str | None = None,
    via_type_value: str | None = None,
    via_table_name: str | None = None,
    cypher_alias: str | None = None,
) -> str | None:
    """The derived GraphQL field of a relationship reaching the table whose GraphQL field name is
    ``target_field`` (:func:`target_field_name`), in ``convention``: the schema build's own rule,
    :func:`provisa.compiler.naming.relationship_field_name`. None for a relationship reaching no
    table (a computed one, served under its function's return table) that is no junction."""
    from provisa.compiler.naming import apply_convention, relationship_field_name

    if not target_field and via_table_name is None and not via_label_source:
        return None
    return apply_convention(
        relationship_field_name(
            cardinality,
            target_field_name=target_field,
            via_label_source=via_label_source,
            via_type_value=via_type_value,
            via_table_name=via_table_name,
            cypher_alias=cypher_alias,
        ),
        convention,
    )


def derive_cypher_alias(source_column: str, cardinality: str) -> str:  # REQ-351
    """Derive Cypher relationship type from FK column + cardinality.

    Rules:
    - Strip _id/_fk/_pk suffix from source_column
    - Singularize with inflect (same helper as derive_graphql_alias)
    - Prepend direction verb based on cardinality:
      one-to-many  → HAS_<ENTITY>
      many-to-one  → BELONGS_TO_<ENTITY>
      other        → <ENTITY>
    """
    col = (source_column or "").strip().lower()
    for suffix in ("_id", "_fk", "_pk"):
        if col.endswith(suffix) and len(col) > len(suffix):
            col = col[: -len(suffix)]
            break
    singular = _to_singular(col) if col else col
    entity = (singular or col).upper()
    if cardinality == "one-to-many":
        return f"HAS_{entity}"
    if cardinality == "many-to-one":
        return f"BELONGS_TO_{entity}"
    return entity


def effective_cypher_type(  # REQ-351, REQ-1586
    *,
    alias: str | None,
    source_column: str,
    cardinality: str,
    via_label_source: str | None,
    via_type_value: str | None,
    via_table_name: str | None,
) -> str | None:
    """The Cypher type a relationship is served under: a junction's nominated label, else the alias
    a steward pinned, else the type derived from its column and cardinality. None for a junction
    whose nominated source is empty (a declaration the registry's checks refuse)."""
    from provisa.gql.label_map import _upper_snake

    if via_label_source == "column":
        return _upper_snake(via_type_value) if via_type_value else None
    if via_label_source == "table":
        return _upper_snake(via_table_name) if via_table_name else None
    if via_label_source == "fixed":
        return _upper_snake(alias) if alias else None
    if alias:
        return alias.upper()
    return derive_cypher_alias(source_column, cardinality)


async def store_effective_names(conn: "Connection", ids: list[str] | None = None) -> None:
    """Derive and store the effective names of the relationships ``ids`` (every one when None),
    in the naming convention now in force. Called where a relationship is saved and by every schema
    build, which follows each save, configuration load and change of naming convention. A row
    whose stored names are already these is not written, and with none to write no change is
    opened: a worker reloading another's change builds outside any model change scope. Names that
    do differ are written inside one (REQ-1524)."""
    from provisa.compiler.naming import active_gql_convention

    from sqlalchemy import select, update

    from provisa.core.schema_org import naming_rules, registered_tables, relationships, sources

    convention = active_gql_convention()
    # REQ-155: the derived field is named for the target's GraphQL field, which the naming rules
    # shape, in the order the schema build applies them.
    rules = [
        {"pattern": r.pattern, "replacement": r.replacement}
        for r in (
            await conn.execute_core(
                select(naming_rules.c.pattern, naming_rules.c.replacement).order_by(
                    naming_rules.c.id
                )
            )
        ).fetchall()
    ]
    target = registered_tables.alias("target")
    target_source = sources.alias("target_source")
    via = registered_tables.alias("via")
    query = select(
        relationships.c.id,
        relationships.c.alias,
        relationships.c.graphql_alias,
        relationships.c.source_column,
        relationships.c.cardinality,
        relationships.c.via_label_source,
        relationships.c.via_type_value,
        relationships.c.cypher_type,
        relationships.c.graphql_field,
        target.c.table_name.label("target_table_name"),
        target.c.alias.label("target_alias"),
        target.c.gql_naming_convention.label("target_convention"),
        target_source.c.gql_naming_convention.label("target_source_convention"),
        via.c.table_name.label("via_table_name"),
    ).select_from(
        relationships.outerjoin(target, target.c.id == relationships.c.target_table_id)
        .outerjoin(target_source, target_source.c.id == target.c.source_id)
        .outerjoin(via, via.c.id == relationships.c.via_table_id)
    )
    if ids is not None:
        query = query.where(relationships.c.id.in_(ids))
    rows = [r._mapping for r in (await conn.execute_core(query)).fetchall()]
    respelled: list[tuple[str, str | None, str | None]] = []
    for r in rows:
        cypher_type = effective_cypher_type(
            alias=r["alias"],
            source_column=r["source_column"] or "",
            cardinality=r["cardinality"] or "",
            via_label_source=r["via_label_source"],
            via_type_value=r["via_type_value"],
            via_table_name=r["via_table_name"],
        )
        graphql_field = r["graphql_alias"] or derive_graphql_alias(
            target_field_name(
                r["target_table_name"],
                r["target_alias"],
                rules,
                r["target_convention"] or r["target_source_convention"],
            )
            if r["target_table_name"]
            else "",
            r["cardinality"] or "",
            convention,
            via_label_source=r["via_label_source"],
            via_type_value=r["via_type_value"],
            via_table_name=r["via_table_name"],
            cypher_alias=r["alias"],
        )
        if (r["cypher_type"], r["graphql_field"]) != (cypher_type, graphql_field):
            respelled.append((r["id"], cypher_type, graphql_field))
    if not respelled:
        return
    # REQ-1524: a model write is part of a change. A save or a load writing these is that change
    # (the scope joins it); a build that finds them in another naming convention -- after the
    # convention changed -- writes them as a change of its own, never outside one.
    from provisa.core import model_change

    async with model_change.scope("relationship names follow the naming convention"):
        for rel_id, cypher_type, graphql_field in respelled:
            await conn.execute_core(
                update(relationships)
                .where(relationships.c.id == rel_id)
                .values(cypher_type=cypher_type, graphql_field=graphql_field)
            )
