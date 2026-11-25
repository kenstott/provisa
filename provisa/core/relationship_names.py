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
GraphQL field by the schema build's own rule (``naming.relationship_field_name``): named for the
GraphQL field of the table it reaches and its cardinality, in the deployment's naming convention. They
are derived here, once, and stored, so every reader -- the schema build, the admin API, the meta
domain (a product's panel, the knowledge graph, SPARQL) -- reads the same names rather than each
deriving its own (REQ-351)."""

from __future__ import annotations

from provisa.core.inflection import singular


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
    - Singularize by dictionary lookup (provisa.core.inflection, as every naming path)
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
    entity = (singular(col) if col else col).upper()
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
