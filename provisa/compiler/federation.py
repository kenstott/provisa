# Copyright (c) 2025 Kenneth Stott
# Canary: d0f9d854-f4b7-4870-a575-5e9aa7e4225f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Apollo Federation v2 subgraph support (Phase AJ).

Wraps an existing graphql-core schema with Federation v2 directives:
@key on entity types, _service, _entities, _Any scalar, _Entity union.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from graphql import (
    GraphQLArgument,
    GraphQLField,
    GraphQLList,
    GraphQLNonNull,
    GraphQLObjectType,
    GraphQLScalarType,
    GraphQLSchema,
    GraphQLString,
    GraphQLUnionType,
    print_schema,
)

from provisa.compiler.introspect import ColumnMetadata

FEDERATION_LINK_URL = (
    "https://specs.apollo.dev/federation/v2.3"
)
FEDERATION_IMPORTS = ["@key", "@shareable", "@external"]


@dataclass
class FederationConfig:
    """Configuration for Apollo Federation v2 subgraph support."""

    enabled: bool = False
    version: int = 2
    service_name: str = "provisa"


@dataclass
class _EntityType:
    """Internal: entity type with key fields for federation."""

    type_name: str
    gql_type: GraphQLObjectType
    key_fields: list[str]


# Federation _Any scalar — accepts arbitrary JSON representations
_AnyScalar = GraphQLScalarType(
    "_Any",
    description="Federation _Any scalar for entity representations.",
    serialize=lambda v: v,
    parse_value=lambda v: v,
    parse_literal=lambda v, _variables=None: v,
)


def extract_pk_columns(
    tables: list[dict],
    column_types: dict[int, list[ColumnMetadata]],
) -> dict[int, list[str]]:
    """Extract primary key columns for each table from Trino metadata.

    Returns table_id -> list of PK column names.
    Heuristic: columns whose column_name is 'id' or ends with '_id' at position 0,
    or all columns marked as non-nullable in single-column tables.

    In practice, Trino INFORMATION_SCHEMA does not expose PK constraints directly.
    This uses the convention that tables declare PK columns via a 'primary_key'
    field in the registration config. Falls back to first non-nullable column.
    """
    result: dict[int, list[str]] = {}
    for table in tables:
        table_id = table["id"]
        # Prefer explicit primary_key from table registration
        explicit_pk = table.get("primary_key")
        if explicit_pk:
            if isinstance(explicit_pk, str):
                result[table_id] = [explicit_pk]
            else:
                result[table_id] = list(explicit_pk)
            continue

        # Fallback: first column named 'id', or first non-nullable column
        cols = column_types.get(table_id, [])
        id_cols = [c for c in cols if c.column_name == "id"]
        if id_cols:
            result[table_id] = ["id"]
            continue

        non_nullable = [c for c in cols if not c.is_nullable]
        if non_nullable:
            result[table_id] = [non_nullable[0].column_name]
        elif cols:
            result[table_id] = [cols[0].column_name]

    return result


def build_federation_schema(
    base_schema: GraphQLSchema,
    tables: list[dict],
    pk_columns: dict[int, list[str]],
) -> GraphQLSchema:
    """Wrap a base GraphQL schema with Apollo Federation v2 fields.

    Adds _service, _entities, _Any scalar, _Entity union.
    Each entity type is annotated with @key metadata (stored as extensions).

    Args:
        base_schema: The role-aware schema from generate_schema().
        tables: Table registration dicts (need 'id' and type name mapping).
        pk_columns: table_id -> list of PK column names.

    Returns:
        New GraphQLSchema with Federation fields added to Query.
    """
    query_type = base_schema.query_type
    if query_type is None:
        raise ValueError("Base schema has no Query type")

    # Build table_id -> type_name mapping from base schema query fields
    # Each root query field returns a list of an object type
    entity_types: list[_EntityType] = []
    type_name_to_entity: dict[str, _EntityType] = {}

    for table in tables:
        table_id = table["id"]
        if table_id not in pk_columns:
            continue

        # Find the GraphQL type for this table in the base schema
        type_name = table.get("_type_name")
        if not type_name:
            continue

        gql_type = base_schema.type_map.get(type_name)
        if not isinstance(gql_type, GraphQLObjectType):
            continue

        key_fields = pk_columns[table_id]
        entity = _EntityType(
            type_name=type_name,
            gql_type=gql_type,
            key_fields=key_fields,
        )
        entity_types.append(entity)
        type_name_to_entity[type_name] = entity

    if not entity_types:
        raise ValueError("No entity types found for federation")

    # _Entity union of all entity types
    entity_union = GraphQLUnionType(
        "_Entity",
        types=[e.gql_type for e in entity_types],
        description="Federation entity union.",
    )

    # Store key directive metadata as extensions on entity types
    key_directives: dict[str, str] = {}
    for entity in entity_types:
        key_str = " ".join(entity.key_fields)
        key_directives[entity.type_name] = key_str

    # Build SDL with federation directives for _service field
    fed_sdl = generate_federation_sdl(base_schema, key_directives)

    # _service field
    service_type = GraphQLObjectType(
        "_Service",
        {"sdl": GraphQLField(GraphQLNonNull(GraphQLString))},
    )

    # Build new query fields = existing + federation fields
    existing_fields = query_type.fields

    def make_query_fields() -> dict[str, GraphQLField]:
        fields = dict(existing_fields)
        fields["_service"] = GraphQLField(
            GraphQLNonNull(service_type),
            resolve=lambda _obj, _info: {"sdl": fed_sdl},
        )
        fields["_entities"] = GraphQLField(
            GraphQLNonNull(GraphQLList(entity_union)),
            args={
                "representations": GraphQLArgument(
                    GraphQLNonNull(GraphQLList(GraphQLNonNull(_AnyScalar)))
                ),
            },
        )
        return fields

    new_query = GraphQLObjectType("Query", make_query_fields)

    return GraphQLSchema(
        query=new_query,
        mutation=base_schema.mutation_type,
        types=[_AnyScalar, entity_union, service_type],
    )


def generate_federation_sdl(
    schema: GraphQLSchema,
    key_directives: dict[str, str] | None = None,
) -> str:
    """Print SDL with Federation v2 directives.

    Args:
        schema: The GraphQL schema to print.
        key_directives: type_name -> key fields string (e.g. "id" or "col1 col2").

    Returns:
        Federation-annotated SDL string.
    """
    base_sdl = print_schema(schema)

    lines: list[str] = []

    # Add Federation v2 schema extension header
    imports = ", ".join(f'"{imp}"' for imp in FEDERATION_IMPORTS)
    lines.append(
        f'extend schema @link(url: "{FEDERATION_LINK_URL}", '
        f"import: [{imports}])"
    )
    lines.append("")

    if key_directives:
        for line in base_sdl.splitlines():
            # Inject @key directive after type declaration
            injected = False
            for type_name, key_str in key_directives.items():
                type_prefix = f"type {type_name} "
                type_prefix_brace = f"type {type_name} {{"
                if line.strip().startswith(type_prefix) or line.strip() == type_prefix_brace.strip():
                    # Replace "type Foo {" with "type Foo @key(fields: "pk") {"
                    line = line.replace(
                        f"type {type_name} {{",
                        f'type {type_name} @key(fields: "{key_str}") {{',
                    )
                    injected = True
                    break
            lines.append(line)
    else:
        lines.append(base_sdl)

    return "\n".join(lines)
