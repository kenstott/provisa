# Copyright (c) 2025 Kenneth Stott
# Canary: fe7dee37-1a51-4599-a719-d5e9249736c4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Build graphql-core schema from registration model + Trino metadata per role.

No third-party GraphQL framework (REQ-007). Uses graphql-core directly.
Domain-scoped, per-role column filtering (REQ-008, REQ-021).
"""

import re
from dataclasses import dataclass, field

from graphql import (
    GraphQLArgument,
    GraphQLBoolean,
    GraphQLEnumType,
    GraphQLEnumValue,
    GraphQLField,
    GraphQLInputField,
    GraphQLInputObjectType,
    GraphQLInt,
    GraphQLList,
    GraphQLNonNull,
    GraphQLObjectType,
    GraphQLScalarType,
    GraphQLSchema,
    GraphQLString,
)

from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.naming import generate_name, to_type_name
from provisa.compiler.type_map import FILTER_TYPE_MAP, trino_to_graphql


@dataclass
class SchemaInput:
    """All data needed to generate a GraphQL schema for one role."""

    tables: list[dict]  # from table_repo.list_all() — includes "columns" sub-list
    relationships: list[dict]  # from rel_repo.list_all()
    column_types: dict[int, list[ColumnMetadata]]  # table_id → Trino column metadata
    naming_rules: list[dict]  # [{pattern, replacement}]
    role: dict  # from role_repo.get()
    domains: list[dict]  # from domain_repo.list_all()
    source_types: dict[str, str] | None = None  # source_id → type (for mutation eligibility)
    domain_prefix: bool = False  # prepend domain_id__ to all names


@dataclass
class _TableInfo:
    """Internal: resolved table info for schema generation."""

    table_id: int
    field_name: str  # snake_case GraphQL field name
    type_name: str  # PascalCase GraphQL type name
    domain_id: str
    source_id: str
    schema_name: str
    table_name: str  # original DB table name
    visible_columns: list[dict]  # [{column_name, visible_to, alias?, description?}]
    column_metadata: dict[str, ColumnMetadata]  # column_name → metadata
    alias: str | None = None  # explicit GraphQL name override
    description: str | None = None  # GraphQL type/field description
    gql_fields: dict[str, GraphQLField] = field(default_factory=dict)


# --- GraphQL enum for ORDER BY direction ---

OrderDirection = GraphQLEnumType(
    "OrderDirection",
    {"ASC": GraphQLEnumValue("ASC"), "DESC": GraphQLEnumValue("DESC")},
)


def _build_visible_tables(si: SchemaInput) -> list[_TableInfo]:
    """Filter tables by role's domain access. Build per-table metadata."""
    role = si.role
    accessible = set(role["domain_access"])
    all_access = "*" in accessible

    result: list[_TableInfo] = []
    for table in si.tables:
        if not all_access and table["domain_id"] not in accessible:
            continue

        table_id = table["id"]
        if table_id not in si.column_types:
            raise ValueError(
                f"No Trino column metadata for table {table['table_name']!r} "
                f"(id={table_id}). Run introspection first."
            )
        col_meta = {m.column_name: m for m in si.column_types[table_id]}

        # Filter columns by role visibility
        visible_cols = [
            c for c in table["columns"]
            if role["id"] in c["visible_to"]
        ]

        if not visible_cols:
            continue

        result.append(_TableInfo(
            table_id=table_id,
            field_name="",  # set after naming
            type_name="",
            domain_id=table["domain_id"],
            source_id=table["source_id"],
            schema_name=table["schema_name"],
            table_name=table["table_name"],
            visible_columns=visible_cols,
            column_metadata=col_meta,
            alias=table.get("alias"),
            description=table.get("description"),
        ))

    return result


def _assign_names(
    tables: list[_TableInfo],
    naming_rules: list[dict],
    domain_prefix: bool = False,
) -> None:
    """Assign unique GraphQL names to each table."""
    # Group by domain for uniqueness scoping
    domain_groups: dict[str, list[_TableInfo]] = {}
    for t in tables:
        domain_groups.setdefault(t.domain_id, []).append(t)

    for domain_id, group in domain_groups.items():
        domain_table_names = [t.table_name for t in group]
        # Normalize domain_id to snake_case for prefix
        domain_snake = re.sub(r"[^a-zA-Z0-9]", "_", domain_id).strip("_")
        for t in group:
            t.field_name = generate_name(
                t.table_name, t.schema_name, t.source_id,
                domain_table_names, naming_rules,
                alias=t.alias,
            )
            if domain_prefix:
                t.field_name = f"{domain_snake}__{t.field_name}"
            t.type_name = to_type_name(t.field_name)


def _build_column_fields(table: _TableInfo) -> dict[str, GraphQLField]:
    """Build GraphQL fields for visible columns."""
    fields: dict[str, GraphQLField] = {}
    for col in table.visible_columns:
        col_name = col["column_name"]
        meta = table.column_metadata.get(col_name)
        if meta is None:
            raise ValueError(
                f"Registered column {col_name!r} on table {table.table_name!r} "
                f"not found in Trino metadata."
            )
        gql_type = trino_to_graphql(meta.data_type)
        if not meta.is_nullable and not isinstance(gql_type, GraphQLList):
            gql_type = GraphQLNonNull(gql_type)
        field_name = col.get("alias") or col_name
        description = col.get("description")
        fields[field_name] = GraphQLField(gql_type, description=description)
    return fields


def _build_where_input(
    table: _TableInfo, type_name: str
) -> GraphQLInputObjectType | None:
    """Build a typed WHERE input for a table's visible columns."""
    input_fields: dict[str, GraphQLInputField] = {}
    for col in table.visible_columns:
        col_name = col["column_name"]
        meta = table.column_metadata.get(col_name)
        if meta is None:
            continue  # already validated in _build_column_fields
        gql_type = trino_to_graphql(meta.data_type)
        scalar = gql_type.of_type if isinstance(gql_type, GraphQLList) else gql_type
        filter_type = FILTER_TYPE_MAP.get(scalar)
        if filter_type:
            input_fields[col_name] = GraphQLInputField(filter_type)

    if not input_fields:
        return None

    name = f"{type_name}Where"
    where_input: GraphQLInputObjectType | None = None

    def thunk():
        fields = dict(input_fields)
        fields["_and"] = GraphQLInputField(GraphQLList(GraphQLNonNull(where_input)))
        fields["_or"] = GraphQLInputField(GraphQLList(GraphQLNonNull(where_input)))
        return fields

    where_input = GraphQLInputObjectType(name, thunk)
    return where_input


def _build_order_by_input(
    table: _TableInfo, type_name: str
) -> GraphQLInputObjectType | None:
    """Build ORDER BY input type with field enum + direction."""
    visible_col_names = [
        c["column_name"] for c in table.visible_columns
        if c["column_name"] in table.column_metadata
    ]
    if not visible_col_names:
        return None

    field_enum = GraphQLEnumType(
        f"{type_name}OrderByField",
        {name: GraphQLEnumValue(name) for name in visible_col_names},
    )

    return GraphQLInputObjectType(
        f"{type_name}OrderBy",
        {
            "field": GraphQLInputField(GraphQLNonNull(field_enum)),
            "direction": GraphQLInputField(OrderDirection),
        },
    )


def _can_see_relationship(
    rel: dict, table_lookup: dict[int, _TableInfo]
) -> bool:
    """Check if both sides of a relationship are visible to the role,
    including the join columns themselves."""
    src_id = rel["source_table_id"]
    tgt_id = rel["target_table_id"]
    if src_id not in table_lookup or tgt_id not in table_lookup:
        return False
    # Join columns must be visible — otherwise the relationship can't be queried
    src_visible = {c["column_name"] for c in table_lookup[src_id].visible_columns}
    tgt_visible = {c["column_name"] for c in table_lookup[tgt_id].visible_columns}
    return (
        rel["source_column"] in src_visible
        and rel["target_column"] in tgt_visible
    )


def generate_schema(si: SchemaInput) -> GraphQLSchema:
    """Generate a graphql-core schema for a specific role.

    The schema includes:
    - Object types per registered table (filtered by domain access + column visibility)
    - Relationship fields (many-to-one → object, one-to-many → list)
    - Root query fields with where, order_by, limit, offset args
    """
    tables = _build_visible_tables(si)
    if not tables:
        raise ValueError(
            f"No tables visible to role {si.role['id']!r}. "
            f"Check domain_access and column visibility."
        )

    _assign_names(tables, si.naming_rules, domain_prefix=si.domain_prefix)

    # Build base column fields
    for t in tables:
        t.gql_fields = _build_column_fields(t)

    table_lookup: dict[int, _TableInfo] = {t.table_id: t for t in tables}

    # Filter relationships to those where both sides are visible
    visible_rels = [
        r for r in si.relationships
        if _can_see_relationship(r, table_lookup)
    ]

    # Create GraphQL object types with thunks (handles circular relationships)
    gql_types: dict[int, GraphQLObjectType] = {}

    for t in tables:
        tid = t.table_id

        def make_fields(tid=tid):
            info = table_lookup[tid]
            fields = dict(info.gql_fields)

            # Add relationship fields
            for rel in visible_rels:
                if rel["source_table_id"] == tid:
                    target = table_lookup[rel["target_table_id"]]
                    target_type = gql_types[target.table_id]
                    if rel["cardinality"] == "many-to-one":
                        fields[target.field_name] = GraphQLField(target_type)
                    elif rel["cardinality"] == "one-to-many":
                        fields[target.field_name] = GraphQLField(
                            GraphQLList(GraphQLNonNull(target_type))
                        )

            return fields

        gql_types[tid] = GraphQLObjectType(
            t.type_name, make_fields, description=t.description,
        )

    # Build root query fields
    query_fields: dict[str, GraphQLField] = {}

    for t in tables:
        gql_type = gql_types[t.table_id]
        args: dict[str, GraphQLArgument] = {
            "limit": GraphQLArgument(GraphQLInt),
            "offset": GraphQLArgument(GraphQLInt),
        }

        where_input = _build_where_input(t, t.type_name)
        if where_input:
            args["where"] = GraphQLArgument(where_input)

        order_by_input = _build_order_by_input(t, t.type_name)
        if order_by_input:
            args["order_by"] = GraphQLArgument(GraphQLList(GraphQLNonNull(order_by_input)))

        query_fields[t.field_name] = GraphQLField(
            GraphQLList(GraphQLNonNull(gql_type)),
            args=args,
        )

    query_type = GraphQLObjectType("Query", lambda: query_fields)

    # Build mutation types for RDBMS tables (REQ-031–REQ-037)
    nosql_types = {"mongodb", "cassandra"}
    mutation_fields: dict[str, GraphQLField] = {}

    for t in tables:
        # Skip NoSQL sources — no mutations
        if si.source_types and si.source_types.get(t.source_id, "") in nosql_types:
            continue

        # Build input type for insert (all visible columns)
        insert_fields: dict[str, GraphQLInputField] = {}
        for col in t.visible_columns:
            col_name = col["column_name"]
            meta = t.column_metadata.get(col_name)
            if meta is None:
                continue
            gql_type = trino_to_graphql(meta.data_type)
            if isinstance(gql_type, GraphQLList):
                gql_type = GraphQLString  # fallback for arrays in input
            insert_fields[col_name] = GraphQLInputField(gql_type)

        if not insert_fields:
            continue

        insert_input = GraphQLInputObjectType(
            f"{t.type_name}InsertInput", lambda fields=insert_fields: fields,
        )

        # Build set input type for update (same columns)
        set_input = GraphQLInputObjectType(
            f"{t.type_name}SetInput", lambda fields=insert_fields: fields,
        )

        # Where input for update/delete (use mutation-specific name to avoid conflict)
        where_input = _build_where_input(t, f"{t.type_name}Mutation")

        # Mutation response type
        response_type = GraphQLObjectType(
            f"{t.type_name}MutationResponse",
            lambda t=t: {
                "affected_rows": GraphQLField(GraphQLNonNull(GraphQLInt)),
            },
        )

        # insert_<table>(input: InsertInput!): MutationResponse!
        mutation_fields[f"insert_{t.field_name}"] = GraphQLField(
            GraphQLNonNull(response_type),
            args={"input": GraphQLArgument(GraphQLNonNull(insert_input))},
        )

        # update_<table>(set: SetInput!, where: WhereInput!): MutationResponse!
        if where_input:
            mutation_fields[f"update_{t.field_name}"] = GraphQLField(
                GraphQLNonNull(response_type),
                args={
                    "set": GraphQLArgument(GraphQLNonNull(set_input)),
                    "where": GraphQLArgument(GraphQLNonNull(where_input)),
                },
            )

            # delete_<table>(where: WhereInput!): MutationResponse!
            mutation_fields[f"delete_{t.field_name}"] = GraphQLField(
                GraphQLNonNull(response_type),
                args={"where": GraphQLArgument(GraphQLNonNull(where_input))},
            )

    mutation_type = None
    if mutation_fields:
        mutation_type = GraphQLObjectType("Mutation", lambda: mutation_fields)

    return GraphQLSchema(query=query_type, mutation=mutation_type)
