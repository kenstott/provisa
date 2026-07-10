# Copyright (c) 2026 Kenneth Stott
# Canary: fe7dee37-1a51-4599-a719-d5e9249736c4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Input-type and column-field builders for the generated GraphQL schema.

Leaf layer below schema_gen: turns a _TableInfo into GraphQL column fields,
WHERE/order_by/distinct_on inputs, and db-field argument maps. Depends only on
schema_directives, naming, type_map, and enum_detect — never on schema_gen.
"""

from typing import cast

from graphql import (
    GraphQLArgument,
    GraphQLEnumType,
    GraphQLEnumValue,
    GraphQLField,
    GraphQLInputField,
    GraphQLInputObjectType,
    GraphQLList,
    GraphQLNonNull,
    GraphQLObjectType,
    GraphQLOutputType,
    GraphQLScalarType,
)

from provisa.compiler.enum_detect import build_enum_filter_types, resolve_column_type
from provisa.compiler.naming import apply_gql_name, to_type_name
from provisa.compiler.type_map import FILTER_TYPE_MAP, column_type_to_graphql
from provisa.compiler.schema_types import _TableInfo
from provisa.compiler.schema_directives import (
    GraphQLBoolean,
    GraphQLFloat,
    GraphQLInt,
    GraphQLString,
    OrderDirection,
)

_OBJECT_FIELD_TYPE_MAP: dict[str, GraphQLScalarType] = {
    "string": GraphQLString,
    "integer": GraphQLInt,
    "number": GraphQLFloat,
    "boolean": GraphQLBoolean,
}


def _build_object_type(
    col_name: str,
    object_fields: list[dict],
    override: str | None,
    registry: dict[str, GraphQLObjectType],
    type_prefix: str = "",
) -> GraphQLObjectType:
    """Build a GraphQLObjectType for an object column, reusing from registry by name."""
    type_name = type_prefix + to_type_name(col_name) + "Object"
    if type_name in registry:
        return registry[type_name]
    sub_fields: dict[str, GraphQLField] = {}
    for sf in object_fields:
        sf_name = sf["name"]
        sf_alias = sf.get("alias") or apply_gql_name(sf_name, override)
        nested = sf.get("fields") or []
        if nested and sf.get("type") == "object":
            sf_gql: GraphQLOutputType = _build_object_type(sf_name, nested, override, registry)
        else:
            sf_gql = _OBJECT_FIELD_TYPE_MAP.get(sf.get("type", "string"), GraphQLString)
        sub_fields[sf_alias] = GraphQLField(sf_gql, description=sf.get("description"))
    obj_type = cast(GraphQLObjectType, GraphQLObjectType(type_name, lambda: sub_fields))
    registry[type_name] = obj_type
    return obj_type


def _build_column_fields(  # REQ-008, REQ-010, REQ-039, REQ-155, REQ-156, REQ-221
    table: _TableInfo,
    override: str | None = None,
    enum_types: dict | None = None,
    object_type_registry: dict[str, GraphQLObjectType] | None = None,
    governed_gql_types: set[str] | None = None,
) -> dict[str, GraphQLField]:
    """Build GraphQL fields for visible columns.

    Naming priority: explicit alias > convention-based alias > raw column name.
    Enum columns are mapped to GraphQLEnumType when enum_types is provided (REQ-221).
    """
    fields: dict[str, GraphQLField] = {}
    _enums = enum_types or {}
    _obj_registry: dict[str, GraphQLObjectType] = (
        object_type_registry if object_type_registry is not None else {}
    )
    _governed = governed_gql_types or set()
    for col in table.visible_columns:
        col_name = col["column_name"]
        if _governed and col.get("gql_object_type") in _governed:
            continue
        meta = table.column_metadata.get(col_name.lower())
        if meta is None:
            import logging as _clog

            _clog.getLogger(__name__).warning(
                "Registered column %r on table %r not found in the engine metadata — skipping.",
                col_name,
                table.table_name,
            )
            continue
        object_fields = col.get("object_fields")
        if isinstance(object_fields, str):
            import json as _json

            object_fields = _json.loads(object_fields)
        if object_fields and meta.data_type in ("json", "jsonb"):
            _type_prefix = ""
            if "__" in table.type_name:
                _type_prefix = table.type_name.split("__", 1)[0] + "__"
            gql_type: GraphQLOutputType = _build_object_type(
                col_name, object_fields, override, _obj_registry, _type_prefix
            )
        else:
            enum_gql = resolve_column_type(meta.data_type, _enums)
            if enum_gql is not None:
                gql_type = enum_gql if meta.is_nullable else GraphQLNonNull(enum_gql)
            else:
                scalar_type = column_type_to_graphql(meta.data_type)
                if not meta.is_nullable and not isinstance(scalar_type, GraphQLList):
                    gql_type = GraphQLNonNull(scalar_type)
                else:
                    gql_type = scalar_type
        # Naming priority: explicit alias > convention > raw name
        explicit_alias = col.get("alias")
        if explicit_alias:
            field_name = explicit_alias
        else:
            conv_alias = apply_gql_name(col_name, override)
            field_name = conv_alias if conv_alias else col_name
        description = col.get("description")
        fields[field_name] = GraphQLField(gql_type, description=description)
    return fields


def _gql_col_name(col: dict, table: _TableInfo) -> str:
    """Return the GQL-exposed name for a column (alias > convention > raw physical)."""
    if alias := col.get("alias"):
        return alias
    raw = col["column_name"]
    conv = apply_gql_name(raw, table.gql_convention_override)
    return conv if conv else raw


def _build_where_input(  # REQ-008, REQ-221
    table: _TableInfo,
    type_name: str,
    enum_types: dict | None = None,
) -> GraphQLInputObjectType | None:
    """Build a typed WHERE input for a table's visible columns."""
    _enums = enum_types or {}
    _enum_filters = build_enum_filter_types(_enums)
    input_fields: dict[str, GraphQLInputField] = {}
    for col in table.visible_columns:
        col_name = col["column_name"]
        gql_name = _gql_col_name(col, table)
        meta = table.column_metadata.get(col_name.lower())
        if meta is None:
            continue  # already validated in _build_column_fields
        # Normalize to unqualified pg_name for filter lookup (REQ-221)
        pg_name = meta.data_type.lower().strip()
        if "." in pg_name:
            pg_name = pg_name.rsplit(".", 1)[1]
        enum_filter = _enum_filters.get(pg_name)
        if enum_filter:
            input_fields[gql_name] = GraphQLInputField(enum_filter)
        else:
            gql_type = column_type_to_graphql(meta.data_type)
            scalar = gql_type.of_type if isinstance(gql_type, GraphQLList) else gql_type
            filter_type = FILTER_TYPE_MAP.get(scalar)
            if filter_type:
                input_fields[gql_name] = GraphQLInputField(filter_type)

    # Virtual columns support eq/neq filtering
    _str_filter = FILTER_TYPE_MAP.get(GraphQLString)
    if _str_filter:
        input_fields["_name_"] = GraphQLInputField(_str_filter)
        input_fields["_domain_"] = GraphQLInputField(_str_filter)

    if not input_fields:
        return None

    name = f"{type_name}Where"
    where_input_holder: list[GraphQLInputObjectType] = []

    def thunk():
        wi = where_input_holder[0]
        fields = dict(input_fields)
        fields["_and"] = GraphQLInputField(GraphQLList(GraphQLNonNull(wi)))
        fields["_or"] = GraphQLInputField(GraphQLList(GraphQLNonNull(wi)))
        return fields

    where_input = cast(GraphQLInputObjectType, GraphQLInputObjectType(name, thunk))
    where_input_holder.append(where_input)
    return where_input


def _build_order_by_inputs(  # REQ-200, REQ-201, REQ-202
    tables: list[_TableInfo],
    visible_rels: list[dict],
    table_lookup: dict[int, _TableInfo],
) -> dict[int, GraphQLInputObjectType]:
    """Build ORDER BY input types using Hasura v2 pattern: {column: direction}.

    Each visible column becomes a field with OrderDirection type.
    Relationship fields become nested order_by input references via thunks.
    Returns table_id → OrderBy input type mapping.
    """
    order_by_types: dict[int, GraphQLInputObjectType] = {}

    for t in tables:
        visible_col_names = [
            _gql_col_name(c, t)
            for c in t.visible_columns
            if c["column_name"].lower() in t.column_metadata
        ]
        if not visible_col_names:
            continue

        # Capture t in closure for the thunk
        def make_fields(table=t, cols=visible_col_names):
            fields: dict[str, GraphQLInputField] = {
                name: GraphQLInputField(OrderDirection) for name in cols
            }
            # Add nested relationship ordering
            for rel in visible_rels:
                if rel["source_table_id"] == table.table_id:
                    tgt_id = rel["target_table_id"]
                    if tgt_id in order_by_types:
                        target = table_lookup[tgt_id]
                        fields[target.field_name] = GraphQLInputField(
                            order_by_types[tgt_id],
                        )
            return fields

        ob_type = cast(
            GraphQLInputObjectType, GraphQLInputObjectType(f"{t.type_name}OrderBy", make_fields)
        )
        order_by_types[t.table_id] = ob_type

    return order_by_types


def _build_distinct_on_enum(
    table: _TableInfo,
) -> GraphQLEnumType | None:
    visible_col_names = [
        _gql_col_name(c, table)
        for c in table.visible_columns
        if c["column_name"].lower() in table.column_metadata
    ]
    if not visible_col_names:
        return None
    return cast(
        GraphQLEnumType,
        GraphQLEnumType(
            f"{table.type_name}DistinctOnColumn",
            {name: GraphQLEnumValue(name) for name in visible_col_names},
        ),
    )


def _build_db_field_args(
    where_input: GraphQLInputObjectType | None,
    order_by_input: GraphQLInputObjectType | None,
    distinct_enum: GraphQLEnumType | None,
) -> dict[str, GraphQLArgument]:
    args: dict[str, GraphQLArgument] = {
        "limit": GraphQLArgument(GraphQLInt),
        "offset": GraphQLArgument(GraphQLInt),
        # REQ-263a: statistical row sampling — TABLESAMPLE BERNOULLI(<pct>), 0-100.
        "sample": GraphQLArgument(GraphQLFloat),
    }
    if where_input:
        args["where"] = GraphQLArgument(where_input)
    if order_by_input:
        args["order_by"] = GraphQLArgument(GraphQLList(GraphQLNonNull(order_by_input)))
    if distinct_enum:
        args["distinct_on"] = GraphQLArgument(GraphQLList(GraphQLNonNull(distinct_enum)))
    return args
