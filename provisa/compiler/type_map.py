# Copyright (c) 2025 Kenneth Stott
# Canary: 497dc85c-db96-44c2-ad8f-48c5ae2ac44b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Trino type → GraphQL scalar mapping (REQ-010).

Nullability preserved from INFORMATION_SCHEMA. Custom scalars for DateTime, JSON.
"""

from graphql import (
    GraphQLBoolean,
    GraphQLFloat,
    GraphQLInputField,
    GraphQLInputObjectType,
    GraphQLInt,
    GraphQLList,
    GraphQLNonNull,
    GraphQLScalarType,
    GraphQLString,
)

# --- Custom scalars ---

DateTime = GraphQLScalarType(
    "DateTime",
    description="ISO 8601 datetime string",
    serialize=str,
    parse_value=str,
)

Date = GraphQLScalarType(
    "Date",
    description="ISO 8601 date string",
    serialize=str,
    parse_value=str,
)

JSONScalar = GraphQLScalarType(
    "JSON",
    description="Arbitrary JSON value",
    serialize=lambda v: v,
    parse_value=lambda v: v,
)

BigInt = GraphQLScalarType(
    "BigInt",
    description="64-bit integer as string",
    serialize=str,
    parse_value=int,
)

# --- Type mapping ---

_TYPE_MAP: dict[str, GraphQLScalarType] = {
    # String types
    "varchar": GraphQLString,
    "char": GraphQLString,
    "varbinary": GraphQLString,
    "uuid": GraphQLString,
    # Integer types
    "tinyint": GraphQLInt,
    "smallint": GraphQLInt,
    "integer": GraphQLInt,
    "int": GraphQLInt,
    # Large integer
    "bigint": BigInt,
    # Floating point
    "real": GraphQLFloat,
    "double": GraphQLFloat,
    "decimal": GraphQLFloat,
    "numeric": GraphQLFloat,
    # Boolean
    "boolean": GraphQLBoolean,
    # Date/time
    "date": Date,
    "time": GraphQLString,
    "time with time zone": GraphQLString,
    "timestamp": DateTime,
    "timestamp with time zone": DateTime,
    # JSON
    "json": JSONScalar,
    "jsonb": JSONScalar,
}


def trino_to_graphql(trino_type: str) -> GraphQLScalarType | GraphQLList:
    """Map a Trino column type to a GraphQL scalar.

    Handles parameterized types like varchar(255), decimal(10,2), array(varchar).
    Raises ValueError for unmapped types.
    """
    normalized = trino_type.lower().strip()

    # Check exact match first
    if normalized in _TYPE_MAP:
        return _TYPE_MAP[normalized]

    # Handle parameterized types: varchar(255) → varchar, decimal(10,2) → decimal
    base = normalized.split("(")[0].strip()
    if base in _TYPE_MAP:
        return _TYPE_MAP[base]

    # Handle array types: array(varchar) → List of mapped type
    if normalized.startswith("array(") and normalized.endswith(")"):
        inner = normalized[6:-1]
        inner_type = trino_to_graphql(inner)
        return GraphQLList(GraphQLNonNull(inner_type))

    raise ValueError(f"Unmapped Trino type: {trino_type!r}")


# --- Filter input types (shared across all schemas) ---

def _filter_fields(scalar: GraphQLScalarType) -> dict[str, GraphQLInputField]:
    """Base comparison fields for a scalar type."""
    return {
        "eq": GraphQLInputField(scalar),
        "neq": GraphQLInputField(scalar),
        "in": GraphQLInputField(GraphQLList(GraphQLNonNull(scalar))),
        "is_null": GraphQLInputField(GraphQLBoolean),
    }


def _ordered_filter_fields(scalar: GraphQLScalarType) -> dict[str, GraphQLInputField]:
    """Comparison fields for ordered types (adds gt/gte/lt/lte)."""
    fields = _filter_fields(scalar)
    fields.update({
        "gt": GraphQLInputField(scalar),
        "gte": GraphQLInputField(scalar),
        "lt": GraphQLInputField(scalar),
        "lte": GraphQLInputField(scalar),
    })
    return fields


StringFilter = GraphQLInputObjectType("StringFilter", lambda: {
    **_filter_fields(GraphQLString),
    "like": GraphQLInputField(GraphQLString),
})

IntFilter = GraphQLInputObjectType("IntFilter", lambda: _ordered_filter_fields(GraphQLInt))

BigIntFilter = GraphQLInputObjectType("BigIntFilter", lambda: _ordered_filter_fields(BigInt))

FloatFilter = GraphQLInputObjectType("FloatFilter", lambda: _ordered_filter_fields(GraphQLFloat))

BooleanFilter = GraphQLInputObjectType("BooleanFilter", lambda: {
    "eq": GraphQLInputField(GraphQLBoolean),
    "is_null": GraphQLInputField(GraphQLBoolean),
})

DateFilter = GraphQLInputObjectType("DateFilter", lambda: _ordered_filter_fields(Date))

DateTimeFilter = GraphQLInputObjectType("DateTimeFilter", lambda: _ordered_filter_fields(DateTime))

JSONFilter = GraphQLInputObjectType("JSONFilter", lambda: {
    "is_null": GraphQLInputField(GraphQLBoolean),
})

# Map GraphQL scalar → filter input type
FILTER_TYPE_MAP: dict[GraphQLScalarType, GraphQLInputObjectType] = {
    GraphQLString: StringFilter,
    GraphQLInt: IntFilter,
    BigInt: BigIntFilter,
    GraphQLFloat: FloatFilter,
    GraphQLBoolean: BooleanFilter,
    Date: DateFilter,
    DateTime: DateTimeFilter,
    JSONScalar: JSONFilter,
}
