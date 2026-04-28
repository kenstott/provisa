# Copyright (c) 2026 Kenneth Stott
# Canary: 9196c89e-1080-4159-a472-21a9aca6aeca
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL enum auto-detection for GraphQL schema generation (REQ-221).

Introspects pg_enum + pg_type via asyncpg to discover enum types and their
allowed values. Generates GraphQLEnumType instances and resolves column types.
"""

import re

from graphql import GraphQLEnumType, GraphQLInputField, GraphQLInputObjectType

_GRAPHQL_NAME_RE = re.compile(r"[^a-zA-Z0-9_]")


def _sanitize_enum_name(pg_name: str) -> str:
    """Convert a PG enum name to a valid GraphQL identifier.

    Replaces non-alphanumeric chars with underscores, ensures it starts
    with a letter, and appends _Enum suffix.
    """
    cleaned = _GRAPHQL_NAME_RE.sub("_", pg_name)
    if cleaned and cleaned[0].isdigit():
        cleaned = f"e_{cleaned}"
    return f"{cleaned}_Enum"


def _sanitize_enum_value(pg_value: str) -> str:
    """Convert a PG enum label to a valid GraphQL enum value.

    GraphQL enum values must match /[_A-Za-z][_0-9A-Za-z]*/.
    """
    cleaned = _GRAPHQL_NAME_RE.sub("_", pg_value)
    if not cleaned or cleaned[0].isdigit():
        cleaned = f"V_{cleaned}"
    return cleaned.upper()


async def fetch_enum_registry(conn) -> dict[str, list[str]]:
    """Query pg_enum + pg_type via asyncpg to discover all enum types.

    Args:
        conn: An asyncpg connection (or pool).

    Returns:
        Dict mapping enum type name to ordered list of enum labels.
        Example: {"order_status": ["pending", "shipped", "delivered"]}
    """
    rows = await conn.fetch(
        "SELECT t.typname AS enum_name, e.enumlabel AS enum_value "
        "FROM pg_enum e "
        "JOIN pg_type t ON e.enumtypid = t.oid "
        "ORDER BY t.typname, e.enumsortorder"
    )
    registry: dict[str, list[str]] = {}
    for row in rows:
        name = row["enum_name"]
        label = row["enum_value"]
        registry.setdefault(name, []).append(label)
    return registry


def build_enum_types(
    registry: dict[str, list[str]],
) -> dict[str, GraphQLEnumType]:
    """Build GraphQLEnumType instances from the enum registry.

    Args:
        registry: Output of fetch_enum_registry().

    Returns:
        Dict mapping PG enum type name to GraphQLEnumType.
        Example: {"order_status": GraphQLEnumType("order_status_Enum", ...)}
    """
    result: dict[str, GraphQLEnumType] = {}
    for pg_name, values in registry.items():
        gql_name = _sanitize_enum_name(pg_name)
        gql_values = {}
        for v in values:
            gql_key = _sanitize_enum_value(v)
            gql_values[gql_key] = v
        result[pg_name] = GraphQLEnumType(
            gql_name,
            gql_values,
            description=f"Auto-detected from PostgreSQL enum '{pg_name}'",
        )
    return result


def build_enum_filter_types(
    enum_types: dict[str, GraphQLEnumType],
) -> dict[str, GraphQLInputObjectType]:
    """Build filter input types for each enum GraphQL type.

    Args:
        enum_types: Output of build_enum_types().

    Returns:
        Dict mapping PG enum name to a filter InputObjectType with eq/neq/in/is_null.
    """
    result: dict[str, GraphQLInputObjectType] = {}
    for pg_name, gql_enum in enum_types.items():
        from graphql import GraphQLBoolean, GraphQLList, GraphQLNonNull

        filter_name = f"{gql_enum.name}Filter"
        result[pg_name] = GraphQLInputObjectType(
            filter_name,
            lambda _enum=gql_enum: {
                "eq": GraphQLInputField(_enum),
                "neq": GraphQLInputField(_enum),
                "in": GraphQLInputField(GraphQLList(GraphQLNonNull(_enum))),
                "is_null": GraphQLInputField(GraphQLBoolean),
            },
        )
    return result


def resolve_column_type(
    column_type: str,
    enum_types: dict[str, GraphQLEnumType],
) -> GraphQLEnumType | None:
    """Check if a column's PG type matches a known enum.

    Args:
        column_type: The column data type string from introspection
            (e.g. "order_status", "USER-DEFINED", or "public.order_status").
        enum_types: Output of build_enum_types().

    Returns:
        The matching GraphQLEnumType, or None if not an enum column.
    """
    normalized = column_type.lower().strip()

    # Direct match: column type name equals enum name
    if normalized in enum_types:
        return enum_types[normalized]

    # Schema-qualified match: "public.order_status" → "order_status"
    if "." in normalized:
        unqualified = normalized.rsplit(".", 1)[1]
        if unqualified in enum_types:
            return enum_types[unqualified]

    return None
