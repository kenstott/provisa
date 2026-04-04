# Copyright (c) 2025 Kenneth Stott
# Canary: 11d34eaf-6954-4b10-9e98-53808147951a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Generate GraphQL mutation fields for tracked DB functions and webhooks.

Functions produce: `SELECT * FROM schema.func($1, $2)` style calls.
Webhooks produce: HTTP POST mutations with inline or table-mapped return types.
"""

from __future__ import annotations

from graphql import (
    GraphQLArgument,
    GraphQLBoolean,
    GraphQLField,
    GraphQLFloat,
    GraphQLInt,
    GraphQLList,
    GraphQLNonNull,
    GraphQLObjectType,
    GraphQLScalarType,
    GraphQLString,
)

from provisa.compiler.type_map import BigInt, Date, DateTime, JSONScalar
from provisa.core.models import Function, FunctionArgument, InlineType, Webhook

# Map config type names to GraphQL scalars
_SCALAR_MAP: dict[str, GraphQLScalarType] = {
    "String": GraphQLString,
    "Int": GraphQLInt,
    "Float": GraphQLFloat,
    "Boolean": GraphQLBoolean,
    "DateTime": DateTime,
    "Date": Date,
    "BigInt": BigInt,
    "JSON": JSONScalar,
}


def _resolve_scalar(type_name: str) -> GraphQLScalarType:
    """Resolve a type name string to a GraphQL scalar type."""
    result = _SCALAR_MAP.get(type_name)
    if result is None:
        raise ValueError(f"Unknown argument type: {type_name!r}")
    return result


def _build_args(arguments: list[FunctionArgument]) -> dict[str, GraphQLArgument]:
    """Build GraphQL arguments from function/webhook argument definitions."""
    args: dict[str, GraphQLArgument] = {}
    for arg in arguments:
        scalar = _resolve_scalar(arg.type)
        args[arg.name] = GraphQLArgument(scalar)
    return args


def _build_inline_return_type(
    name: str, fields: list[InlineType],
) -> GraphQLObjectType:
    """Build a GraphQL object type from inline type definitions."""
    gql_fields: dict[str, GraphQLField] = {}
    for f in fields:
        scalar = _resolve_scalar(f.type)
        gql_fields[f.name] = GraphQLField(scalar)
    return GraphQLObjectType(f"{name}Result", lambda fields=gql_fields: fields)


def build_function_mutations(
    functions: list[Function],
    webhooks: list[Webhook],
    table_gql_types: dict[str, GraphQLObjectType] | None = None,
    role_id: str | None = None,
) -> dict[str, GraphQLField]:
    """Build GraphQL mutation fields for tracked functions and webhooks.

    Args:
        functions: list of Function config models
        webhooks: list of Webhook config models
        table_gql_types: mapping of table reference (source_id.schema.table) to
            existing GraphQL object types (for function return types)
        role_id: current role for visibility filtering

    Returns:
        dict of mutation field name -> GraphQLField
    """
    if table_gql_types is None:
        table_gql_types = {}

    mutation_fields: dict[str, GraphQLField] = {}

    # --- DB Functions ---
    for func in functions:
        # Role visibility check
        if role_id and func.visible_to and role_id not in func.visible_to:
            continue

        # Resolve return type from registered table
        return_type = table_gql_types.get(func.returns)
        if return_type is None:
            # If table type not found, skip (schema_gen should provide types)
            continue

        args = _build_args(func.arguments)
        list_type = GraphQLList(GraphQLNonNull(return_type))

        mutation_fields[func.name] = GraphQLField(
            list_type,
            args=args,
            description=func.description or f"Call DB function {func.function_name}",
        )

    # --- Webhooks ---
    for wh in webhooks:
        if role_id and wh.visible_to and role_id not in wh.visible_to:
            continue

        # Resolve return type: table-backed or inline
        if wh.returns and wh.returns in table_gql_types:
            return_type = GraphQLList(
                GraphQLNonNull(table_gql_types[wh.returns])
            )
        elif wh.inline_return_type:
            inline_type = _build_inline_return_type(wh.name, wh.inline_return_type)
            return_type = inline_type
        else:
            # Default: return JSON scalar
            return_type = JSONScalar

        args = _build_args(wh.arguments)

        mutation_fields[wh.name] = GraphQLField(
            return_type,
            args=args,
            description=wh.description or f"Webhook: {wh.method} {wh.url}",
        )

    return mutation_fields


def build_function_sql(func: Function, arg_values: list) -> tuple[str, list]:
    """Build SQL for a tracked function call.

    Returns (sql, params) tuple.
    Example: SELECT * FROM "public"."process_order"($1, $2)
    """
    placeholders = ", ".join(f"${i + 1}" for i in range(len(arg_values)))
    sql = (
        f'SELECT * FROM "{func.schema_name}"."{func.function_name}"'
        f"({placeholders})"
    )
    return sql, list(arg_values)
