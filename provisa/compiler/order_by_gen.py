# Copyright (c) 2026 Kenneth Stott
# Canary: req-157-order-by-gen
#
# This source code is licensed under the Business Source License 1.1

"""Order-by input type builder — thin public wrapper around schema_gen internals.

Exposes build_order_by_input_type() for use in tests and external callers.
Preserves original column case (REQ-157).
"""

from __future__ import annotations

from graphql import GraphQLInputField, GraphQLInputObjectType

from provisa.compiler.schema_gen import OrderDirection


def build_order_by_input_type(
    type_name: str,
    visible_columns: list[dict],
    column_metadata: dict,
) -> GraphQLInputObjectType:
    """Build an ORDER BY input type for *type_name*.

    Args:
        type_name: GraphQL type name (e.g. "Orders").
        visible_columns: list of {"column_name": str, ...} dicts.
        column_metadata: mapping of lower-cased column name to ColumnMetadata.

    Returns a GraphQLInputObjectType whose fields use the original column name
    (preserving case — REQ-157) rather than uppercasing it.
    """
    fields: dict[str, GraphQLInputField] = {}
    for col in visible_columns:
        col_name: str = col["column_name"]
        if col_name.lower() in column_metadata:
            fields[col_name] = GraphQLInputField(OrderDirection)

    return GraphQLInputObjectType(f"{type_name}OrderBy", fields)
