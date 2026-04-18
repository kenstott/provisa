# Copyright (c) 2026 Kenneth Stott
# Canary: 2d64a447-3257-42f8-977c-73532c3acb87
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Aggregate type generation for GraphQL schema (Phase AD3).

Generates <Table>_aggregate root query fields with count, sum, avg, min, max.
"""

from __future__ import annotations

from graphql import (
    GraphQLArgument,
    GraphQLField,
    GraphQLFloat,
    GraphQLInt,
    GraphQLList,
    GraphQLNonNull,
    GraphQLObjectType,
)

from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.type_map import trino_to_graphql

# Trino types eligible for SUM/AVG (numeric only)
NUMERIC_TRINO_TYPES = {
    "tinyint", "smallint", "integer", "int", "bigint",
    "real", "double", "decimal", "numeric",
}

# Trino types eligible for MIN/MAX (numeric + comparable string/temporal)
COMPARABLE_TRINO_TYPES = NUMERIC_TRINO_TYPES | {
    "varchar", "char", "date", "timestamp", "timestamp with time zone",
}


def _base_type(trino_type: str) -> str:
    """Normalize parameterized types: varchar(100) -> varchar."""
    return trino_type.lower().split("(")[0].strip()


def _is_numeric(trino_type: str) -> bool:
    return _base_type(trino_type) in NUMERIC_TRINO_TYPES


def _is_comparable(trino_type: str) -> bool:
    return _base_type(trino_type) in COMPARABLE_TRINO_TYPES


def build_aggregate_types(
    type_name: str,
    visible_columns: list[dict],
    column_metadata: dict[str, ColumnMetadata],
    row_type: GraphQLObjectType,
) -> GraphQLObjectType | None:
    """Build <Table>Aggregate type with aggregate fields and nodes.

    Returns None if no columns are eligible for aggregation.
    """
    # Classify columns
    numeric_cols: list[tuple[str, str]] = []  # (col_name, trino_type)
    comparable_cols: list[tuple[str, str]] = []

    for col in visible_columns:
        col_name = col["column_name"]
        meta = column_metadata.get(col_name.lower())
        if meta is None:
            continue
        if _is_numeric(meta.data_type):
            numeric_cols.append((col_name, meta.data_type))
        if _is_comparable(meta.data_type):
            comparable_cols.append((col_name, meta.data_type))

    # Build sum fields type (numeric only)
    sum_type = None
    if numeric_cols:
        sum_fields = {}
        for col_name, trino_type in numeric_cols:
            sum_fields[col_name] = GraphQLField(GraphQLFloat)
        sum_type = GraphQLObjectType(f"{type_name}SumFields", lambda f=sum_fields: f)

    # Build avg fields type (numeric only)
    avg_type = None
    if numeric_cols:
        avg_fields = {}
        for col_name, trino_type in numeric_cols:
            avg_fields[col_name] = GraphQLField(GraphQLFloat)
        avg_type = GraphQLObjectType(f"{type_name}AvgFields", lambda f=avg_fields: f)

    # Build min fields type (comparable)
    min_type = None
    if comparable_cols:
        min_fields = {}
        for col_name, trino_type in comparable_cols:
            gql_type = trino_to_graphql(trino_type)
            min_fields[col_name] = GraphQLField(gql_type)
        min_type = GraphQLObjectType(f"{type_name}MinFields", lambda f=min_fields: f)

    # Build max fields type (comparable)
    max_type = None
    if comparable_cols:
        max_fields = {}
        for col_name, trino_type in comparable_cols:
            gql_type = trino_to_graphql(trino_type)
            max_fields[col_name] = GraphQLField(gql_type)
        max_type = GraphQLObjectType(f"{type_name}MaxFields", lambda f=max_fields: f)

    # Build AggregateFields type
    agg_inner_fields: dict[str, GraphQLField] = {
        "count": GraphQLField(GraphQLNonNull(GraphQLInt)),
    }
    if sum_type:
        agg_inner_fields["sum"] = GraphQLField(sum_type)
    if avg_type:
        agg_inner_fields["avg"] = GraphQLField(avg_type)
    if min_type:
        agg_inner_fields["min"] = GraphQLField(min_type)
    if max_type:
        agg_inner_fields["max"] = GraphQLField(max_type)

    agg_fields_type = GraphQLObjectType(
        f"{type_name}AggregateFields",
        lambda f=agg_inner_fields: f,
    )

    # Build top-level Aggregate type
    aggregate_type = GraphQLObjectType(
        f"{type_name}Aggregate",
        lambda: {
            "aggregate": GraphQLField(agg_fields_type),
            "nodes": GraphQLField(GraphQLList(GraphQLNonNull(row_type))),
        },
    )

    return aggregate_type
