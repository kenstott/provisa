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

# Requirements: REQ-196, REQ-197

from __future__ import annotations

from typing import cast

from graphql import (
    GraphQLField,
    GraphQLFloat as _GraphQLFloat,
    GraphQLInputField,
    GraphQLInputObjectType,
    GraphQLInt as _GraphQLInt,
    GraphQLList,
    GraphQLNonNull,
    GraphQLObjectType,
    GraphQLScalarType,
)

GraphQLFloat: GraphQLScalarType = cast(GraphQLScalarType, _GraphQLFloat)
GraphQLInt: GraphQLScalarType = cast(GraphQLScalarType, _GraphQLInt)

from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.type_map import FILTER_TYPE_MAP, trino_to_graphql

# Trino types eligible for SUM/AVG (numeric only)
NUMERIC_TRINO_TYPES = {
    "tinyint",
    "smallint",
    "integer",
    "int",
    "bigint",
    "real",
    "double",
    "decimal",
    "numeric",
}

# Trino types eligible for MIN/MAX (numeric + comparable string/temporal)
COMPARABLE_TRINO_TYPES = NUMERIC_TRINO_TYPES | {
    "varchar",
    "char",
    "date",
    "timestamp",
    "timestamp with time zone",
}


def _base_type(trino_type: str) -> str:
    """Normalize parameterized types: varchar(100) -> varchar."""
    return trino_type.lower().split("(")[0].strip()


def _is_numeric(trino_type: str) -> bool:
    return _base_type(trino_type) in NUMERIC_TRINO_TYPES


def _is_comparable(trino_type: str) -> bool:
    return _base_type(trino_type) in COMPARABLE_TRINO_TYPES


def _classify_columns(
    visible_columns: list[dict],
    column_metadata: dict[str, ColumnMetadata],
) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
    """Return (numeric_cols, comparable_cols) as (col_name, trino_type) tuples."""
    numeric_cols: list[tuple[str, str]] = []
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
    return numeric_cols, comparable_cols


def build_agg_fields_type(  # REQ-196
    type_name: str,
    visible_columns: list[dict],
    column_metadata: dict[str, ColumnMetadata],
) -> GraphQLObjectType:
    """Build {TypeName}AggregateFields type (count, sum, avg, stddev, variance, min, max)."""
    numeric_cols, comparable_cols = _classify_columns(visible_columns, column_metadata)

    sum_type: GraphQLObjectType | None = None
    if numeric_cols:
        sum_fields = {col_name: GraphQLField(GraphQLFloat) for col_name, _ in numeric_cols}
        sum_type = cast(GraphQLObjectType, GraphQLObjectType(f"{type_name}SumFields", lambda f=sum_fields: f))

    avg_type: GraphQLObjectType | None = None
    if numeric_cols:
        avg_fields = {col_name: GraphQLField(GraphQLFloat) for col_name, _ in numeric_cols}
        avg_type = cast(GraphQLObjectType, GraphQLObjectType(f"{type_name}AvgFields", lambda f=avg_fields: f))

    stddev_type: GraphQLObjectType | None = None
    variance_type: GraphQLObjectType | None = None
    if numeric_cols:
        stddev_fields = {col_name: GraphQLField(GraphQLFloat) for col_name, _ in numeric_cols}
        stddev_type = cast(
            GraphQLObjectType,
            GraphQLObjectType(f"{type_name}StddevFields", lambda f=stddev_fields: f),
        )
        variance_fields = {col_name: GraphQLField(GraphQLFloat) for col_name, _ in numeric_cols}
        variance_type = cast(
            GraphQLObjectType,
            GraphQLObjectType(f"{type_name}VarianceFields", lambda f=variance_fields: f),
        )

    min_type: GraphQLObjectType | None = None
    if comparable_cols:
        min_fields = {}
        for col_name, trino_type in comparable_cols:
            gql_type = trino_to_graphql(trino_type)
            min_fields[col_name] = GraphQLField(gql_type)  # type: ignore[arg-type]
        min_type = cast(GraphQLObjectType, GraphQLObjectType(f"{type_name}MinFields", lambda f=min_fields: f))

    max_type: GraphQLObjectType | None = None
    if comparable_cols:
        max_fields = {}
        for col_name, trino_type in comparable_cols:
            gql_type = trino_to_graphql(trino_type)
            max_fields[col_name] = GraphQLField(gql_type)  # type: ignore[arg-type]
        max_type = cast(GraphQLObjectType, GraphQLObjectType(f"{type_name}MaxFields", lambda f=max_fields: f))

    agg_inner_fields: dict[str, GraphQLField] = {
        "count": GraphQLField(GraphQLNonNull(GraphQLInt)),
    }
    if sum_type:
        agg_inner_fields["sum"] = GraphQLField(sum_type)
    if avg_type:
        agg_inner_fields["avg"] = GraphQLField(avg_type)
    if stddev_type:
        agg_inner_fields["stddev"] = GraphQLField(stddev_type)
    if variance_type:
        agg_inner_fields["variance"] = GraphQLField(variance_type)
    if min_type:
        agg_inner_fields["min"] = GraphQLField(min_type)
    if max_type:
        agg_inner_fields["max"] = GraphQLField(max_type)

    return cast(GraphQLObjectType, GraphQLObjectType(
        f"{type_name}AggregateFields",
        lambda f=agg_inner_fields: f,
    ))


def build_having_exp_type(  # REQ-197
    type_name: str,
    visible_columns: list[dict],
    column_metadata: dict[str, ColumnMetadata],
) -> GraphQLInputObjectType | None:
    """Build {TypeName}HavingExp input type for HAVING clause filtering.

    Mirrors AggregateFields structure but uses comparison operator inputs.
    """
    numeric_cols, comparable_cols = _classify_columns(visible_columns, column_metadata)

    int_filter = FILTER_TYPE_MAP.get(GraphQLInt)
    float_filter = FILTER_TYPE_MAP.get(GraphQLFloat)

    fields: dict[str, GraphQLInputField] = {}

    if int_filter:
        fields["count"] = GraphQLInputField(int_filter)

    for fname in ("sum", "avg", "stddev", "variance"):
        if numeric_cols and float_filter:
            sub_fields = {col_name: GraphQLInputField(float_filter) for col_name, _ in numeric_cols}
            sub_type = cast(
                GraphQLInputObjectType,
                GraphQLInputObjectType(
                    f"{type_name}HavingExp{fname.capitalize()}",
                    lambda f=sub_fields: f,
                ),
            )
            fields[fname] = GraphQLInputField(sub_type)

    for fname in ("min", "max"):
        if comparable_cols:
            sub_fields: dict[str, GraphQLInputField] = {}
            for col_name, trino_type in comparable_cols:
                gql_type = trino_to_graphql(trino_type)
                scalar = gql_type.of_type if isinstance(gql_type, GraphQLList) else gql_type
                filter_type = FILTER_TYPE_MAP.get(scalar)  # type: ignore[arg-type]
                if filter_type:
                    sub_fields[col_name] = GraphQLInputField(filter_type)
            if sub_fields:
                sub_type = cast(
                    GraphQLInputObjectType,
                    GraphQLInputObjectType(
                        f"{type_name}HavingExp{fname.capitalize()}",
                        lambda f=sub_fields: f,
                    ),
                )
                fields[fname] = GraphQLInputField(sub_type)

    if not fields:
        return None

    return cast(GraphQLInputObjectType, GraphQLInputObjectType(f"{type_name}HavingExp", lambda f=fields: f))


def build_aggregate_types(  # REQ-196
    type_name: str,
    visible_columns: list[dict],
    column_metadata: dict[str, ColumnMetadata],
    row_type: GraphQLObjectType,
    agg_fields_type: GraphQLObjectType | None = None,
) -> GraphQLObjectType | None:
    """Build <Table>Aggregate type with aggregate fields and nodes.

    Returns None if no columns are eligible for aggregation.
    """
    if agg_fields_type is None:
        agg_fields_type = build_agg_fields_type(type_name, visible_columns, column_metadata)

    aggregate_type = cast(GraphQLObjectType, GraphQLObjectType(
        f"{type_name}Aggregate",
        lambda: {
            "aggregate": GraphQLField(agg_fields_type),
            "nodes": GraphQLField(GraphQLList(GraphQLNonNull(row_type))),
        },
    ))

    return aggregate_type
