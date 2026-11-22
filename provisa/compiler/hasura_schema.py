# Copyright (c) 2026 Kenneth Stott
# Canary: 8c2e5a14-7b3d-4f9a-a0d6-51e9c3b7f248
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Hasura's shapes for a Hasura-preset table (REQ-194, maintainer 2026-11-22).

A table whose convention is a Hasura preset (``naming.HASURA_PRESETS``) is served as Hasura
serves it: comparison expressions with ``_eq``-style operators, Hasura's order_by enum, a
``select_column`` enum for distinct_on, ``_by_pk`` reads and Hasura's mutations. Every name comes
from the naming authority (``naming.hasura_*``). Leaf layer below schema_gen, beside
schema_inputs.
"""

# Requirements: REQ-194

from __future__ import annotations

import weakref
from typing import cast

from graphql import (
    GraphQLArgument,
    GraphQLEnumType,
    GraphQLEnumValue,
    GraphQLField,
    GraphQLInputField,
    GraphQLInputObjectType,
    GraphQLScalarType,
    GraphQLList,
    GraphQLNonNull,
    GraphQLObjectType,
)

from provisa.compiler.naming import (
    HASURA_DEFAULT,
    HASURA_ORDER_DIRECTIONS,
    hasura_comparison_type,
    hasura_enum_value,
    hasura_operator,
    hasura_order_by_type,
    hasura_root_field,
    hasura_type_name,
)
from provisa.compiler.schema_directives import GraphQLInt, OrderDirection
from provisa.compiler.schema_types import _TableInfo

#: Each Provisa filter type's Hasura comparison expression, per convention: one instance per
#: filter instance, so a schema holds each name once, as it holds each filter once.
_COMPARISONS: dict[str, weakref.WeakKeyDictionary] = {}


def hasura_comparison(
    convention: str, provisa_filter: GraphQLInputObjectType
) -> GraphQLInputObjectType:
    """The Hasura comparison expression of ``provisa_filter`` (``IntFilter`` is
    ``Int_comparison_exp`` / ``IntComparisonExp``): the same comparisons, each named as Hasura
    names its operator (``eq`` is ``_eq``)."""
    cache = _COMPARISONS.setdefault(convention, weakref.WeakKeyDictionary())
    found = cache.get(provisa_filter)
    if found is not None:
        return found
    scalar = provisa_filter.name.removesuffix("Filter")

    def fields() -> dict[str, GraphQLInputField]:
        return {
            hasura_operator(convention, op): GraphQLInputField(field.type)
            for op, field in provisa_filter.fields.items()
        }

    comparison = cast(
        GraphQLInputObjectType,
        GraphQLInputObjectType(hasura_comparison_type(convention, scalar), fields),
    )
    cache[provisa_filter] = comparison
    return comparison


_ORDER_BY: dict[str, GraphQLEnumType] = {
    # hasura-default's enum is Provisa's own order_by: the same name and values.
    HASURA_DEFAULT: OrderDirection,
}


def hasura_order_by_enum(convention: str) -> GraphQLEnumType:
    """Hasura's order_by enum under ``convention``; each value's internal value is the
    direction as hasura-default spells it."""
    if convention not in _ORDER_BY:
        _ORDER_BY[convention] = cast(
            GraphQLEnumType,
            GraphQLEnumType(
                hasura_order_by_type(convention),
                {
                    hasura_enum_value(convention, v, "order_by"): GraphQLEnumValue(v)
                    for v in HASURA_ORDER_DIRECTIONS
                },
            ),
        )
    return _ORDER_BY[convention]


def type_name(t: _TableInfo, suffix: str) -> str:
    """Table ``t``'s Hasura type ``suffix`` (``bool_exp``)."""
    assert t.hasura is not None
    return hasura_type_name(t.hasura, t.type_name, suffix)


def root_field(t: _TableInfo, prefix: str = "", suffix: str = "") -> str:
    """Table ``t``'s Hasura root field (``insert`` / ``by_pk``)."""
    assert t.hasura is not None
    return hasura_root_field(t.hasura, t.field_name, prefix, suffix)


def column_enum(t: _TableInfo, suffix: str, columns: list[str]) -> GraphQLEnumType:
    """A Hasura column enum (``select_column``, ``update_column``): each column's field name."""
    return cast(
        GraphQLEnumType,
        GraphQLEnumType(type_name(t, suffix), {c: GraphQLEnumValue(c) for c in columns}),
    )


def pk_arguments(
    pk_fields: dict[str, GraphQLScalarType | GraphQLEnumType],
) -> dict[str, GraphQLArgument]:
    """A ``_by_pk`` read's or delete's arguments: each primary-key column, required."""
    return {name: GraphQLArgument(GraphQLNonNull(gql)) for name, gql in pk_fields.items()}


def by_pk_field(
    row_type: GraphQLObjectType, pk_fields: dict[str, GraphQLScalarType | GraphQLEnumType]
) -> GraphQLField:
    """``<table>_by_pk``: the row of the primary key given, or null."""
    return GraphQLField(row_type, args=pk_arguments(pk_fields))


def mutation_response(t: _TableInfo, row_type: GraphQLObjectType) -> GraphQLObjectType:
    """``<table>_mutation_response``: the rows a mutation touched, counted and returned."""
    return cast(
        GraphQLObjectType,
        GraphQLObjectType(
            type_name(t, "mutation_response"),
            lambda: {
                "affected_rows": GraphQLField(GraphQLNonNull(GraphQLInt)),
                "returning": GraphQLField(GraphQLNonNull(GraphQLList(GraphQLNonNull(row_type)))),
            },
        ),
    )
