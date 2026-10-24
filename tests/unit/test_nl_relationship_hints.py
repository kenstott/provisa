# Copyright (c) 2026 Kenneth Stott
# Canary: 31238b62-4956-4a0f-9853-ef0b2e5654f4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-603: a natural-language query refused for joining two tables outside a relationship is
retried with the registered path between them -- and refused for good, it says which tables
have none and the path there is."""

# Requirements: REQ-603

from __future__ import annotations

from provisa.compiler.sql_validator import ValidationViolation
from provisa.gql.label_map import CypherLabelMap, NodeMapping, RelationshipMapping
from provisa.nl.paths import relationship_hints


def _node(type_name: str, table_id: int, table: str) -> NodeMapping:
    return NodeMapping(
        label=type_name,
        type_name=type_name,
        domain_label=None,
        table_label=type_name,
        table_id=table_id,
        source_id="pg",
        id_column="id",
        pk_columns=["id"],
        catalog_name="pg",
        schema_name="public",
        table_name=table,
        properties={},
        domain_id="sales",
    )


def _rel(rel_type: str, source: str, target: str, source_col: str, target_col: str):
    return RelationshipMapping(
        rel_type=rel_type,
        source_label=source,
        target_label=target,
        join_source_column=source_col,
        join_target_column=target_col,
        field_name=rel_type.lower(),
    )


_LM = CypherLabelMap(
    nodes={
        "Orders": _node("Orders", 1, "orders"),
        "Customers": _node("Customers", 2, "customers"),
        "Regions": _node("Regions", 3, "regions"),
        "Notes": _node("Notes", 4, "notes"),
    },
    relationships={
        "PLACED_BY": _rel("PLACED_BY", "Orders", "Customers", "customer_id", "id"),
        "IN_REGION": _rel("IN_REGION", "Customers", "Regions", "region_id", "id"),
    },
)


def _v002(left: int, right: int) -> ValidationViolation:
    return ValidationViolation("V002", "Invalid JOIN: ...", tables=(left, right))


def _sql(domain_id: str | None) -> str:
    return domain_id or "default"


def test_a_refused_pair_is_given_the_registered_relationship_between_them():
    assert relationship_hints(_LM, [_v002(1, 2)], _sql) == [
        "sales.orders and sales.customers are related along a registered relationship: "
        "JOIN sales.customers ON orders.customer_id = customers.id"
    ]


def test_a_pair_related_through_a_third_table_is_given_that_path():
    assert relationship_hints(_LM, [_v002(1, 3)], _sql) == [
        "sales.orders and sales.regions are related along a registered relationship: "
        "JOIN sales.customers ON orders.customer_id = customers.id "
        "JOIN sales.regions ON customers.region_id = regions.id"
    ]


def test_a_pair_named_the_other_way_round_is_given_the_path_reversed():
    """The relationship's direction is not the join's: the target joins back to its source."""
    assert relationship_hints(_LM, [_v002(2, 1)], _sql) == [
        "sales.customers and sales.orders are related along a registered relationship: "
        "JOIN sales.orders ON customers.id = orders.customer_id"
    ]


def test_a_pair_no_relationship_relates_is_said_to_have_none():
    assert relationship_hints(_LM, [_v002(1, 4)], _sql) == [
        "No registered relationship relates sales.orders and sales.notes: they cannot be "
        "joined, and the question needs another way to combine them or one of them alone"
    ]


def test_only_a_refusal_naming_two_tables_is_answered():
    other = ValidationViolation("V003", "Column not visible")
    assert relationship_hints(_LM, [other, _v002(1, 2), _v002(1, 2)], _sql) == [
        "sales.orders and sales.customers are related along a registered relationship: "
        "JOIN sales.customers ON orders.customer_id = customers.id"
    ]
