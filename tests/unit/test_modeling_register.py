# Copyright (c) 2026 Kenneth Stott
# Canary: d8ee603a-cc7a-4d79-9275-9780d3eaaffe
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1164: entity/fact inputs lower to the admin registration inputs."""

from __future__ import annotations

from provisa.api.admin.modeling_register import entity_table_input, fact_table_input
from provisa.api.admin.types import DimRefInput, EntityInput, FactInput, MeasureInput


def test_entity_scd2_lowers_to_bitemporal_table_input():
    ti = entity_table_input(
        EntityInput(
            name="Customer",
            source="raw.customers",
            domain_id="sales",
            key=["id"],
            attributes=["name", "region"],
            history="scd2",
        )
    )
    assert ti.source_id == "__provisa__"
    assert ti.table_name == "Customer"
    assert ti.materialize is True
    assert ti.view_sql == 'SELECT "id", "name", "region" FROM "raw"."customers"'
    assert ti.mv_bitemporal_mode == "delta"
    assert ti.mv_bitemporal_key == ["id"]
    assert [c.name for c in ti.columns] == ["id", "name", "region"]
    assert ti.columns[0].visible_to == ["public"]


def test_entity_no_history_is_plain_materialized_table_input():
    ti = entity_table_input(
        EntityInput(name="Product", source="raw.products", domain_id="sales", key=["sku"])
    )
    assert ti.materialize is True
    assert ti.mv_bitemporal_mode is None


def test_fact_lowers_to_aggregate_table_input_and_relationships():
    ti, rels = fact_table_input(
        FactInput(
            name="Sales",
            source="raw.orders",
            domain_id="sales",
            grain=["order_id"],
            measures=[MeasureInput(column="amount", agg="sum")],
            dimensions=[
                DimRefInput(entity="Customer", via="customer_id"),
                DimRefInput(entity="Product", via="product_id"),
            ],
        )
    )
    assert ti.table_name == "Sales"
    assert ti.materialize is True
    assert "GROUP BY" in (ti.view_sql or "")
    assert 'SUM("amount")' in (ti.view_sql or "")
    # one relationship per dimension, fact → dimension (many_to_one)
    assert [(r.source_table_id, r.source_column, r.target_table_id, r.cardinality) for r in rels] == [
        ("Sales", "customer_id", "Customer", "many_to_one"),
        ("Sales", "product_id", "Product", "many_to_one"),
    ]
