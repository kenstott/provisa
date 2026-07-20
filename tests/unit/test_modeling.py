# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1164: entity/fact sugar lowers to MV primitives."""

from __future__ import annotations

import duckdb
import pytest

from provisa.mv.modeling import (
    DimRef,
    Entity,
    Fact,
    Measure,
    entity_registration,
    fact_registration,
)


# ── validation ────────────────────────────────────────────────────────────────


def test_entity_requires_key():
    with pytest.raises(ValueError, match="business key"):
        Entity(name="Customer", source="raw.customers", key=())


def test_entity_bad_history_rejected():
    with pytest.raises(ValueError, match="history"):
        Entity(name="C", source="s", key=("id",), history="type7")


def test_measure_bad_agg_rejected():
    with pytest.raises(ValueError, match="agg"):
        Measure(column="amount", agg="median")


def test_fact_requires_grain():
    with pytest.raises(ValueError, match="grain"):
        Fact(name="Orders", source="raw.orders", grain=())


# ── entity lowering ───────────────────────────────────────────────────────────


def test_entity_no_history_is_plain_materialized_view():
    reg = entity_registration(
        Entity(name="Customer", source="raw.customers", key=("id",), attributes=("name", "region"))
    )
    assert reg["materialize"] is True
    assert reg["view_sql"] == 'SELECT "id", "name", "region" FROM "raw"."customers"'
    assert reg["columns"] == ["id", "name", "region"]
    assert "mv_bitemporal_mode" not in reg  # no history → not bitemporal


def test_entity_scd2_lowers_to_delta_bitemporal_mv():
    reg = entity_registration(
        Entity(name="Customer", source="raw.customers", key=("id",), attributes=("tier",), history="scd2")
    )
    assert reg["mv_bitemporal_mode"] == "delta"
    assert reg["mv_bitemporal_key"] == ["id"]


def test_entity_snapshot_history_lowers_to_snapshot_mode():
    reg = entity_registration(Entity(name="C", source="s", key=("id",), history="snapshot"))
    assert reg["mv_bitemporal_mode"] == "snapshot"


def test_entity_key_not_duplicated_in_columns():
    reg = entity_registration(
        Entity(name="C", source="s", key=("id",), attributes=("id", "name"))  # id repeated
    )
    assert reg["columns"] == ["id", "name"]


# ── fact lowering ─────────────────────────────────────────────────────────────


def test_fact_lowers_to_grain_projection_with_aggregated_measures():
    reg = fact_registration(
        Fact(
            name="Sales",
            source="raw.orders",
            grain=("order_id",),
            measures=(Measure("amount", "sum"), Measure("qty", "sum")),
            dimensions=(DimRef(entity="Customer", via="customer_id"),),
        )
    )
    assert reg["materialize"] is True
    assert reg["view_sql"] == (
        'SELECT "order_id", "customer_id", SUM("amount") AS "amount", SUM("qty") AS "qty" '
        'FROM "raw"."orders" GROUP BY "order_id", "customer_id"'
    )
    assert reg["columns"] == ["order_id", "customer_id", "amount", "qty"]


def test_fact_registers_relationships_to_dimensions():
    reg = fact_registration(
        Fact(
            name="Sales",
            source="raw.orders",
            grain=("order_id",),
            measures=(Measure("amount"),),
            dimensions=(
                DimRef(entity="Customer", via="customer_id"),
                DimRef(entity="Product", via="product_id"),
            ),
        )
    )
    assert reg["relationships"] == [
        {"source_column": "customer_id", "target_table": "Customer"},
        {"source_column": "product_id", "target_table": "Product"},
    ]


def test_measureless_fact_is_a_key_set_no_group_by():
    # A Data Vault LINK: only the keys, no measures, no aggregation.
    reg = fact_registration(
        Fact(
            name="OrderCustomer",
            source="raw.orders",
            grain=("order_id",),
            dimensions=(DimRef(entity="Customer", via="customer_id"),),
        )
    )
    assert "GROUP BY" not in reg["view_sql"]
    assert reg["view_sql"] == 'SELECT "order_id", "customer_id" FROM "raw"."orders"'


# ── the generated SQL actually runs (behavioral) ─────────────────────────────


def test_generated_entity_and_fact_sql_execute_on_duckdb():
    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA raw")
    con.execute("CREATE TABLE raw.customers (id INTEGER, name VARCHAR, region VARCHAR)")
    con.execute("INSERT INTO raw.customers VALUES (1,'a','west'), (2,'b','east')")
    con.execute("CREATE TABLE raw.orders (order_id INTEGER, customer_id INTEGER, amount INTEGER)")
    con.execute("INSERT INTO raw.orders VALUES (10,1,100),(11,1,50),(12,2,20)")

    ent = entity_registration(Entity("Customer", "raw.customers", ("id",), ("name", "region")))
    assert {r[0] for r in con.execute(ent["view_sql"]).fetchall()} == {1, 2}

    fact = fact_registration(
        Fact("Sales", "raw.orders", ("customer_id",), (Measure("amount", "sum"),))
    )
    # aggregate to customer grain: customer 1 → 150, customer 2 → 20
    assert set(con.execute(fact["view_sql"]).fetchall()) == {(1, 150), (2, 20)}
