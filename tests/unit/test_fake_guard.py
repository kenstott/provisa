# Copyright (c) 2026 Kenneth Stott
# Canary: 4c672a22-7513-471c-9ba2-9a44be221821
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The model's fakes checked together when a table or relationship is saved (REQ-1494)."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.api.admin._fake_guard import _with_model, check_model
from provisa.fakes.kinds import FakeRefused


def _col(name, data_type="integer", fake=None, stable=False):
    return {"column_name": name, "data_type": data_type, "fake": fake, "fake_stable": stable}


def _tables(order_total=None, line_fake=None, customer_id_fake=None):
    return [
        {
            "id": 1,
            "source_id": "pg",
            "schema_name": "public",
            "table_name": "orders",
            "columns": [
                _col("id"),
                _col("customer_id", fake=customer_id_fake),
                _col("total", "double", order_total),
            ],
        },
        {
            "id": 2,
            "source_id": "pg",
            "schema_name": "public",
            "table_name": "order_lines",
            "columns": [_col("id"), _col("order_id"), _col("amount", "double", line_fake)],
        },
        {
            "id": 3,
            "source_id": "pg",
            "schema_name": "public",
            "table_name": "customers",
            "columns": [_col("id", fake="hash()")],
        },
    ]


_RELS = [
    {
        "id": "order_lines",
        "source_table_id": 1,
        "target_table_id": 2,
        "source_column": "id",
        "target_column": "order_id",
        "cardinality": "one-to-many",
        "graphql_alias": "lines",
        "via_table_id": None,
    },
    {
        "id": "order_customer",
        "source_table_id": 1,
        "target_table_id": 3,
        "source_column": "customer_id",
        "target_column": "id",
        "cardinality": "many-to-one",
        "graphql_alias": "customer",
        "via_table_id": None,
    },
]


def test_a_sql_group_fake_reads_children_through_the_relationship_name():
    check_model(
        _tables(order_total="sql_group(SUM(lines.amount), fake=uniform(min=0, max=9))"), _RELS
    )
    with pytest.raises(FakeRefused, match="reads 'items', which is no relationship"):
        check_model(
            _tables(order_total="sql_group(SUM(items.amount), fake=uniform(min=0, max=9))"), _RELS
        )


def test_a_many_to_one_child_is_read_by_its_table_name():
    tables = _tables()
    tables[2]["columns"].append(
        _col("spend", "double", "sql_group(SUM(orders.total), fake=uniform(min=0, max=9))")
    )
    check_model(tables, _RELS)


def test_joined_columns_must_declare_one_fake():
    check_model(_tables(customer_id_fake="hash()"), _RELS)
    with pytest.raises(
        FakeRefused, match="'order_customer' joins orders.customer_id to customers.id"
    ):
        check_model(_tables(customer_id_fake="encrypt()"), _RELS)


def test_a_table_being_saved_is_checked_in_place_of_its_stored_columns():
    model = SimpleNamespace(
        source_id="pg",
        schema_name="public",
        table_name="orders",
        columns=[
            SimpleNamespace(name="id", data_type="integer", fake=None, fake_stable=False),
            SimpleNamespace(
                name="customer_id", data_type="integer", fake="encrypt()", fake_stable=False
            ),
        ],
    )
    tables = _with_model(_tables(), model)
    assert [c["column_name"] for c in tables[0]["columns"]] == ["id", "customer_id"]
    with pytest.raises(FakeRefused, match="joins orders.customer_id to customers.id"):
        check_model(tables, _RELS)


def test_a_cycle_through_a_parents_children_is_refused_naming_its_columns():
    """orders.total sums its lines' amounts; each line's amount takes its share of its order's
    total -- through the relationship back to orders, a cycle across the two tables."""
    tables = _tables(
        order_total="sql_group(SUM(lines.amount), fake=uniform(min=0, max=9))",
        line_fake="sql_group(SUM(back.total), fake=uniform(min=0, max=9))",
    )
    rels = _RELS + [
        {
            "id": "line_orders",
            "source_table_id": 2,
            "target_table_id": 1,
            "source_column": "order_id",
            "target_column": "id",
            "cardinality": "one-to-many",
            "graphql_alias": "back",
            "via_table_id": None,
        }
    ]
    with pytest.raises(
        FakeRefused,
        match="order_lines.amount, orders.total name one another in a cycle",
    ):
        check_model(tables, rels)
