# Copyright (c) 2025 Kenneth Stott
# Canary: b1e4fa9b-266b-456d-8c07-8b148e402946
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for serialize_rows — JOIN rows → nested GraphQL JSON."""

from decimal import Decimal

from provisa.compiler.sql_gen import ColumnRef
from provisa.executor.serialize import serialize_rows, serialize_aggregate


def _make_columns(specs: list[tuple[str, str | None]]) -> list[ColumnRef]:
    """Build ColumnRef list from (field_name, nested_in) tuples."""
    return [
        ColumnRef(alias=None, column=name, field_name=name, nested_in=nested)
        for name, nested in specs
    ]


class TestSerializeFlatRows:
    def test_simple_flat(self):
        columns = _make_columns([("id", None), ("amount", None), ("status", None)])
        rows = [
            (1, Decimal("19.99"), "completed"),
            (2, Decimal("99.98"), "pending"),
        ]
        result = serialize_rows(rows, columns, "orders")
        assert result == {
            "data": {
                "orders": [
                    {"id": 1, "amount": 19.99, "status": "completed"},
                    {"id": 2, "amount": 99.98, "status": "pending"},
                ]
            }
        }

    def test_empty_rows(self):
        columns = _make_columns([("id", None)])
        result = serialize_rows([], columns, "orders")
        assert result == {"data": {"orders": []}}

    def test_integer_decimal(self):
        """Decimal with no fractional part converts to int."""
        columns = _make_columns([("count", None)])
        rows = [(Decimal("42"),)]
        result = serialize_rows(rows, columns, "stats")
        assert result["data"]["stats"][0]["count"] == 42
        assert isinstance(result["data"]["stats"][0]["count"], int)


class TestSerializeNestedRows:
    def test_many_to_one_nested(self):
        columns = [
            ColumnRef(alias="t0", column="id", field_name="id", nested_in=None),
            ColumnRef(alias="t0", column="amount", field_name="amount", nested_in=None),
            ColumnRef(alias="t1", column="name", field_name="name", nested_in="customers"),
            ColumnRef(alias="t1", column="email", field_name="email", nested_in="customers"),
        ]
        rows = [
            (1, Decimal("19.99"), "Alice", "alice@example.com"),
            (2, Decimal("29.99"), "Bob", "bob@example.com"),
        ]
        result = serialize_rows(rows, columns, "orders")
        assert result["data"]["orders"][0]["customers"] == {
            "name": "Alice",
            "email": "alice@example.com",
        }

    def test_null_relationship(self):
        columns = [
            ColumnRef(alias="t0", column="id", field_name="id", nested_in=None),
            ColumnRef(alias="t1", column="name", field_name="name", nested_in="customers"),
        ]
        rows = [
            (1, "Alice"),
            (2, None),
        ]
        result = serialize_rows(rows, columns, "orders")
        assert result["data"]["orders"][0]["customers"] == {"name": "Alice"}
        assert result["data"]["orders"][1]["customers"] is None

    def test_multiple_nested_groups(self):
        columns = [
            ColumnRef(alias="t0", column="id", field_name="id", nested_in=None),
            ColumnRef(alias="t1", column="name", field_name="name", nested_in="customer"),
            ColumnRef(alias="t2", column="name", field_name="name", nested_in="product"),
        ]
        rows = [(1, "Alice", "Widget")]
        result = serialize_rows(rows, columns, "orders")
        row = result["data"]["orders"][0]
        assert row["customer"] == {"name": "Alice"}
        assert row["product"] == {"name": "Widget"}


class TestSerializeAggregate:
    def test_aggregate_with_nodes(self):
        """serialize_aggregate merges aggregate row and nodes rows into correct shape."""
        agg_columns = [
            ColumnRef(alias=None, column="count", field_name="count", nested_in="aggregate"),
        ]
        nodes_columns = [
            ColumnRef(alias=None, column="id", field_name="id", nested_in=None),
            ColumnRef(alias=None, column="amount", field_name="amount", nested_in=None),
        ]
        agg_rows = [(Decimal("3"),)]
        nodes_rows = [(1, Decimal("10.00")), (2, Decimal("20.00")), (3, Decimal("30.00"))]

        result = serialize_aggregate(
            agg_rows=agg_rows,
            agg_columns=agg_columns,
            nodes_rows=nodes_rows,
            nodes_columns=nodes_columns,
            root_field="orders_aggregate",
        )
        data = result["data"]["orders_aggregate"]
        assert data["aggregate"]["count"] == 3
        assert len(data["nodes"]) == 3
        assert data["nodes"][0] == {"id": 1, "amount": 10.0}
        assert data["nodes"][2] == {"id": 3, "amount": 30.0}

    def test_aggregate_without_nodes(self):
        """serialize_aggregate with no nodes rows returns empty nodes list."""
        agg_columns = [
            ColumnRef(alias=None, column="count", field_name="count", nested_in="aggregate"),
        ]
        agg_rows = [(Decimal("5"),)]
        result = serialize_aggregate(
            agg_rows=agg_rows,
            agg_columns=agg_columns,
            nodes_rows=None,
            nodes_columns=None,
            root_field="orders_aggregate",
        )
        data = result["data"]["orders_aggregate"]
        assert data["aggregate"]["count"] == 5
        assert "nodes" not in data

    def test_aggregate_sum_nested(self):
        """serialize_aggregate handles nested aggregate paths like aggregate.sum."""
        agg_columns = [
            ColumnRef(alias=None, column="count", field_name="count", nested_in="aggregate"),
            ColumnRef(alias=None, column="amount", field_name="amount", nested_in="aggregate.sum"),
        ]
        nodes_columns = [
            ColumnRef(alias=None, column="id", field_name="id", nested_in=None),
        ]
        agg_rows = [(Decimal("2"), Decimal("50.00"))]
        nodes_rows = [(1,), (2,)]
        result = serialize_aggregate(
            agg_rows=agg_rows,
            agg_columns=agg_columns,
            nodes_rows=nodes_rows,
            nodes_columns=nodes_columns,
            root_field="orders_aggregate",
        )
        data = result["data"]["orders_aggregate"]
        assert data["aggregate"]["count"] == 2
        assert data["aggregate"]["sum"]["amount"] == 50.0
        assert len(data["nodes"]) == 2
