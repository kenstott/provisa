# Copyright (c) 2025 Kenneth Stott
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
from provisa.executor.serialize import serialize_rows


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
