# Copyright (c) 2026 Kenneth Stott
# Canary: 66caa52a-3e4b-4226-93a2-b2c95c3b884b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for GraphQL name generation."""

import pytest

from provisa.compiler.naming import generate_name, to_type_name


class TestGenerateName:
    def test_simple_unique_name(self):
        result = generate_name(
            "orders", "public", "sales-pg",
            domain_table_names=["orders", "customers"],
            naming_rules=[],
        )
        assert result == "orders"

    def test_conflict_adds_schema_qualifier(self):
        result = generate_name(
            "orders", "sales", "pg1",
            domain_table_names=["orders", "orders"],
            naming_rules=[],
        )
        assert result == "sales_orders"

    def test_conflict_adds_source_qualifier_when_schema_taken(self):
        result = generate_name(
            "orders", "public", "pg1",
            domain_table_names=["orders", "orders", "public_orders"],
            naming_rules=[],
        )
        assert result == "pg1_orders"

    def test_naming_rules_applied(self):
        result = generate_name(
            "prod_pg_orders", "public", "pg1",
            domain_table_names=["prod_pg_orders"],
            naming_rules=[{"pattern": "^prod_pg_", "replacement": ""}],
        )
        assert result == "orders"

    def test_multiple_naming_rules(self):
        result = generate_name(
            "prod_pg_raw_orders", "public", "pg1",
            domain_table_names=["prod_pg_raw_orders"],
            naming_rules=[
                {"pattern": "^prod_pg_", "replacement": ""},
                {"pattern": "^raw_", "replacement": ""},
            ],
        )
        assert result == "orders"

    def test_alias_overrides_everything(self):
        result = generate_name(
            "ugly_internal_name", "public", "pg1",
            domain_table_names=["ugly_internal_name"],
            naming_rules=[],
            alias="sales_orders",
        )
        assert result == "sales_orders"

    def test_hyphens_in_name_replaced(self):
        result = generate_name(
            "my-table", "public", "pg1",
            domain_table_names=["my-table"],
            naming_rules=[],
        )
        assert result == "my_table"

    def test_empty_after_rules_raises(self):
        with pytest.raises(ValueError, match="empty name"):
            generate_name(
                "orders", "public", "pg1",
                domain_table_names=["orders"],
                naming_rules=[{"pattern": ".*", "replacement": ""}],
            )


class TestToTypeName:
    def test_snake_case(self):
        assert to_type_name("orders") == "Orders"

    def test_multi_word(self):
        assert to_type_name("order_items") == "OrderItems"

    def test_already_pascal(self):
        assert to_type_name("Orders") == "Orders"

    def test_with_hyphens(self):
        assert to_type_name("order-items") == "OrderItems"

    def test_single_char(self):
        assert to_type_name("a") == "A"
