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
        assert result == "salesOrders"

    def test_conflict_adds_source_qualifier_when_schema_taken(self):
        result = generate_name(
            "orders", "public", "pg1",
            domain_table_names=["orders", "orders", "public_orders"],
            naming_rules=[],
        )
        assert result == "pg1Orders"

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
        assert result == "salesOrders"

    def test_hyphens_in_name_replaced(self):
        result = generate_name(
            "my-table", "public", "pg1",
            domain_table_names=["my-table"],
            naming_rules=[],
        )
        assert result == "myTable"

    def test_empty_after_rules_raises(self):
        with pytest.raises(ValueError, match="empty name"):
            generate_name(
                "orders", "public", "pg1",
                domain_table_names=["orders"],
                naming_rules=[{"pattern": ".*", "replacement": ""}],
            )


class TestToTypeName:
    def test_simple(self):
        assert to_type_name("orders") == "Orders"

    def test_camel_case_input(self):
        assert to_type_name("orderItems") == "OrderItems"

    def test_already_pascal(self):
        assert to_type_name("Orders") == "Orders"

    def test_domain_prefix(self):
        assert to_type_name("sa__orderItems") == "SA__OrderItems"

    def test_single_char(self):
        assert to_type_name("a") == "A"


class TestNamingConvention:
    """REQ-194/195: convention → casing, with Hasura v2 / DDN literal parity."""

    def test_hasura_graphql_is_snake_case(self):
        # REQ-194: hasura_graphql GQL convention is snake_case (not camelCase).
        from provisa.compiler.naming import apply_convention

        assert apply_convention("orderItems", "hasura_graphql") == "order_items"

    def test_apollo_graphql_is_camel_case(self):
        from provisa.compiler.naming import apply_convention

        assert apply_convention("order_items", "apollo_graphql") == "orderItems"

    def test_snake_is_snake_case(self):
        from provisa.compiler.naming import apply_convention

        assert apply_convention("orderItems", "snake") == "order_items"

    def test_hasura_default_literal_maps_to_snake(self):
        # REQ-195: hasura-default → snake_case.
        from provisa.compiler.naming import apply_convention, normalize_convention

        assert normalize_convention("hasura-default") == "hasura_graphql"
        assert apply_convention("orderItems", "hasura-default") == "order_items"

    def test_graphql_default_literal_maps_to_camel(self):
        # REQ-195: graphql-default → camelCase.
        from provisa.compiler.naming import apply_convention, normalize_convention

        assert normalize_convention("graphql-default") == "apollo_graphql"
        assert apply_convention("order_items", "graphql-default") == "orderItems"

    def test_ddn_graphql_literal_maps_to_camel(self):
        # REQ-195: DDN namingConvention: graphql → camelCase.
        from provisa.compiler.naming import apply_convention, normalize_convention

        assert normalize_convention("graphql") == "apollo_graphql"
        assert apply_convention("order_items", "graphql") == "orderItems"

    def test_literals_are_valid_conventions(self):
        from provisa.compiler.naming import VALID_CONVENTIONS

        for lit in ("hasura-default", "graphql-default", "graphql"):
            assert lit in VALID_CONVENTIONS

    def test_mutation_style_hasura_is_snake(self):
        from provisa.compiler.naming import mutation_style

        assert mutation_style("hasura_graphql") == "snake"
        assert mutation_style("hasura-default") == "snake"
        assert mutation_style("apollo_graphql") == "camel"
