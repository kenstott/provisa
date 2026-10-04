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

from provisa.compiler.naming import (
    SqlAddressTaken,
    generate_name,
    refuse_taken_sql_addresses,
    to_type_name,
)


class TestGenerateName:
    def test_simple_unique_name(self):
        assert generate_name("orders", naming_rules=[]) == "orders"

    def test_naming_rules_applied(self):
        result = generate_name(
            "prod_pg_orders", naming_rules=[{"pattern": "^prod_pg_", "replacement": ""}]
        )
        assert result == "orders"

    def test_multiple_naming_rules(self):
        result = generate_name(
            "prod_pg_raw_orders",
            naming_rules=[
                {"pattern": "^prod_pg_", "replacement": ""},
                {"pattern": "^raw_", "replacement": ""},
            ],
        )
        assert result == "orders"

    def test_alias_overrides_everything(self):
        result = generate_name("ugly_internal_name", naming_rules=[], alias="sales_orders")
        assert result == "salesOrders"

    def test_hyphens_in_name_replaced(self):
        assert generate_name("my-table", naming_rules=[]) == "myTable"

    def test_empty_after_rules_raises(self):
        with pytest.raises(ValueError, match="empty name"):
            generate_name("orders", naming_rules=[{"pattern": ".*", "replacement": ""}])


def _t(table_name, *, source="pg1", schema="sales", domain="d", alias=None):
    return {
        "domain_id": domain,
        "source_id": source,
        "schema_name": schema,
        "table_name": table_name,
        "alias": alias,
    }


class TestOneSqlAddressPerTable:
    """REQ-1933: two tables never share a SQL address in a domain. A name is never qualified with
    its schema or source to make it unique; which table kept the bare name was only list order."""

    def test_a_name_the_rules_make_taken_is_refused_naming_the_holder(self):
        # us_orders -> "orders" under the rule, then "orders" is the same address.
        with pytest.raises(SqlAddressTaken) as refused:
            refuse_taken_sql_addresses(
                [_t("us_orders"), _t("orders", schema="public")],
                [{"pattern": "^us_", "replacement": ""}],
            )
        assert refused.value.address == "orders"
        assert refused.value.holder == "pg1.sales.us_orders"
        assert refused.value.newcomer == "pg1.public.orders"

    def test_two_sources_tables_of_one_name_in_a_domain_are_refused(self):
        with pytest.raises(SqlAddressTaken):
            refuse_taken_sql_addresses(
                [_t("getInventory", source="petstore-api"), _t("getInventory", source="copy")], []
            )

    def test_an_alias_or_another_domain_gives_a_table_its_own_address(self):
        refuse_taken_sql_addresses(
            [
                _t("orders"),
                _t("orders", source="pg2", alias="eu_orders"),
                _t("orders", source="pg3", domain="other"),
            ],
            [],
        )

    def test_spellings_of_one_address_are_one_address(self):
        with pytest.raises(SqlAddressTaken):
            refuse_taken_sql_addresses([_t("get_inventory"), _t("getInventory", source="x")], [])


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


class TestApplyGqlName:
    """REQ-471: apply_gql_name is the sole naming authority for GraphQL identifiers — its output
    must always satisfy the Name grammar ([_A-Za-z][_0-9A-Za-z]*), even for a raw external name
    apply_convention's casing alone cannot fix. Reproduced live: a health-domain CSV column header
    verbatim as its physical name ("1844YearsDosesAdministered") crashed GraphQLEnumType's own
    name assertion in the distinct_on enum builder, taking the whole schema down at startup."""

    def test_leading_digit_gets_prefixed(self):
        from provisa.compiler.naming import apply_gql_name

        assert apply_gql_name("1844YearsDosesAdministered") == "_1844YearsDosesAdministered"

    def test_invalid_characters_become_underscores(self):
        from provisa.compiler.naming import apply_gql_name

        assert apply_gql_name("Weird Column! Name") == "weird_Column__Name"

    def test_empty_name_does_not_crash(self):
        from provisa.compiler.naming import apply_gql_name

        assert apply_gql_name("") == "_"

    def test_already_valid_name_is_unchanged_by_sanitization(self):
        from provisa.compiler.naming import apply_gql_name

        assert apply_gql_name("order_items") == "orderItems"


class TestJunctionFieldName:
    """REQ-1586: a junction-backed edge is named for its nomination, not its target table."""

    def test_several_edges_through_one_junction_get_distinct_names(self):
        # The demo's pets reach pets three ways through one pet_companions row set. Named after the
        # target table all three would be "pets", collapsing ctx.joins and the GraphQL field dict.
        from provisa.compiler.naming import junction_field_name

        names = {
            junction_field_name("column", "one-to-many", type_value=v)
            for v in ("bonded pair", "littermate", "shares enclosure")
        }
        assert names == {"bondedPairs", "littermates", "sharesEnclosures"}

    def test_table_and_fixed_nominations(self):
        from provisa.compiler.naming import junction_field_name

        assert (
            junction_field_name("table", "one-to-many", via_table_name="pet_companions")
            == "petCompanions"
        )
        assert junction_field_name("fixed", "many-to-one", cypher_alias="KIND_OF") == "kindOf"

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"label_source": "column"},
            {"label_source": "table"},
            {"label_source": "fixed"},
            {"label_source": "guess", "type_value": "x"},
        ],
    )
    def test_absent_nomination_raises(self, kwargs):
        # No fallback: the registry's CHECK constraints admit only the three nominations, so a
        # missing one is a row that never should have been stored.
        from provisa.compiler.naming import junction_field_name

        with pytest.raises(ValueError):
            junction_field_name(cardinality="one-to-many", **kwargs)

    def test_admin_derivation_matches_the_compiler(self):
        from provisa.api.admin.db_queries import derive_graphql_alias

        assert (
            derive_graphql_alias(
                "pets", "one-to-many", via_label_source="column", via_type_value="bonded pair"
            )
            == "bondedPairs"
        )
