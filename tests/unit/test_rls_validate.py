# Copyright (c) 2026 Kenneth Stott
# Canary: 7c2d9e4a-1f6b-4a8e-b3c5-0d9e2f7a6b41
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1676: an RLS predicate is parsed and resolved against the model when it is saved."""

from provisa.compiler.rls_validate import validate_rls_predicate


def _col(name, data_type="varchar", alias=None):
    return {"column_name": name, "data_type": data_type, "alias": alias}


ORDERS = {
    "id": 1,
    "table_name": "orders",
    "alias": None,
    "domain_id": "sales",
    "columns": [
        _col("id", "integer"),
        _col("region"),
        _col("amount", "decimal"),
        _col("is_open", "boolean"),
        _col("custId", "integer", alias="customer_id"),
    ],
}
CUSTOMERS = {
    "id": 2,
    "table_name": "customers",
    "alias": None,
    "domain_id": "sales",
    "columns": [_col("id", "integer"), _col("region"), _col("tier")],
}
SHIPMENTS = {
    "id": 3,
    "table_name": "shipments",
    "alias": None,
    "domain_id": "sales",
    "columns": [_col("id", "integer"), _col("order_id", "integer")],
}
REGISTRY = [ORDERS, CUSTOMERS, SHIPMENTS]


class TestAccepted:
    def test_column_against_session_var(self):
        assert (
            validate_rls_predicate("region = current_setting('provisa.region')", [ORDERS], REGISTRY)
            is None
        )

    def test_tautology(self):
        assert validate_rls_predicate("1 = 1", [ORDERS], REGISTRY) is None

    def test_connectives_and_parens(self):
        assert (
            validate_rls_predicate(
                "(region = 'east' OR region = 'west') AND NOT is_open", [ORDERS], REGISTRY
            )
            is None
        )

    def test_boolean_column_alone(self):
        assert validate_rls_predicate("is_open", [ORDERS], REGISTRY) is None

    def test_exposed_alias_is_the_columns_name(self):
        assert validate_rls_predicate("customer_id = 7", [ORDERS], REGISTRY) is None

    def test_physical_name_still_binds(self):
        assert validate_rls_predicate('"custId" = 7', [ORDERS], REGISTRY) is None

    def test_qualified_by_own_table(self):
        assert validate_rls_predicate("orders.region = 'east'", [ORDERS], REGISTRY) is None

    def test_exists_subquery_over_registered_table(self):
        pred = (
            "EXISTS (SELECT 1 FROM customers c WHERE c.id = customer_id "
            "AND c.tier = current_setting('provisa.tier'))"
        )
        assert validate_rls_predicate(pred, [ORDERS], REGISTRY) is None

    def test_in_subquery_domain_qualified_table(self):
        pred = "id IN (SELECT order_id FROM sales.shipments)"
        assert validate_rls_predicate(pred, [ORDERS], REGISTRY) is None

    def test_domain_rule_column_on_every_table(self):
        assert validate_rls_predicate("region = 'east'", [ORDERS, CUSTOMERS], REGISTRY) is None

    def test_cast_to_boolean(self):
        assert (
            validate_rls_predicate("current_setting('provisa.all')::boolean", [ORDERS], REGISTRY)
            is None
        )


class TestRefused:
    def test_parse_error(self):
        msg = validate_rls_predicate("region = = 'x'", [ORDERS], REGISTRY)
        assert msg is not None and msg.startswith("RLS predicate does not parse")

    def test_empty(self):
        assert validate_rls_predicate("   ", [ORDERS], REGISTRY) == "RLS predicate is empty"

    def test_unknown_column_named(self):
        msg = validate_rls_predicate("regon = 'east'", [ORDERS], REGISTRY)
        assert msg == "RLS predicate: column 'regon' is not a column of 'orders'"

    def test_unknown_qualifier(self):
        msg = validate_rls_predicate("o.region = 'east'", [ORDERS], REGISTRY)
        assert msg == "RLS predicate: column qualifier 'o' names no table the predicate can see"

    def test_unregistered_table_in_subquery(self):
        msg = validate_rls_predicate("id IN (SELECT order_id FROM invoices)", [ORDERS], REGISTRY)
        assert msg == "RLS predicate reads table 'invoices', which the model does not register"

    def test_unknown_column_inside_subquery(self):
        msg = validate_rls_predicate(
            "EXISTS (SELECT 1 FROM customers c WHERE c.tiers = 'gold')", [ORDERS], REGISTRY
        )
        assert msg == "RLS predicate: column 'tiers' is not a column of 'customers'"

    def test_domain_rule_column_missing_on_one_table(self):
        msg = validate_rls_predicate("region = 'east'", [ORDERS, CUSTOMERS, SHIPMENTS], REGISTRY)
        assert msg == "RLS predicate: column 'region' is not a column of 'shipments'"

    def test_non_boolean_expression(self):
        msg = validate_rls_predicate("amount + 1", [ORDERS], REGISTRY)
        assert msg is not None and msg.startswith("RLS predicate must be a boolean expression")

    def test_non_boolean_column_alone(self):
        msg = validate_rls_predicate("region", [ORDERS], REGISTRY)
        assert msg is not None and msg.startswith("RLS predicate must be a boolean expression")

    def test_statement_not_predicate(self):
        msg = validate_rls_predicate("SELECT 1", [ORDERS], REGISTRY)
        assert msg == "RLS predicate must be a boolean expression, not a SELECT"
