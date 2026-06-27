# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""BDD step implementations for REQ-157 / REQ-542 — Naming & Schema.

REQ-157: Order-by enum values preserve original column case (not uppercased).
REQ-542: Ordered regex naming rules are applied to table names in order when
         generating GraphQL field names, before uniqueness resolution.
"""

from __future__ import annotations

import re as _re

import pytest
from graphql import GraphQLEnumType, GraphQLInputObjectType
from pytest_bdd import given, scenario, then, when

from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.naming import generate_name


@pytest.fixture
def shared_data() -> dict:
    return {}


@scenario(
    "REQ-157.feature",
    "REQ-157 default behaviour",
)
def test_req_157_default_behaviour() -> None:
    """Order-by enum values preserve original column case."""


@given("a column with mixed-case name")
def given_mixed_case_column(shared_data: dict) -> None:
    # Original, mixed-case column names as they exist in the source system.
    column_name = "OrderDate"
    second_column = "customerID"

    shared_data["original_columns"] = [column_name, second_column]
    shared_data["visible_columns"] = [
        {"column_name": column_name},
        {"column_name": second_column},
    ]
    shared_data["column_metadata"] = {
        column_name.lower(): ColumnMetadata(
            column_name=column_name,
            data_type="timestamp",
            is_nullable=True,
            ordinal_position=1,
        ),
        second_column.lower(): ColumnMetadata(
            column_name=second_column,
            data_type="bigint",
            is_nullable=False,
            ordinal_position=2,
        ),
    }

    # Sanity: ensure our fixture actually has mixed case (not already lower/upper).
    assert any(c != c.upper() and c != c.lower() for c in shared_data["original_columns"])


@when("order-by enum values are generated")
def when_order_by_generated(shared_data: dict) -> None:
    from provisa.compiler import order_by_gen

    builder = None
    for name in ("build_order_by_input_type", "build_order_by_type", "build_order_by_enum"):
        builder = getattr(order_by_gen, name, None)
        if builder is not None:
            break

    assert builder is not None, "No order-by builder found in provisa.compiler.order_by_gen"

    result = builder(
        "Orders",
        shared_data["visible_columns"],
        shared_data["column_metadata"],
    )
    shared_data["order_by_result"] = result


@then("the original case is preserved without uppercasing")
def then_case_preserved(shared_data: dict) -> None:
    result = shared_data["order_by_result"]
    originals = shared_data["original_columns"]

    if isinstance(result, GraphQLEnumType):
        generated = set(result.values.keys())
    elif isinstance(result, GraphQLInputObjectType):
        generated = set(result.fields.keys())
    else:
        raise AssertionError(f"Unexpected order-by type: {type(result)!r}")

    for original in originals:
        assert original in generated, (
            f"Expected original-case column {original!r} in order-by "
            f"values {sorted(generated)!r}"
        )
        # Explicitly verify it was NOT uppercased.
        if original != original.upper():
            assert original.upper() not in generated, (
                f"Column {original!r} was uppercased to {original.upper()!r} "
                f"in order-by values {sorted(generated)!r}"
            )


@scenario(
    "REQ-542.feature",
    "REQ-542 default behaviour",
)
def test_req_542_default_behaviour() -> None:
    """Ordered regex naming rules applied before uniqueness resolution."""


@given("a config with ordered regex naming rules")
def given_ordered_naming_rules(shared_data: dict) -> None:
    # Ordered rules: first strip the source prefix, then the layer prefix.
    # Order matters: ^prod_pg_ must run before ^raw_ to fully reduce names.
    shared_data["naming_rules"] = [
        {"pattern": "^prod_pg_", "replacement": ""},
        {"pattern": "^raw_", "replacement": ""},
    ]
    # Table names as registered from the source, all sharing the prod_pg_ prefix.
    shared_data["table_names"] = [
        "prod_pg_orders",
        "prod_pg_raw_customers",
        "prod_pg_raw_orders",
    ]
    shared_data["schema"] = "public"
    shared_data["source_id"] = "pg1"

    # Sanity: rules are ordered and non-empty.
    assert len(shared_data["naming_rules"]) == 2
    assert shared_data["naming_rules"][0]["pattern"] == "^prod_pg_"


@when("GraphQL field names are generated for table names")
def when_field_names_generated(shared_data: dict) -> None:
    table_names = shared_data["table_names"]
    rules = shared_data["naming_rules"]

    generated: dict[str, str] = {}
    for table in table_names:
        name = generate_name(
            table,
            shared_data["schema"],
            shared_data["source_id"],
            domain_table_names=table_names,
            naming_rules=rules,
        )
        generated[table] = name
    shared_data["generated_names"] = generated


@then("each rule is applied in order before uniqueness resolution")
def then_rules_applied_in_order(shared_data: dict) -> None:
    generated = shared_data["generated_names"]

    # prod_pg_orders → strip ^prod_pg_ → orders (unique, no qualifier needed).
    assert generated["prod_pg_orders"] == "orders", generated

    # prod_pg_raw_customers → strip ^prod_pg_ → raw_customers → strip ^raw_ → customers.
    assert generated["prod_pg_raw_customers"] == "customers", generated

    # prod_pg_raw_orders → strip ^prod_pg_ → raw_orders → strip ^raw_ → orders,
    # which now collides with prod_pg_orders → uniqueness resolution qualifies it.
    assert generated["prod_pg_raw_orders"] != "prod_pg_raw_orders", generated
    assert not generated["prod_pg_raw_orders"].startswith("prod_pg_"), generated

    # All generated names must be valid GraphQL identifiers (no leftover prefixes).
    for original, name in generated.items():
        assert _re.fullmatch(r"[_A-Za-z][_0-9A-Za-z]*", name), (
            f"{original!r} produced invalid GraphQL name {name!r}"
        )
        assert "prod_pg_" not in name, f"prefix not stripped for {original!r}: {name!r}"

    # The two colliding tables must resolve to distinct field names.
    assert generated["prod_pg_orders"] != generated["prod_pg_raw_orders"], generated
