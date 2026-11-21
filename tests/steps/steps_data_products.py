# Copyright (c) 2026 Kenneth Stott
# Canary: a8b4c7e2-1f3a-4d5b-9e8c-6f2a0d1b7c3e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""pytest-bdd step implementations for REQ-1634 - Data Products."""

from __future__ import annotations

import pytest
from pytest_bdd import given, when, then, scenarios

from provisa.api.metadata_export import build_snapshot
from provisa.core.models import (
    Column,
    DataProduct,
    Domain,
    ProvisaConfig,
    Source,
    SourceType,
    Table,
)

scenarios("../features/REQ-1634.feature")


def _table(*, domain_id: str, table_name: str = "orders") -> Table:
    return Table(
        source_id="wh",
        domain_id=domain_id,
        schema_name="public",
        table_name=table_name,
        columns=[Column(name="id", data_type="integer", visible_to=["analyst"])],
    )


# ---------------------------------------------------------------------------
# Given
# ---------------------------------------------------------------------------


@pytest.fixture
def shared_data() -> dict:
    return {}


# The generated feature (tests/features/REQ-1634.feature, built from the requirement's own
# scenario text) states the whole scenario — its givens, whens and thens — as ONE Given
# sentence. The steps below are that sentence's parts; this step is the sentence, and runs them
# in the order it names them, so the scenario asserts both outcomes it describes.
_REQ_1634_SCENARIO = (
    'a DataProduct "customer_360" owned by "alice" in domain "sales", and two tables in domain '
    '"sales" both set product_id="customer_360", when metadata is exported, then both tables '
    'publish as one data product named "customer_360" attributed to owner "alice" instead of two '
    'separate products; given an attempt to set product_id="customer_360" on a table in domain '
    '"marketing", when saved, then it is rejected because the table\'s domain_id does not match '
    "the DataProduct's domain_id."
)


@given(_REQ_1634_SCENARIO)
def the_whole_scenario_as_the_requirement_states_it(shared_data: dict) -> None:
    dataproduct_exists(shared_data)
    tables_assigned_to_product(shared_data)
    metadata_export_runs(shared_data)
    tables_published_as_single_product(shared_data)

    table_in_marketing_domain(shared_data)
    attempt_cross_domain_product_assignment(shared_data)
    change_saved(shared_data)
    cross_domain_assignment_rejected(shared_data)


@given('a DataProduct "customer_360" owned by "alice" in domain "sales"')
def dataproduct_exists(shared_data: dict) -> None:
    shared_data["product"] = DataProduct(
        id="customer_360", domain_id="sales", name="customer_360", owner_role="alice"
    )


@given('two tables in domain "sales" both set product_id="customer_360"')
def tables_assigned_to_product(shared_data: dict) -> None:
    # REQ-1634 (amended 2026-11-07): membership is set on the product, which lists both tables.
    shared_data["tables"] = [
        _table(domain_id="sales", table_name="customers"),
        _table(domain_id="sales", table_name="orders"),
    ]
    shared_data["product"] = shared_data["product"].model_copy(
        update={"tables": ["customers", "orders"]}
    )


@given('a table in domain "marketing"')
def table_in_marketing_domain(shared_data: dict) -> None:
    shared_data["table"] = _table(domain_id="marketing", table_name="campaigns")


# ---------------------------------------------------------------------------
# When
# ---------------------------------------------------------------------------


@when("metadata is exported")
def metadata_export_runs(shared_data: dict) -> None:
    config = ProvisaConfig(
        sources=[Source(id="wh", type=SourceType.postgresql, description="Warehouse")],
        domains=[Domain(id="sales", description="Sales")],
        tables=shared_data["tables"],
        data_products=[shared_data["product"]],
        roles=[],
    )
    shared_data["snapshot"] = build_snapshot(config, org_id="acme", dialect="postgres", contexts={})


@when('an attempt is made to set product_id="customer_360" on the marketing table')
def attempt_cross_domain_product_assignment(shared_data: dict) -> None:
    # The product lists the marketing table among its members.
    shared_data["attempt"] = shared_data["product"].model_copy(
        update={"tables": [*shared_data["product"].tables, "campaigns"]}
    )


@when("the change is saved")
def change_saved(shared_data: dict) -> None:
    try:
        ProvisaConfig(
            sources=[Source(id="wh", type=SourceType.postgresql, description="Warehouse")],
            domains=[
                Domain(id="sales", description="Sales"),
                Domain(id="marketing", description="Marketing"),
            ],
            tables=[*shared_data["tables"], shared_data["table"]],
            data_products=[shared_data["attempt"]],
            roles=[],
        )
    except ValueError as exc:
        shared_data["save_error"] = exc


# ---------------------------------------------------------------------------
# Then
# ---------------------------------------------------------------------------


@then('both tables publish as one data product named "customer_360" attributed to owner "alice"')
def tables_published_as_single_product(shared_data: dict) -> None:
    products = shared_data["snapshot"].data_products
    assert len(products) == 1, products
    product = products[0]
    assert product.name == "customer_360"
    assert product.owner is not None and product.owner.id == "alice"
    assert len(product.members) == 2


@then("it is rejected because the table's domain_id does not match the DataProduct's domain_id")
def cross_domain_assignment_rejected(shared_data: dict) -> None:
    assert "save_error" in shared_data, "expected the product's members to be refused"
    message = str(shared_data["save_error"])
    assert "marketing" in message and "sales" in message
