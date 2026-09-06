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

import asyncio
from typing import cast
from unittest.mock import AsyncMock, MagicMock

import pytest
from pytest_bdd import given, when, then, scenarios
from sqlalchemy.sql import Select

from provisa.api.metadata_export import build_snapshot
from provisa.core.database import Connection
from provisa.core.models import (
    Column,
    DataProduct,
    Domain,
    ProvisaConfig,
    Source,
    SourceType,
    Table,
)
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import data_products

scenarios("../features/REQ-1634.feature")


class _FakeConn:
    """Enough of Connection for table_repo.upsert to run against a canned data_products row."""

    def __init__(self, product_row: dict | None):
        self.product_row = product_row
        self.upsert = AsyncMock()
        self.upsert_returning = AsyncMock(return_value=7)

    async def execute_core(self, stmt, *args, **kwargs):
        del args, kwargs
        if isinstance(stmt, Select) and stmt.get_final_froms()[0] is data_products:
            result = MagicMock()
            row = MagicMock()
            row._mapping = self.product_row
            result.fetchone.return_value = row if self.product_row is not None else None
            return result
        result = MagicMock()
        result.fetchall.return_value = []
        result.fetchone.return_value = None
        return result


def _table(*, domain_id: str, product_id: str | None) -> Table:
    return Table(
        source_id="s",
        domain_id=domain_id,
        schema_name="public",
        table_name="orders",
        product_id=product_id,
        columns=[],
    )


# ---------------------------------------------------------------------------
# Given
# ---------------------------------------------------------------------------


@pytest.fixture
def shared_data() -> dict:
    return {}


@given('a DataProduct "customer_360" owned by "alice" in domain "sales"')
def dataproduct_exists(shared_data: dict) -> None:
    shared_data["product_row"] = {"id": "customer_360", "domain_id": "sales", "owner": "alice"}
    shared_data["product"] = DataProduct(
        id="customer_360", domain_id="sales", name="customer_360", owner="alice"
    )


@given('two tables in domain "sales" both set product_id="customer_360"')
def tables_assigned_to_product(shared_data: dict) -> None:
    shared_data["tables"] = [
        Table(
            source_id="wh",
            domain_id="sales",
            schema_name="public",
            table_name="customers",
            product_id="customer_360",
            columns=[Column(name="id", data_type="integer", visible_to=["analyst"])],
        ),
        Table(
            source_id="wh",
            domain_id="sales",
            schema_name="public",
            table_name="orders",
            product_id="customer_360",
            columns=[Column(name="id", data_type="integer", visible_to=["analyst"])],
        ),
    ]


@given('a table in domain "marketing"')
def table_in_marketing_domain(shared_data: dict) -> None:
    shared_data["table"] = _table(domain_id="marketing", product_id=None)


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
    shared_data["snapshot"] = build_snapshot(config, org_id="acme", dialect="postgres")


@when('an attempt is made to set product_id="customer_360" on the marketing table')
def attempt_cross_domain_product_assignment(shared_data: dict) -> None:
    shared_data["table"].product_id = "customer_360"


@when("the change is saved")
def change_saved(shared_data: dict) -> None:
    async def _body() -> None:
        conn = _FakeConn(product_row=shared_data["product_row"])
        try:
            await table_repo.upsert(cast(Connection, conn), shared_data["table"])
        except ValueError as exc:
            shared_data["save_error"] = exc

    asyncio.run(_body())


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
    assert "save_error" in shared_data, "expected table_repo.upsert to raise ValueError"
    message = str(shared_data["save_error"])
    assert "marketing" in message and "sales" in message
