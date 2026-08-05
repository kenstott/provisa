# Copyright (c) 2026 Kenneth Stott
# Canary: 7d2f91ea-4c60-4b17-a3e8-52bc0d9f1a73
#
# This source code is licensed under the Business Source License 1.1

"""Step definitions for REQ-1070 — the metadata payload published to external catalogs."""

from __future__ import annotations

import pytest

from pytest_bdd import given, scenarios, then, when

from provisa.api.metadata_export import build_snapshot
from provisa.core.models import (
    Cardinality,
    Column,
    Domain,
    ProvisaConfig,
    Relationship,
    Source,
    SourceType,
    Table,
)

scenarios("../features/REQ-1070.feature")


@pytest.fixture
def shared_data() -> dict:
    return {}


def _table(name: str, columns: list[Column], view_sql: str | None = None) -> Table:
    return Table(
        source_id="wh",
        domain_id="sales",
        schema_name="public",
        table_name=name,
        columns=columns,
        view_sql=view_sql,
        materialize=view_sql is not None,
        # Marked so the table publishes: only Data Product tables enter the snapshot.
        data_product=True,
    )


@given(
    "a governed config with a stewarded domain, a table with described columns, an approved "
    "relationship, and a view derived from that table"
)
def _governed_config(shared_data):
    shared_data["config"] = ProvisaConfig(
        sources=[Source(id="wh", type=SourceType.postgresql, description="Warehouse")],
        domains=[
            Domain(id="sales", description="Sales", steward="data-steward"),
            Domain(id="lab", description="Unowned lab data"),
        ],
        tables=[
            _table(
                "orders",
                [
                    Column(name="id", data_type="integer", visible_to=["admin"]),
                    Column(
                        name="amount",
                        data_type="numeric",
                        visible_to=["admin"],
                        description="Order total",
                    ),
                    Column(name="customer_id", data_type="integer", visible_to=["admin"]),
                ],
            ),
            _table("customers", [Column(name="id", data_type="integer", visible_to=["admin"])]),
            _table(
                "order_totals",
                [
                    Column(name="id", data_type="integer", visible_to=["admin"]),
                    Column(name="net", data_type="numeric", visible_to=["admin"]),
                ],
                view_sql="SELECT orders.id AS id, orders.amount * 2 AS net FROM orders",
            ),
        ],
        relationships=[
            Relationship(
                id="orders-to-customers",
                source_table_id="orders",
                target_table_id="customers",
                source_column="customer_id",
                target_column="id",
                cardinality=Cardinality.many_to_one,
                owner="join-steward",
                version=3,
                needs_review=True,
            )
        ],
        roles=[],
    )


@when("a metadata snapshot is built for the org")
def _build(shared_data):
    shared_data["snapshot"] = build_snapshot(
        shared_data["config"], org_id="acme", dialect="postgres"
    )


@then("the snapshot publishes the source, domain, table and every column as addressable assets")
def _assets_published(shared_data):
    snapshot = shared_data["snapshot"]
    assert snapshot.asset_count() == {"source": 1, "domain": 2, "table": 3, "column": 6}
    fqns = {c.ref.fqn() for c in snapshot.columns()}
    assert "wh.public.orders.amount" in fqns
    amount = [c for c in snapshot.columns() if c.ref.fqn() == "wh.public.orders.amount"][0]
    assert amount.description == "Order total"


@then("the approved relationship carries its defining steward, version and review flag")
def _relationship_published(shared_data):
    edge = shared_data["snapshot"].relationships[0]
    assert edge.owner is not None
    assert edge.owner.id == "join-steward"
    assert edge.version == 3
    assert edge.needs_review is True


@then("the view's lineage is published per column with the transform that produces it")
def _lineage_published(shared_data):
    lineage = shared_data["snapshot"].lineage
    pairs = {(e.upstream.fqn(), e.downstream.fqn()) for e in lineage}
    assert pairs == {
        ("wh.public.orders.id", "wh.public.order_totals.id"),
        ("wh.public.orders.amount", "wh.public.order_totals.net"),
    }
    net = [e for e in lineage if e.downstream.fqn() == "wh.public.order_totals.net"][0]
    assert net.transforms and "amount" in net.transforms[0]


@then("a domain with no designated steward is published as pending rather than omitted")
def _pending_domain(shared_data):
    by_id = {d.id: d for d in shared_data["snapshot"].domains}
    assert by_id["lab"].pending is True
    assert by_id["lab"].steward is None
    assert by_id["sales"].pending is False
