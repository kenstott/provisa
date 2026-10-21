# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2118: the Dataplex target, against a stand-in Dataplex API.

A data product becomes one Dataplex data product -- its fields, its owners (the owner role's
holders, by address) and its members as BigQuery assets: a BigQuery source's table in its own
project, a replica's export view in the engine's project. A member with no BigQuery object, and a
product whose owner role has no holder with an address, are reported by name."""

# Requirements: REQ-2118

from __future__ import annotations

import json
from typing import Any

import httpx
import pytest
from pydantic import ValidationError

from provisa.api.metadata_export import dataplex
from provisa.api.metadata_export.builder import build_snapshot
from provisa.api.metadata_export.dataplex import DataplexExport, member_resource, resource_id
from provisa.api.metadata_export.registry import metadata_export, registered_providers
from provisa.core.models import (
    Column,
    DataProduct,
    Domain,
    MetadataExportTarget,
    ProvisaConfig,
    Source,
    SourceType,
    Table,
)

pytestmark = pytest.mark.unit

PARENT = "projects/proj-x/locations/us"
PRODUCT = f"{PARENT}/dataProducts/sales-360"
EXPORT_VIEW = ("org_acme__export", "pg__public__orders")


def _target(**extra: Any) -> MetadataExportTarget:
    return MetadataExportTarget(
        id="dataplex",
        provider="dataplex",
        endpoint="https://dataplex.example",
        auth_mode="bearer",
        token="t0k",
        gcp_project="proj-x",
        gcp_location="us",
        timeout_seconds=5,
        **extra,
    )


def _table(source: str, name: str, **extra: Any) -> Table:
    return Table(
        source_id=source,
        domain_id="sales",
        schema_name="public" if source == "pg" else "warehouse",
        table_name=name,
        product_id="sales_360",
        columns=[Column(name="id", data_type="integer", visible_to=["admin"])],
        **extra,
    )


def _snapshot(*, owners: tuple[str, ...] = ("owner@example.com",), tables: list | None = None):
    config = ProvisaConfig(
        sources=[
            Source(id="bq", type=SourceType.bigquery, database="proj-src", description="BQ"),
            Source(
                id="pg",
                type=SourceType.postgresql,
                host="h",
                port=5432,
                database="d",
                username="u",
                password="p",
            ),
        ],
        domains=[Domain(id="sales", description="Sales")],
        tables=tables
        if tables is not None
        else [_table("bq", "sales"), _table("pg", "orders", cache_ttl=3600)],
        data_products=[
            DataProduct(
                id="sales_360",
                domain_id="sales",
                name="Sales 360",
                owner_role="data_owner",
                purpose="Every sale",
                limitations="No refunds",
                version="1.2.0",
                status="active",
            )
        ],
        roles=[],
    )
    return build_snapshot(
        config,
        org_id="acme",
        dialect="postgres",
        contexts={},
        owner_emails={"data_owner": owners},
    )


class _Dataplex:
    """A stand-in Dataplex API: data products and their assets, every change a finished
    long-running operation."""

    def __init__(self, products: dict | None = None, assets: dict | None = None) -> None:
        self.products: dict[str, dict] = dict(products or {})
        self.assets: dict[str, dict] = dict(assets or {})
        self.calls: list[tuple[str, str, dict, dict]] = []

    def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path.removeprefix("/v1/")
        params = dict(request.url.params)
        body = json.loads(request.content) if request.content else {}
        self.calls.append((request.method, path, params, body))
        assert request.headers["Authorization"] == "Bearer t0k"
        done = {"name": "operations/op-1", "done": True}
        if request.method == "GET" and path.endswith("/dataAssets"):
            return httpx.Response(200, json={"dataAssets": list(self.assets.values())})
        if request.method == "GET":
            if path not in self.products:
                return httpx.Response(404, json={"error": {"message": "not found"}})
            return httpx.Response(200, json=self.products[path])
        if request.method == "POST" and path.endswith("/dataProducts"):
            self.products[f"{path}/{params['dataProductId']}"] = body
            return httpx.Response(200, json=done)
        if request.method == "POST" and path.endswith("/dataAssets"):
            name = f"{path}/{params['dataAssetId']}"
            self.assets[name] = {**body, "name": name}
            return httpx.Response(200, json=done)
        if request.method == "PATCH":
            store = self.products if path in self.products else self.assets
            store[path] = {**store[path], **body}
            return httpx.Response(200, json=done)
        if request.method == "DELETE":
            self.assets.pop(path)
            return httpx.Response(200, json=done)
        raise AssertionError(f"unexpected {request.method} {path}")


@pytest.fixture
def api(monkeypatch):
    fake = _Dataplex()
    real = httpx.AsyncClient
    monkeypatch.setattr(
        dataplex.httpx,
        "AsyncClient",
        lambda **kw: real(transport=httpx.MockTransport(fake.handle), **kw),
    )
    monkeypatch.setattr(dataplex, "bigquery_engine_project", lambda: "proj-x")
    return fake


def _export() -> DataplexExport:
    exporter = metadata_export(_target())
    assert isinstance(exporter, DataplexExport)
    exporter.export_views = {("pg", "public", "orders"): EXPORT_VIEW}
    return exporter


def test_dataplex_is_resolvable_by_name_and_bigquery_dataplex_is_gone():
    assert "dataplex" in registered_providers()
    assert "bigquery_dataplex" not in registered_providers()


def test_a_dataplex_target_needs_its_project_and_location():
    with pytest.raises(ValidationError, match="GCP project and location"):
        MetadataExportTarget(id="d", provider="dataplex", endpoint="https://x", gcp_project="p")


def test_ids_are_rfc_1034():
    assert resource_id("sales_360") == "sales-360"
    assert resource_id("360_sales") == "p-360-sales"
    long = resource_id("x" * 80)
    assert len(long) == 63 and long.startswith("x")


async def test_a_new_product_is_created_with_its_fields_owners_and_assets(api):
    result = await _export().publish(_snapshot())
    assert result.ok, result.errors
    assert result.published == {"data_products": 1, "data_assets": 2}
    product = api.products[PRODUCT]
    assert product["displayName"] == "Sales 360"
    assert product["ownerEmails"] == ["owner@example.com"]
    assert product["accessApprovalConfig"] == {"approverEmails": ["owner@example.com"]}
    assert (
        "Every sale" in product["description"]
        and "Limitations: No refunds" in (product["description"])
    )
    assert product["labels"] == {
        "provisa-product": "sales_360",
        "provisa-version": "1_2_0",
        "provisa-status": "active",
    }
    by_resource = {a["resource"]: a for a in api.assets.values()}
    # A BigQuery source's table, in its own project; a replica's export view, in the engine's.
    assert set(by_resource) == {
        "//bigquery.googleapis.com/projects/proj-src/datasets/warehouse/tables/sales",
        "//bigquery.googleapis.com/projects/proj-x/datasets/org_acme__export/tables/"
        "pg__public__orders",
    }
    orders = by_resource[
        "//bigquery.googleapis.com/projects/proj-x/datasets/org_acme__export/tables/"
        "pg__public__orders"
    ]
    # The member's refresh cadence, from its freshness.
    assert orders["labels"]["provisa-freshness-seconds"] == "3600"


async def test_an_existing_product_is_updated_and_a_former_member_removed(api):
    stale = f"{PRODUCT}/dataAssets/old-table"
    api.products[PRODUCT] = {"displayName": "Old", "etag": "e1"}
    api.assets[stale] = {"name": stale, "resource": "//bigquery.googleapis.com/projects/p/x"}
    result = await _export().publish(_snapshot())
    assert result.ok, result.errors
    patch = next(c for c in api.calls if c[0] == "PATCH" and c[1] == PRODUCT)
    assert set(patch[2]["updateMask"].split(",")) == {
        "displayName",
        "description",
        "ownerEmails",
        "labels",
        "accessApprovalConfig",
    }
    assert patch[3]["etag"] == "e1"
    assert api.products[PRODUCT]["displayName"] == "Sales 360"
    assert stale not in api.assets and len(api.assets) == 2


async def test_a_member_with_no_bigquery_object_is_reported_by_name(api):
    snapshot = _snapshot(tables=[_table("bq", "sales"), _table("pg", "customers")])
    result = await _export().publish(snapshot)
    assert [e.message for e in result.errors] == [
        "pg.public.customers has no BigQuery table or view to publish as an asset of sales_360"
    ]
    assert result.published == {"data_products": 1, "data_assets": 1}


async def test_a_product_whose_owner_role_has_no_address_is_reported_and_not_sent(api):
    result = await _export().publish(_snapshot(owners=()))
    assert len(result.errors) == 1 and "has no holder with an email address" in (
        result.errors[0].message
    )
    assert api.calls == []


def test_a_view_member_has_no_bigquery_object():
    view = _table("pg", "totals", view_sql="SELECT 1 AS id")
    view = view.model_copy(update={"source_id": "__derived__", "schema_name": "views"})
    snapshot = _snapshot(tables=[_table("bq", "sales"), view])
    (product,) = snapshot.data_products
    members = {m.parts[-1]: m for m in product.members}
    kwargs = {
        "sources": {s.id: s for s in snapshot.sources},
        "tables": {t.ref.fqn(): t for t in snapshot.tables},
        "engine_project": "proj-x",
        "export_views": {},
    }
    assert member_resource(members["totals"], **kwargs) is None
    assert member_resource(members["sales"], **kwargs) == (
        "//bigquery.googleapis.com/projects/proj-src/datasets/warehouse/tables/sales"
    )


def test_the_snapshot_carries_each_products_owner_addresses():
    (product,) = _snapshot(owners=("a@example.com", "b@example.com")).data_products
    assert product.owner_emails == ("a@example.com", "b@example.com")
