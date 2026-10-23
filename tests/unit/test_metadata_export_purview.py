# Copyright (c) 2026 Kenneth Stott
# Canary: 2f27b4de-2e83-4f17-bff8-b4e01f9db8cf
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2118: the Purview target, against a stand-in Entra, Graph and Unified Catalog.

The Data Map publishes through the Atlas adapter (stood in here by the guids it returns); each
data product then becomes a Unified Catalog data product in the governance domain of its
domain's name, owned by its owner role's holders as Entra users, with its members' Data Map
entities as its data assets."""

# Requirements: REQ-2118

from __future__ import annotations

import json
from typing import Any

import httpx
import pytest
from pydantic import ValidationError

from provisa.api.metadata_export import purview
from provisa.api.metadata_export.atlas import AtlasExport
from provisa.api.metadata_export.builder import build_snapshot
from provisa.api.metadata_export.provider import PublishResult
from provisa.api.metadata_export.purview import (
    PurviewExport,
    catalog_status,
    product_id,
    update_frequency,
)
from provisa.api.metadata_export.registry import metadata_export
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

TENANT = "tenant-1"
CATALOG = f"https://{TENANT}-api.purview-service.microsoft.com/datagovernance/catalog"
DOMAIN_ID = "11111111-1111-1111-1111-111111111111"


def _target() -> MetadataExportTarget:
    return MetadataExportTarget(
        id="purview",
        provider="purview",
        endpoint="https://acct.purview.azure.com/catalog",
        auth_mode="entra",
        entra_tenant_id=TENANT,
        entra_client_id="client",
        entra_client_secret="secret",
        timeout_seconds=5,
    )


def _snapshot(*, owners=("owner@example.com",), domain="sales", status="live"):
    config = ProvisaConfig(
        sources=[
            Source(
                id="pg",
                type=SourceType.postgresql,
                host="h",
                port=5432,
                database="d",
                username="u",
                password="p",
            )
        ],
        domains=[Domain(id=domain, description="Sales")],
        tables=[
            Table(
                source_id="pg",
                domain_id=domain,
                schema_name="public",
                table_name=name,
                product_id="sales_360",
                cache_ttl=ttl,
                columns=[Column(name="id", data_type="integer", visible_to=["admin"])],
            )
            for name, ttl in (("orders", 3600), ("customers", 86400))
        ],
        data_products=[
            DataProduct(
                id="sales_360",
                domain_id=domain,
                name="Sales 360",
                owner_role="data_owner",
                purpose="Every sale",
                usage="Join on customer_id",
                limitations="No refunds",
                status=status,
            )
        ],
        roles=[],
    )
    return build_snapshot(
        config, org_id="acme", dialect="postgres", contexts={}, owner_emails={"data_owner": owners}
    )


class _Purview:
    """Stand-in Entra token endpoint, Graph users and Unified Catalog."""

    def __init__(self, *, domains: dict[str, str] | None = None, users: dict | None = None):
        self.domains = {"sales": DOMAIN_ID} if domains is None else domains
        self.users = {"owner@example.com": "oid-owner"} if users is None else users
        self.products: dict[str, dict] = {}
        self.assets: dict[str, str] = {}  # data map guid -> data asset id
        self.related: dict[str, set[str]] = {}
        self.calls: list[tuple[str, str]] = []

    def handle(self, request: httpx.Request) -> httpx.Response:
        url, method = request.url, request.method
        self.calls.append((method, url.path))
        if url.host == "login.microsoftonline.com":
            scope = dict(httpx.QueryParams(request.content.decode()))["scope"]
            return httpx.Response(200, json={"access_token": f"token:{scope}"})
        if url.host == "graph.microsoft.com":
            assert request.headers["Authorization"].endswith("graph.microsoft.com/.default")
            email = url.path.rsplit("/", 1)[-1]
            if email not in self.users:
                return httpx.Response(404, json={"error": {"code": "Request_ResourceNotFound"}})
            return httpx.Response(200, json={"id": self.users[email]})
        assert request.headers["Authorization"].endswith("purview.azure.net/.default")
        body = json.loads(request.content) if request.content else {}
        assert url.params["api-version"] == purview.API_VERSION
        path = url.path.removeprefix("/datagovernance/catalog")
        if path == "/businessdomains":
            return httpx.Response(
                200, json={"value": [{"name": n, "id": i} for n, i in self.domains.items()]}
            )
        if path == "/dataAssets/query":
            wanted = body["sourceAssetIds"]
            found = [
                {"id": a, "source": {"assetId": g}} for g, a in self.assets.items() if g in wanted
            ]
            return httpx.Response(200, json={"value": found})
        if path == "/dataAssets":
            asset_id = f"asset-{body['source']['assetId']}"
            self.assets[body["source"]["assetId"]] = asset_id
            return httpx.Response(201, json={"id": asset_id})
        if path == "/dataProducts" and method == "POST":
            self.products[body["id"]] = body
            return httpx.Response(201, json=body)
        if path.endswith("/relationships"):
            pid = path.split("/")[2]
            related = self.related.setdefault(pid, set())
            if method == "GET":
                return httpx.Response(200, json={"value": [{"entityId": e} for e in related]})
            if method == "POST":
                related.update(v["entityId"] for v in body.values())
                return httpx.Response(200, json={})
            related.discard(url.params["entityId"])
            return httpx.Response(204)
        pid = path.split("/")[2]
        if method == "GET":
            if pid not in self.products:
                return httpx.Response(404, json={"error": {"code": "NotFound"}})
            return httpx.Response(200, json=self.products[pid])
        if method == "PUT":
            self.products[pid] = body
            return httpx.Response(200, json=body)
        raise AssertionError(f"unexpected {method} {url}")


@pytest.fixture
def api(monkeypatch):
    fake = _Purview()
    real = httpx.AsyncClient
    monkeypatch.setattr(
        purview.httpx,
        "AsyncClient",
        lambda **kw: real(transport=httpx.MockTransport(fake.handle), **kw),
    )
    return fake


@pytest.fixture
def data_map(monkeypatch):
    """The Data Map publish: each table's guid, as the Atlas adapter returns it."""
    guids: dict[str, str] = {}

    async def _published(self, snapshot):
        result = PublishResult(provider_name=self.provider_name)
        for table in snapshot.tables:
            name = table.ref.parts[-1]
            if name in guids:
                result.bindings[table.semantic_uri] = (guids[name], "qn")
        return result

    monkeypatch.setattr(AtlasExport, "publish", _published)
    guids.update(orders="guid-orders", customers="guid-customers")
    return guids


def _export() -> PurviewExport:
    exporter = metadata_export(_target())
    assert isinstance(exporter, PurviewExport)
    return exporter


def test_a_purview_target_signs_in_with_entra():
    with pytest.raises(ValidationError, match="Microsoft Entra"):
        MetadataExportTarget(id="p", provider="purview", endpoint="https://x", auth_mode="bearer")


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        ("draft", "DRAFT"),
        ("live", "PUBLISHED"),
        ("deprecated", "PUBLISHED"),  # REQ-1634 STATUS: still in service
        ("retired", "EXPIRED"),
    ],
)
def test_the_status_is_the_catalogs(status: Any, expected: str):
    product = type("P", (), {"status": status})()
    assert catalog_status(product) == expected  # type: ignore[arg-type]


def test_the_update_frequency_is_the_shortest_members_freshness():
    tables = [type("T", (), {"cache_ttl": t})() for t in (86400, 3600, None, -1)]
    assert update_frequency(tables) == "Hourly"  # type: ignore[arg-type]
    assert update_frequency([type("T", (), {"cache_ttl": -1})()]) is None  # type: ignore[list-item]


async def test_a_product_is_created_in_its_domain_with_owners_and_assets(api, data_map):
    snapshot = _snapshot()
    result = await _export().publish(snapshot)
    assert result.ok, result.errors
    assert result.published["data_products"] == 1 and result.published["data_assets"] == 2
    (product,) = snapshot.data_products
    body = api.products[product_id(product)]
    assert body["name"] == "Sales 360" and body["domain"] == DOMAIN_ID
    assert body["contacts"]["owner"] == [{"id": "oid-owner", "description": "owner@example.com"}]
    assert body["status"] == "PUBLISHED" and body["updateFrequency"] == "Hourly"
    assert body["businessUse"] == "Join on customer_id"
    assert "Every sale" in body["description"] and "Limitations: No refunds" in body["description"]
    assert set(api.assets) == {"guid-orders", "guid-customers"}
    assert api.related[product_id(product)] == {"asset-guid-orders", "asset-guid-customers"}


async def test_a_second_publish_updates_in_place_and_drops_a_former_member(api, data_map):
    await _export().publish(_snapshot())
    del data_map["customers"]  # no longer in the Data Map's result: no longer an asset
    result = await _export().publish(_snapshot())
    (product,) = _snapshot().data_products
    assert ("PUT", f"/datagovernance/catalog/dataProducts/{product_id(product)}") in api.calls
    assert api.related[product_id(product)] == {"asset-guid-orders"}
    assert [e.message for e in result.errors] == [
        "pg.public.customers is not in the Purview Data Map, so it cannot be an asset of sales_360"
    ]


async def test_a_product_with_no_governance_domain_of_its_name_is_reported(api, data_map):
    api.domains = {}
    result = await _export().publish(_snapshot())
    assert [e.message for e in result.errors] == ["Purview has no governance domain named 'sales'"]
    assert api.products == {}


async def test_an_owner_entra_does_not_know_is_reported(api, data_map):
    result = await _export().publish(_snapshot(owners=("owner@example.com", "gone@example.com")))
    assert [e.message for e in result.errors] == [
        "owner gone@example.com is not a user of the Entra tenant"
    ]
    (body,) = api.products.values()
    assert [o["id"] for o in body["contacts"]["owner"]] == ["oid-owner"]
