# Copyright (c) 2026 Kenneth Stott
# Canary: 7d3e9f21-4b6c-4a1e-9c5d-2f8b0a6e4d17
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1634: DataProductAsset publishes to OpenMetadata's native /api/v1/dataProducts."""

# Requirements: REQ-1634

from __future__ import annotations

import dataclasses

import httpx
import pytest

from provisa.api.metadata_export import build_snapshot
from provisa.api.metadata_export.model import AssetRef
from provisa.api.metadata_export.openmetadata import (
    OpenMetadataExport,
    _for_openmetadata,
    to_entities,
)
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

# OpenMetadata's FQN of the "orders" table (service.database.schema.table): what its table upsert
# returns, and the key a data product member and a data contract resolve the table's id by.
TABLE_FQN = "wh.default.public.orders"


def _config(*, owner: str | None = "alice") -> ProvisaConfig:
    return ProvisaConfig(
        sources=[Source(id="wh", type=SourceType.postgresql, description="Warehouse")],
        domains=[Domain(id="sales", description="Sales")],
        tables=[
            Table(
                source_id="wh",
                domain_id="sales",
                schema_name="public",
                table_name="orders",
                product_id="customer_360",
                columns=[Column(name="id", data_type="integer", visible_to=["analyst"])],
            )
        ],
        data_products=[
            DataProduct(
                id="customer_360",
                domain_id="sales",
                name="Customer 360",
                owner_role=owner,
                purpose="Unified customer view",
            )
        ],
        roles=[],
    )


def _export_config() -> MetadataExportTarget:
    return MetadataExportTarget(
        id="catalog",
        provider="openmetadata",
        endpoint="https://catalog.example/",
        timeout_seconds=5,
    )


def _mock_transport(monkeypatch):
    """Every PUT succeeds; tables/users echo a server-assigned id for later requests to resolve; a
    data product holds no assets until they are added."""
    puts: list[tuple[str, dict]] = []

    async def _put(self, url, json=None, headers=None, params=None):
        puts.append((url, json))
        if "/assets/" in url:
            return httpx.Response(
                200, json={"status": "success"}, request=httpx.Request("PUT", url)
            )
        body = dict(json)
        if url.endswith("/tables"):
            body["id"] = "table-uuid-1"
            body["fullyQualifiedName"] = TABLE_FQN
        elif url.endswith("/users"):
            body["id"] = f"user-uuid-{json['name']}"
        return httpx.Response(200, json=body, request=httpx.Request("PUT", url))

    async def _get(self, url, params=None, headers=None):
        if url.endswith("/dataProducts/name/customer_360/assets"):
            return httpx.Response(200, json={"data": []}, request=httpx.Request("GET", url))
        return httpx.Response(404, text="not found", request=httpx.Request("GET", url))

    monkeypatch.setattr(httpx.AsyncClient, "put", _put)
    monkeypatch.setattr(httpx.AsyncClient, "get", _get)
    return puts


def _data_product_put(puts) -> dict:
    return next(body for url, body in puts if url.endswith("/dataProducts"))


@pytest.mark.asyncio
async def test_data_product_publishes_with_resolved_member_and_owner(monkeypatch):
    snapshot = build_snapshot(_config(), org_id="acme", dialect="postgres", contexts={})
    puts = _mock_transport(monkeypatch)

    result = await OpenMetadataExport(_export_config()).publish(snapshot)

    assert result.ok, result.errors
    body = _data_product_put(puts)
    assert body["name"] == "customer_360"
    assert body["domains"] == ["sales"]
    # OpenMetadata 2 ignores a body's assets; the member is added by the id its upsert returned.
    assert "assets" not in body
    assert [b for u, b in puts if u.endswith("/dataProducts/customer_360/assets/add")] == [
        {"assets": [{"id": "table-uuid-1", "type": "table"}]}
    ]
    assert body["owners"] == [{"id": "user-uuid-alice", "type": "user"}]
    assert result.published["data_product"] == 1


@pytest.mark.asyncio
async def test_data_product_without_owner_publishes_with_no_owners_field(monkeypatch):
    snapshot = build_snapshot(_config(owner=None), org_id="acme", dialect="postgres", contexts={})
    puts = _mock_transport(monkeypatch)

    result = await OpenMetadataExport(_export_config()).publish(snapshot)

    assert result.ok, result.errors
    assert "owners" not in _data_product_put(puts)


@pytest.mark.asyncio
async def test_data_product_owner_is_upserted_as_a_user_even_without_a_stewarded_domain(
    monkeypatch,
):
    # alice is a data-product owner but not any domain's steward — the /api/v1/users upsert
    # must still happen, or owner resolution fails for every such product.
    snapshot = build_snapshot(_config(), org_id="acme", dialect="postgres", contexts={})
    puts = _mock_transport(monkeypatch)

    await OpenMetadataExport(_export_config()).publish(snapshot)

    assert any(url.endswith("/users") and body["name"] == "alice" for url, body in puts)


@pytest.mark.asyncio
async def test_data_product_reports_error_when_member_table_upsert_failed(monkeypatch):
    snapshot = build_snapshot(_config(), org_id="acme", dialect="postgres", contexts={})

    async def _put(self, url, json=None, headers=None, params=None):
        if url.endswith("/tables"):
            return httpx.Response(500, text="boom", request=httpx.Request("PUT", url))
        body = dict(json)
        if url.endswith("/users"):
            body["id"] = f"user-uuid-{json['name']}"
        return httpx.Response(200, json=body, request=httpx.Request("PUT", url))

    async def _get(self, url, params=None, headers=None):
        return httpx.Response(404, text="not found", request=httpx.Request("GET", url))

    monkeypatch.setattr(httpx.AsyncClient, "put", _put)
    monkeypatch.setattr(httpx.AsyncClient, "get", _get)

    result = await OpenMetadataExport(_export_config()).publish(snapshot)

    assert not result.ok
    messages = [e.message for e in result.errors]
    assert any("was not upserted, so it cannot be addressed" in m for m in messages)
    assert "data_product" not in result.published


@pytest.mark.asyncio
async def test_each_member_contract_is_imported_onto_its_table(monkeypatch):
    """REQ-2118: the member's ODCS contract goes through OpenMetadata's ODCS import, attached to
    the table by the id its own upsert returned, replacing whatever contract was there."""
    snapshot = build_snapshot(_config(), org_id="acme", dialect="postgres", contexts={})
    contracts: list[tuple[str, dict, dict]] = []
    puts = _mock_transport(monkeypatch)
    put = httpx.AsyncClient.put

    async def _put(self, url, json=None, headers=None, params=None):
        if url.endswith("/dataContracts/odcs"):
            contracts.append((url, json, params))
        return await put(self, url, json=json, headers=headers, params=params)

    monkeypatch.setattr(httpx.AsyncClient, "put", _put)

    result = await OpenMetadataExport(_export_config()).publish(snapshot)

    assert result.ok, result.errors
    ((_, body, params),) = contracts
    assert params == {"entityId": "table-uuid-1", "entityType": "table", "mode": "replace"}
    assert body["apiVersion"] == "v3.1.0"
    assert body["schema"][0]["name"] == "orders"
    # OpenMetadata reads ODCS 3.0's member array, not the 3.1 Team object.
    assert body["team"] == [{"name": "alice", "role": "ownerRole"}]
    assert result.published["data_contract"] == 1
    assert puts  # the tables went first


@pytest.mark.asyncio
async def test_a_contract_whose_table_failed_is_reported_not_sent(monkeypatch):
    snapshot = build_snapshot(_config(), org_id="acme", dialect="postgres", contexts={})
    sent: list[str] = []

    async def _put(self, url, json=None, headers=None, params=None):
        sent.append(url)
        if url.endswith("/tables"):
            return httpx.Response(500, text="boom", request=httpx.Request("PUT", url))
        body = dict(json)
        if url.endswith("/users"):
            body["id"] = f"user-uuid-{json['name']}"
        return httpx.Response(200, json=body, request=httpx.Request("PUT", url))

    async def _get(self, url, params=None, headers=None):
        return httpx.Response(404, text="not found", request=httpx.Request("GET", url))

    monkeypatch.setattr(httpx.AsyncClient, "put", _put)
    monkeypatch.setattr(httpx.AsyncClient, "get", _get)

    result = await OpenMetadataExport(_export_config()).publish(snapshot)

    assert not any(url.endswith("/dataContracts/odcs") for url in sent)
    assert any("has nothing to attach to" in e.message for e in result.errors)


def test_a_view_sits_under_a_service_and_database_of_its_own():
    """A view's derived-view source is no snapshot source; OpenMetadata resolves the view's schema
    through a service and database, so they are created for it before the schema."""
    snapshot = build_snapshot(_config(), org_id="acme", dialect="postgres", contexts={})
    (orders,) = snapshot.tables
    view_ref = AssetRef(orders.ref.kind, ("__derived__", "views", "order_totals"))
    snapshot.tables.append(
        dataclasses.replace(
            orders, ref=view_ref, name="order_totals", source_id="__derived__", is_view=True
        )
    )
    kinds = [(e.kind, e.body.get("name"), e.body.get("service")) for e in to_entities(snapshot)]
    service = kinds.index(("service", "__derived__", None))
    database = kinds.index(("database", "default", "__derived__"))
    schema = next(i for i, k in enumerate(kinds) if k[:2] == ("schema", "views"))
    assert service < database < schema


@pytest.mark.parametrize(
    "seconds, value, unit",
    [(3600, 1, "hour"), (86400, 1, "day"), (120, 2, "minute"), (90, 2, "minute")],
)
def test_a_latency_reaches_openmetadata_in_a_unit_it_takes(seconds, value, unit):
    """OpenMetadata's latency units are minute, hour and day; a part minute rounds up, still an
    upper bound on how far a read trails its source."""
    contract = {"slaProperties": [{"property": "latency", "value": seconds, "unit": "s"}]}
    (latency,) = _for_openmetadata(contract)["slaProperties"]
    assert (latency["value"], latency["unit"]) == (value, unit)


@pytest.mark.asyncio
async def test_a_held_asset_that_is_not_a_member_is_removed(monkeypatch):
    # REQ-1634: the product's assets are exactly its members.
    snapshot = build_snapshot(_config(), org_id="acme", dialect="postgres", contexts={})
    puts = _mock_transport(monkeypatch)

    async def _get(self, url, params=None, headers=None):
        held = [
            {"id": "table-uuid-1", "type": "table"},
            {"id": "table-uuid-gone", "type": "table"},
        ]
        return httpx.Response(200, json={"data": held}, request=httpx.Request("GET", url))

    monkeypatch.setattr(httpx.AsyncClient, "get", _get)

    result = await OpenMetadataExport(_export_config()).publish(snapshot)

    assert result.ok, result.errors
    calls = [(u.rsplit("/", 1)[-1], b) for u, b in puts if "/assets/" in u]
    assert calls == [("remove", {"assets": [{"id": "table-uuid-gone", "type": "table"}]})]
