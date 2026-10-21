# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Microsoft Purview Unified Catalog data products (REQ-2118).

A Purview target publishes in two steps. First the Data Map, through the Atlas adapter it extends:
Purview's ingestion API is the Atlas API, at the target's ``endpoint``, with Entra client
credentials. Each published table's Data Map guid comes back in the publish's bindings. Then the
Unified Catalog, at the tenant's own host (``https://{tenant}-api.purview-service.microsoft.com``):
each data product becomes a Unified Catalog data product in the governance domain named after the
product's Provisa domain, with its members' Data Map entities as its data assets.

- Owners are the owner role's holders, their addresses resolved to Entra object ids through
  Microsoft Graph. An address Graph does not know is reported.
- A product whose domain has no governance domain of that name is reported and not sent;
  governance domains are not created.
- Status: active is PUBLISHED, deprecated and retired are EXPIRED, any other is DRAFT; a product
  with no status is PUBLISHED when the product is published and DRAFT when not.
- The update frequency is the shortest freshness among its members.
- Approvers and access policies are not published yet.
"""

# Requirements: REQ-2118

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any

import httpx

from provisa.api.metadata_export.atlas import PURVIEW_SCOPE, AtlasExport
from provisa.api.metadata_export.provider import AssetError, PublishResult
from provisa.api.metadata_export.registry import register_provider

if TYPE_CHECKING:
    from provisa.api.metadata_export.model import DataProductAsset, MetadataSnapshot, TableAsset

API_VERSION = "2026-03-20-preview"
GRAPH = "https://graph.microsoft.com/v1.0"
GRAPH_SCOPE = "https://graph.microsoft.com/.default"
DATA_ASSET = "DATAASSET"

# Seconds of freshness to the Unified Catalog's update frequencies, shortest first.
_FREQUENCIES = (
    (3600, "Hourly"),
    (86400, "Daily"),
    (7 * 86400, "Weekly"),
    (31 * 86400, "Monthly"),
    (92 * 86400, "Quarterly"),
)


def catalog_status(product: "DataProductAsset") -> str:
    """The product's Unified Catalog status (see the module docstring)."""
    if product.status is None:
        return "PUBLISHED" if product.publish else "DRAFT"
    if product.status == "active":
        return "PUBLISHED"
    if product.status in ("deprecated", "retired"):
        return "EXPIRED"
    return "DRAFT"


def update_frequency(members: list["TableAsset"]) -> str | None:
    """The update frequency of the members' shortest freshness; None when none declares one or
    every member never expires (-1)."""
    ttls = [t.cache_ttl for t in members if t.cache_ttl is not None and t.cache_ttl > 0]
    if not ttls:
        return None
    shortest = min(ttls)
    return next((name for limit, name in _FREQUENCIES if shortest <= limit), "Yearly")


def product_id(product: "DataProductAsset") -> str:
    """The product's Unified Catalog id: a uuid of its Provisa URI, the same on every publish."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, product.semantic_uri))


def product_body(
    product: "DataProductAsset", domain_id: str, owners: dict[str, str], members: list
) -> dict[str, Any]:
    """The Unified Catalog data product for ``product``: its fields in the catalog's terms."""
    parts = [product.description]
    for heading, text in (
        ("Limitations", product.limitations),
        ("Service level", product.sla),
        ("Support", product.support),
        ("Support contact", product.support_contact),
        ("Version", product.version),
    ):
        if text:
            parts.append(f"{heading}: {text}")
    body: dict[str, Any] = {
        "id": product_id(product),
        "name": product.name,
        "domain": domain_id,
        "description": "\n\n".join(p for p in parts if p),
        "businessUse": product.usage,
        "status": catalog_status(product),
        "contacts": {"owner": [{"id": oid, "description": email} for email, oid in owners.items()]},
    }
    frequency = update_frequency(members)
    if frequency is not None:
        body["updateFrequency"] = frequency
    if product.documentation_url:
        body["documentation"] = [{"url": product.documentation_url, "name": "Provisa"}]
    return body


@register_provider
class PurviewExport(AtlasExport):  # REQ-2118
    """The Purview Data Map through Atlas, then each data product in the Unified Catalog."""

    provider_name = "purview"

    def _catalog(self, path: str) -> str:
        tenant = self._config.entra_tenant_id
        return f"https://{tenant}-api.purview-service.microsoft.com/datagovernance/catalog{path}"

    async def _entra_token(self, client: httpx.AsyncClient, scope: str) -> str:
        response = await client.post(
            f"https://login.microsoftonline.com/{self._config.entra_tenant_id}/oauth2/v2.0/token",
            data={
                "grant_type": "client_credentials",
                "client_id": self._config.entra_client_id,
                "client_secret": self._config.entra_client_secret.get_secret_value(),
                "scope": scope,
            },
        )
        response.raise_for_status()
        return response.json()["access_token"]

    async def publish(self, snapshot: "MetadataSnapshot") -> PublishResult:
        result = await super().publish(snapshot)
        if not snapshot.data_products:
            return result
        tables = {t.ref.fqn(): t for t in snapshot.tables}
        async with httpx.AsyncClient(timeout=self._config.timeout_seconds) as client:
            catalog = {"Authorization": f"Bearer {await self._entra_token(client, PURVIEW_SCOPE)}"}
            graph = {"Authorization": f"Bearer {await self._entra_token(client, GRAPH_SCOPE)}"}
            domains = await self._domains(client, catalog)
            for product in snapshot.data_products:
                try:
                    await self._publish_product(
                        client, catalog, graph, product, tables, domains, result
                    )
                except httpx.HTTPStatusError as exc:
                    result.errors.append(
                        AssetError(
                            asset=product.ref,
                            message=f"HTTP {exc.response.status_code}: {exc.response.text[:500]}",
                        )
                    )
                except httpx.HTTPError as exc:
                    result.errors.append(AssetError(asset=product.ref, message=str(exc)))
        return result

    async def _paged(self, client: httpx.AsyncClient, headers: dict, url: str, **params) -> list:
        """Every item of a paged Unified Catalog listing."""
        items: list = []
        response = await client.get(
            url, params={"api-version": API_VERSION, **params}, headers=headers
        )
        while True:
            response.raise_for_status()
            page = response.json()
            items.extend(page.get("value", []))
            if not page.get("nextLink"):
                return items
            response = await client.get(page["nextLink"], headers=headers)

    async def _domains(self, client: httpx.AsyncClient, headers: dict) -> dict[str, str]:
        """Each governance domain's id, by name."""
        listed = await self._paged(client, headers, self._catalog("/businessdomains"))
        return {d["name"]: d["id"] for d in listed}

    async def _owners(
        self, client: httpx.AsyncClient, headers: dict, product: "DataProductAsset", result
    ) -> dict[str, str]:
        """The product's owners' Entra object ids, by address; an unknown address is reported."""
        owners: dict[str, str] = {}
        for email in product.owner_emails:
            found = await client.get(
                f"{GRAPH}/users/{email}", params={"$select": "id"}, headers=headers
            )
            if found.status_code == 404:
                result.errors.append(
                    AssetError(
                        asset=product.ref,
                        message=f"owner {email} is not a user of the Entra tenant",
                    )
                )
                continue
            found.raise_for_status()
            owners[email] = found.json()["id"]
        return owners

    async def _publish_product(
        self,
        client: httpx.AsyncClient,
        catalog: dict,
        graph: dict,
        product: "DataProductAsset",
        tables: dict[str, "TableAsset"],
        domains: dict[str, str],
        result: PublishResult,
    ) -> None:
        domain_id = domains.get(product.domain_id)
        if domain_id is None:
            result.errors.append(
                AssetError(
                    asset=product.ref,
                    message=f"Purview has no governance domain named {product.domain_id!r}",
                )
            )
            return
        members = [tables[m.fqn()] for m in product.members if m.fqn() in tables]
        guids: dict[str, str] = {}
        for member in members:
            bound = result.bindings.get(member.semantic_uri)
            if bound is None:
                result.errors.append(
                    AssetError(
                        asset=member.ref,
                        message=f"{member.ref.fqn()} is not in the Purview Data Map, so it "
                        f"cannot be an asset of {product.id}",
                    )
                )
                continue
            guids[member.semantic_uri] = bound[0]
        owners = await self._owners(client, graph, product, result)
        params = {"api-version": API_VERSION}
        body = product_body(product, domain_id, owners, members)
        url = self._catalog(f"/dataProducts/{body['id']}")
        found = await client.get(url, params=params, headers=catalog)
        if found.status_code == 404:
            created = await client.post(
                self._catalog("/dataProducts"), params=params, json=body, headers=catalog
            )
            created.raise_for_status()
        else:
            found.raise_for_status()
            updated = await client.put(url, params=params, json=body, headers=catalog)
            updated.raise_for_status()
        assets = await self._data_assets(client, catalog, list(guids.values()))
        related = {
            r["entityId"]
            for r in await self._paged(
                client, catalog, f"{url}/relationships", entityType=DATA_ASSET
            )
        }
        for asset_id in sorted(set(assets.values()) - related):
            linked = await client.post(
                f"{url}/relationships",
                params={**params, "entityType": DATA_ASSET},
                json={asset_id: {"entityId": asset_id, "relationshipType": "Related"}},
                headers=catalog,
            )
            linked.raise_for_status()
        for asset_id in sorted(related - set(assets.values())):
            unlinked = await client.delete(
                f"{url}/relationships",
                params={**params, "entityType": DATA_ASSET, "entityId": asset_id},
                headers=catalog,
            )
            unlinked.raise_for_status()
        result.published["data_products"] = result.published.get("data_products", 0) + 1
        result.published["data_assets"] = result.published.get("data_assets", 0) + len(assets)

    async def _data_assets(
        self, client: httpx.AsyncClient, headers: dict, guids: list[str]
    ) -> dict[str, str]:
        """The Unified Catalog data asset of each Data Map guid, by guid: found, else created."""
        if not guids:
            return {}
        params = {"api-version": API_VERSION}
        queried = await client.post(
            self._catalog("/dataAssets/query"),
            params=params,
            json={"sourceAssetIds": guids},
            headers=headers,
        )
        queried.raise_for_status()
        assets = {a["source"]["assetId"]: a["id"] for a in queried.json().get("value", [])}
        for guid in guids:
            if guid in assets:
                continue
            created = await client.post(
                self._catalog("/dataAssets"),
                params=params,
                json={"source": {"assetId": guid}},
                headers=headers,
            )
            created.raise_for_status()
            assets[guid] = created.json()["id"]
        return assets
