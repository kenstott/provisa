# Copyright (c) 2026 Kenneth Stott
# Canary: afbe2803-a454-480d-a4cf-60cea56cd216
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Google Dataplex data products (REQ-2118).

Each of the org's data products becomes one Dataplex data product
(``projects/{project}/locations/{location}/dataProducts/{id}``): its name, its fields as prose, its
owners -- the owner role's holders, by email address -- who also approve access requests, and its
version and status as labels. Each member table is one of its data assets, by the BigQuery
object it is:

- a member on a BigQuery source is its own table, in the source's project (the source's
  ``database``);
- a member the BigQuery engine serves from a replica is the replica's export view, in the org's
  export dataset of the engine's project (REQ-1912), which the reconcile publishes
  (``BigQueryFederationRuntime.publish_replica_view``);
- any other member has no BigQuery object, and is reported by name.

Each asset carries its freshness (the member's ``cache_ttl``) as a label. Assets no longer members
are removed. Dataplex answers create, update and delete with a long-running operation, which is
followed to its end. Access groups are not published yet.
"""

# Requirements: REQ-2118

from __future__ import annotations

import asyncio
import hashlib
import re
import time
from typing import TYPE_CHECKING, Any

import httpx

from provisa.api.metadata_export.provider import AssetError, MetadataExport, PublishResult
from provisa.api.metadata_export.registry import register_provider
from provisa.api.metadata_export.reader import (
    AssetMetadata,
    CatalogAsset,
    CatalogReader,
    register_reader,
)

if TYPE_CHECKING:
    from provisa.api.metadata_export.model import (
        AssetRef,
        DataProductAsset,
        MetadataSnapshot,
        SourceAsset,
        TableAsset,
    )

SCOPE = "https://www.googleapis.com/auth/cloud-platform"
BIGQUERY = "//bigquery.googleapis.com"
_LABEL_DISALLOWED = re.compile(r"[^a-z0-9_-]")


def resource_id(text: str) -> str:
    """An RFC-1034 id from ``text``: lower case letters, digits and hyphens, starting with a
    letter, at most 63 characters; a longer one ends in a hash of the whole, so it stays unique."""
    slug = re.sub(r"[^a-z0-9-]+", "-", text.lower()).strip("-")
    if not slug or not slug[0].isalpha():
        slug = f"p-{slug}"
    if len(slug) <= 63:
        return slug
    digest = hashlib.sha256(text.encode()).hexdigest()[:8]
    return f"{slug[:54].rstrip('-')}-{digest}"


def asset_id(resource: str) -> str:
    """A data asset's id: its BigQuery dataset and table, ``.../datasets/D/tables/T``."""
    parts = resource.split("/")
    return resource_id(f"{parts[-3]}-{parts[-1]}")


def label(value: str) -> str:
    """``value`` in Dataplex's label alphabet."""
    return _LABEL_DISALLOWED.sub("_", value.lower())[:63]


def bigquery_engine_project() -> str | None:
    """The BigQuery engine's project when BigQuery is the configured engine, else None."""
    from urllib.parse import urlparse

    from provisa.federation.engine import configured_engine_url

    url = configured_engine_url()
    if not url or not url.startswith("bigquery://"):
        return None
    from provisa.federation.bigquery_runtime import BigQueryFederationRuntime

    return urlparse(url).hostname or BigQueryFederationRuntime(url=url).project


def member_resource(
    ref: "AssetRef",
    *,
    sources: dict[str, "SourceAsset"],
    tables: dict[str, "TableAsset"],
    engine_project: str | None,
    export_views: Any,
) -> str | None:
    """The BigQuery full resource name of a member table, or None when it has no BigQuery
    object (see the module docstring)."""
    from provisa.core.secrets import resolve_secrets

    source_id, schema_name, table_name = ref.parts
    table = tables.get(ref.fqn())
    if table is not None and table.is_view:
        return None
    source = sources.get(source_id)
    if source is not None and source.source_type == "bigquery" and source.database:
        project = resolve_secrets(source.database)
        return f"{BIGQUERY}/projects/{project}/datasets/{schema_name}/tables/{table_name}"
    view = export_views.get((source_id, schema_name, table_name))
    if engine_project is not None and view is not None:
        return f"{BIGQUERY}/projects/{engine_project}/datasets/{view[0]}/tables/{view[1]}"
    return None


def product_body(product: "DataProductAsset") -> dict[str, Any]:
    """The Dataplex data product for ``product``: its fields in Dataplex's terms."""
    parts = [product.description]
    for heading, text in (
        ("Limitations", product.limitations),
        ("Usage", product.usage),
        ("Service level", product.sla),
        ("Support", product.support),
        ("Support contact", product.support_contact),
        ("Documentation", product.documentation_url),
    ):
        if text:
            parts.append(f"{heading}: {text}")
    labels = {"provisa-product": label(product.id)}
    if product.version:
        labels["provisa-version"] = label(product.version)
    if product.status:
        labels["provisa-status"] = label(product.status)
    owners = list(product.owner_emails)
    return {
        "displayName": product.name,
        "description": "\n\n".join(p for p in parts if p),
        "ownerEmails": owners,
        "labels": labels,
        "accessApprovalConfig": {"approverEmails": owners},
    }


def picked_resource(entry_name: str) -> str:
    """The resource of a picked Dataplex entry, from its name: an entry of a BigQuery table is
    named ``.../entries/bigquery.googleapis.com/projects/<p>/datasets/<d>/tables/<t>``, the
    resource's own path (REQ-2122)."""
    _, _, resource = entry_name.partition("/entries/")
    if not resource:
        raise ValueError(f"{entry_name!r} is not the name of a Dataplex entry")
    return f"//{resource}"


@register_provider
class DataplexExport(MetadataExport):  # REQ-2118
    """Publishes each data product as a Google Dataplex data product with its members as assets."""

    provider_name = "dataplex"
    # REQ-1912: a replica-served member is published through its export view.
    needs_export_views = True

    def _parent(self) -> str:
        return f"projects/{self._config.gcp_project}/locations/{self._config.gcp_location}"

    def _url(self, path: str) -> str:
        return f"{self._config.endpoint.rstrip('/')}/v1/{path}"

    async def _token(self) -> str:
        """The bearer token: given (``bearer``), or minted from the service account's key."""
        if self._config.auth_mode != "google_service_account":
            return self._config.token.get_secret_value()
        import json

        from google.auth.transport.requests import Request
        from google.oauth2 import service_account

        info = json.loads(self._config.google_service_account_json.get_secret_value())
        credentials = service_account.Credentials.from_service_account_info(info, scopes=[SCOPE])
        await asyncio.to_thread(credentials.refresh, Request())
        return credentials.token

    async def _done(self, client: httpx.AsyncClient, response: httpx.Response) -> dict:
        """The finished long-running operation ``response`` started; an error it ended in, or an
        HTTP error, is raised."""
        response.raise_for_status()
        operation = response.json()
        deadline = time.monotonic() + self._config.timeout_seconds
        while not operation.get("done"):
            if time.monotonic() > deadline:
                raise TimeoutError(f"operation {operation.get('name')} did not finish")
            await asyncio.sleep(1)
            polled = await client.get(self._url(operation["name"]))
            polled.raise_for_status()
            operation = polled.json()
        if "error" in operation:
            raise RuntimeError(operation["error"].get("message") or str(operation["error"]))
        return operation

    async def publish(self, snapshot: "MetadataSnapshot") -> PublishResult:
        result = PublishResult(provider_name=self.provider_name)
        if not snapshot.data_products:
            return result
        sources = {s.id: s for s in snapshot.sources}
        tables = {t.ref.fqn(): t for t in snapshot.tables}
        engine_project = bigquery_engine_project()
        headers = {"Authorization": f"Bearer {await self._token()}"}
        async with httpx.AsyncClient(
            timeout=self._config.timeout_seconds, headers=headers
        ) as client:
            for product in snapshot.data_products:
                assets: dict[str, "TableAsset | None"] = {}
                for member in product.members:
                    table = tables.get(member.fqn())
                    picked = self.picked.get(table.semantic_uri) if table is not None else None
                    if picked is not None and table is not None:
                        # REQ-2122: the member is the BigQuery table a steward picked for it.
                        resource = picked_resource(picked[0])
                        result.bindings[table.semantic_uri] = picked
                    else:
                        resource = member_resource(
                            member,
                            sources=sources,
                            tables=tables,
                            engine_project=engine_project,
                            export_views=self.export_views,
                        )
                    if resource is None:
                        result.errors.append(
                            AssetError(
                                asset=member,
                                message=f"{member.fqn()} has no BigQuery table or view to "
                                f"publish as an asset of {product.id}",
                            )
                        )
                        continue
                    assets[resource] = tables.get(member.fqn())
                if not product.owner_emails:
                    result.errors.append(
                        AssetError(
                            asset=product.ref,
                            message=f"its owner role {product.owner.id if product.owner else None!r} "
                            "has no holder with an email address: Dataplex needs an owner",
                        )
                    )
                    continue
                if not assets:
                    result.errors.append(
                        AssetError(asset=product.ref, message="none of its members is in BigQuery")
                    )
                    continue
                try:
                    count = await self._publish_product(client, product, assets)
                except (httpx.HTTPError, RuntimeError, TimeoutError) as exc:
                    message = str(exc)
                    if isinstance(exc, httpx.HTTPStatusError):
                        message = f"HTTP {exc.response.status_code}: {exc.response.text[:500]}"
                    result.errors.append(AssetError(asset=product.ref, message=message))
                    continue
                result.published["data_products"] = result.published.get("data_products", 0) + 1
                result.published["data_assets"] = result.published.get("data_assets", 0) + count
        return result

    async def _publish_product(
        self,
        client: httpx.AsyncClient,
        product: "DataProductAsset",
        assets: dict[str, "TableAsset | None"],
    ) -> int:
        """Create or update the data product and converge its assets to ``assets``; the number of
        assets it has."""
        product_id = resource_id(product.id)
        name = f"{self._parent()}/dataProducts/{product_id}"
        body = product_body(product)
        found = await client.get(self._url(name))
        if found.status_code == 404:
            await self._done(
                client,
                await client.post(
                    self._url(f"{self._parent()}/dataProducts"),
                    params={"dataProductId": product_id},
                    json=body,
                ),
            )
        else:
            found.raise_for_status()
            etag = found.json().get("etag")
            await self._done(
                client,
                await client.patch(
                    self._url(name),
                    params={"updateMask": ",".join(body)},
                    json={**body, **({"etag": etag} if etag else {})},
                ),
            )
        listed = await client.get(self._url(f"{name}/dataAssets"))
        listed.raise_for_status()
        have = {a["resource"]: a["name"] for a in listed.json().get("dataAssets", [])}
        for resource, asset_name in have.items():
            if resource not in assets:
                await self._done(client, await client.delete(self._url(asset_name)))
        for resource, table in assets.items():
            labels = {"provisa-product": label(product.id)}
            if table is not None and table.cache_ttl is not None:
                # REQ-2118: the member's refresh cadence, from its freshness (-1: never expires).
                labels["provisa-freshness-seconds"] = label(str(table.cache_ttl))
            asset = {"resource": resource, "labels": labels}
            if resource in have:
                await self._done(
                    client,
                    await client.patch(
                        self._url(have[resource]), params={"updateMask": "labels"}, json=asset
                    ),
                )
            else:
                await self._done(
                    client,
                    await client.post(
                        self._url(f"{name}/dataAssets"),
                        params={"dataAssetId": asset_id(resource)},
                        json=asset,
                    ),
                )
        return len(assets)

    async def health(self) -> None:
        headers = {"Authorization": f"Bearer {await self._token()}"}
        async with httpx.AsyncClient(
            timeout=self._config.timeout_seconds, headers=headers
        ) as client:
            response = await client.get(
                self._url(f"{self._parent()}/dataProducts"), params={"pageSize": 1}
            )
        response.raise_for_status()


def _aspect(entry: dict[str, Any], suffix: str) -> dict[str, Any]:
    """The data of the entry's aspect whose key ends with ``suffix`` (keys are
    ``<project>.<location>.<aspect type>``, so the project part differs between projects)."""
    for key, aspect in (entry.get("aspects") or {}).items():
        if key.endswith(suffix):
            return aspect.get("data") or {}
    return {}


@register_reader
class DataplexReader(CatalogReader):  # REQ-2122
    """Reads the Dataplex Universal Catalog's BigQuery table entries: search through
    ``searchEntries``, and an entry's description, its schema aspect's column descriptions and
    its contacts aspect's people by the entry's name. Dataplex keeps no tags or glossary links on
    an entry in the sense the other catalogs do, so none are read."""

    provider_name = "dataplex"

    def _export(self) -> DataplexExport:
        return DataplexExport(self._target())

    @staticmethod
    def _asset(entry: dict[str, Any]) -> CatalogAsset:
        qualified = entry.get("fullyQualifiedName") or entry["name"]
        path = qualified.split(":", 1)[-1]
        source = entry.get("entrySource") or {}
        return CatalogAsset(
            ref=entry["name"],
            name=qualified,
            physical=tuple(path.split(".")),
            description=source.get("description") or "",
        )

    async def search(self, text: str, limit: int) -> list[CatalogAsset]:
        export = self._export()
        headers = {"Authorization": f"Bearer {await export._token()}"}
        async with httpx.AsyncClient(
            timeout=self._catalog.timeout_seconds, headers=headers
        ) as client:
            response = await client.post(
                export._url(f"projects/{self._catalog.gcp_project}/locations/global:searchEntries"),
                params={"query": text or "*", "pageSize": limit},
            )
        response.raise_for_status()
        # A table's entry type is ``.../entryTypes/bigquery-table`` and the like; datasets,
        # views' columns and the rest are not offered.
        return [
            self._asset(r["dataplexEntry"])
            for r in response.json().get("results") or []
            if str((r.get("dataplexEntry") or {}).get("entryType", "")).endswith("table")
        ]

    async def read(self, ref: str) -> AssetMetadata:
        export = self._export()
        headers = {"Authorization": f"Bearer {await export._token()}"}
        async with httpx.AsyncClient(
            timeout=self._catalog.timeout_seconds, headers=headers
        ) as client:
            response = await client.get(export._url(ref), params={"view": "ALL"})
        response.raise_for_status()
        entry = response.json()
        columns = {
            c["column"]: c.get("description") or ""
            for c in _aspect(entry, ".global.schema").get("columns") or []
        }
        contacts = _aspect(entry, ".global.contacts").get("identities") or []
        return AssetMetadata(
            asset=self._asset(entry),
            description=self._asset(entry).description,
            columns=columns,
            owners=tuple(
                c.get("name") or c.get("id") for c in contacts if c.get("name") or c.get("id")
            ),
        )
