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

The org's glossary publishes into a Dataplex business glossary Provisa owns, ``provisa-<org>``,
in the target's location (REQ-1387). Its terms sit in one category per role, Classes,
Properties and Concepts (REQ-2141), and carry the role as the ``provisa-role`` label; a
deprecated term carries ``provisa-deprecated``. A steward's own labels stand. SYNONYM_OF is a
synonym entry link and every other edge a related one: Dataplex's glossary has no hierarchy
between terms. A Class term is linked to the BigQuery entries of the tables it grounds and a
Property term to their columns, by definition entry links (REQ-1389); a table with no BigQuery
object, or whose entry does not appear in the target's location within a minute (Dataplex catalogs
a new BigQuery view some time after it is made), is reported by name. Provisa chooses
every entry link's id, and the ``provisa-`` prefix is the ownership test: Provisa creates and
deletes only its own links, and a term that left the snapshot keeps its entry and loses only
those.
"""

# Requirements: REQ-2118, REQ-1387, REQ-1389, REQ-2141

from __future__ import annotations

import asyncio
import hashlib
import re
import time
from typing import TYPE_CHECKING, Any

import httpx

from provisa.api.metadata_export.provider import (
    AssetError,
    AssetRefStub,
    MetadataExport,
    PublishResult,
)
from provisa.api.metadata_export.registry import register_provider

if TYPE_CHECKING:
    from provisa.api.metadata_export.model import (
        AssetRef,
        DataProductAsset,
        GlossaryTermAsset,
        MetadataSnapshot,
        SourceAsset,
        TableAsset,
    )

SCOPE = "https://www.googleapis.com/auth/cloud-platform"
LINK_TYPES = "projects/dataplex-types/locations/global/entryLinkTypes"
# A term's catalog entry is named by the project NUMBER, inside and out; Dataplex echoes a
# term's name with whatever project the request named, and an entry named by the project id is
# refused as "permission denied (or it may not exist)", to an owner too. Every Dataplex role
# carries resourcemanager.projects.get, so the number is read with the grants publishing needs.
RESOURCE_MANAGER = "https://cloudresourcemanager.googleapis.com/v3"
ROLE_CATEGORIES = {"class": "Classes", "property": "Properties", "concept": "Concepts"}
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


def _table_of(column: "AssetRef") -> "AssetRef":
    """The table a column ref belongs to."""
    from provisa.api.metadata_export.model import AssetKind, AssetRef

    return AssetRef(kind=AssetKind.TABLE, parts=column.parts[:-1])


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
    labels["provisa-status"] = label(product.status)
    owners = list(product.owner_emails)
    return {
        "displayName": product.name,
        "description": "\n\n".join(p for p in parts if p),
        "ownerEmails": owners,
        "labels": labels,
        "accessApprovalConfig": {"approverEmails": owners},
    }


@register_provider
class DataplexExport(MetadataExport):  # REQ-2118
    """Publishes each data product as a Google Dataplex data product with its members as assets."""

    provider_name = "dataplex"
    # REQ-1912: a replica-served member is published through its export view.
    needs_export_views = True
    # REQ-1389: how long a definition link waits for its BigQuery table's entry to appear.
    entry_wait_seconds = 60
    binds_products = False  # REQ-1634 STATUS: see MetadataExport.binds_products

    def _parent(self) -> str:
        return f"projects/{self._config.gcp_project}/locations/{self._config.gcp_location}"

    def _url(self, path: str) -> str:
        return f"{self._config.endpoint.rstrip('/')}/v1/{path}"

    async def _numbered_parent(self, client: httpx.AsyncClient) -> str:
        """``_parent`` with the project named by its number (RESOURCE_MANAGER)."""
        found = await client.get(f"{RESOURCE_MANAGER}/projects/{self._config.gcp_project}")
        found.raise_for_status()
        return f"{found.json()['name']}/locations/{self._config.gcp_location}"

    @staticmethod
    def _term_entry(numbered: str, term_name: str) -> str:
        """The catalog entry of the term ``term_name``, named under ``numbered``."""
        term = numbered + term_name[len("/".join(term_name.split("/")[:4])) :]
        return f"{numbered}/entryGroups/@dataplex/entries/{term}"

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
        if not snapshot.data_products and not snapshot.glossary_terms:
            return result
        sources = {s.id: s for s in snapshot.sources}
        tables = {t.ref.fqn(): t for t in snapshot.tables}
        engine_project = bigquery_engine_project()
        headers = {"Authorization": f"Bearer {await self._token()}"}
        async with httpx.AsyncClient(
            timeout=self._config.timeout_seconds, headers=headers
        ) as client:
            if snapshot.glossary_terms:
                try:
                    await self._publish_glossary(
                        client, snapshot, sources, tables, engine_project, result
                    )
                except (httpx.HTTPError, RuntimeError, TimeoutError) as exc:
                    message = str(exc)
                    if isinstance(exc, httpx.HTTPStatusError):
                        message = f"HTTP {exc.response.status_code}: {exc.response.text[:500]}"
                    result.errors.append(
                        AssetError(asset=AssetRefStub("glossary"), message=f"glossary: {message}")
                    )
            for product in snapshot.data_products:
                assets: dict[str, "TableAsset | None"] = {}
                for member in product.members:
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

    # --- REQ-1387: business glossary ----------------------------------------------------------

    async def _ensure(
        self, client: httpx.AsyncClient, name: str, create: str, id_param: str, body: dict
    ) -> dict:
        """The resource ``name``: read, else created under ``create`` with ``id_param``. A
        create that answers with a long-running operation is followed to its result."""
        found = await client.get(self._url(name))
        if found.status_code != 404:
            found.raise_for_status()
            return found.json()
        response = await client.post(
            self._url(create), params={id_param: name.rsplit("/", 1)[-1]}, json=body
        )
        response.raise_for_status()
        made = response.json()
        if "/operations/" in made.get("name", ""):
            return (await self._done(client, response))["response"]
        return made

    async def _publish_glossary(
        self,
        client: httpx.AsyncClient,
        snapshot: "MetadataSnapshot",
        sources: dict[str, "SourceAsset"],
        tables: dict[str, "TableAsset"],
        engine_project: str | None,
        result: PublishResult,
    ) -> None:
        """The org's glossary, its role categories, its terms and Provisa's entry links
        (module docstring)."""
        glossary = f"{self._parent()}/glossaries/{resource_id(f'provisa-{snapshot.org_id}')}"
        await self._ensure(
            client,
            glossary,
            f"{self._parent()}/glossaries",
            "glossary_id",
            {
                "displayName": "Provisa Glossary",
                "description": "Business vocabulary Provisa publishes from its governed model.",
            },
        )
        categories: dict[str, str] = {}
        for role in sorted({t.role for t in snapshot.glossary_terms}):
            name = f"{glossary}/categories/{role}"
            await self._ensure(
                client,
                name,
                f"{glossary}/categories",
                "category_id",
                {"displayName": ROLE_CATEGORIES[role], "parent": glossary},
            )
            categories[role] = name
        numbered = await self._numbered_parent(client)
        entries: dict[int, str] = {}  # term id -> its catalog entry
        for term in snapshot.glossary_terms:
            saved = await self._upsert_term(client, glossary, categories[term.role], term, result)
            if saved is not None:
                entries[term.term_id] = self._term_entry(numbered, saved)
        # A term's catalog entry appears some time after the term; until it does, Dataplex
        # answers a lookup of it 403 "(or it may not exist)", not 404.
        for term in snapshot.glossary_terms:
            entry = entries.get(term.term_id)
            if entry is not None and not await self._entry_appears(
                client, entry, absent=(403, 404)
            ):
                del entries[term.term_id]
                result.errors.append(
                    AssetError(
                        asset=AssetRefStub(term.semantic_uri),
                        message=f"term {term.name!r} is not linked: Dataplex had no catalog "
                        f"entry for it after {self.entry_wait_seconds} s",
                    )
                )
        # Provisa's entry links, by the id it gives each: (type, the two references).
        wanted: dict[str, tuple[str, list[dict[str, str]]]] = {}

        def _link(kind: str, refs: list[dict[str, str]]) -> None:
            key = "|".join([kind, *(f"{r['name']}#{r.get('path', '')}" for r in refs)])
            link_id = "provisa-" + hashlib.sha256(key.encode()).hexdigest()[:24]
            wanted[link_id] = (kind, refs)

        for edge in snapshot.glossary_edges:
            if edge.from_term_id in entries and edge.to_term_id in entries:
                kind = "synonym" if edge.rel_type == "SYNONYM_OF" else "related"
                _link(
                    kind,
                    [
                        {"name": entries[edge.from_term_id], "type": "UNSPECIFIED"},
                        {"name": entries[edge.to_term_id], "type": "UNSPECIFIED"},
                    ],
                )
        location = self._parent()
        for term in snapshot.glossary_terms:
            if term.term_id not in entries:
                continue
            for ref in term.assigned_to():
                table_ref = ref if term.role == "class" else _table_of(ref)
                resource = member_resource(
                    table_ref,
                    sources=sources,
                    tables=tables,
                    engine_project=engine_project,
                    export_views=self.export_views,
                )
                if resource is None:
                    result.errors.append(
                        AssetError(
                            asset=ref,
                            message=f"{ref.fqn()} has no BigQuery table or view, so term "
                            f"{term.name!r} is not linked to it",
                        )
                    )
                    continue
                source: dict[str, str] = {
                    "name": f"{location}/entryGroups/@bigquery/entries/"
                    f"{resource.removeprefix('//')}",
                    "type": "SOURCE",
                }
                if term.role == "property":
                    source["path"] = f"Schema.{ref.parts[-1]}"
                _link("definition", [source, {"name": entries[term.term_id], "type": "TARGET"}])
        # Every term in the Provisa glossary, so a term that left the snapshot loses its links too.
        live = await self._provisa_links(
            client, await self._glossary_entries(client, glossary, numbered)
        )
        present: dict[str, bool] = {}
        for link_id in sorted(set(wanted) - set(live)):
            kind, refs = wanted[link_id]
            if kind == "definition":
                asset = refs[0]["name"]
                if asset in present:
                    if not present[asset]:
                        continue  # reported with its first link
                else:
                    present[asset] = await self._entry_appears(client, asset)
                if not present[asset]:
                    result.errors.append(
                        AssetError(
                            asset=AssetRefStub(asset),
                            message=f"{asset.rsplit('/entries/', 1)[-1]} is not linked to its "
                            f"term: Dataplex had no entry for it after {self.entry_wait_seconds} s",
                        )
                    )
                    continue
            group = (
                refs[0]["name"].split("/entries/", 1)[0]
                if kind == "definition"
                else "/".join(refs[0]["name"].split("/")[:4]) + "/entryGroups/@dataplex"
            )
            response = await client.post(
                self._url(f"{group}/entryLinks"),
                params={"entry_link_id": link_id},
                json={"entryLinkType": f"{LINK_TYPES}/{kind}", "entryReferences": refs},
            )
            if response.status_code >= 400:
                result.errors.append(
                    AssetError(
                        asset=AssetRefStub(link_id),
                        message=f"{kind} link {' -> '.join(r['name'] for r in refs)}: HTTP "
                        f"{response.status_code}: {response.text[:300]}",
                    )
                )
                continue
            key = "term_assignment" if kind == "definition" else "term_relationship"
            result.published[key] = result.published.get(key, 0) + 1
        for link_id, name in sorted(live.items()):
            if link_id not in wanted:
                deleted = await client.delete(self._url(name))
                deleted.raise_for_status()

    async def _upsert_term(
        self,
        client: httpx.AsyncClient,
        glossary: str,
        category: str,
        term: "GlossaryTermAsset",
        result: PublishResult,
    ) -> str | None:
        """Create or update one term; its name, or None when refused (reported)."""
        name = f"{glossary}/terms/{resource_id(term.name)}"
        labels = {"provisa-role": term.role}
        if term.in_deprecation:
            labels["provisa-deprecated"] = "true"
        body = {
            "displayName": term.name,
            "description": term.definition or "",
            "parent": category,
        }
        found = await client.get(self._url(name))
        if found.status_code != 404 and found.status_code >= 400:
            # Written blind, the term would lose a steward's labels; refused by name, and the
            # other terms publish.
            result.errors.append(
                AssetError(
                    asset=AssetRefStub(term.semantic_uri),
                    message=f"term {term.name!r}: not written: the live read its steward "
                    f"labels are kept from failed (HTTP {found.status_code})",
                )
            )
            return None
        if found.status_code == 404:
            response = await client.post(
                self._url(f"{glossary}/terms"),
                params={"term_id": name.rsplit("/", 1)[-1]},
                json={**body, "labels": labels},
            )
        else:
            live = found.json()
            # Provisa's labels track the snapshot; a steward's stand.
            steward = {
                k: v for k, v in (live.get("labels") or {}).items() if not k.startswith("provisa-")
            }
            fields = ["displayName", "description", "labels"]
            if live.get("parent") != category:
                fields.append("parent")
            response = await client.patch(
                self._url(name),
                params={"updateMask": ",".join(fields)},
                json={**body, "labels": {**steward, **labels}},
            )
        if response.status_code >= 400:
            result.errors.append(
                AssetError(
                    asset=AssetRefStub(term.semantic_uri),
                    message=f"term {term.name!r}: HTTP {response.status_code}: "
                    f"{response.text[:300]}",
                )
            )
            return None
        result.published["glossary_term"] = result.published.get("glossary_term", 0) + 1
        return response.json()["name"]

    async def _entry_appears(
        self, client: httpx.AsyncClient, entry: str, absent: tuple[int, ...] = (404,)
    ) -> bool:
        """Whether the catalog has ``entry``, waited for up to ``entry_wait_seconds``: Dataplex
        catalogs a BigQuery table, or a glossary term, some time after it is created, so one
        just published may not be there yet. ``absent``: the statuses that mean not yet."""
        deadline = time.monotonic() + self.entry_wait_seconds
        while True:
            found = await client.get(
                self._url(f"{self._parent()}:lookupEntry"), params={"entry": entry}
            )
            if found.status_code not in absent:
                found.raise_for_status()
                return True
            if time.monotonic() >= deadline:
                return False
            await asyncio.sleep(2)

    async def _glossary_entries(
        self, client: httpx.AsyncClient, glossary: str, numbered: str
    ) -> list[str]:
        """The catalog entry of every term in the Provisa glossary."""
        entries: list[str] = []
        params: dict[str, str] = {}
        while True:
            response = await client.get(self._url(f"{glossary}/terms"), params=params)
            response.raise_for_status()
            page = response.json()
            for term in page.get("terms", []):
                entries.append(self._term_entry(numbered, term["name"]))
            if not page.get("nextPageToken"):
                return entries
            params["pageToken"] = page["nextPageToken"]

    async def _provisa_links(self, client: httpx.AsyncClient, entries: list[str]) -> dict[str, str]:
        """Every entry link Provisa made that references one of ``entries``: id -> name."""
        links: dict[str, str] = {}
        for entry in entries:
            params: dict[str, str] = {"entry": entry, "pageSize": "10"}
            while True:
                response = await client.get(
                    self._url(f"{self._parent()}:lookupEntryLinks"), params=params
                )
                response.raise_for_status()
                page = response.json()
                for link in page.get("entryLinks", []):
                    link_id = link["name"].rsplit("/", 1)[-1]
                    if link_id.startswith("provisa-"):
                        links[link_id] = link["name"]
                if not page.get("nextPageToken"):
                    break
                params["pageToken"] = page["nextPageToken"]
        return links

    async def health(self) -> None:
        headers = {"Authorization": f"Bearer {await self._token()}"}
        async with httpx.AsyncClient(
            timeout=self._config.timeout_seconds, headers=headers
        ) as client:
            response = await client.get(
                self._url(f"{self._parent()}/dataProducts"), params={"pageSize": 1}
            )
        response.raise_for_status()
