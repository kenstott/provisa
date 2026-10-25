# Copyright (c) 2026 Kenneth Stott
# Canary: dae8b886-c07d-48bb-84ae-15d006e09345
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

The org's glossary also publishes into the Unified Catalog's business glossary (REQ-1387): each
term in the governance domain named after its Provisa domain (the first, alphabetically, that
Purview has; a term none of whose domains Purview has is reported), with an id derived from its
Provisa URI. Deprecated is EXPIRED, else PUBLISHED. A KIND_OF parent is the term's parent; a
term with several keeps the lowest-id one as parent and relates to the rest. SYNONYM_OF is a
Synonym relationship and every other edge a Related one. A Class term is related to the data
assets of the tables it grounds (REQ-1389); a Property term's columns have no Unified Catalog
id this adapter can resolve, so Property assignments reach Purview through the Data Map only.
The Unified Catalog term has no free property for the term's role, which the Data Map term
carries (REQ-2141). The service lists a term's relationships with no author, so each one Provisa
writes is recorded in its bindings, and that record is the ownership test: Provisa adds and
removes only its own, and a term that left the snapshot loses only those.
"""

# Requirements: REQ-2118, REQ-1387, REQ-1389, REQ-2141

from __future__ import annotations

import base64
import json
import uuid
from typing import TYPE_CHECKING, Any

import httpx

from provisa.api.metadata_export.atlas import PURVIEW_SCOPE, AtlasExport
from provisa.api.metadata_export.provider import AssetError, AssetRefStub, PublishResult
from provisa.api.metadata_export.registry import register_provider

if TYPE_CHECKING:
    from provisa.api.metadata_export.model import (
        DataProductAsset,
        GlossaryTermAsset,
        MetadataSnapshot,
        TableAsset,
    )

API_VERSION = "2026-03-20-preview"
GRAPH = "https://graph.microsoft.com/v1.0"
GRAPH_SCOPE = "https://graph.microsoft.com/.default"
DATA_ASSET = "DATAASSET"
TERM = "TERM"

# Seconds of freshness to the Unified Catalog's update frequencies, shortest first.
_FREQUENCIES = (
    (3600, "Hourly"),
    (86400, "Daily"),
    (7 * 86400, "Weekly"),
    (31 * 86400, "Monthly"),
    (92 * 86400, "Quarterly"),
)


def _relationship_key(term: str, category: str, entity_id: str, kind: str) -> str:
    """The binding that records a relationship Provisa wrote on Unified Catalog term ``term``
    (REQ-1389): the service lists a relationship with no author, so this record is what tells
    Provisa's from a person's. Outside the org's term URIs, which the Data Map pass reads as
    its terms."""
    return f"purview:relationship/{term}/{category}/{kind}/{entity_id}"


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


def term_id(term: "GlossaryTermAsset") -> str:
    """The id a term's create proposes: a uuid of its Provisa URI. The service assigns its own
    and answers with it (run 40107606920), which is the id every later call uses; a term is
    found again by name in its domain."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, term.semantic_uri))


def token_oid(token: str) -> str:
    """The object id of the principal an Entra access token was issued to: Provisa's own
    identity in the catalog, which ``systemData.createdBy`` names on what it wrote."""
    payload = token.split(".")[1]
    claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
    return claims["oid"]


def term_edges(
    terms: list["GlossaryTermAsset"], edges: list
) -> tuple[dict[int, int], dict[int, set[tuple[int, str]]]]:
    """Each term's KIND_OF parent (its lowest-id one) and its relationships to other terms, as
    (other term id, Synonym|Related); a KIND_OF edge that is not the parent is Related."""
    published = {t.term_id for t in terms}
    parent: dict[int, int] = {}
    related: dict[int, set[tuple[int, str]]] = {}
    for edge in sorted(edges, key=lambda e: (e.from_term_id, e.to_term_id)):
        if edge.from_term_id not in published or edge.to_term_id not in published:
            continue
        if edge.rel_type == "KIND_OF" and edge.from_term_id not in parent:
            parent[edge.from_term_id] = edge.to_term_id
            continue
        kind = "Synonym" if edge.rel_type == "SYNONYM_OF" else "Related"
        related.setdefault(edge.from_term_id, set()).add((edge.to_term_id, kind))
    # A cycle cannot be a hierarchy: the term closing it relates to its parent instead.
    for start in sorted(parent):
        seen: set[int] = set()
        cursor = start
        while cursor in parent:
            if cursor in seen:
                related.setdefault(cursor, set()).add((parent.pop(cursor), "Related"))
                break
            seen.add(cursor)
            cursor = parent[cursor]
    return parent, related


def _asset_name(table: "TableAsset") -> str:
    """A table's Data Map entity name (atlas.to_native_entities): its business name."""
    return table.aliases[0] if table.aliases else table.name


def product_id(product: "DataProductAsset") -> str:
    """The id a product's create proposes: a uuid of its Provisa URI. As with a term, the
    service answers with the id it uses, and a product is found again by name in its domain."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, product.semantic_uri))


def product_body(
    product: "DataProductAsset", domain_id: str, owners: dict[str, str], members: list
) -> dict[str, Any]:
    """The Unified Catalog data product for ``product``: its fields in the catalog's terms."""
    parts = [product.description]
    for heading, text in (
        ("Limitations", product.limitations),
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
        # The service requires a type (400, "Property \"type\" is required."), though the
        # published schema marks it optional. A Provisa data product is curated, governed data
        # for analysis: Analytical, of the catalog's data product types.
        "type": "Analytical",
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
        if not snapshot.data_products and not snapshot.glossary_terms:
            return result
        tables = {t.ref.fqn(): t for t in snapshot.tables}
        async with httpx.AsyncClient(timeout=self._config.timeout_seconds) as client:
            token = await self._entra_token(client, PURVIEW_SCOPE)
            catalog = {"Authorization": f"Bearer {token}"}
            domains = await self._domains(client, catalog)
            try:
                await self._publish_terms(
                    client, catalog, token_oid(token), snapshot, domains, result
                )
            except httpx.HTTPStatusError as exc:
                result.errors.append(
                    AssetError(
                        asset=AssetRefStub("glossary"),
                        message=(
                            f"Unified Catalog glossary: {exc.request.method} "
                            f"{exc.request.url.path}: HTTP {exc.response.status_code}: "
                            f"{exc.response.text[:500]}"
                        ),
                    )
                )
            except httpx.HTTPError as exc:
                result.errors.append(
                    AssetError(
                        asset=AssetRefStub("glossary"), message=f"Unified Catalog glossary: {exc}"
                    )
                )
            if not snapshot.data_products:
                return result
            graph = {"Authorization": f"Bearer {await self._entra_token(client, GRAPH_SCOPE)}"}
            for product in snapshot.data_products:
                try:
                    await self._publish_product(
                        client, catalog, graph, token_oid(token), product, tables, domains, result
                    )
                except httpx.HTTPStatusError as exc:
                    result.errors.append(
                        AssetError(
                            asset=product.ref,
                            message=f"{exc.request.method} {exc.request.url.path}: HTTP "
                            f"{exc.response.status_code}: {exc.response.text[:500]}",
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
        author: str,
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
        named = [
            p
            for p in await self._paged(
                client, catalog, self._catalog("/dataProducts"), domainId=domain_id
            )
            if p["name"] == body["name"]
        ]
        if not named:
            created = await client.post(
                self._catalog("/dataProducts"), params=params, json=body, headers=catalog
            )
            created.raise_for_status()
            url = self._catalog(f"/dataProducts/{created.json()['id']}")
        elif any((p.get("systemData") or {}).get("createdBy") != author for p in named):
            result.errors.append(
                AssetError(
                    asset=product.ref,
                    message="not written: its domain holds a data product of that name "
                    "Provisa did not make",
                )
            )
            return
        else:
            (live,) = named
            url = self._catalog(f"/dataProducts/{live['id']}")
            updated = await client.put(
                url, params=params, json={**body, "id": live["id"]}, headers=catalog
            )
            updated.raise_for_status()
        by_uri = {m.semantic_uri: m for m in members}
        assets = await self._data_assets(
            client, catalog, {guid: by_uri[uri] for uri, guid in guids.items()}
        )
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
                json={"entityId": asset_id, "relationshipType": "Related"},
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
        self, client: httpx.AsyncClient, headers: dict, tables: dict[str, "TableAsset"]
    ) -> dict[str, str]:
        """The Unified Catalog data asset of each Data Map guid in ``tables`` (guid -> the table it
        is), by guid: found, else created.

        A create names the asset and its type: the service refuses one without them (400,
        "Required property 'name'/'type' not found"), though the published request schema lists
        only ``source``. The name is the Data Map entity's own; the type is General -- not an
        Azure SQL table or an ADLS path."""
        guids = sorted(tables)
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
                json={
                    "name": _asset_name(tables[guid]),
                    "type": "General",
                    # The asset is the Data Map entity's: its source names the Data Map, as the
                    # service answers it.
                    "source": {"type": "DataMap", "assetId": guid},
                },
                headers=headers,
            )
            created.raise_for_status()
            assets[guid] = created.json()["id"]
        return assets

    async def _publish_terms(
        self,
        client: httpx.AsyncClient,
        catalog: dict,
        author: str,
        snapshot: "MetadataSnapshot",
        domains: dict[str, str],
        result: PublishResult,
    ) -> None:
        """The org's glossary in the Unified Catalog's business glossary (module docstring)."""
        params = {"api-version": API_VERSION}
        placed: dict[int, str] = {}  # term id -> its governance domain's id
        for term in snapshot.glossary_terms:
            domain_id = next((domains[d] for d in term.domains if d in domains), None)
            if domain_id is None:
                result.errors.append(
                    AssetError(
                        asset=AssetRefStub(term.semantic_uri),
                        message=(
                            f"term {term.name!r}: Purview has no governance domain named "
                            f"{' or '.join(map(repr, term.domains)) or 'for it (it has no domain)'}"
                        ),
                    )
                )
                continue
            placed[term.term_id] = domain_id
        terms = [t for t in snapshot.glossary_terms if t.term_id in placed]
        parent, related = term_edges(terms, snapshot.glossary_edges)

        def _depth(tid: int) -> int:
            depth = 0
            while tid in parent:
                tid = parent[tid]
                depth += 1
            return depth

        # The service gives a term an id of its own, whatever the create sends, so Provisa's terms
        # are found by name in their domain -- and adopted only when Provisa's principal made
        # them. A domain whose terms cannot be listed has none of its terms written.
        live_terms: dict[tuple[str, str], dict[str, Any]] = {}
        unlisted: set[str] = set()
        for domain_id in sorted(set(placed.values())):
            try:
                listed = await self._paged(
                    client, catalog, self._catalog("/terms"), domainId=domain_id
                )
            except httpx.HTTPStatusError as exc:
                unlisted.add(domain_id)
                result.errors.append(
                    AssetError(
                        asset=AssetRefStub(f"domain:{domain_id}"),
                        message=f"its terms were not written: the read of the domain's terms "
                        f"failed (HTTP {exc.response.status_code})",
                    )
                )
                continue
            for live in listed:
                live_terms[(domain_id, live["name"])] = live
        ids: dict[int, str] = {}  # Provisa term id -> its Unified Catalog id
        published: set[str] = set()
        for term in sorted(terms, key=lambda t: (_depth(t.term_id), t.term_id)):
            domain_id = placed[term.term_id]
            if domain_id in unlisted:
                continue
            body: dict[str, Any] = {
                "name": term.name,
                "domain": domain_id,
                "description": term.definition or "",
                "status": "EXPIRED" if term.in_deprecation else "PUBLISHED",
            }
            if term.term_id in parent and parent[term.term_id] in ids:
                body["parentId"] = ids[parent[term.term_id]]
            live = live_terms.get((domain_id, term.name))
            if live is None:
                response = await client.post(
                    self._catalog("/terms"),
                    params=params,
                    json={"id": term_id(term), **body},
                    headers=catalog,
                )
            elif (live.get("systemData") or {}).get("createdBy") != author:
                result.errors.append(
                    AssetError(
                        asset=AssetRefStub(term.semantic_uri),
                        message=f"term {term.name!r}: not written: its domain holds a term of "
                        "that name Provisa did not make",
                    )
                )
                continue
            else:
                # A steward's contacts, acronyms, resources and attributes stand.
                current = {k: v for k, v in live.items() if k != "parentId"}
                response = await client.put(
                    self._catalog(f"/terms/{live['id']}"),
                    params=params,
                    json={**current, **body, "id": live["id"]},
                    headers=catalog,
                )
            if response.status_code >= 400:
                result.errors.append(
                    AssetError(
                        asset=AssetRefStub(term.semantic_uri),
                        message=f"term {term.name!r}: HTTP {response.status_code}: "
                        f"{response.text[:300]}",
                    )
                )
                continue
            ids[term.term_id] = response.json()["id"]
            published.add(ids[term.term_id])
            # Counted apart from the Data Map's terms, which the Atlas pass counts as glossary_term.
            result.published["unified_catalog_term"] = (
                result.published.get("unified_catalog_term", 0) + 1
            )
        # The data assets of the tables a Class term grounds -- the only ones a term is related to.
        grounded = {ref.fqn() for t in terms if t.role == "class" for ref in t.assigned_to()}
        guids = {
            t.ref.fqn(): bound[0]
            for t in snapshot.tables
            if t.ref.fqn() in grounded
            and (bound := result.bindings.get(t.semantic_uri)) is not None
        }
        by_fqn = {t.ref.fqn(): t for t in snapshot.tables}
        assets = await self._data_assets(
            client, catalog, {guid: by_fqn[fqn] for fqn, guid in guids.items()}
        )
        for term in terms:
            if term.term_id not in ids:
                continue
            tid = ids[term.term_id]
            wanted_terms = {
                (ids[other], kind)
                for other, kind in related.get(term.term_id, set())
                if other in ids
            }
            await self._sync_relationships(client, catalog, tid, TERM, wanted_terms, result)
            wanted_assets = {
                (assets[guids[ref.fqn()]], "Related")
                for ref in term.assigned_to()
                if term.role == "class" and ref.fqn() in guids
            }
            await self._sync_relationships(client, catalog, tid, DATA_ASSET, wanted_assets, result)
        # A term this publish no longer carries keeps its catalog entry, and loses Provisa's
        # relationships (REQ-1389).
        for domain_id in sorted(set(domains.values())):
            for live in await self._paged(
                client, catalog, self._catalog("/terms"), domainId=domain_id
            ):
                created_by = (live.get("systemData") or {}).get("createdBy")
                if created_by != author or live["id"] in published:
                    continue
                for category in (TERM, DATA_ASSET):
                    await self._sync_relationships(
                        client, catalog, live["id"], category, set(), result
                    )

    async def _sync_relationships(
        self,
        client: httpx.AsyncClient,
        catalog: dict,
        term: str,
        category: str,
        wanted: set[tuple[str, str]],
        result: PublishResult,
    ) -> None:
        """Make Provisa's relationships of one category on one term exactly ``wanted``
        ((entity id, relationship type) pairs). The service lists a relationship with no
        author, so Provisa's are those its publishes recorded writing (``_relationship_key``);
        one with no record is a person's: it is never removed, and Provisa adds none beside it."""
        url = self._catalog(f"/terms/{term}/relationships")
        live = {
            (r["entityId"], r["relationshipType"])
            for r in await self._paged(client, catalog, url, entityType=category)
        }
        ours = {pair for pair in live if _relationship_key(term, category, *pair) in self._bindings}
        held = live - ours
        for pair in sorted(wanted & ours):
            result.bindings[_relationship_key(term, category, *pair)] = pair
        params = {"api-version": API_VERSION, "entityType": category}
        for entity_id, kind in sorted(wanted - ours - held):
            added = await client.post(
                url,
                params=params,
                json={"entityId": entity_id, "relationshipType": kind},
                headers=catalog,
            )
            added.raise_for_status()
            result.bindings[_relationship_key(term, category, entity_id, kind)] = (
                entity_id,
                kind,
            )
            key = (
                "unified_catalog_term_assignment"
                if category == DATA_ASSET
                else "unified_catalog_term_relationship"
            )
            result.published[key] = result.published.get(key, 0) + 1
        for entity_id, kind in sorted(ours - wanted):
            removed = await client.delete(
                url,
                params={**params, "entityId": entity_id, "relationshipType": kind},
                headers=catalog,
            )
            removed.raise_for_status()
