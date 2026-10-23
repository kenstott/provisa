# Copyright (c) 2026 Kenneth Stott
# Canary: bb6a5df2-2e68-4cd6-81d4-096192ea7287
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Amazon DataZone (SageMaker Catalog) data products (REQ-2118).

Each data product becomes one DataZone data product in the target's owning project, which is
its owner, published so that consumers find it and request it by subscription. Its members are
assets of a Provisa table asset type, which this target creates in the domain the first time:

- the ``ProvisaTableForm`` form type: the member's Provisa URI, its address in Provisa, its
  columns and its freshness;
- the ``ProvisaTable`` asset type, which carries that form;
- the ``ProvisaDataProductForm`` form type: the product's fields (purpose, limitations, usage,
  version, status, service level, support, documentation).

A member is read through Provisa, the one path that enforces its governance, so DataZone grants
nothing itself: the producer project approves each subscription.

Each asset's and product's DataZone id is recorded in the target's bindings (REQ-1389), and a
later publish revises it by that id. Only what no earlier publish recorded is looked for with
the project's search (an asset by its external identifier, the member's Provisa URI; a product
by its name), which DataZone indexes some seconds late. An entity deleted in DataZone since it
was recorded is published anew.

The org's glossary publishes into one DataZone business glossary Provisa owns, named
``Provisa glossary (<org>)`` in the owning project (REQ-1387). Each term carries its definition
as the long description; a deprecated term is DISABLED. KIND_OF is DataZone's own ``isA``
relation; DataZone's glossary has no synonym or related-term relation, so the other edges have
no native place here. Terms are revised by the ids recorded in the bindings, else found by name
in the Provisa glossary, and never deleted. DataZone records no author on a term's assignment
to an asset, so Provisa's assignments could not be told from a steward's: terms are not
assigned to assets on this target (REQ-1389). The glossary term has no free property, so the
term's role is not exported here either (REQ-2141).
"""

# Requirements: REQ-2118, REQ-1387, REQ-1389, REQ-2141

from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING, Any

from provisa.api.metadata_export.provider import (
    AssetError,
    AssetRefStub,
    MetadataExport,
    PublishResult,
)
from provisa.api.metadata_export.registry import register_provider

if TYPE_CHECKING:
    from provisa.api.metadata_export.model import DataProductAsset, MetadataSnapshot, TableAsset

TABLE_FORM = "ProvisaTableForm"
TABLE_TYPE = "ProvisaTable"
PRODUCT_FORM = "ProvisaDataProductForm"

TABLE_FORM_MODEL = f"""
structure {TABLE_FORM} {{
    provisaUri: String
    address: String
    columns: String
    freshnessSeconds: Integer
}}
"""

PRODUCT_FORM_MODEL = f"""
structure {PRODUCT_FORM} {{
    purpose: String
    limitations: String
    usage: String
    version: String
    status: String
    serviceLevel: String
    support: String
    supportContact: String
    documentation: String
}}
"""


def table_form(table: "TableAsset") -> dict[str, Any]:
    """The member's ``ProvisaTableForm``."""
    form: dict[str, Any] = {
        "provisaUri": table.semantic_uri,
        "address": table.ref.fqn(),
        "columns": json.dumps([{"name": c.name, "type": c.data_type} for c in table.columns]),
    }
    if table.cache_ttl is not None:
        form["freshnessSeconds"] = table.cache_ttl
    return form


def product_form(product: "DataProductAsset") -> dict[str, Any]:
    """The product's ``ProvisaDataProductForm``: each field it has."""
    fields = {
        "purpose": product.description,
        "limitations": product.limitations,
        "usage": product.usage,
        "version": product.version,
        "status": product.status,
        "serviceLevel": product.sla,
        "support": product.support,
        "supportContact": product.support_contact,
        "documentation": product.documentation_url,
    }
    return {k: v for k, v in fields.items() if v}


@register_provider
class DataZoneExport(MetadataExport):  # REQ-2118
    """Publishes each data product as an Amazon DataZone data product over Provisa table assets."""

    provider_name = "datazone"

    def _client(self) -> Any:
        import boto3
        from botocore.config import Config

        return boto3.client(
            "datazone",
            region_name=self._config.aws_region,
            aws_access_key_id=self._config.aws_access_key_id.get_secret_value(),
            aws_secret_access_key=self._config.aws_secret_access_key.get_secret_value(),
            config=Config(
                connect_timeout=self._config.timeout_seconds,
                read_timeout=self._config.timeout_seconds,
            ),
        )

    async def publish(self, snapshot: "MetadataSnapshot") -> PublishResult:
        if not snapshot.data_products and not snapshot.glossary_terms:
            return PublishResult(provider_name=self.provider_name)
        return await asyncio.to_thread(self._publish, snapshot)

    def _publish(self, snapshot: "MetadataSnapshot") -> PublishResult:
        from botocore.exceptions import BotoCoreError, ClientError

        result = PublishResult(provider_name=self.provider_name)
        client = self._client()
        if snapshot.glossary_terms:
            try:
                self._publish_glossary(client, snapshot, result)
            except (ClientError, BotoCoreError) as exc:
                result.errors.append(AssetError(asset=AssetRefStub("glossary"), message=str(exc)))
        if not snapshot.data_products:
            return result
        tables = {t.ref.fqn(): t for t in snapshot.tables}
        types = self._types(client)
        assets, products = self._known(client, snapshot, tables)
        for product in snapshot.data_products:
            try:
                count = self._publish_product(
                    client, product, tables, types, assets, products, result
                )
            except (ClientError, BotoCoreError) as exc:
                result.errors.append(AssetError(asset=product.ref, message=str(exc)))
                continue
            result.published["data_products"] = result.published.get("data_products", 0) + 1
            result.published["data_assets"] = result.published.get("data_assets", 0) + count
        return result

    def _known(
        self, client: Any, snapshot: "MetadataSnapshot", tables: dict[str, "TableAsset"]
    ) -> tuple[dict[str, str], dict[str, str]]:
        """The DataZone id of each member asset and each product already published, by Provisa
        URI: the id an earlier publish recorded in the target's bindings (REQ-1389), else -- the
        first publish of it -- what the project's search finds under the member's URI or the
        product's name. The search is eventually consistent, so a bound id is never searched."""
        bound = self.stored_bindings
        members = {tables[m.fqn()].semantic_uri for p in snapshot.data_products for m in p.members}
        assets = {uri: bound[uri][0] for uri in members if uri in bound}
        products = {
            p.semantic_uri: bound[p.semantic_uri][0]
            for p in snapshot.data_products
            if p.semantic_uri in bound
        }
        if members - set(assets):
            found = self._existing(client, "ASSET", "assetItem", "externalIdentifier", "identifier")
            assets.update({uri: found[uri] for uri in members - set(assets) if uri in found})
        if any(p.semantic_uri not in products for p in snapshot.data_products):
            found = self._existing(client, "DATA_PRODUCT", "dataProductItem", "name", "id")
            for p in snapshot.data_products:
                if p.semantic_uri not in products and p.name in found:
                    products[p.semantic_uri] = found[p.name]
        return assets, products

    @staticmethod
    def _revise(client: Any, known: str | None, revise: Any, create: Any) -> dict:
        """A revision of the entity ``known`` names, or a new one: none is known, or the known
        one was deleted in DataZone since it was recorded."""
        if known is not None:
            try:
                return revise(known)
            except client.exceptions.ResourceNotFoundException:
                pass  # deleted in DataZone since it was bound: published anew below
        return create()

    def _scope(self) -> dict[str, str]:
        return {"domainIdentifier": self._config.datazone_domain_id}

    def _form_revision(self, client: Any, name: str, model: str) -> str:
        """The latest revision of the form type ``name``, created from ``model`` when absent."""
        try:
            return client.get_form_type(**self._scope(), formTypeIdentifier=name)["revision"]
        except client.exceptions.ResourceNotFoundException:
            created = client.create_form_type(
                **self._scope(),
                name=name,
                model={"smithy": model},
                owningProjectIdentifier=self._config.datazone_project_id,
                status="ENABLED",
            )
            return created["revision"]

    def _types(self, client: Any) -> dict[str, str]:
        """The Provisa form and asset types' revisions, each created when absent."""
        table_form_rev = self._form_revision(client, TABLE_FORM, TABLE_FORM_MODEL)
        product_form_rev = self._form_revision(client, PRODUCT_FORM, PRODUCT_FORM_MODEL)
        try:
            table_type_rev = client.get_asset_type(**self._scope(), identifier=TABLE_TYPE)[
                "revision"
            ]
        except client.exceptions.ResourceNotFoundException:
            table_type_rev = client.create_asset_type(
                **self._scope(),
                name=TABLE_TYPE,
                description="A table governed by Provisa, read through Provisa.",
                formsInput={
                    TABLE_FORM: {
                        "typeIdentifier": TABLE_FORM,
                        "typeRevision": table_form_rev,
                        "required": True,
                    }
                },
                owningProjectIdentifier=self._config.datazone_project_id,
            )["revision"]
        return {
            TABLE_FORM: table_form_rev,
            PRODUCT_FORM: product_form_rev,
            TABLE_TYPE: table_type_rev,
        }

    def _existing(self, client: Any, scope: str, item: str, key: str, value: str) -> dict:
        """The project's assets or data products: ``value`` by ``key``."""
        pages = client.get_paginator("search").paginate(
            **self._scope(),
            owningProjectIdentifier=self._config.datazone_project_id,
            searchScope=scope,
        )
        found: dict[str, str] = {}
        for page in pages:
            for hit in page.get("items", []):
                entry = hit.get(item)
                if entry and entry.get(key):
                    found[entry[key]] = entry[value]
        return found

    def _publish_product(
        self,
        client: Any,
        product: "DataProductAsset",
        tables: dict[str, "TableAsset"],
        types: dict[str, str],
        assets: dict[str, str],
        products: dict[str, str],
        result: PublishResult,
    ) -> int:
        """Upsert and publish each member's asset, then the product over them; the number of
        assets it has."""
        items = []
        for ref in product.members:
            table = tables[ref.fqn()]
            forms = [
                {
                    "formName": TABLE_FORM,
                    "typeIdentifier": TABLE_FORM,
                    "typeRevision": types[TABLE_FORM],
                    "content": json.dumps(table_form(table)),
                }
            ]
            name = table.aliases[0] if table.aliases else table.name
            asset = self._revise(
                client,
                assets.get(table.semantic_uri),
                lambda known, forms=forms, name=name, table=table: client.create_asset_revision(
                    **self._scope(),
                    identifier=known,
                    name=name,
                    description=table.description,
                    formsInput=forms,
                ),
                lambda forms=forms, name=name, table=table: client.create_asset(
                    **self._scope(),
                    name=name,
                    description=table.description,
                    externalIdentifier=table.semantic_uri,
                    typeIdentifier=TABLE_TYPE,
                    typeRevision=types[TABLE_TYPE],
                    formsInput=forms,
                    owningProjectIdentifier=self._config.datazone_project_id,
                ),
            )
            assets[table.semantic_uri] = asset["id"]
            # REQ-1389: the next publish revises this asset by its id, not by a search.
            result.bindings[table.semantic_uri] = (asset["id"], table.semantic_uri)
            client.create_listing_change_set(
                **self._scope(),
                entityIdentifier=asset["id"],
                entityType="ASSET",
                entityRevision=asset["revision"],
                action="PUBLISH",
            )
            items.append({"itemType": "ASSET", "identifier": asset["id"]})
        forms = [
            {
                "formName": PRODUCT_FORM,
                "typeIdentifier": PRODUCT_FORM,
                "typeRevision": types[PRODUCT_FORM],
                "content": json.dumps(product_form(product)),
            }
        ]
        created = self._revise(
            client,
            products.get(product.semantic_uri),
            lambda known: client.create_data_product_revision(
                **self._scope(),
                identifier=known,
                name=product.name,
                description=product.description,
                items=items,
                formsInput=forms,
            ),
            lambda: client.create_data_product(
                **self._scope(),
                name=product.name,
                description=product.description,
                owningProjectIdentifier=self._config.datazone_project_id,
                items=items,
                formsInput=forms,
            ),
        )
        products[product.semantic_uri] = created["id"]
        # REQ-1389: the next publish revises this product by its id, not by a search.
        result.bindings[product.semantic_uri] = (created["id"], product.name)
        client.create_listing_change_set(
            **self._scope(),
            entityIdentifier=created["id"],
            entityType="DATA_PRODUCT",
            entityRevision=created["revision"],
            action="PUBLISH",
        )
        return len(items)

    def _glossary_id(self, client: Any, org_id: str, result: PublishResult) -> str:
        """The Provisa glossary: its recorded id, else found by name in the project, else
        created."""
        uri = f"provisa://{org_id}/glossary"
        name = f"Provisa glossary ({org_id})"
        bound = self.stored_bindings.get(uri)
        if bound is not None:
            glossary_id = bound[0]
        else:
            found = self._existing(client, "GLOSSARY", "glossaryItem", "name", "id")
            glossary_id = (
                found.get(name)
                or client.create_glossary(
                    **self._scope(),
                    name=name,
                    description="Business vocabulary Provisa publishes from its governed model.",
                    owningProjectIdentifier=self._config.datazone_project_id,
                    status="ENABLED",
                )["id"]
            )
        result.bindings[uri] = (glossary_id, name)
        return glossary_id

    def _publish_glossary(
        self, client: Any, snapshot: "MetadataSnapshot", result: PublishResult
    ) -> None:
        """The org's terms in the Provisa glossary, then each term's isA (module docstring)."""
        glossary_id = self._glossary_id(client, snapshot.org_id, result)
        found: dict[str, str] | None = None
        ids: dict[int, str] = {}
        live_isa: dict[int, list[str]] = {}
        for term in snapshot.glossary_terms:
            fields = {
                "name": term.name,
                "longDescription": term.definition or "",
                "status": "DISABLED" if term.in_deprecation else "ENABLED",
            }
            known = (self.stored_bindings.get(term.semantic_uri) or (None,))[0]
            if known is None:
                if found is None:
                    found = self._glossary_terms(client, glossary_id)
                known = found.get(term.name)
            saved = self._revise(
                client,
                known,
                lambda known, fields=fields: client.update_glossary_term(
                    **self._scope(), identifier=known, glossaryIdentifier=glossary_id, **fields
                ),
                lambda fields=fields: client.create_glossary_term(
                    **self._scope(), glossaryIdentifier=glossary_id, **fields
                ),
            )
            ids[term.term_id] = saved["id"]
            live_isa[term.term_id] = (saved.get("termRelations") or {}).get("isA") or []
            # REQ-1389: the next publish revises this term by its id.
            result.bindings[term.semantic_uri] = (saved["id"], term.name)
            result.published["glossary_term"] = result.published.get("glossary_term", 0) + 1
        parents: dict[int, list[str]] = {}
        for edge in snapshot.glossary_edges:
            if edge.rel_type == "KIND_OF" and edge.to_term_id in ids:
                parents.setdefault(edge.from_term_id, []).append(ids[edge.to_term_id])
        for tid, term_ref in ids.items():
            wanted = sorted(parents.get(tid, []))
            if wanted == sorted(live_isa[tid]):
                continue
            client.update_glossary_term(
                **self._scope(),
                identifier=term_ref,
                glossaryIdentifier=glossary_id,
                # isA takes at least one term; an empty relation set clears it.
                termRelations={"isA": wanted} if wanted else {},
            )

    def _glossary_terms(self, client: Any, glossary_id: str) -> dict[str, str]:
        """The Provisa glossary's terms: id by name."""
        pages = client.get_paginator("search").paginate(
            **self._scope(),
            owningProjectIdentifier=self._config.datazone_project_id,
            searchScope="GLOSSARY_TERM",
        )
        return {
            hit["glossaryTermItem"]["name"]: hit["glossaryTermItem"]["id"]
            for page in pages
            for hit in page.get("items", [])
            if hit.get("glossaryTermItem", {}).get("glossaryId") == glossary_id
        }

    async def health(self) -> None:
        def _check() -> None:
            self._client().get_project(**self._scope(), identifier=self._config.datazone_project_id)

        await asyncio.to_thread(_check)
