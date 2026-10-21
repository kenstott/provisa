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
nothing itself: the producer project approves each subscription. An asset is found again by its
external identifier, the member's Provisa URI; a product by its name in the project. Each
publish adds a revision to what changed and publishes it.
"""

# Requirements: REQ-2118

from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING, Any

from provisa.api.metadata_export.provider import (
    AssetError,
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
        if not snapshot.data_products:
            return PublishResult(provider_name=self.provider_name)
        return await asyncio.to_thread(self._publish, snapshot)

    def _publish(self, snapshot: "MetadataSnapshot") -> PublishResult:
        from botocore.exceptions import BotoCoreError, ClientError

        result = PublishResult(provider_name=self.provider_name)
        client = self._client()
        tables = {t.ref.fqn(): t for t in snapshot.tables}
        types = self._types(client)
        assets = self._existing(client, "ASSET", "assetItem", "externalIdentifier", "identifier")
        products = self._existing(client, "DATA_PRODUCT", "dataProductItem", "name", "id")
        for product in snapshot.data_products:
            try:
                count = self._publish_product(client, product, tables, types, assets, products)
            except (ClientError, BotoCoreError) as exc:
                result.errors.append(AssetError(asset=product.ref, message=str(exc)))
                continue
            result.published["data_products"] = result.published.get("data_products", 0) + 1
            result.published["data_assets"] = result.published.get("data_assets", 0) + count
        return result

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
            if table.semantic_uri in assets:
                asset = client.create_asset_revision(
                    **self._scope(),
                    identifier=assets[table.semantic_uri],
                    name=name,
                    description=table.description,
                    formsInput=forms,
                )
            else:
                asset = client.create_asset(
                    **self._scope(),
                    name=name,
                    description=table.description,
                    externalIdentifier=table.semantic_uri,
                    typeIdentifier=TABLE_TYPE,
                    typeRevision=types[TABLE_TYPE],
                    formsInput=forms,
                    owningProjectIdentifier=self._config.datazone_project_id,
                )
                assets[table.semantic_uri] = asset["id"]
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
        if product.name in products:
            created = client.create_data_product_revision(
                **self._scope(),
                identifier=products[product.name],
                name=product.name,
                description=product.description,
                items=items,
                formsInput=forms,
            )
        else:
            created = client.create_data_product(
                **self._scope(),
                name=product.name,
                description=product.description,
                owningProjectIdentifier=self._config.datazone_project_id,
                items=items,
                formsInput=forms,
            )
            products[product.name] = created["id"]
        client.create_listing_change_set(
            **self._scope(),
            entityIdentifier=created["id"],
            entityType="DATA_PRODUCT",
            entityRevision=created["revision"],
            action="PUBLISH",
        )
        return len(items)

    async def health(self) -> None:
        def _check() -> None:
            self._client().get_project(**self._scope(), identifier=self._config.datazone_project_id)

        await asyncio.to_thread(_check)
