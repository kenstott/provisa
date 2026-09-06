# Copyright (c) 2026 Kenneth Stott
# Canary: 3d7e9a04-5b1c-4f28-9d63-0a2e8c4b1f7d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""BigQuery Dataplex / Analytics Hub adapter (REQ-1636).

The BigQuery counterpart to ``snowflake_horizon.py`` (REQ-1635): same engine-aware shape,
same reason it cannot be a pure HTTP payload-builder like the six vendor-neutral adapters
(openmetadata.py, atlan.py, collibra.py, datahub.py, atlas.py, openlineage.py — REQ-1069).
An Analytics Hub listing identifies a table by its ``project.dataset.table`` in the org's
actual BigQuery project, which ``MetadataSnapshot`` does not carry — its ``AssetRef`` parts
are Provisa's own ``(source_id, schema_name, table_name)`` triple. Resolving the real
identity means reaching the org's live ``BigQueryFederationRuntime`` and asking its
``_phys_parts`` helper, the same private-but-consistent method every federation runtime
exposes (``snowflake_runtime.py``, ``databricks_runtime.py``, ...).

Wired through the same injected seam as the Snowflake adapter — ``set_runtime_resolver`` —
rather than reaching into ``provisa.api.org_runtime``/``AppState`` directly, so this module
stays free of the data-plane object graph and stays testable with an injected fake runtime.
Until REQ-1633 gives BigQuery an ``attach_landed_source`` landing terminal, ``publish`` finds
no runtime with that attribute for a BigQuery-backed org and reports nothing published — the
documented "otherwise skipped" state in REQ-1636.

The listing itself: one DataProductAsset becomes one BigQuery Analytics Hub listing, backed
by a dataset containing the product's member tables.
"""

# Requirements: REQ-1633, REQ-1634, REQ-1636

from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, Callable

import httpx

from provisa.api.metadata_export.provider import (
    AssetError,
    MetadataExport,
    PublishResult,
)
from provisa.api.metadata_export.registry import register_provider

if TYPE_CHECKING:
    from provisa.api.metadata_export.model import DataProductAsset, MetadataSnapshot

RuntimeResolver = Callable[[str], Any]

# REQ-1636: the injected seam, same shape and same reason as snowflake_horizon.py's. Kept as
# a separate module-level variable (not shared with the Snowflake adapter) so a deployment
# running both engines can wire each to its own accessor.
_get_runtime: RuntimeResolver | None = None


def set_runtime_resolver(resolver: RuntimeResolver | None) -> None:
    """Wire (or clear, with ``None``) the org id -> live federation runtime accessor.

    See ``snowflake_horizon.set_runtime_resolver`` for the full rationale: the registry
    (``metadata_export.py``) builds a provider from ``MetadataExportConfig`` alone, so there
    is no call site to thread a resolver through the constructor.
    """
    global _get_runtime
    _get_runtime = resolver


def _resolve_member(runtime: Any, ref: Any) -> str:
    """A member's ``project.dataset.table`` in the org's actual BigQuery project."""
    source_id, schema_name, table_name = ref.parts
    source_like = SimpleNamespace(id=source_id, schema_name=schema_name, table_name=table_name)
    project, dataset, table = runtime._phys_parts(source_like)
    return f"{project}.{dataset}.{table}"


@dataclass
class ListingSpec:  # REQ-1636
    """The Analytics Hub listing payload for one DataProductAsset."""

    product_id: str
    name: str
    description: str
    owner_id: str | None
    dataset_tables: list[str]

    def as_payload(self) -> dict[str, Any]:
        return {
            "displayName": self.name,
            "description": self.description,
            **({"owner": self.owner_id} if self.owner_id is not None else {}),
            "bigQueryDataset": {"tables": self.dataset_tables},
        }


def _build_listing(product: "DataProductAsset", runtime: Any) -> ListingSpec:
    return ListingSpec(
        product_id=product.id,
        name=product.name,
        description=product.description,
        owner_id=product.owner.id if product.owner is not None else None,
        dataset_tables=[_resolve_member(runtime, member) for member in product.members],
    )


@register_provider
class BigQueryDataplexExport(MetadataExport):  # REQ-1636
    """Publishes each DataProductAsset as a BigQuery Analytics Hub listing."""

    provider_name = "bigquery_dataplex"

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        token = self._config.token.get_secret_value() or self._config.api_key.get_secret_value()
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return headers

    def _url(self, path: str) -> str:
        return f"{self._config.endpoint.rstrip('/')}{path}"

    async def publish(self, snapshot: "MetadataSnapshot") -> PublishResult:
        result = PublishResult(provider_name=self.provider_name)
        if _get_runtime is None:
            return result
        runtime = _get_runtime(snapshot.org_id)
        # REQ-1633: no landing terminal for this org's runtime (not yet implemented for
        # BigQuery) means there is nothing to resolve a member table's identity against.
        if runtime is None or not hasattr(runtime, "attach_landed_source"):
            return result
        async with httpx.AsyncClient(timeout=self._config.timeout_seconds) as client:
            for product in snapshot.data_products:
                try:
                    listing = _build_listing(product, runtime)
                except Exception as exc:  # noqa: BLE001 - reported per-product, not raised
                    result.errors.append(AssetError(asset=product.ref, message=str(exc)))
                    continue
                try:
                    response = await client.put(
                        self._url(f"/v1/dataProducts/{listing.product_id}"),
                        json=listing.as_payload(),
                        headers=self._headers(),
                    )
                except httpx.HTTPError as exc:
                    result.errors.append(AssetError(asset=product.ref, message=str(exc)))
                    continue
                if response.status_code >= 400:
                    result.errors.append(
                        AssetError(
                            asset=product.ref,
                            message=f"HTTP {response.status_code}: {response.text[:500]}",
                        )
                    )
                    continue
                result.published["data_product"] = result.published.get("data_product", 0) + 1
        return result

    async def health(self) -> None:
        async with httpx.AsyncClient(timeout=self._config.timeout_seconds) as client:
            response = await client.get(self._url("/v1/dataProducts"), headers=self._headers())
        response.raise_for_status()
