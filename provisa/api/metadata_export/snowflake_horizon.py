# Copyright (c) 2026 Kenneth Stott
# Canary: 8b3f5c1a-9e02-4d76-8b4e-2f6a7c1d9e3b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Snowflake Horizon Catalog adapter (REQ-1635).

Every other provider in this package (openmetadata.py, atlan.py, collibra.py, datahub.py,
atlas.py, openlineage.py — REQ-1069) is a pure HTTP payload-builder: it maps
``MetadataSnapshot`` to a vendor wire shape and never needs to know what engine, if any,
actually runs the org's queries. This adapter is different — Horizon Catalog identifies a
Snowflake object by its account-local ``database.schema.table``, which is NOT something
``MetadataSnapshot`` carries (its ``AssetRef`` parts are Provisa's own ``(source_id,
schema_name, table_name)`` triple). Resolving the real Snowflake identity means reaching the
org's live ``SnowflakeFederationRuntime`` and asking its ``_phys_parts`` helper — the same
private-but-consistent method every federation runtime exposes.

No existing seam hands a provider a live runtime; ``MetadataExportConfig`` is
HTTP-endpoint-shaped (REQ-1068) and the registry constructs a provider from config alone
(``metadata_export.py`` factory). Rather than reach across into ``provisa.api.org_runtime``
or ``AppState`` from here — which would make a metadata-export adapter depend on the whole
data-plane object graph — this module exposes one small injected seam,
``set_runtime_resolver``: the deployment wires it once at startup to whatever knows how to
turn an org id into its live runtime, and a test wires it to a fake. Until REQ-1633 gives
Snowflake an ``attach_landed_source`` landing terminal, ``publish`` finds no runtime with
that attribute for a Snowflake-backed org and reports nothing published — not an error, the
documented "otherwise skipped" state in REQ-1635.

The listing itself: one DataProductAsset becomes one Snowflake Data Product / Marketplace
listing, backed by a SHARE that grants the listing's consumers access to every member
table's real Snowflake object.
"""

# Requirements: REQ-1633, REQ-1634, REQ-1635

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

# REQ-1635: the injected seam. ``None`` until a deployment calls ``set_runtime_resolver`` —
# with no resolver wired, ``publish`` has no way to reach a live runtime and reports nothing
# published, the same as when a resolver is wired but returns no usable runtime.
_get_runtime: RuntimeResolver | None = None


def set_runtime_resolver(resolver: RuntimeResolver | None) -> None:
    """Wire (or clear, with ``None``) the org id -> live federation runtime accessor.

    A deployment calls this once at startup with whatever knows how to turn an org id into
    its ``OrgRuntime``/``EngineBackend`` pair; a test calls it with a lambda returning a fake
    runtime object. Kept as a plain module-level function rather than a constructor
    parameter because the registry (``metadata_export.py``) builds a provider from
    ``MetadataExportConfig`` alone — there is no call site to thread a resolver through.
    """
    global _get_runtime
    _get_runtime = resolver


def _resolve_member(runtime: Any, ref: Any) -> str:
    """A member's ``database.schema.table`` in the org's actual Snowflake account.

    ``ref.parts`` is Provisa's own ``(source_id, schema_name, table_name)`` triple
    (``refs.py`` ``table_ref``) — the runtime's ``_phys_parts`` turns the source id into the
    Snowflake database name it was registered under.
    """
    source_id, schema_name, table_name = ref.parts
    source_like = SimpleNamespace(id=source_id, schema_name=schema_name, table_name=table_name)
    database, schema, table = runtime._phys_parts(source_like)
    return f"{database}.{schema}.{table}"


@dataclass
class ListingSpec:  # REQ-1635
    """The Horizon Catalog listing payload for one DataProductAsset."""

    product_id: str
    name: str
    description: str
    owner_id: str | None
    share_objects: list[str]

    def as_payload(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            **({"owner": self.owner_id} if self.owner_id is not None else {}),
            "share_objects": self.share_objects,
        }


def _build_listing(product: "DataProductAsset", runtime: Any) -> ListingSpec:
    return ListingSpec(
        product_id=product.id,
        name=product.name,
        description=product.description,
        owner_id=product.owner.id if product.owner is not None else None,
        share_objects=[_resolve_member(runtime, member) for member in product.members],
    )


@register_provider
class SnowflakeHorizonExport(MetadataExport):  # REQ-1635
    """Publishes each DataProductAsset as a Snowflake Data Product / Marketplace listing."""

    provider_name = "snowflake_horizon"

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
        # Snowflake) means there is nothing to resolve a member table's identity against.
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
                        self._url(f"/api/v2/data-products/{listing.product_id}"),
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
            response = await client.get(self._url("/api/v2/data-products"), headers=self._headers())
        response.raise_for_status()
