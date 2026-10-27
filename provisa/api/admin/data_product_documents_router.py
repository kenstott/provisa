# Copyright (c) 2026 Kenneth Stott
# Canary: 6e1a9c03-7f48-4d25-b0e7-2c5d8a4f1b39
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A data product's open-standard documents, served read-only (REQ-2118).

A catalog that imports the standards pulls them from here: the product's ODPS document, and the
ODCS contract of each member it names. They are built from the same snapshot the export
publishes (``publishing.org_snapshot``), so a catalog that pulls reads what one that is pushed to
was told. Reading them asks what reading a data product asks (``data_product_read``), and they are
part of the metadata export, so the org's tier must include it (REQ-1073; nothing gates it on a
self-hosted install, REQ-1066).
"""

# Requirements: REQ-2118, REQ-1073

from __future__ import annotations

import yaml
from fastapi import APIRouter, Request
from fastapi.responses import Response

from provisa.api.admin._guards import require_active_org_id
from provisa.api.admin.capabilities import require_capability_request
from provisa.api.errors import ApiError
from provisa.control_plane.entitlements import EntitlementError, Feature, require_feature

router = APIRouter()

_YAML = "application/yaml"


async def _documents(request: Request, product_id: str):
    require_capability_request(request, "data_product_read")
    org_id = require_active_org_id(request)
    try:
        await require_feature(org_id, Feature.METADATA_EXPORT)
    except (EntitlementError, KeyError) as exc:
        raise ApiError(403, "metadata_export.not_entitled", str(exc)) from exc
    from provisa.api.metadata_export.odps import product_documents
    from provisa.api.metadata_export.publishing import org_snapshot

    snapshot = await org_snapshot(org_id)
    product = next((p for p in snapshot.data_products if p.id == product_id), None)
    if product is None:
        # A product with no exported member builds nothing (DataProductAsset), so it has no
        # documents either: there is nothing behind it to describe.
        raise ApiError(
            404,
            "data_products.no_documents",
            f"data product {product_id!r} does not exist or has no member to describe",
        )
    return product_documents(snapshot, product)


def _yaml(document: dict) -> Response:
    return Response(yaml.safe_dump(document, sort_keys=False, allow_unicode=True), media_type=_YAML)


@router.get("/data-products/{product_id}/odps.yaml")
async def data_product_odps(product_id: str, request: Request) -> Response:  # REQ-2118
    """The product's ODPS 1.0 document."""
    document, _ = await _documents(request, product_id)
    return _yaml(document)


@router.get("/data-products/{product_id}/contracts/{member}.yaml")
async def data_product_contract(product_id: str, member: str, request: Request) -> Response:
    """A member's ODCS 3.1 contract, the member named by its address (source.schema.table)."""
    _, contracts = await _documents(request, product_id)
    contract = contracts.get(member)
    if contract is None:
        raise ApiError(
            404,
            "data_products.no_contract",
            f"data product {product_id!r} has no output port {member!r}; its output ports are "
            f"{', '.join(sorted(contracts))}",
        )
    return _yaml(contract)
