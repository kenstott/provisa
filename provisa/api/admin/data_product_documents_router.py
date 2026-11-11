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
self-hosted install, REQ-1066). The product's own Contracts panel reads the same contracts as JSON,
asking only what reading the product asks, and each contract's saved versions, which the standard
has no element for (REQ-2158).
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


async def _documents(request: Request, product_id: str, *, export: bool = True):
    """The product's documents. ``export`` is a catalog's pull, part of the metadata export; the
    product's own page reads the same contracts without it (REQ-2158), as it reads the product."""
    require_capability_request(request, "data_product_read")
    org_id = require_active_org_id(request)
    if export:
        try:
            await require_feature(org_id, Feature.METADATA_EXPORT)
        except (EntitlementError, KeyError) as exc:
            raise ApiError(403, "metadata_export.not_entitled", str(exc)) from exc
    from provisa.api.metadata_export.odps import product_documents
    from provisa.api.metadata_export.publishing import org_snapshot

    # Built for this product: only what its documents carry is read (REQ-2158).
    snapshot = await org_snapshot(org_id, product_id=product_id)
    product = next((p for p in snapshot.data_products if p.id == product_id), None)
    if product is None:
        # A product with no exported member builds nothing (DataProductAsset), so it has no
        # documents either: there is nothing behind it to describe.
        raise ApiError(
            404,
            "data_products.no_documents",
            f"data product {product_id!r} does not exist or has no member to describe",
        )
    return product_documents(snapshot, product, await offer_exports(product_id))


async def offer_exports(product_id: str) -> list:
    """The product's offers as the export writes them (REQ-2180), each with its stored contract
    on each dataset, keyed by the dataset's address (source.schema.table)."""
    from sqlalchemy import select

    from provisa.api.app import state
    from provisa.api.metadata_export.odps import OfferExport
    from provisa.core.repositories import data_product as data_product_repo
    from provisa.core.repositories import data_product_offer as offers
    from provisa.core.schema_org import registered_tables as rt

    assert state.model_db is not None  # the org's model store is bound with its runtime
    async with state.model_db.acquire() as conn:
        stored = await data_product_repo.get(conn, product_id)
        freshness = None if stored is None else stored.get("freshness")
        exports = []
        for row in await offers.list_offers(conn, product_id):
            contracts = await offers.contracts(conn, row["id"])
            addresses = {
                int(r.id): f"{r.source_id}.{r.schema_name}.{r.table_name}"
                for r in (
                    await conn.execute_core(
                        select(rt.c.id, rt.c.source_id, rt.c.schema_name, rt.c.table_name).where(
                            rt.c.id.in_(list(contracts))
                        )
                    )
                ).fetchall()
            }
            exports.append(
                OfferExport(
                    name=row["name"],
                    role_id=row["role_id"],
                    data_level=row["data_level"],
                    dq_results=row["dq_results"],
                    profiles=row["profiles"],
                    open=row["open"],
                    duration_days=row["duration_days"],
                    contracts={addresses[tid]: c for tid, c in contracts.items()},
                    freshness=freshness,
                )
            )
    return exports


def _yaml(document: dict) -> Response:
    return Response(yaml.safe_dump(document, sort_keys=False, allow_unicode=True), media_type=_YAML)


@router.get("/data-products/{product_id}/odps.yaml")
async def data_product_odps(product_id: str, request: Request) -> Response:  # REQ-2118
    """The product's ODPS 1.0 document."""
    document, _ = await _documents(request, product_id)
    return _yaml(document)


@router.get("/data-products/{product_id}/contracts.json")
async def data_product_contracts(product_id: str, request: Request) -> dict:  # REQ-2158
    """Every output port's ODCS 3.1 contract -- one per offer and dataset (REQ-2180), each keyed
    ``<offer>/<source.schema.table>`` -- as the product's offers show them: the one document a
    catalog pulls."""
    _, contracts = await _documents(request, product_id, export=False)
    return contracts


def _port_contract(product_id: str, offer: str, member: str, contracts: dict[str, dict]) -> dict:
    """The contract ``offer`` gives on ``member``, or why there is none."""
    contract = contracts.get(f"{offer}/{member}")
    if contract is None:
        raise ApiError(
            404,
            "data_products.no_contract",
            f"data product {product_id!r} has no contract of offer {offer!r} on {member!r}; its "
            f"contracts are {', '.join(sorted(contracts))}",
        )
    return contract


@router.get("/data-products/{product_id}/offers/{offer}/contracts/{member}.json")
async def data_product_contract_json(
    product_id: str, offer: str, member: str, request: Request
) -> dict:
    """The ODCS 3.1 contract an offer gives on one dataset, as the product's offer shows it when
    that contract opens (REQ-2180): asking what reading the product asks, as contracts.json does."""
    _, contracts = await _documents(request, product_id, export=False)
    return _port_contract(product_id, offer, member, contracts)


@router.get("/data-products/{product_id}/offers/{offer}/contracts/{member}/versions.json")
async def data_product_contract_versions(
    product_id: str, offer: str, member: str, request: Request
) -> list[dict]:
    """The saved versions of the contract an offer gives on one dataset, oldest first, each with
    when and by whom it was saved and its differences from the one before (REQ-2180), asking what
    reading the product asks."""
    require_capability_request(request, "data_product_read")
    require_active_org_id(request)
    exports = {o.name: o for o in await offer_exports(product_id)}
    contract = None if offer not in exports else exports[offer].contracts.get(member)
    if contract is None:
        raise ApiError(
            404,
            "data_products.no_contract",
            f"data product {product_id!r} has no contract of offer {offer!r} on {member!r}",
        )
    return [v.model_dump(mode="json") for v in contract.versions]


@router.get("/data-products/{product_id}/offers/{offer}/contracts/{member}.yaml")
async def data_product_contract(
    product_id: str, offer: str, member: str, request: Request
) -> Response:
    """The ODCS 3.1 contract an offer gives on a dataset, named by its address
    (source.schema.table)."""
    _, contracts = await _documents(request, product_id)
    return _yaml(_port_contract(product_id, offer, member, contracts))
