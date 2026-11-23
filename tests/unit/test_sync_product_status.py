# Copyright (c) 2026 Kenneth Stott
# Canary: 8f2b6d40-1c7e-4a93-b5d8-3e0a9c4f7b21
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2259: every publish of a Syncing entry is recorded with each data product's outcome in it.

The Data Products list shows each product's sync status from that record, read through an endpoint
gated on ``data_product_read``; Context Sync filters its entries by product from the same record,
carried on each entry. What is asserted is what a wrong record gets wrong silently: a product the
catalog refused shown as published, an entry-wide refusal leaving its products looking current,
an older outcome outliving a newer one, and the status read open to a caller who reads no product.
"""

# Requirements: REQ-2259

from __future__ import annotations

import dataclasses

import httpx
import pytest

from provisa.api.errors import ApiError
from provisa.api.metadata_export.model import (
    AssetKind,
    AssetRef,
    DataProductAsset,
    MetadataSnapshot,
)
from provisa.api.metadata_export.provider import AssetError, AssetRefStub, PublishResult
from provisa.core.repositories import catalog_publish
from provisa.core.schema_org import catalog_publishes
from tests.unit.test_syncing_admin_surface import (  # noqa: F401 - the shared fixtures
    ORG_ID,
    _catalog,
    _get,
    _Partial,
    _request,
    _served,
    _stub_publish,
    surface,
)


def _product(product_id: str, name: str | None = None) -> DataProductAsset:
    return DataProductAsset(
        ref=AssetRef(kind=AssetKind.DATA_PRODUCT, parts=(product_id,)),
        id=product_id,
        name=name or product_id.title(),
        domain_id="sales",
        owner=None,
        description="",
    )


def _sending(monkeypatch, *products: DataProductAsset) -> None:
    """Each entry is sent ``products``: what a role is sent is test_metadata_export_scope's."""
    import provisa.api.metadata_export.publishing as sync_mod

    async def _for_entry(state, snapshot, domains, role):
        return dataclasses.replace(MetadataSnapshot(org_id=ORG_ID), data_products=list(products))

    monkeypatch.setattr(sync_mod, "for_entry", _for_entry)


def _conn():
    from provisa.api.app import state

    assert state.model_db is not None
    return state.model_db.acquire()


def _reads_products(monkeypatch, *capabilities: str) -> None:
    import provisa.api.admin.capabilities as capmod

    monkeypatch.setattr(capmod, "_resolved_capabilities", lambda identity, state: set(capabilities))


async def _publish(entry_id: str = "mz") -> dict:
    from provisa.api.admin.syncing_router import publish_entry

    return await publish_entry(_request(), entry_id)


async def _status(monkeypatch) -> dict:
    from provisa.api.admin.syncing_router import product_status

    _reads_products(monkeypatch, "data_product_read")
    try:
        return (await product_status(_request()))["products"]
    finally:
        _reads_products(monkeypatch, "org_settings")


def _outcomes(statuses: list[dict]) -> list[tuple[str, str, str | None]]:
    return [(s["entry"], s["outcome"], s["error"]) for s in statuses]


class _Refusing:
    """A catalog that refuses the request itself: no per-asset report, the target's failure."""

    needs_export_views = False
    binds_products = False

    async def publish(self, snapshot):
        request = httpx.Request("POST", "http://mz/api")
        raise httpx.HTTPStatusError(
            "refused", request=request, response=httpx.Response(401, request=request, text="no")
        )


@pytest.mark.asyncio
async def test_a_publish_records_each_product_published_or_refused(surface, monkeypatch):  # noqa: F811
    surface.settings["catalogs"] = [_catalog(role="steward")]
    _sending(monkeypatch, _product("orders"), _product("revenue"), _product("stock", "Stock"))
    _stub_publish(
        monkeypatch,
        _Partial(
            PublishResult(
                provider_name="openlineage",
                errors=[
                    AssetError(
                        asset=AssetRef(kind=AssetKind.DATA_PRODUCT, parts=("revenue",)),
                        message="422 owner unknown",
                    ),
                    # Snowflake Horizon reports a product by its name.
                    AssetError(asset=AssetRefStub("Stock"), message="listing refused"),
                    # A member table's refusal is the table's, not the product's.
                    AssetError(
                        asset=AssetRef(kind=AssetKind.TABLE, parts=("wh", "orders")),
                        message="422 column type",
                    ),
                ],
            )
        ),
    )

    await _publish()

    status = await _status(monkeypatch)
    assert _outcomes(status["orders"]) == [("mz", "published", None)]
    assert _outcomes(status["revenue"]) == [("mz", "failed", "422 owner unknown")]
    assert _outcomes(status["stock"]) == [("mz", "failed", "listing refused")]
    assert status["orders"][0]["published_at"]


@pytest.mark.asyncio
async def test_a_refused_request_fails_every_product_it_carried(surface, monkeypatch):  # noqa: F811
    surface.settings["catalogs"] = [_catalog(role="steward")]
    _sending(monkeypatch, _product("orders"), _product("revenue"))
    _stub_publish(monkeypatch, _Refusing())

    await _publish()

    status = await _status(monkeypatch)
    for product in ("orders", "revenue"):
        ((entry, outcome, error),) = _outcomes(status[product])
        assert (entry, outcome) == ("mz", "failed")
        assert error is not None and "HTTP 401" in error


@pytest.mark.asyncio
async def test_the_latest_publish_decides_and_each_entry_keeps_its_own(surface, monkeypatch):  # noqa: F811
    surface.settings["catalogs"] = [
        _catalog(role="steward"),
        _catalog(id="dh", provider="datahub", endpoint="http://dh", role="steward"),
    ]
    _sending(monkeypatch, _product("orders"))
    _stub_publish(monkeypatch, _Refusing())
    await _publish("mz")
    await _publish("dh")
    _stub_publish(monkeypatch, _Partial(PublishResult(provider_name="openlineage")))
    await _publish("mz")

    status = await _status(monkeypatch)
    assert [(e, o) for e, o, _ in _outcomes(status["orders"])] == [
        ("dh", "failed"),
        ("mz", "published"),
    ]


@pytest.mark.asyncio
async def test_a_publish_with_no_role_is_recorded_failed_and_sends_no_product(
    surface,  # noqa: F811
    monkeypatch,
):
    """Nothing was sent, so each product keeps the outcome of the publish that last carried it."""
    surface.settings["catalogs"] = [_catalog(role="steward")]
    _sending(monkeypatch, _product("orders"))
    _stub_publish(monkeypatch, _Partial(PublishResult(provider_name="openlineage")))
    await _publish()
    surface.settings["catalogs"] = [_catalog()]
    await _publish()

    async with _conn() as conn:
        rows = (
            await conn.execute_core(catalog_publishes.select().order_by(catalog_publishes.c.id))
        ).fetchall()
    assert [(r.entry, r.outcome) for r in rows] == [("mz", "ok"), ("mz", "failed")]
    assert "names no role" in rows[1].error
    status = await _status(monkeypatch)
    assert _outcomes(status["orders"]) == [("mz", "published", None)]


@pytest.mark.asyncio
async def test_an_entry_that_no_longer_exports_shows_no_status(surface, monkeypatch):  # noqa: F811
    surface.settings["catalogs"] = [_catalog(role="steward")]
    _sending(monkeypatch, _product("orders"))
    _stub_publish(monkeypatch, _Partial(PublishResult(provider_name="openlineage")))
    await _publish()
    surface.settings["catalogs"] = [_catalog(direction="import", role="steward")]

    assert await _status(monkeypatch) == {}


@pytest.mark.asyncio
async def test_each_entry_read_carries_its_products_for_the_sync_page_filter(surface, monkeypatch):  # noqa: F811
    surface.settings["catalogs"] = [_catalog(role="steward")]
    _sending(monkeypatch, _product("orders"))
    _stub_publish(monkeypatch, _Partial(PublishResult(provider_name="openlineage")))
    await _publish()

    (entry,) = (await _get())["entries"]
    assert [(p["product_id"], p["outcome"]) for p in entry["products"]] == [("orders", "published")]


@pytest.mark.asyncio
async def test_the_status_read_asks_data_product_read(surface, monkeypatch):  # noqa: F811
    from provisa.api.admin.syncing_router import product_status

    # The surface's caller holds org_settings, the Syncing page's right, and no data product right.
    with pytest.raises(ApiError) as exc:
        await product_status(_request())
    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_record_refuses_an_unknown_outcome(surface):  # noqa: F811
    async with _conn() as conn:
        with pytest.raises(ValueError, match="unknown data product outcome"):
            await catalog_publish.record(
                conn,
                "mz",
                outcome=catalog_publish.OK,
                products=[catalog_publish.ProductOutcome("orders", "maybe")],
            )
