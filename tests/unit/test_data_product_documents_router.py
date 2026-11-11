# Copyright (c) 2026 Kenneth Stott
# Canary: 9a4e1c27-3b58-4f06-8d2e-7c1f0b6a5e93
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2158, REQ-2118, REQ-2180: a product's offers show the ODCS contracts they give as JSON,
asking what reading the product asks; a catalog's pull is the metadata export's."""

from __future__ import annotations

import asyncio

import pytest

from provisa.api.admin import data_product_documents_router as router
from provisa.api.errors import ApiError
from provisa.api.metadata_export import build_snapshot
from provisa.control_plane.entitlements import EntitlementError, Tier
from datetime import UTC, datetime

from provisa.api.metadata_export.odps import OfferExport
from provisa.core import offer_contracts
from provisa.core.models import DataProduct, DataProductOffer
from tests.integration.metadata_export_fixture import ORG_ID, PROD, governed_config


@pytest.fixture
def org(monkeypatch):
    config = governed_config()
    config.data_products = [
        DataProduct(id="prod", domain_id="sales", name="Customer 360", tables=list(PROD.tables))
    ]
    snapshot = build_snapshot(config, org_id=ORG_ID, dialect="postgres", contexts={})
    asked: list[str] = []

    async def snapshot_of(org_id, *, product_id):
        # REQ-2158: the snapshot is built for the one product asked for.
        asked.append(f"snapshot:{product_id}")
        return snapshot

    async def not_entitled(org_id, feature):
        raise EntitlementError(org_id, feature, "free", Tier.PREMIUM)

    monkeypatch.setattr(router, "require_capability_request", lambda r, c: asked.append(c))
    monkeypatch.setattr(router, "require_active_org_id", lambda r: ORG_ID)
    monkeypatch.setattr(router, "require_feature", not_entitled)
    monkeypatch.setattr("provisa.api.metadata_export.publishing.org_snapshot", snapshot_of)

    async def offer_exports(product_id):
        # REQ-2180: the product's offers and their contracts, as the store holds them.
        asked.append(f"offers:{product_id}")
        return [_default_offer(snapshot)]

    monkeypatch.setattr(router, "offer_exports", offer_exports)
    return asked


_AT = datetime(2026, 11, 11, tzinfo=UTC)


def _contract(columns, previous=None):
    offer = DataProductOffer(name="default", role_id="viewer")
    seen = [offer_contracts.SeenColumn(c, "integer") for c in columns]
    terms = offer_contracts.terms(
        offer, seen, row_rules=[], sample_views=[], checks=[], profiles=[]
    )
    return offer_contracts.recorded(previous, terms, at=_AT)


def _default_offer(snapshot) -> OfferExport:
    members = {"wh.public.orders", "wh.public.customers", "wh.public.order_totals"}
    return OfferExport(
        name="default",
        role_id="viewer",
        data_level="full",
        dq_results=True,
        profiles=False,
        open=False,
        duration_days=None,
        contracts={
            t.ref.fqn(): _contract([c.name for c in t.columns])
            for t in snapshot.tables
            if t.ref.fqn() in members
        },
    )


def test_the_panel_reads_every_contract_without_the_export(org):
    contracts = asyncio.run(router.data_product_contracts("prod", object()))
    assert set(contracts) == {
        "default/wh.public.orders",
        "default/wh.public.customers",
        "default/wh.public.order_totals",
    }
    assert contracts["default/wh.public.orders"]["kind"] == "DataContract"
    assert org == ["data_product_read", "snapshot:prod", "offers:prod"]


def test_a_catalogs_pull_is_the_metadata_exports(org):
    with pytest.raises(ApiError) as refused:
        asyncio.run(router.data_product_contract("prod", "default", "wh.public.orders", object()))
    assert refused.value.status_code == 403


def test_a_rows_contract_is_read_alone_without_the_export(org):
    # REQ-2180: an offer's contract on a dataset is read on its own when it opens.
    contract = asyncio.run(
        router.data_product_contract_json("prod", "default", "wh.public.orders", object())
    )
    assert contract["kind"] == "DataContract"
    assert contract["schema"][0]["physicalName"] == "wh.public.orders"
    assert org == ["data_product_read", "snapshot:prod", "offers:prod"]


def test_a_port_with_no_contract_names_the_ports_there_are(org):
    with pytest.raises(ApiError) as missing:
        asyncio.run(
            router.data_product_contract_json("prod", "default", "wh.public.nope", object())
        )
    assert missing.value.status_code == 404
    assert "default/wh.public.orders" in missing.value.detail


@pytest.fixture
def versions_of(org, monkeypatch):
    one = _contract(["id", "note"])
    two = _contract(["id"], one)
    export = OfferExport(
        name="default",
        role_id="viewer",
        data_level="full",
        dq_results=True,
        profiles=False,
        open=False,
        duration_days=None,
        contracts={"wh.public.orders": two},
    )

    async def offer_exports(product_id):
        org.append(f"offers:{product_id}")
        return [export]

    monkeypatch.setattr(router, "offer_exports", offer_exports)
    monkeypatch.setattr(router, "require_active_org_id", lambda r: ORG_ID)
    return org


def test_a_rows_versions_are_read_with_when_and_by_whom_and_their_differences(versions_of):
    versions = asyncio.run(
        router.data_product_contract_versions("prod", "default", "wh.public.orders", object())
    )
    assert [v["version"] for v in versions] == ["1.0.0", "2.0.0"]
    assert versions[0]["changes"] == []
    assert versions[1]["changes"] == [
        {
            "kind": "field_removed",
            "subject": "note",
            "before": "integer",
            "after": None,
            "breaking": True,
        }
    ]
    assert versions[1]["saved_at"] is not None and versions[1]["saved_by"] is None
    assert versions_of == ["data_product_read", "offers:prod"]


def test_a_port_with_no_contract_has_no_versions(versions_of):
    with pytest.raises(ApiError) as missing:
        asyncio.run(
            router.data_product_contract_versions("prod", "default", "wh.public.nope", object())
        )
    assert missing.value.status_code == 404
    assert "wh.public.nope" in missing.value.detail
