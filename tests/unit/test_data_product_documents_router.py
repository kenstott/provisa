# Copyright (c) 2026 Kenneth Stott
# Canary: 9a4e1c27-3b58-4f06-8d2e-7c1f0b6a5e93
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2158, REQ-2118: the product's Contracts panel reads every output port's ODCS contract as
JSON, asking what reading the product asks; a catalog's pull is the metadata export's."""

from __future__ import annotations

import asyncio

import pytest

from provisa.api.admin import data_product_documents_router as router
from provisa.api.errors import ApiError
from provisa.api.metadata_export import build_snapshot
from provisa.control_plane.entitlements import EntitlementError, Tier
from provisa.core.models import DataProduct
from tests.integration.metadata_export_fixture import ORG_ID, governed_config


@pytest.fixture
def org(monkeypatch):
    config = governed_config()
    config.data_products = [DataProduct(id="prod", domain_id="sales", name="Customer 360")]
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
    return asked


def test_the_panel_reads_every_contract_without_the_export(org):
    contracts = asyncio.run(router.data_product_contracts("prod", object()))
    assert set(contracts) == {"wh.public.orders", "wh.public.customers", "wh.public.order_totals"}
    assert contracts["wh.public.orders"]["kind"] == "DataContract"
    assert org == ["data_product_read", "snapshot:prod"]


def test_a_catalogs_pull_is_the_metadata_exports(org):
    with pytest.raises(ApiError) as refused:
        asyncio.run(router.data_product_contract("prod", "wh.public.orders", object()))
    assert refused.value.status_code == 403


def test_a_rows_contract_is_read_alone_without_the_export(org):
    # REQ-2158: a contract row of the product's list reads its own port's contract when it opens.
    contract = asyncio.run(router.data_product_contract_json("prod", "wh.public.orders", object()))
    assert contract["kind"] == "DataContract"
    assert contract["schema"][0]["physicalName"] == "wh.public.orders"
    assert org == ["data_product_read", "snapshot:prod"]


def test_a_port_with_no_contract_names_the_ports_there_are(org):
    with pytest.raises(ApiError) as missing:
        asyncio.run(router.data_product_contract_json("prod", "wh.public.nope", object()))
    assert missing.value.status_code == 404
    assert "wh.public.orders" in missing.value.detail
