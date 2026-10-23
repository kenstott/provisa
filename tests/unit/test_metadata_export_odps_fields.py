# Copyright (c) 2026 Kenneth Stott
# Canary: 5f2d8b61-0a47-4c39-9e13-7b6c2a0d4e85
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2118: every ODPS field of a data product reaches each target, in its own terms.

Request shaping only: whether a target stores them is its end-to-end test's question.
"""

from __future__ import annotations

import json

from provisa.api.metadata_export import build_snapshot
from provisa.api.metadata_export.atlas import _data_product_entities, _GuidCounter
from provisa.api.metadata_export.datahub import _data_product_proposals
from provisa.api.metadata_export.openmetadata import _data_product_entity
from provisa.core.models import DataProduct
from tests.integration.metadata_export_fixture import ORG_ID, governed_config

FIELDS = {
    "purpose": "One view of each customer",
    "ownerRole": "data_owner",
    "teamRole": "analyst",
    "limitations": "No orders before 2020",
    "usage": "Join on customer_id",
    "version": "1.2.0",
    "status": "live",
    "sla": "Refreshed hourly",
    "support": "The data team's channel",
    "supportContact": "data-team@example.com",
    "custom.cost_center": "CC-42",
}


def _product():
    config = governed_config()
    config.data_products = [
        DataProduct(
            id="prod",
            domain_id="sales",
            name="Customer 360",
            owner_role="data_owner",
            team_role="analyst",
            purpose=FIELDS["purpose"],
            limitations=FIELDS["limitations"],
            usage=FIELDS["usage"],
            version=FIELDS["version"],
            status=FIELDS["status"],
            sla=FIELDS["sla"],
            support=FIELDS["support"],
            support_contact=FIELDS["supportContact"],
            custom_properties={"cost_center": "CC-42"},
        )
    ]
    snapshot = build_snapshot(config, org_id=ORG_ID, dialect="postgres", contexts={})
    (product,) = snapshot.data_products
    return snapshot, product


def test_the_product_carries_every_odps_field():
    _, product = _product()
    assert product.odps_fields() == FIELDS


def test_an_unset_field_is_left_out():
    _, product = _product()
    product.limitations = ""
    product.sla = None
    assert "limitations" not in product.odps_fields()
    assert "sla" not in product.odps_fields()


def test_datahub_carries_them_as_custom_properties():
    snapshot, _ = _product()
    (props,) = [p for p in _data_product_proposals(snapshot) if p.kind == "data_product"]
    custom = props.aspect["customProperties"]
    assert {k[len("odps.") :]: v for k, v in custom.items() if k.startswith("odps.")} == FIELDS


def test_atlas_carries_them_as_one_document():
    snapshot, _ = _product()
    table_guids = {t.ref.fqn(): f"-{i}" for i, t in enumerate(snapshot.tables, start=10)}
    (entity,) = _data_product_entities(snapshot, _GuidCounter(), "provisa_table", table_guids)
    assert json.loads(entity.attributes["provisaOdps"]) == FIELDS


def test_a_target_with_only_a_description_reads_them_after_the_purpose():
    _, product = _product()
    body = _data_product_entity(product).body
    assert body["description"].startswith(FIELDS["purpose"] + "\n\n")
    for line in (
        "- Owner role: data_owner",
        "- Limitations: No orders before 2020",
        "- SLA: Refreshed hourly",
        "- Support contact: data-team@example.com",
        "- cost_center: CC-42",
    ):
        assert line in body["description"]
