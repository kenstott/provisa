# Copyright (c) 2026 Kenneth Stott
# Canary: 8d4c2f17-5b03-4e96-a1d8-7c0e3b9f6a25
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2118: a data product's ODPS document and its members' ODCS contracts.

Each document is validated against the standard's own JSON Schema (tests/fixtures/standards), so
what a catalog importing the standard reads is what the standard defines.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from jsonschema import Draft201909Validator

from provisa.api.metadata_export import build_snapshot
from provisa.api.metadata_export.odps import (
    UNVERSIONED,
    contract_id,
    logical_type,
    odcs_contract,
    odps_document,
    product_documents,
)
from provisa.core.models import DataProduct
from tests.integration.metadata_export_fixture import MASK_PATTERN, ORG_ID, governed_config

_SCHEMAS = Path(__file__).resolve().parents[1] / "fixtures" / "standards"


def _validator(name: str) -> Draft201909Validator:
    return Draft201909Validator(json.loads((_SCHEMAS / name).read_text()))


ODPS = _validator("odps-json-schema-v1.0.0.json")
ODCS = _validator("odcs-json-schema-v3.1.0.json")


def _snapshot(**product):
    config = governed_config()
    config.tables[0].cache_ttl = 3600  # orders: kept an hour
    config.data_products = [
        DataProduct(
            id="prod",
            domain_id="sales",
            name="Customer 360",
            owner_role="data_owner",
            team_role="analyst",
            purpose="One view of each customer",
            limitations="No orders before 2020",
            usage="Join on customer_id",
            sla="Refreshed hourly",
            support="The data team's channel",
            support_contact="data-team@example.com",
            custom_properties={"cost_center": "CC-42"},
            **product,
        )
    ]
    snapshot = build_snapshot(config, org_id=ORG_ID, dialect="postgres", contexts={})
    (built,) = snapshot.data_products
    return snapshot, built


def _errors(validator: Draft201909Validator, document: dict) -> list[str]:
    return [f"{list(e.absolute_path)}: {e.message}" for e in validator.iter_errors(document)]


def test_the_odps_document_is_valid_odps_and_names_each_members_contract():
    snapshot, product = _snapshot(version="1.2.0", status="live")
    document = odps_document(snapshot, product)

    assert _errors(ODPS, document) == []
    assert document["description"] == {
        "purpose": "One view of each customer",
        "limitations": "No orders before 2020",
        "usage": "Join on customer_id",
    }
    assert (document["version"], document["status"], document["domain"]) == (
        "1.2.0",
        "active",  # ODPS's word for a live product
        "sales",
    )
    ports = {port["name"]: port for port in document["outputPorts"]}
    assert set(ports) == {"orders", "customers", "order_totals"}
    orders = next(t for t in snapshot.tables if t.name == "orders")
    assert ports["orders"]["contractId"] == contract_id(orders)
    assert ports["order_totals"]["type"] == "view"
    # The SLA stays prose, beside the product's own custom properties.
    custom = {p["property"]: p["value"] for p in document["customProperties"]}
    assert custom == {"cost_center": "CC-42", "sla": "Refreshed hourly"}
    assert document["support"] == [
        {
            "channel": "support",
            "url": "mailto:data-team@example.com",
            "tool": "email",
            "description": "The data team's channel",
        }
    ]


def test_each_member_contract_is_valid_odcs_with_its_schema_freshness_and_tags():
    snapshot, product = _snapshot(version="1.2.0")
    _, contracts = product_documents(snapshot, product)

    for contract in contracts.values():
        assert _errors(ODCS, contract) == []
    orders = contracts["wh.public.orders"]
    (schema,) = orders["schema"]
    columns = {p["name"]: p for p in schema["properties"]}
    assert columns["amount"]["logicalType"] == "number"
    assert columns["amount"]["description"] == "Order total in cents"
    assert columns["id"]["logicalType"] == "integer"
    # Governance rides as tags: that a rule applies, never the rule itself.
    assert columns["ssn"]["tags"] == ["masked"]
    assert columns["margin"]["tags"] == ["visibility_restricted"]
    assert schema["tags"] == ["rls_restricted"]
    assert MASK_PATTERN not in json.dumps(contracts)
    assert orders["slaProperties"] == [
        {
            "property": "latency",
            "value": 3600,
            "unit": "s",
            "description": "A read may trail its source by at most this long",
        }
    ]
    # A member that declares no freshness makes no freshness promise.
    assert "slaProperties" not in contracts["wh.public.customers"]
    assert orders["version"] == "1.2.0"


def test_an_unversioned_product_says_so_and_an_unpublished_one_is_draft():
    snapshot, product = _snapshot()
    document, contracts = product_documents(snapshot, product)
    assert document["version"] == UNVERSIONED
    assert document["status"] == "draft"
    assert {c["version"] for c in contracts.values()} == {UNVERSIONED}
    assert _errors(ODPS, document) == []


def test_a_contract_names_the_product_it_belongs_to():
    snapshot, product = _snapshot()
    orders = next(t for t in snapshot.tables if t.name == "orders")
    contract = odcs_contract(snapshot, product, orders)
    assert {"property": "dataProduct", "value": product.semantic_uri} in contract[
        "customProperties"
    ]


@pytest.mark.parametrize(
    ("declared", "logical"),
    [
        ("integer", "integer"),
        ("bigint", "integer"),
        ("numeric(10,2)", "number"),
        ("double precision", "number"),
        ("text", "string"),
        ("varchar(20)", "string"),
        ("boolean", "boolean"),
        ("timestamp with time zone", "timestamp"),
        ("date", "date"),
        ("jsonb", "object"),
        ("text[]", "array"),
        ("geometry", None),
    ],
)
def test_a_declared_type_maps_to_its_logical_type_or_to_none(declared, logical):
    assert logical_type(declared) == logical
