# Copyright (c) 2026 Kenneth Stott
# Canary: 8d4c2f17-5b03-4e96-a1d8-7c0e3b9f6a25
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2118, REQ-2158: a data product's ODPS document and its members' declared ODCS contracts.

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
from provisa.core.models import (
    ContractColumn,
    ContractSlaProperty,
    ContractTerms,
    DataContract,
    DataProduct,
)
from tests.integration.metadata_export_fixture import MASK_PATTERN, ORG_ID, governed_config

_SCHEMAS = Path(__file__).resolve().parents[1] / "fixtures" / "standards"


def _validator(name: str) -> Draft201909Validator:
    return Draft201909Validator(json.loads((_SCHEMAS / name).read_text()))


ODPS = _validator("odps-json-schema-v1.0.0.json")
ODCS = _validator("odcs-json-schema-v3.1.0.json")


#: orders' declared contract: a release of its own, apart from the product's version.
ORDERS_CONTRACT = DataContract(
    version="2.0.0",
    status="active",
    description="Every order placed",
    terms=ContractTerms(usage="Join on id", limitations="No refunds", notice_period="P3M"),
    sla=[ContractSlaProperty(property="availability", value=99.9, unit="%")],
    columns=[
        ContractColumn(name="id", type="integer", required=True),
        ContractColumn(name="amount", type="numeric", description="The order's total"),
        ContractColumn(name="ssn", type="text"),
        ContractColumn(name="margin", type="numeric"),
    ],
)


def _snapshot(orders_contract: DataContract | None = None, **product):
    config = governed_config()
    config.tables[0].cache_ttl = 3600  # orders: kept an hour
    if orders_contract is not None:
        config.tables[0].contract = orders_contract
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
    snapshot, product = _snapshot(ORDERS_CONTRACT, version="1.2.0", status="live")
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
    # Each port is versioned by its contract, not by the product.
    assert ports["orders"]["version"] == "2.0.0"
    assert ports["customers"]["version"] == "0.1.0"
    # The product has no SLA of its own: each contract holds its member's.
    custom = {p["property"]: p["value"] for p in document["customProperties"]}
    assert custom == {"cost_center": "CC-42"}
    assert document["support"] == [
        {
            "channel": "support",
            "url": "mailto:data-team@example.com",
            "tool": "email",
            "description": "The data team's channel",
        }
    ]


def test_each_member_contract_is_valid_odcs_with_its_schema_freshness_and_tags():
    snapshot, product = _snapshot(ORDERS_CONTRACT, version="1.2.0")
    _, contracts = product_documents(snapshot, product)

    for contract in contracts.values():
        assert _errors(ODCS, contract) == []
    orders = contracts["wh.public.orders"]
    (schema,) = orders["schema"]
    columns = {p["name"]: p for p in schema["properties"]}
    assert columns["amount"]["logicalType"] == "number"
    # A promise's own description; else the member column's.
    assert columns["amount"]["description"] == "The order's total"
    assert columns["id"]["logicalType"] == "integer"
    assert columns["id"]["required"] is True
    # The contract promises its declared columns, and no other of the member's.
    assert set(columns) == {"id", "amount", "ssn", "margin"}
    # Governance rides as tags: that a rule applies, never the rule itself.
    assert columns["ssn"]["tags"] == ["masked"]
    assert columns["margin"]["tags"] == ["visibility_restricted"]
    assert schema["tags"] == ["rls_restricted"]
    assert MASK_PATTERN not in json.dumps(contracts)
    # Its freshness, then the service levels it declares.
    assert orders["slaProperties"] == [
        {
            "property": "latency",
            "value": 3600,
            "unit": "s",
            "description": "A read may trail its source by at most this long",
        },
        {"property": "availability", "value": 99.9, "unit": "%"},
    ]
    # A member that declares no freshness makes no freshness promise.
    assert "slaProperties" not in contracts["wh.public.customers"]
    assert (orders["version"], orders["status"]) == ("2.0.0", "active")
    assert orders["description"] == {
        "purpose": "Every order placed",
        "usage": "Join on id",
        "limitations": "No refunds",
    }
    assert {"property": "noticePeriod", "value": "P3M"} in orders["customProperties"]


def test_an_unversioned_product_says_so_and_an_unpublished_one_is_draft():
    snapshot, product = _snapshot()
    document, contracts = product_documents(snapshot, product)
    assert document["version"] == UNVERSIONED
    assert document["status"] == "draft"
    # Each member's first contract is a draft at 0.1.0, exported as one.
    assert {(c["version"], c["status"]) for c in contracts.values()} == {("0.1.0", "draft")}
    assert _errors(ODPS, document) == []


def test_a_member_with_no_contract_is_no_output_port():
    # A checker table the product owns is a member by product_id, with no contract (REQ-2157).
    config = governed_config()
    config.tables[1].contract = None  # customers
    config.data_products = [DataProduct(id="prod", domain_id="sales", name="Customer 360")]
    snapshot = build_snapshot(config, org_id=ORG_ID, dialect="postgres", contexts={})
    (product,) = snapshot.data_products
    document, contracts = product_documents(snapshot, product)
    assert {p["name"] for p in document["outputPorts"]} == {"orders", "order_totals"}
    assert "wh.public.customers" not in contracts


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


def test_a_retired_product_is_retired_with_no_output_ports():
    """REQ-1634 STATUS: retired, its members went to its successor; the document says so, with
    no member contracts, and is still a valid ODPS document."""
    config = governed_config()
    for table in config.tables:
        table.product_id = None  # moved to the successor
    config.data_products = [
        DataProduct(
            id="prod",
            domain_id="sales",
            name="Customer 360",
            purpose="One view of each customer",
            status="retired",
            status_reason="Replaced by Customer 360 v2.",
        )
    ]
    snapshot = build_snapshot(config, org_id=ORG_ID, dialect="postgres", contexts={})
    (product,) = snapshot.data_products
    assert product.members == ()
    document, contracts = product_documents(snapshot, product)
    assert document["status"] == "retired"
    assert contracts == {}
    assert _errors(ODPS, document) == []
