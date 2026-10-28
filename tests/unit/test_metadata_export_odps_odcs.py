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
from provisa.core import data_contracts
from provisa.core.models import (
    Column,
    ContractColumn,
    ContractSlaProperty,
    ContractTerms,
    DataContract,
    DataProduct,
    UniqueConstraint,
    WriteGrant,
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
    # A masked column, or one hidden from some readers, is restricted: that a rule applies,
    # never the rule itself (REQ-2118). Its table's row rule rides as the object's tag.
    assert columns["ssn"]["classification"] == "restricted"
    assert columns["margin"]["classification"] == "restricted"
    assert "classification" not in columns["id"]
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
    # ... and it is named, never left out silently (REQ-2118).
    custom = {p["property"]: p["value"] for p in document["customProperties"]}
    assert custom["membersWithoutContract"] == ["customers"]


def test_a_checker_table_is_no_member_without_a_contract():
    """A checker table belongs to its product with no contract by design (REQ-2157)."""
    config = governed_config()
    config.tables[1].contract = None  # customers, standing in for a checker table
    config.data_products = [DataProduct(id="prod", domain_id="sales", name="Customer 360")]
    snapshot = build_snapshot(config, org_id=ORG_ID, dialect="postgres", contexts={})
    (product,) = snapshot.data_products
    orders, customers = (
        next(t for t in snapshot.tables if t.name == n) for n in ("orders", "customers")
    )
    snapshot.assertions.append(_check(orders, customers, owner="prod"))
    document = odps_document(snapshot, product)
    assert "customProperties" not in document


def test_a_contract_names_the_product_it_belongs_to_in_its_own_field():
    snapshot, product = _snapshot()
    orders = next(t for t in snapshot.tables if t.name == "orders")
    contract = odcs_contract(snapshot, product, orders)
    assert contract["dataProduct"] == product.name
    assert "customProperties" not in contract  # nothing declared rides there


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


def _check(observed, results, *, owner: str | None, column: str | None = None):
    from provisa.api.metadata_export.model import AssetKind, AssetRef, DataQualityAssertion

    parts = observed.ref.parts + ((column,) if column else ())
    return DataQualityAssertion(
        asset=AssetRef(kind=AssetKind.COLUMN if column else AssetKind.TABLE, parts=parts),
        checker="soda",
        check_type="missing",
        definition="missing_count(id) = 0",
        severity="fail",
        results_table=results.ref,
        owner_product_id=owner,
    )


def _two_products():
    """orders is upstream's output port, under its declared contract; prod's view order_totals
    reads it, beside prod's own customers."""
    config = governed_config()
    config.tables[0].product_id = "upstream"
    config.tables[0].contract = ORDERS_CONTRACT
    config.data_products = [
        DataProduct(id="upstream", domain_id="sales", name="Orders"),
        DataProduct(id="prod", domain_id="sales", name="Customer 360"),
    ]
    snapshot = build_snapshot(config, org_id=ORG_ID, dialect="postgres", contexts={})
    product = next(p for p in snapshot.data_products if p.id == "prod")
    return snapshot, product


def test_an_input_another_product_promises_is_an_input_port_with_the_products_expectations():
    snapshot, product = _two_products()
    orders = next(t for t in snapshot.tables if t.name == "orders")
    customers = next(t for t in snapshot.tables if t.name == "customers")
    snapshot.assertions += [
        _check(orders, customers, owner="prod", column="id"),  # prod's expectation of orders
        _check(orders, customers, owner="upstream"),  # upstream's promise: not prod's
    ]
    document = odps_document(snapshot, product)

    assert _errors(ODPS, document) == []
    assert document["inputPorts"] == [
        {
            "name": "orders",
            "version": "2.0.0",  # versioned by the upstream contract
            "contractId": contract_id(orders),
            "customProperties": [
                {
                    "property": "expectations",
                    "value": [
                        {
                            "checkType": "missing",
                            "definition": "missing_count(id) = 0",
                            "severity": "fail",
                            "column": "id",
                            "outcome": "never_run",
                        }
                    ],
                }
            ],
        }
    ]
    ports = {port["name"]: port for port in document["outputPorts"]}
    assert ports["order_totals"]["inputContracts"] == [
        {"id": contract_id(orders), "version": "2.0.0"}
    ]
    assert "inputContracts" not in ports["customers"]


def test_the_document_carries_its_team_tags_tenant_creation_and_management_ports():
    from datetime import UTC, datetime

    from provisa.api.metadata_export.model import ModelTag

    config = governed_config()
    config.data_products = [
        DataProduct(
            id="prod",
            domain_id="sales",
            name="Customer 360",
            owner_role="data_owner",
            team_role="analyst",
        )
    ]
    snapshot = build_snapshot(
        config,
        org_id=ORG_ID,
        dialect="postgres",
        contexts={},
        documentation_base_url="https://app.example.com",
        owner_emails={"data_owner": ("owner@example.com",), "analyst": ("ana@example.com",)},
        product_created={"prod": datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)},
    )
    (product,) = snapshot.data_products
    orders = next(t for t in snapshot.tables if t.name == "orders")
    snapshot.model_tags.append(ModelTag(tag_id="finance", is_system=False, asset=orders.ref))
    document = odps_document(snapshot, product)

    assert _errors(ODPS, document) == []
    assert document["tenant"] == snapshot.org_id
    assert document["productCreatedTs"] == "2026-01-02T03:04:05Z"
    assert document["tags"] == ["finance"]
    assert document["team"] == {
        "name": "analyst",
        "description": "Owner role data_owner; team role analyst",
        "members": [
            {"username": "owner@example.com", "role": "owner"},
            {"username": "ana@example.com", "role": "team"},
        ],
    }
    page = product.documentation_url
    assert page is not None
    assert document["authoritativeDefinitions"] == [
        {"type": "businessDefinition", "url": page, "description": "The data product's page"}
    ]
    origin = page.split("/data-products?")[0]
    assert [(p["name"], p["content"], p["url"]) for p in document["managementPorts"]] == [
        ("page", "discoverability", page),
        ("lineage", "observability", f"{origin}/data-products/prod/lineage"),
        ("admin", "control", f"{origin}/admin/graphql"),
    ]
    # None of these rides as a custom property.
    assert "customProperties" not in document


# --- REQ-2118 (amended 2026-10-28): every ODCS element Provisa knows is derived ----------------


def _orders(snapshot):
    return next(t for t in snapshot.tables if t.name == "orders")


def test_the_schema_object_is_addressed_and_its_keys_and_types_are_derived():
    config = governed_config()
    orders = config.tables[0]
    orders.columns[0].is_primary_key = True
    orders.columns.append(Column(name="email", data_type="varchar(120)", visible_to=["admin"]))
    orders.columns.append(Column(name="ip", data_type="text", subtype="inet", visible_to=["admin"]))
    orders.columns.append(Column(name="total", data_type="numeric(10,2)", visible_to=["admin"]))
    orders.columns.append(
        Column(name="placed", data_type="timestamp with time zone", visible_to=["admin"])
    )
    orders.columns.append(Column(name="n", data_type="bigint", visible_to=["admin"]))
    orders.unique_constraints = [UniqueConstraint(name="orders_email", columns=["email"])]
    orders.contract = data_contracts.draft({c.name: c.data_type for c in orders.columns})
    config.data_products = [DataProduct(id="prod", domain_id="sales", name="Customer 360")]
    snapshot = build_snapshot(config, org_id=ORG_ID, dialect="postgres", contexts={})
    (product,) = snapshot.data_products
    contract = odcs_contract(snapshot, product, _orders(snapshot))

    assert _errors(ODCS, contract) == []
    (schema,) = contract["schema"]
    assert schema["physicalName"] == "wh.public.orders"
    props = {p["name"]: p for p in schema["properties"]}
    assert (props["id"]["primaryKey"], props["id"]["unique"]) == (True, True)
    assert props["email"]["unique"] is True
    assert "unique" not in props["amount"]
    assert props["email"]["logicalTypeOptions"] == {"maxLength": 120}
    assert props["ip"]["logicalTypeOptions"] == {"format": "inet"}
    # numeric(10,2): two decimals, eight integer digits.
    assert props["total"]["logicalTypeOptions"] == {
        "multipleOf": 0.01,
        "exclusiveMaximum": 10**8,
        "exclusiveMinimum": -(10**8),
    }
    assert props["placed"]["logicalTypeOptions"] == {"timezone": True}
    assert props["n"]["logicalTypeOptions"] == {"format": "i64"}
    # The contract is created when the member's draft is, and says so.
    assert contract["contractCreatedTs"].endswith("Z")


def test_a_sensitive_column_is_confidential():
    from provisa.api.metadata_export.model import ModelTag

    snapshot, product = _snapshot()
    orders = _orders(snapshot)
    name = next(c for c in orders.columns if c.name == "amount")
    snapshot.model_tags.append(ModelTag(tag_id="pii", is_system=True, asset=name.ref))
    contract = odcs_contract(snapshot, product, orders)
    props = {p["name"]: p for p in contract["schema"][0]["properties"]}
    assert props["amount"]["classification"] == "confidential"


def test_a_view_members_columns_name_their_sources_and_transforms():
    snapshot, product = _snapshot()
    totals = next(t for t in snapshot.tables if t.name == "order_totals")
    contract = odcs_contract(snapshot, product, totals)
    assert _errors(ODCS, contract) == []
    props = {p["name"]: p for p in contract["schema"][0]["properties"]}
    assert props["net"]["transformSourceObjects"] == ["wh.public.orders.amount"]
    assert props["net"]["transformLogic"]
    # A table read from a source is derived by nothing.
    orders = odcs_contract(snapshot, product, _orders(snapshot))
    assert all("transformSourceObjects" not in p for p in orders["schema"][0]["properties"])


def test_a_member_names_its_foreign_keys():
    snapshot, product = _snapshot()
    contract = odcs_contract(snapshot, product, _orders(snapshot))
    assert contract["schema"][0]["relationships"] == [
        {"type": "foreignKey", "from": "orders.customer_id", "to": "customers.id"}
    ]


def test_roles_read_or_write_and_are_approved_by_the_owners():
    config = governed_config()
    config.tables[0].write_grants = [WriteGrant(role="admin", operation="insert", columns=["id"])]
    config.data_products = [
        DataProduct(id="prod", domain_id="sales", name="Customer 360", owner_role="data_owner")
    ]
    snapshot = build_snapshot(
        config,
        org_id=ORG_ID,
        dialect="postgres",
        contexts={},
        owner_emails={"data_owner": ("owner@example.com", "lead@example.com")},
    )
    (product,) = snapshot.data_products
    contract = odcs_contract(snapshot, product, _orders(snapshot))
    assert _errors(ODCS, contract) == []
    roles = {r["role"]: r for r in contract["roles"]}
    assert roles["admin"]["access"] == "write"
    assert roles["itest_analyst"]["access"] == "read"
    assert {r["firstLevelApprovers"] for r in roles.values()} == {
        "owner@example.com, lead@example.com"
    }


def test_servers_are_the_endpoints_a_consumer_reads_it_through():
    from provisa.api.metadata_export.model import ServingEndpoints

    config = governed_config()
    config.data_products = [DataProduct(id="prod", domain_id="sales", name="Customer 360")]
    snapshot = build_snapshot(
        config,
        org_id=ORG_ID,
        dialect="postgres",
        contexts={},
        endpoints=ServingEndpoints(
            hostname="data.example.com",
            database="provisa",
            http_origin="https://data.example.com",
            pgwire_port=5439,
            flight_port=8815,
        ),
    )
    (product,) = snapshot.data_products
    contract = odcs_contract(snapshot, product, _orders(snapshot))
    assert _errors(ODCS, contract) == []
    servers = {s["server"]: s for s in contract["servers"]}
    assert servers["pgwire"] == {
        "server": "pgwire",
        "type": "postgres",
        "description": "PostgreSQL wire protocol",
        "host": "data.example.com",
        "port": 5439,
        "database": "provisa",
        "schema": "sales",
    }
    assert (servers["flight-sql"]["type"], servers["flight-sql"]["port"]) == ("custom", 8815)
    assert servers["graphql"]["location"] == "https://data.example.com/data/graphql"
    assert servers["rest"]["location"] == "https://data.example.com/data/rest/sales/orders"


def test_a_pgwire_listener_not_started_is_no_server():
    from provisa.api.metadata_export.model import ServingEndpoints

    snapshot, product = _snapshot()
    snapshot.endpoints = ServingEndpoints(
        hostname="h", database="provisa", http_origin="https://h", flight_port=8815
    )
    contract = odcs_contract(snapshot, product, _orders(snapshot))
    assert {s["server"] for s in contract["servers"]} == {"flight-sql", "graphql", "rest"}


def test_the_contract_carries_the_products_tags():
    from provisa.api.metadata_export.model import ModelTag

    snapshot, product = _snapshot()
    orders = _orders(snapshot)
    snapshot.model_tags.append(ModelTag(tag_id="finance", is_system=False, asset=orders.ref))
    contract = odcs_contract(snapshot, product, orders)
    assert contract["tags"] == ["finance"]


def _soda(observed, results, check_type, definition, *, column=None, owner="prod"):
    check = _check(observed, results, owner=owner, column=column)
    check.check_type, check.definition = check_type, definition
    return check


def test_quality_holds_only_the_products_promises_typed_by_the_odcs_library():
    snapshot, product = _snapshot()
    orders = _orders(snapshot)
    customers = next(t for t in snapshot.tables if t.name == "customers")
    snapshot.assertions += [
        _soda(orders, customers, "missing", "", column="id"),
        _soda(orders, customers, "row_count", "threshold:\n  must_be_greater_than: 10"),
        _soda(
            orders, customers, "duplicate", "threshold:\n  must_be: 0\n  level: warn", column="ssn"
        ),
        _soda(orders, customers, "aggregate", "function: avg", column="amount"),
        # Another product's check on the member, and one no product owns: neither is a promise.
        _soda(orders, customers, "missing", "", column="amount", owner="upstream"),
        _soda(orders, customers, "row_count", "", owner=None),
    ]
    contract = odcs_contract(snapshot, product, orders)

    assert _errors(ODCS, contract) == []
    (schema,) = contract["schema"]
    props = {p["name"]: p for p in schema["properties"]}
    (missing,) = props["id"]["quality"]
    assert {k: missing[k] for k in ("type", "metric", "mustBe", "dimension")} == {
        "type": "library",
        "metric": "missingValues",
        "mustBe": 0,
        "dimension": "completeness",
    }
    (duplicate,) = props["ssn"]["quality"]
    assert (duplicate["metric"], duplicate["dimension"]) == ("duplicateValues", "uniqueness")
    (aggregate,) = props["amount"]["quality"]  # the other product's is not here
    assert (aggregate["type"], aggregate["engine"]) == ("custom", "soda")
    (row_count,) = schema["quality"]  # the unowned check is not here
    assert (row_count["metric"], row_count["mustBeGreaterThan"]) == ("rowCount", 10)


@pytest.mark.parametrize(
    ("check_type", "kwargs", "expected"),
    [
        (
            "expect_column_values_to_not_be_null",
            {"column": "id"},
            {"metric": "nullValues", "mustBe": 0},
        ),
        (
            "expect_column_values_to_be_unique",
            {"column": "id"},
            {"metric": "duplicateValues", "mustBe": 0},
        ),
        (
            "expect_table_row_count_to_be_between",
            {"min_value": 1, "max_value": 9},
            {"metric": "rowCount", "mustBeBetween": [1, 9]},
        ),
        (
            "expect_column_values_to_be_in_set",
            {"column": "id", "value_set": [1, 2]},
            {"metric": "invalidValues", "mustBe": 0, "arguments": {"validValues": [1, 2]}},
        ),
        ("expect_column_values_to_not_be_null", {"column": "id", "mostly": 0.9}, None),
    ],
)
def test_a_great_expectations_check_is_a_library_rule_where_it_is_one(check_type, kwargs, expected):
    from provisa.api.metadata_export.odps import _quality_rule

    snapshot, _ = _snapshot()
    orders = _orders(snapshot)
    check = _check(orders, orders, owner="prod")
    check.checker, check.check_type, check.definition = (
        "great_expectations",
        check_type,
        json.dumps(kwargs),
    )
    rule = _quality_rule(check)
    if expected is None:
        assert (rule["type"], rule["engine"]) == ("custom", "great-expectations")
    else:
        assert {k: rule[k] for k in expected} == expected
        assert rule["type"] == "library"
