# Copyright (c) 2026 Kenneth Stott
# Canary: 8d4c2f17-5b03-4e96-a1d8-7c0e3b9f6a25
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2118, REQ-2158, REQ-2180: a data product's ODPS document and the ODCS contracts its offers
give on its members (REQ-2180, CONTRACTS ARE DERIVED FROM THE OFFERS).

Each document is validated against the standard's own JSON Schema (tests/fixtures/standards), so
what a catalog importing the standard reads is what the standard defines.
"""

from __future__ import annotations

import dataclasses
import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
from jsonschema import Draft201909Validator

from provisa.api.admin.system_health import HTTP_SURFACES
from provisa.api.metadata_export import build_snapshot
from provisa.api.metadata_export.odps import (
    UNVERSIONED,
    OfferExport,
    contract_id,
    logical_type,
    odps_document,
    offer_contract,
    product_documents,
)
from provisa.api.metadata_export.publishing import serving_endpoints
from provisa.core import offer_contracts
from provisa.core.models import (
    Column,
    DataProduct,
    UniqueConstraint,
    WriteGrant,
)
from provisa.grpc.descriptor import SERVICE_NAME
from provisa.security.rights import SQL_ONLY_TABLES
from tests.integration.metadata_export_fixture import MASK_PATTERN, ORG_ID, PROD, governed_config

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
            support="The data team's channel",
            support_contact="data-team@example.com",
            custom_properties={"cost_center": "CC-42"},
            **product,
            tables=list(PROD.tables),
        )
    ]
    snapshot = build_snapshot(config, org_id=ORG_ID, dialect="postgres", contexts={})
    (built,) = snapshot.data_products
    return snapshot, built


_AT = datetime(2026, 11, 11, tzinfo=UTC)


def _offers(snapshot, product, *, skip=(), masked=None, unread=(), freshness=None, **terms):
    """The product's default offer as the export writes it (REQ-2180): its contract on each member
    but ``skip`` (names), each column read but ``unread``, ``masked`` {column: mask kind}."""
    from provisa.core.models import DataProductOffer

    offer = DataProductOffer(name="default", role_id="viewer", **terms)
    masked = masked or {}
    members = {m.fqn() for m in product.members}
    contracts = {
        table.ref.fqn(): offer_contracts.recorded(
            None,
            offer_contracts.terms(
                offer,
                [
                    offer_contracts.SeenColumn(
                        c.name, c.data_type, read=c.name not in unread, mask=masked.get(c.name)
                    )
                    for c in table.columns
                ],
                row_rules=_ROW_RULES.get(table.name, []),
                sample_views=[],
                checks=[],
                profiles=[f"{table.name}_profile"],
            ),
            at=_AT,
        )
        for table in snapshot.tables
        if table.ref.fqn() in members and table.name not in skip
    }
    return [
        OfferExport(
            name=offer.name,
            role_id=offer.role_id,
            data_level=offer.data_level,
            dq_results=offer.dq_results,
            profiles=offer.profiles,
            open=offer.open,
            duration_days=offer.duration_days,
            contracts=contracts,
            freshness=freshness,
        )
    ]


_ROW_RULES = {"orders": ["only the orders of the reader's region"]}


def _with(snapshot, product, offers):
    """``snapshot`` with ``offers`` as the product's (REQ-2180: the snapshot carries them)."""
    snapshot.offers = {**snapshot.offers, product.id: tuple(offers)}
    return snapshot


def _contract(snapshot, product, table, **terms):
    """The contract the product's default offer, reading every column, gives on ``table``."""
    (offer,) = _offers(snapshot, product, **terms)
    return offer_contract(snapshot, product, table, offer)


def _errors(validator: Draft201909Validator, document: dict) -> list[str]:
    return [f"{list(e.absolute_path)}: {e.message}" for e in validator.iter_errors(document)]


def test_the_odps_document_is_valid_odps_and_names_each_offers_contract():
    snapshot, product = _snapshot(version="1.2.0", status="live")
    document = odps_document(_with(snapshot, product, _offers(snapshot, product)), product)

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
    # REQ-2180: a port per offer and dataset, naming the contract the offer gives on it; the
    # per-table contract once written by hand is gone.
    ports = {port["name"]: port for port in document["outputPorts"]}
    assert set(ports) == {"orders (default)", "customers (default)", "order_totals (default)"}
    orders = next(t for t in snapshot.tables if t.name == "orders")
    assert ports["orders (default)"]["contractId"] == f"{contract_id(orders)}/offers/default"
    assert ports["order_totals (default)"]["type"] == "view"
    # Each port is versioned by its offer's contract, not by the product.
    assert {p["version"] for p in ports.values()} == {"1.0.0"}
    terms = {p["property"]: p["value"] for p in ports["orders (default)"]["customProperties"]}
    assert terms["role"] == "viewer"
    assert terms["dataLevel"] == "full"
    assert terms["rowRules"] == ["only the orders of the reader's region"]
    custom = {p["property"]: p["value"] for p in document["customProperties"]}
    assert custom["cost_center"] == "CC-42"
    (offer,) = custom["offers"]
    assert (offer["name"], offer["role"], offer["dataQualityResults"], offer["profiles"]) == (
        "default",
        "viewer",
        True,
        False,
    )
    assert {c["id"] for c in offer["contracts"]} == {
        p["contractId"] for p in document["outputPorts"]
    }
    assert document["support"] == [
        {
            "channel": "support",
            "url": "mailto:data-team@example.com",
            "tool": "email",
            "description": "The data team's channel",
        }
    ]


def test_each_offer_contract_is_valid_odcs_with_its_schema_freshness_and_tags():
    snapshot, product = _snapshot(version="1.2.0")
    _, contracts = product_documents(_with(snapshot, product, _offers(snapshot, product)), product)

    for contract in contracts.values():
        assert _errors(ODCS, contract) == []
    orders = contracts["default/wh.public.orders"]
    (schema,) = orders["schema"]
    columns = {p["name"]: p for p in schema["properties"]}
    assert columns["amount"]["logicalType"] == "number"
    # REQ-2158: a contract's schema is its member's columns, each described as the member is.
    assert columns["amount"]["description"] == "Order total in cents"
    assert columns["id"]["logicalType"] == "integer"
    assert "required" not in columns["id"]
    assert set(columns) == {"id", "amount", "customer_id", "ssn", "margin"}
    # A masked column, or one hidden from some readers, is restricted: that a rule applies,
    # never the rule itself (REQ-2118). Its table's row rule rides as the object's tag.
    assert columns["ssn"]["classification"] == "restricted"
    assert columns["margin"]["classification"] == "restricted"
    assert "classification" not in columns["id"]
    assert schema["tags"] == ["rls_restricted"]
    assert MASK_PATTERN not in json.dumps(contracts)
    # Its freshness: the member's, where the product states none.
    assert orders["slaProperties"] == [
        {
            "property": "latency",
            "value": 3600,
            "unit": "s",
            "description": "A read may trail its source by at most this long",
        },
    ]
    # A member that declares no freshness makes no freshness promise.
    assert "slaProperties" not in contracts["default/wh.public.customers"]
    # REQ-2180: what every contract shares is written once on the product.
    assert (orders["version"], orders["status"]) == ("1.0.0", "draft")
    assert orders["description"] == {
        "purpose": "One view of each customer",
        "usage": "Join on customer_id",
        "limitations": "No orders before 2020",
    }
    assert "noticePeriod" not in json.dumps(orders["customProperties"])


def test_an_unversioned_product_says_so_and_an_unpublished_one_is_draft():
    snapshot, product = _snapshot()
    document, contracts = product_documents(
        _with(snapshot, product, _offers(snapshot, product)), product
    )
    assert document["version"] == UNVERSIONED
    assert document["status"] == "draft"
    # Each offer contract's first version, a draft while its product is.
    assert {(c["version"], c["status"]) for c in contracts.values()} == {("1.0.0", "draft")}
    assert _errors(ODPS, document) == []


def test_a_member_no_offer_gives_a_contract_on_is_no_output_port():
    config = governed_config()
    config.data_products = [
        DataProduct(id="prod", domain_id="sales", name="Customer 360", tables=list(PROD.tables))
    ]
    snapshot = build_snapshot(config, org_id=ORG_ID, dialect="postgres", contexts={})
    (product,) = snapshot.data_products
    offers = _offers(snapshot, product, skip={"customers"})
    document, contracts = product_documents(_with(snapshot, product, offers), product)
    assert {p["name"] for p in document["outputPorts"]} == {
        "orders (default)",
        "order_totals (default)",
    }
    assert "default/wh.public.customers" not in contracts
    # ... and it is named, never left out silently (REQ-2118).
    custom = {p["property"]: p["value"] for p in document["customProperties"]}
    assert custom["membersWithoutContract"] == ["customers"]


def test_a_checker_table_is_no_member_without_a_contract():
    """A checker table belongs to its product with no contract by design (REQ-2157)."""
    config = governed_config()
    config.data_products = [
        DataProduct(id="prod", domain_id="sales", name="Customer 360", tables=list(PROD.tables))
    ]
    snapshot = build_snapshot(config, org_id=ORG_ID, dialect="postgres", contexts={})
    (product,) = snapshot.data_products
    orders, customers = (
        next(t for t in snapshot.tables if t.name == n) for n in ("orders", "customers")
    )
    snapshot.assertions.append(_check(orders, customers, owner="prod"))
    document = odps_document(
        _with(snapshot, product, _offers(snapshot, product, skip={"customers"})), product
    )
    custom = {p["property"] for p in document["customProperties"]}
    assert custom == {"offers"}


def test_a_contract_names_the_product_it_belongs_to_in_its_own_field():
    snapshot, product = _snapshot()
    orders = next(t for t in snapshot.tables if t.name == "orders")
    contract = _contract(snapshot, product, orders)
    assert contract["dataProduct"] == product.name
    # Nothing written by hand rides as a custom property: the offer's terms alone do (REQ-2180).
    assert {p["property"] for p in contract["customProperties"]} == {
        "offer",
        "role",
        "dataLevel",
        "dataQualityResults",
        "rowRules",
    }


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
    config.data_products = [
        DataProduct(
            id="prod",
            domain_id="sales",
            name="Customer 360",
            purpose="One view of each customer",
            status="retired",
            status_reason="Replaced by Customer 360 v2.",
            tables=[],  # its members moved to the successor
        )
    ]
    snapshot = build_snapshot(config, org_id=ORG_ID, dialect="postgres", contexts={})
    (product,) = snapshot.data_products
    assert product.members == ()
    document, contracts = product_documents(_with(snapshot, product, []), product)
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
        owner_product_ids=(owner,) if owner is not None else (),
    )


def _two_products(*, upstream_offers: bool = True):
    """orders is upstream's output port, under the contract its default offer gives, at its
    second major version; prod's view order_totals reads it, beside prod's own customers."""
    config = governed_config()
    config.data_products = [
        DataProduct(id="upstream", domain_id="sales", name="Orders", tables=["orders"]),
        DataProduct(
            id="prod", domain_id="sales", name="Customer 360", tables=["customers", "order_totals"]
        ),
    ]
    snapshot = build_snapshot(config, org_id=ORG_ID, dialect="postgres", contexts={})
    product = next(p for p in snapshot.data_products if p.id == "prod")
    upstream = next(p for p in snapshot.data_products if p.id == "upstream")
    if upstream_offers:
        (default,) = _offers(snapshot, upstream)
        orders = _orders(snapshot).ref.fqn()
        released = default.contracts[orders].model_copy(update={"version": "2.0.0"})
        default = dataclasses.replace(default, contracts={orders: released})
        _with(snapshot, upstream, [default])
    _with(snapshot, product, _offers(snapshot, product))
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
    # REQ-2180: an input port names the contract the upstream product's default offer gives.
    assert document["inputPorts"] == [
        {
            "name": "orders",
            "version": "2.0.0",  # versioned by the upstream contract
            "contractId": f"{contract_id(orders)}/offers/default",
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
    assert ports["order_totals (default)"]["inputContracts"] == [
        {"id": f"{contract_id(orders)}/offers/default", "version": "2.0.0"}
    ]
    assert "inputContracts" not in ports["customers (default)"]


def test_an_input_whose_product_offers_no_default_contract_is_named_and_no_port():
    snapshot, product = _two_products(upstream_offers=False)
    document = odps_document(snapshot, product)
    assert document["inputPorts"] == []
    custom = {p["property"]: p["value"] for p in document["customProperties"]}
    assert custom["inputsWithoutContract"] == ["orders"]
    ports = {port["name"]: port for port in document["outputPorts"]}
    assert "inputContracts" not in ports["order_totals (default)"]


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
            tables=list(PROD.tables),
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
    document = odps_document(_with(snapshot, product, _offers(snapshot, product)), product)

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
    # None of these rides as a custom property: the offers alone do (REQ-2180).
    assert {p["property"] for p in document["customProperties"]} == {"offers"}


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
    config.data_products = [
        DataProduct(id="prod", domain_id="sales", name="Customer 360", tables=list(PROD.tables))
    ]
    snapshot = build_snapshot(config, org_id=ORG_ID, dialect="postgres", contexts={})
    (product,) = snapshot.data_products
    contract = _contract(snapshot, product, _orders(snapshot))

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
    # The contract is created when its offer's contract on the member was first derived.
    assert contract["contractCreatedTs"] == "2026-11-11T00:00:00Z"


def test_a_sensitive_column_is_confidential():
    from provisa.api.metadata_export.model import ModelTag

    snapshot, product = _snapshot()
    orders = _orders(snapshot)
    name = next(c for c in orders.columns if c.name == "amount")
    snapshot.model_tags.append(ModelTag(tag_id="pii", is_system=True, asset=name.ref))
    contract = _contract(snapshot, product, orders)
    props = {p["name"]: p for p in contract["schema"][0]["properties"]}
    assert props["amount"]["classification"] == "confidential"


def test_a_view_members_columns_name_their_sources_and_transforms():
    snapshot, product = _snapshot()
    totals = next(t for t in snapshot.tables if t.name == "order_totals")
    contract = _contract(snapshot, product, totals)
    assert _errors(ODCS, contract) == []
    props = {p["name"]: p for p in contract["schema"][0]["properties"]}
    assert props["net"]["transformSourceObjects"] == ["wh.public.orders.amount"]
    assert props["net"]["transformLogic"]
    # A table read from a source is derived by nothing.
    orders = _contract(snapshot, product, _orders(snapshot))
    assert all("transformSourceObjects" not in p for p in orders["schema"][0]["properties"])


def test_a_member_names_its_foreign_keys():
    snapshot, product = _snapshot()
    contract = _contract(snapshot, product, _orders(snapshot))
    assert contract["schema"][0]["relationships"] == [
        {"type": "foreignKey", "from": "orders.customer_id", "to": "customers.id"}
    ]


def test_roles_read_or_write_and_are_approved_by_the_owners():
    config = governed_config()
    config.tables[0].write_grants = [WriteGrant(role="admin", operation="insert", columns=["id"])]
    config.data_products = [
        DataProduct(
            id="prod",
            domain_id="sales",
            name="Customer 360",
            owner_role="data_owner",
            tables=list(PROD.tables),
        )
    ]
    snapshot = build_snapshot(
        config,
        org_id=ORG_ID,
        dialect="postgres",
        contexts={},
        owner_emails={"data_owner": ("owner@example.com", "lead@example.com")},
    )
    (product,) = snapshot.data_products
    contract = _contract(snapshot, product, _orders(snapshot))
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
    config.data_products = [
        DataProduct(id="prod", domain_id="sales", name="Customer 360", tables=list(PROD.tables))
    ]
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
            grpc_port=50051,
            http_paths=HTTP_SURFACES,
        ),
    )
    (product,) = snapshot.data_products
    contract = _contract(snapshot, product, _orders(snapshot))
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
    assert servers["grpc"] == {
        "server": "grpc",
        "type": "custom",
        "description": "gRPC",
        "host": "data.example.com",
        "port": 50051,
        "serviceName": SERVICE_NAME,
    }
    assert servers["jsonapi"] == {
        "server": "jsonapi",
        "type": "api",
        "description": "JSON:API",
        "location": "https://data.example.com/data/jsonapi/sales/orders",
    }
    assert servers["openapi"] == {
        "server": "openapi",
        "type": "api",
        "description": "OpenAPI description of the REST endpoints",
        "location": "https://data.example.com/data/rest/openapi.json?domains=sales&tables=orders",
    }
    assert servers["sparql"] == {
        "server": "sparql",
        "type": "api",
        "description": "SPARQL 1.1 Protocol",
        "location": "https://data.example.com/data/sparql",
    }


def test_a_pgwire_listener_not_started_is_no_server():
    from provisa.api.metadata_export.model import ServingEndpoints

    snapshot, product = _snapshot()
    snapshot.endpoints = ServingEndpoints(
        hostname="h",
        database="provisa",
        http_origin="https://h",
        flight_port=8815,
        http_paths={"GraphQL": "/data/graphql", "REST": "/data/rest"},
    )
    contract = _contract(snapshot, product, _orders(snapshot))
    assert {s["server"] for s in contract["servers"]} == {
        "flight-sql",
        "graphql",
        "rest",
        "openapi",
    }


def test_a_surface_not_mounted_is_no_server():
    from provisa.api.metadata_export.model import ServingEndpoints

    snapshot, product = _snapshot()
    snapshot.endpoints = ServingEndpoints(hostname="h", database="provisa", http_origin="https://h")
    assert "servers" not in _contract(snapshot, product, _orders(snapshot))


def test_a_declared_junction_is_no_sparql_server():
    """A junction is an edge of the graph, no class of its vocabulary (REQ-1586); it is read
    everywhere else."""
    from provisa.api.metadata_export.model import JunctionRef, ServingEndpoints

    snapshot, product = _snapshot()
    snapshot.endpoints = ServingEndpoints(
        hostname="h",
        database="provisa",
        http_origin="https://h",
        grpc_port=50051,
        http_paths=HTTP_SURFACES,
    )
    orders = _orders(snapshot)
    edge = snapshot.relationships[0]
    snapshot.relationships[0] = dataclasses.replace(
        edge,
        kind="junction",
        via=JunctionRef(table=orders.ref, source_column="a", target_column="b"),
    )
    assert {s["server"] for s in _contract(snapshot, product, orders)["servers"]} == {
        "grpc",
        "graphql",
        "rest",
        "openapi",
        "jsonapi",
    }


def test_a_table_sql_alone_serves_has_only_its_sql_servers():
    from provisa.api.metadata_export.model import ServingEndpoints

    snapshot, product = _snapshot()
    snapshot.endpoints = ServingEndpoints(
        hostname="h",
        database="provisa",
        http_origin="https://h",
        pgwire_port=5439,
        flight_port=8815,
        grpc_port=50051,
        http_paths=HTTP_SURFACES,
    )
    (domain_id, table_name) = sorted(SQL_ONLY_TABLES)[0]
    served = dataclasses.replace(_orders(snapshot), name=table_name)
    product = dataclasses.replace(product, domain_id=domain_id)
    assert {s["server"] for s in _contract(snapshot, product, served)["servers"]} == {
        "pgwire",
        "flight-sql",
    }


def _deployment(monkeypatch, *, grpc_port=50051, flight_port=8815, **state):
    from provisa.core import settings_registry

    settings = {
        "server.hostname": "h",
        "server.pgwire_port": 5439,
        "server.flight_port": flight_port,
        "server.grpc_port": grpc_port,
    }
    monkeypatch.setattr(settings_registry, "value", settings.__getitem__)
    return SimpleNamespace(multitenancy=False, **state)


def test_a_deployment_serves_what_it_started_and_mounted(monkeypatch):
    state = _deployment(monkeypatch, _grpc_server=object(), http_surfaces={"SPARQL", "REST"})
    endpoints = serving_endpoints(state, ORG_ID, "https://h/")
    assert endpoints.grpc_port == 50051
    assert endpoints.http_paths == {"SPARQL": "/data/sparql", "REST": "/data/rest"}
    assert endpoints.http_origin == "https://h"


def test_a_grpc_server_not_started_is_no_port(monkeypatch):
    state = _deployment(monkeypatch, _grpc_server=None, http_surfaces=set())
    endpoints = serving_endpoints(state, ORG_ID, "https://h")
    assert endpoints.grpc_port is None
    assert endpoints.http_paths == {}


@pytest.mark.parametrize("listener", ["grpc_port", "flight_port"])
def test_a_listener_port_of_zero_is_no_port(monkeypatch, listener):
    state = _deployment(monkeypatch, _grpc_server=object(), http_surfaces=set(), **{listener: 0})
    assert getattr(serving_endpoints(state, ORG_ID, "https://h"), listener) is None


def test_the_contract_carries_the_products_tags():
    from provisa.api.metadata_export.model import ModelTag

    snapshot, product = _snapshot()
    orders = _orders(snapshot)
    snapshot.model_tags.append(ModelTag(tag_id="finance", is_system=False, asset=orders.ref))
    contract = _contract(snapshot, product, orders)
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
    contract = _contract(snapshot, product, orders)

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


# --- REQ-2180: an offer's contract is the dataset as the offer's role sees it ----------------------


def test_an_offer_contract_states_masked_columns_and_leaves_out_unread_ones():
    snapshot, product = _snapshot(status="live")
    (offer,) = _offers(snapshot, product, masked={"amount": "truncate"}, unread={"ssn"})
    contract = offer_contract(snapshot, product, _orders(snapshot), offer)

    assert _errors(ODCS, contract) == []
    assert contract["id"] == f"{contract_id(_orders(snapshot))}/offers/default"
    assert (contract["version"], contract["status"]) == ("1.0.0", "active")
    props = {p["name"]: p for p in contract["schema"][0]["properties"]}
    assert "ssn" not in props
    assert props["amount"]["classification"] == "restricted"
    assert {"property": "masked", "value": "truncate"} in props["amount"]["customProperties"]
    assert "customProperties" not in props["id"]
    custom = {p["property"]: p["value"] for p in contract["customProperties"]}
    assert custom["rowRules"] == ["only the orders of the reader's region"]
    assert (custom["offer"], custom["role"], custom["dataLevel"]) == ("default", "viewer", "full")
    assert custom["dataQualityResults"] is True


def test_profiles_are_stated_only_where_the_offer_includes_them():
    snapshot, product = _snapshot()
    (without,) = _offers(snapshot, product)
    (included,) = _offers(snapshot, product, profiles=True)
    orders = _orders(snapshot)
    plain = {
        p["property"]
        for p in offer_contract(snapshot, product, orders, without)["customProperties"]
    }
    assert "profiles" not in plain
    rich = {
        p["property"]: p["value"]
        for p in offer_contract(snapshot, product, orders, included)["customProperties"]
    }
    assert rich["profiles"] == ["orders_profile"]


def test_the_products_freshness_is_every_contracts():
    snapshot, product = _snapshot()
    (offer,) = _offers(snapshot, product, freshness=60)
    for table in (_orders(snapshot), next(t for t in snapshot.tables if t.name == "customers")):
        (latency,) = offer_contract(snapshot, product, table, offer)["slaProperties"]
        assert (latency["property"], latency["value"], latency["unit"]) == ("latency", 60, "s")


def test_a_contracts_dq_promises_are_stated_whether_or_not_its_results_are_given():
    snapshot, product = _snapshot()
    orders = _orders(snapshot)
    customers = next(t for t in snapshot.tables if t.name == "customers")
    snapshot.assertions.append(_check(orders, customers, owner="prod", column="id"))
    (offer,) = _offers(snapshot, product, dq_results=False)
    contract = offer_contract(snapshot, product, orders, offer)
    props = {p["name"]: p for p in contract["schema"][0]["properties"]}
    assert props["id"]["quality"]
    custom = {p["property"]: p["value"] for p in contract["customProperties"]}
    assert custom["dataQualityResults"] is False


def test_an_offers_contract_states_the_offers_terms_of_use():
    """REQ-2180 (amended 2026-11-11, THE PRODUCT PAGE IS SUMMARY ROWS): each offer carries its
    terms of use, shown in its contract: a term the offer states is its contract's, one it leaves
    unstated is the product's, written once on it."""
    snapshot, product = _snapshot()
    orders = next(t for t in snapshot.tables if t.name == "orders")
    contract = _contract(
        snapshot, product, orders, usage="Monthly churn models", limitations="No credit decisions"
    )
    assert _errors(ODCS, contract) == []
    assert contract["description"] == {
        "purpose": "One view of each customer",
        "usage": "Monthly churn models",
        "limitations": "No credit decisions",
    }
