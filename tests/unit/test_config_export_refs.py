# Copyright (c) 2026 Kenneth Stott
# Canary: 7c3e9a52-41d8-4b6f-a0e2-5d91c8f3b7a4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The addressed config export (REQ-1364): every internal cross-reference carries an RFC 6901
JSON Pointer that resolves to the object it names, every table and column carries the physical
resourcePath composed from its source's connection, and the document still loads."""

# Requirements: REQ-1364

from __future__ import annotations

import copy
from typing import Any
from urllib.parse import unquote

import pytest
import yaml

from provisa.api.admin.config_export import address_config, normalize_config


def _resolve(doc: Any, pointer: str) -> Any:
    """RFC 6901 evaluation of a URI-fragment pointer, written from the RFC, not the code under test."""
    assert pointer.startswith("#/"), pointer
    node = doc
    for raw in pointer[2:].split("/"):
        token = unquote(raw).replace("~1", "/").replace("~0", "~")
        node = node[int(token)] if isinstance(node, list) else node[token]
    return node


def _config() -> dict:
    return {
        "sources": [
            {
                "id": "pg",
                "type": "postgresql",
                "host": "db.example",
                "port": 5432,
                "database": "shop",
            },
            {"id": "files", "type": "parquet", "path": "/data/lake"},
            {"id": "api", "type": "openapi", "base_url": "https://api.example.com/v1/"},
        ],
        "domains": [
            {"id": "sales", "description": "Sales."},
            {"id": "sales/emea", "description": "EMEA.", "parent_domain_id": "sales"},
        ],
        "roles": [
            {
                "id": "analyst",
                "description": "Reads.",
                "capabilities": [],
                "domain_access": ["sales"],
            },
            {
                "id": "lead",
                "description": "Leads.",
                "capabilities": [],
                "domain_access": ["sales/emea"],
                "parents": ["analyst"],
            },
        ],
        "tags": [{"id": "pii", "description": "Personal."}],
        "tables": [
            {
                "source_id": "pg",
                "domain_id": "sales",
                "schema": "public",
                "table": "orders",
                "governance": "pre-approved",
                "columns": [
                    {"name": "id", "visible_to": ["analyst"]},
                    {"name": "customer_id", "visible_to": ["analyst"]},
                    {"name": "amount", "visible_to": ["analyst"]},
                ],
            },
            {
                "source_id": "files",
                "domain_id": "sales/emea",
                "schema": "lake",
                "table": "cust",
                "alias": "customers",
                "governance": "pre-approved",
                "columns": [{"name": "id", "visible_to": ["analyst"]}],
            },
            {
                "source_id": "api",
                "domain_id": "sales",
                "schema": "rest",
                "table": "rates",
                "governance": "pre-approved",
                "columns": [{"name": "code", "visible_to": ["analyst"]}],
            },
        ],
        "relationships": [
            {
                "id": "order-customer",
                "source_table_id": "orders",
                "target_table_id": "customers",
                "source_column": "customer_id",
                "target_column": "id",
                "cardinality": "many-to-one",
            },
            {
                # A seeded table stays out of the export (REQ-1634): the reference is not internal.
                "id": "to-seeded",
                "source_table_id": "orders",
                "target_table_id": "meta_v",
                "source_column": "id",
                "target_column": "id",
                "cardinality": "many-to-one",
            },
        ],
        "rls_rules": [
            {
                "table_id": "public.orders",
                "role_id": "analyst",
                "filter": "amount > 0",
                "description": "Positive orders.",
            },
            {"tag_id": "pii", "role_id": "lead", "filter": "1=1", "description": "Tagged."},
        ],
        "metrics": [
            {
                "name": "revenue",
                "expression": "SUM(orders.amount) - COUNT(customers.id)",
                "visible_to": ["analyst"],
            }
        ],
        "functions": [
            {
                "name": "place_order",
                "source_id": "pg",
                "schema": "public",
                "function_name": "place_order",
                "returns": "pg.public.orders",
                "domain_id": "sales",
            }
        ],
        "data_products": [
            {
                "id": "sales-core",
                "name": "Sales core",
                "domain_id": "sales",
                "tables": ["orders", "rates"],
            }
        ],
    }


def _named(doc: dict, section: str, key: str, value: str) -> dict:
    return next(item for item in doc[section] if item.get(key) == value)


def test_relationship_endpoints_point_at_their_tables_and_columns():
    doc = address_config(_config())
    rel = _named(doc, "relationships", "id", "order-customer")
    refs = rel["$refs"]
    assert _resolve(doc, refs["source_table_id"])["table"] == "orders"
    assert _resolve(doc, refs["target_table_id"])["alias"] == "customers"
    assert _resolve(doc, refs["source_column"])["name"] == "customer_id"
    target_col_table = _resolve(doc, refs["target_column"].rsplit("/columns/", 1)[0])
    assert target_col_table["alias"] == "customers"
    assert _resolve(doc, refs["target_column"])["name"] == "id"
    # The name references stay: the model definition is name-based.
    assert (rel["source_table_id"], rel["target_table_id"]) == ("orders", "customers")


def test_a_reference_to_a_table_outside_the_document_gets_no_pointer():
    doc = address_config(_config())
    rel = _named(doc, "relationships", "id", "to-seeded")
    assert "target_table_id" not in rel["$refs"] and "target_column" not in rel["$refs"]
    assert _resolve(doc, rel["$refs"]["source_table_id"])["table"] == "orders"


def test_roles_domains_rules_metrics_commands_and_products_point_at_what_they_name():
    doc = address_config(_config())
    lead = _named(doc, "roles", "id", "lead")
    assert [_resolve(doc, p)["id"] for p in lead["$refs"]["parents"]] == ["analyst"]
    assert [_resolve(doc, p)["id"] for p in lead["$refs"]["domain_access"]] == ["sales/emea"]

    emea = _named(doc, "domains", "id", "sales/emea")
    assert _resolve(doc, emea["$refs"]["parent_domain_id"])["id"] == "sales"

    table_rule = _named(doc, "rls_rules", "role_id", "analyst")
    assert _resolve(doc, table_rule["$refs"]["table_id"])["table"] == "orders"
    assert _resolve(doc, table_rule["$refs"]["role_id"])["id"] == "analyst"
    tag_rule = _named(doc, "rls_rules", "role_id", "lead")
    assert _resolve(doc, tag_rule["$refs"]["tag_id"])["id"] == "pii"

    metric = doc["metrics"][0]
    named = [_resolve(doc, p)["name"] for p in metric["$refs"]["expression"]]
    assert named == ["amount", "id"]
    assert _resolve(doc, metric["$refs"]["visible_to"][0])["id"] == "analyst"

    command = doc["functions"][0]
    assert _resolve(doc, command["$refs"]["returns"])["table"] == "orders"
    assert _resolve(doc, command["$refs"]["source_id"])["id"] == "pg"

    product = doc["data_products"][0]
    assert _resolve(doc, product["$refs"]["domain_id"])["id"] == "sales"
    assert sorted(_resolve(doc, p)["table"] for p in product["$refs"]["tables"]) == [
        "orders",
        "rates",
    ]

    for table in doc["tables"]:
        assert _resolve(doc, table["$refs"]["source_id"])["id"] == table["source_id"]
        assert _resolve(doc, table["$refs"]["domain_id"])["id"] == table["domain_id"]


def test_a_pointer_escapes_slashes_in_a_name_per_rfc_6901():
    from provisa.api.admin.config_export import _pointer

    assert _pointer("domains", "sales/emea", "a~b") == "#/domains/sales~1emea/a~0b"


def test_tables_and_columns_carry_their_physical_resource_path():
    doc = address_config(_config())
    orders = _named(doc, "tables", "table", "orders")
    assert orders["resourcePath"] == "postgresql://db.example:5432/shop/public/orders"
    amount = _named(orders, "columns", "name", "amount")
    assert amount["resourcePath"] == "postgresql://db.example:5432/shop/public/orders#amount"

    cust = _named(doc, "tables", "table", "cust")
    assert (
        cust["resourcePath"] == "parquet:///data/lake/lake/cust"
    )  # the physical name, not the alias
    assert cust["columns"][0]["resourcePath"] == "parquet:///data/lake/lake/cust#id"

    rates = _named(doc, "tables", "table", "rates")
    assert rates["resourcePath"] == "https://api.example.com/v1/rest/rates"


def test_a_name_two_tables_answer_is_refused():
    cfg = _config()
    cfg["tables"].append(copy.deepcopy(cfg["tables"][0]) | {"source_id": "files"})
    with pytest.raises(ValueError, match="orders"):
        address_config(cfg)


def test_the_addressed_document_indexes_the_order_it_is_written_in():
    """The pointers index the normalized document, and it is dumped without normalizing again:
    the written YAML read back resolves every pointer to the same object."""
    cfg = _config()
    cfg["tables"].reverse()  # store order differs from the normalized order
    doc = yaml.safe_load(yaml.dump(address_config(cfg), sort_keys=False))
    rel = _named(doc, "relationships", "id", "order-customer")
    assert _resolve(doc, rel["$refs"]["source_table_id"])["table"] == "orders"


def test_the_addressed_document_loads_as_the_plain_config_does():
    """Round trip: the loader reads the addressed export into the same model as the plain one."""
    from provisa.core.config_loader import parse_config_dict

    plain = parse_config_dict(normalize_config(_config()))
    addressed = parse_config_dict(address_config(_config()))
    assert addressed.model_dump() == plain.model_dump()


def test_address_config_leaves_its_input_untouched():
    cfg = _config()
    before = copy.deepcopy(cfg)
    address_config(cfg)
    assert cfg == before
