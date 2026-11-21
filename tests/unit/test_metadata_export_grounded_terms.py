# Copyright (c) 2026 Kenneth Stott
# Canary: 2f7a9c41-8d36-4b05-a1e9-6c3d0b8f4e72
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2238: a data product's exports carry each field's grounded term.

Only a term an agent may see is named (core.glossary.agent_view): an approved term, or a defined
draft on a path holding an approved one, marked draft. An undefined term on such a path is a hop,
and so is a term excluded from export; a draft off every approved path and a deprecated term are
not seen at all. The ODCS contract names a field's term as an authoritative definition of type
semantics, the ODPS output port carries the same links, and the Ossie ontology holds the terms the
product reaches. A Class term holds its tables' key columns, and a catalog links them to it.
"""

# Requirements: REQ-2238, REQ-1389, REQ-2141

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml
from jsonschema import Draft201909Validator, Draft202012Validator
from referencing import Registry, Resource

from provisa.api.metadata_export.builder import build_snapshot
from provisa.api.metadata_export.datahub import _assignments
from provisa.api.metadata_export.model import term_note
from provisa.api.metadata_export.odps import SEMANTICS, product_documents
from provisa.core.models import (
    Column,
    Domain,
    ProvisaConfig,
    Source,
    SourceType,
    Table,
)
from provisa.ossie.convert import OSSIE_VERSION, OssieExportRefused
from provisa.ossie.ontology import ossie_ontology, ossie_ontology_yaml
from tests.unit.product_members import with_members
from tests.unit.test_metadata_export_odps_odcs import _offers, _with

pytestmark = pytest.mark.unit

_FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
_STANDARDS = _FIXTURES / "standards"
_OSSIE = _FIXTURES / "ossie"
ODCS = Draft201909Validator(json.loads((_STANDARDS / "odcs-json-schema-v3.1.0.json").read_text()))
ODPS = Draft201909Validator(json.loads((_STANDARDS / "odps-json-schema-v1.0.0.json").read_text()))
#: The ontology schema refers to Ossie's core schema by its URL on main; the pinned copy answers.
_CORE_URL = "https://raw.githubusercontent.com/apache/ossie/main/core-spec/ossie-schema.json"
ONTOLOGY = Draft202012Validator(
    json.loads((_OSSIE / "ontology.json").read_text()),
    registry=Registry().with_resource(
        _CORE_URL, Resource.from_contents(json.loads((_OSSIE / "ossie-schema.json").read_text()))
    ),
)

_ORDERS_COLUMNS = {
    "id": "integer",
    "amount": "numeric(12,2)",
    "status": "text",
    "placed_at": "time",
    "line_no": "integer",
    "legacy": "integer",
    "note": "text",
}


def _table(name: str, columns: dict[str, str]) -> Table:
    return Table(
        source_id="wh",
        domain_id="sales",
        schema_name="public",
        table_name=name,
        columns=[
            Column(name=c, data_type=t, visible_to=["admin"], is_primary_key=c == "id")
            for c, t in columns.items()
        ],
    )


def _term(term_id: int, name: str, definition: str | None, **flags) -> dict:
    return {
        "id": term_id,
        "name": name,
        "definition": definition,
        "is_abstract": False,
        "deprecated": False,
        "deprecation_asserted": False,
        "approved": True,
        "retired": False,
        "export_excluded": False,
        "calculation_method": None,
        "status_reason": None,
        "retire_on": None,
        **flags,
    }


def _ref(term_id: int, table: str, column: str) -> dict:
    return {
        "term_id": term_id,
        "column_name": column,
        "source_id": "wh",
        "schema_name": "public",
        "table_name": table,
    }


def _edge(a: int, b: int, rel_type: str) -> dict:
    return {"from_term_id": a, "to_term_id": b, "rel_type": rel_type}


def _glossary() -> dict:
    return {
        "terms": [
            _term(1, "order", "A customer's order."),  # Class of orders, holding its key
            _term(2, "order amount", "What the order costs."),
            _term(3, "sale", "A completed sale."),  # an approved Concept
            _term(4, "order status", "Where the order stands.", approved=False),  # defined draft
            _term(5, "placed time", "When the order was placed."),
            _term(6, "loose", "A draft off every approved path.", approved=False),
            _term(7, "order line", None, approved=False),  # undefined draft: a hop
            _term(8, "old amount", "Superseded.", deprecation_asserted=True),
            _term(9, "secret", "Kept out of exports.", export_excluded=True),
            _term(10, "elsewhere", "Another product's column."),
            _term(12, "transaction", "Any exchange of value."),
        ],
        "refs": [
            _ref(1, "orders", "id"),
            _ref(2, "orders", "amount"),
            _ref(4, "orders", "status"),
            _ref(5, "orders", "placed_at"),
            _ref(6, "customers", "id"),
            _ref(7, "orders", "line_no"),
            _ref(8, "orders", "legacy"),
            _ref(9, "orders", "note"),
            _ref(10, "other", "x"),
        ],
        "table_refs": [
            {"term_id": 1, "source_id": "wh", "schema_name": "public", "table_name": "orders"}
        ],
        "edges": [
            _edge(2, 1, "PART_OF"),  # an order has an order amount
            _edge(2, 3, "PART_OF"),
            _edge(4, 3, "PART_OF"),
            _edge(7, 3, "PART_OF"),
            _edge(3, 12, "KIND_OF"),  # a sale is a kind of transaction
        ],
        "experts": [],
        "domains": [],
    }


def _snapshot(glossary: dict | None = None):
    orders = _table("orders", _ORDERS_COLUMNS)
    customers = _table("customers", {"id": "integer"})
    other = _table("other", {"x": "integer"})
    config = ProvisaConfig(
        sources=[Source(id="wh", type=SourceType.postgresql, description="warehouse")],
        domains=[Domain(id="sales", description="Sales")],
        tables=[orders, customers, other],
        data_products=with_members([], {"prod": [orders, customers], "prod2": [other]}),
        roles=[],
    )
    return build_snapshot(
        config,
        org_id="acme",
        dialect="postgres",
        contexts={},
        glossary=_glossary() if glossary is None else glossary,
    )


def _product(snapshot, product_id: str = "prod"):
    return next(p for p in snapshot.data_products if p.id == product_id)


def _column_names(snapshot) -> dict[str, str]:
    """Each column with a grounded term, by its table and name, to the term's name."""
    return {
        ".".join(fqn.split(".")[-2:]): str(term.name)
        for fqn, term in snapshot.column_terms().items()
    }


def test_only_a_term_an_agent_may_see_is_named_for_a_column():
    named = _column_names(_snapshot())
    assert named == {
        "orders.id": "order",  # a Class holds its table's key column
        "orders.amount": "order amount",
        "orders.status": "order status",  # a defined draft under the approved sale
        "orders.placed_at": "placed time",
        "other.x": "elsewhere",
    }
    # Not named: the undefined draft (a hop), the term excluded from export (a hop), the
    # deprecated term and the draft on no approved path (not seen at all).
    statuses = {t.term_id: t.status for t in _snapshot().grounded_terms}
    assert statuses[7] == "hop" and statuses[9] == "hop"
    assert 6 not in statuses and 8 not in statuses
    assert statuses[4] == "draft"


def test_a_hop_carries_no_name_definition_or_uri():
    hop = next(t for t in _snapshot().grounded_terms if t.term_id == 9)
    assert (hop.name, hop.definition, hop.semantic_uri) == (None, None, "")
    assert not hop.named


def test_each_contract_field_names_its_grounded_term_as_a_semantics_definition():
    snapshot = _snapshot()
    product = _product(snapshot)
    _, contracts = product_documents(_with(snapshot, product, _offers(snapshot, product)), product)
    orders = contracts["default/wh.public.orders"]
    assert not list(ODCS.iter_errors(orders))
    (schema_object,) = orders["schema"]
    fields = {p["name"]: p.get("authoritativeDefinitions") for p in schema_object["properties"]}
    assert fields["amount"] == [
        {
            "type": SEMANTICS,
            "url": "provisa://acme/terms/order%20amount",
            "description": "order amount: What the order costs.",
        }
    ]
    assert fields["status"] == [
        {
            "type": "semantics",
            "url": "provisa://acme/terms/order%20status",
            "description": "order status (draft): Where the order stands.",
        }
    ]
    assert fields["id"][0]["url"] == "provisa://acme/terms/order"
    assert fields["line_no"] is None and fields["legacy"] is None and fields["note"] is None
    customers = contracts["default/wh.public.customers"]
    assert customers["schema"][0]["properties"][0].get("authoritativeDefinitions") is None


def test_the_odps_output_port_carries_its_fields_term_links():
    snapshot = _snapshot()
    product = _product(snapshot)
    document, contracts = product_documents(
        _with(snapshot, product, _offers(snapshot, product)), product
    )
    assert not list(ODPS.iter_errors(document))
    ports = {port["name"]: port for port in document["outputPorts"]}
    field_links = {
        json.dumps(d, sort_keys=True)
        for p in contracts["default/wh.public.orders"]["schema"][0]["properties"]
        for d in p.get("authoritativeDefinitions", [])
    }
    port_links = {
        json.dumps(d, sort_keys=True) for d in ports["orders (default)"]["authoritativeDefinitions"]
    }
    assert port_links == field_links and len(port_links) == 4
    assert "authoritativeDefinitions" not in ports["customers (default)"]


def test_the_ossie_ontology_holds_the_terms_the_product_reaches():
    snapshot = _snapshot()
    document = ossie_ontology(snapshot, _product(snapshot))
    assert not list(ONTOLOGY.iter_errors(document))
    assert document["version"] == OSSIE_VERSION and document["name"] == "prod"
    concepts = {c["concept"]: c for c in document["ontology"]}
    # elsewhere reaches only prod2's column; placed time's type has no Ossie built-in.
    assert set(concepts) == {
        "order",
        "order amount",
        "sale",
        "order status",
        "transaction",
        "(hop 1)",
        "(hop 2)",
    }
    assert concepts["order"]["type"] == "EntityType"
    assert concepts["order"]["iri"] == "provisa://acme/terms/order"
    assert concepts["order amount"] == {
        "concept": "order amount",
        "type": "ValueType",
        "description": "What the order costs.",
        "extends": ["Decimal"],
        "iri": "provisa://acme/terms/order%20amount",
    }
    assert concepts["sale"]["extends"] == ["transaction"]
    assert {r["name"]: r for r in concepts["order"]["relationships"]} == {
        "has_order_amount": {
            "name": "has_order_amount",
            "roles": [{"concept": "order amount"}],
            "verbalizes": ["{order} has {order amount}"],
        }
    }
    assert [r["name"] for r in concepts["sale"]["relationships"]] == [
        "has_order_amount",
        "has_order_status",
        "has_hop_1",
    ]
    # A hop is a concept with no name of its own, no description and no iri.
    assert concepts["(hop 1)"] == {
        "concept": "(hop 1)",
        "type": "ValueType",
        "extends": ["Integer"],
    }
    assert document["ai_context"] == {
        "draft_terms": ["order status"],
        "withheld_terms": [
            {
                "term": "placed time",
                "reason": "its columns' type is none of Ossie's built-in value types",
            }
        ],
    }
    assert yaml.safe_load(ossie_ontology_yaml(snapshot, _product(snapshot))) == document


def test_a_product_reaching_no_term_has_no_ossie_ontology():
    glossary = _glossary()
    glossary["terms"] = [t for t in glossary["terms"] if t["id"] != 10]
    glossary["refs"] = [r for r in glossary["refs"] if r["term_id"] != 10]
    snapshot = _snapshot(glossary)
    with pytest.raises(OssieExportRefused, match="'prod2' reaches no term"):
        ossie_ontology(snapshot, _product(snapshot, "prod2"))


def test_a_class_is_linked_to_its_key_columns_beside_its_tables():
    snapshot = _snapshot()
    order = next(t for t in snapshot.glossary_terms if t.name == "order")
    assert [r.fqn() for r in order.assigned_to()] == ["wh.public.orders", "wh.public.orders.id"]
    assigned = _assignments(snapshot)
    assert [a["urn"] for a in assigned["wh.public.orders.id"]] == [
        a["urn"] for a in assigned["wh.public.orders"]
    ]


def test_a_column_comment_note_names_the_term_and_its_definition():
    terms = {t.term_id: t for t in _snapshot().grounded_terms}
    assert term_note(terms[2]) == "[provisa:term order amount] What the order costs."
    assert term_note(terms[4]) == "[provisa:term order status draft] Where the order stands."
    assert term_note(None) == ""
