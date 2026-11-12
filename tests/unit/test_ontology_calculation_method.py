# Copyright (c) 2026 Kenneth Stott
# Canary: 8d3f0a62-5e1b-4c97-b2a4-6f9c1e7d3b08
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2243: a term's calculation method is written out on ontology export -- RDF as
``provisa:calculationMethod``, Ossie under ``ai_context.calculation_methods`` -- and an import maps
both back onto the term's method, keeping neither as a statement. No other annotation is a method."""

# Requirements: REQ-2243

from __future__ import annotations

import types
from typing import Any, cast

import pytest
import yaml
from rdflib import Graph, Literal, URIRef

from provisa.api.admin import ontology_import_router as router
from provisa.api.metadata_export.model import GroundedTerm
from provisa.core.repositories import glossary as glossary_repo
from provisa.ontology_import import parse
from provisa.ossie.ontology import ossie_ontology
from tests.unit.test_ontology_import_api import _request, _terms, _upload, api  # noqa: F401

pytestmark = pytest.mark.asyncio

METHOD = "Gross order value less refunds and discounts, on the order date."
REVENUE = "https://example.org/NetRevenue"
PREDICATE = "https://provisa.dev/ns#calculationMethod"
TURTLE = f"""
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix provisa: <https://provisa.dev/ns#> .
<{REVENUE}> a owl:Class ;
    skos:definition "Revenue kept after refunds." ;
    provisa:calculationMethod "{METHOD}" .
<https://example.org/Margin> a owl:Class ;
    skos:note "Revenue less cost of goods, per order." .
""".encode()


async def _imported(api) -> dict[str, dict]:  # noqa: F811
    review = await router.import_ontology(_request(_upload(TURTLE, "revenue.ttl")))
    await router.apply_import(review["import"]["id"], _request())
    return await _terms(api)


async def test_an_rdf_import_maps_the_method_and_keeps_no_statement_of_it(api):  # noqa: F811
    terms = await _imported(api)
    revenue, margin = terms["net revenue"], terms["margin"]
    assert revenue["calculation_method"] == METHOD
    async with api.db.acquire() as conn:
        kept = await glossary_repo.term_statements(conn, [revenue["id"], margin["id"]])
    assert all(s.get("predicate") != PREDICATE for s in kept.get(revenue["id"], []))
    # The other annotation is kept on its term.
    assert any(s.get("predicate", "").endswith("#note") for s in kept[margin["id"]])
    # Another vocabulary's annotation is kept as a statement, never read as a method.
    assert margin["calculation_method"] is None


async def test_an_rdf_export_writes_the_method_and_reads_back(api):  # noqa: F811
    terms = await _imported(api)
    imports = await router.list_imports(_request())
    [held] = imports["imports"]
    response = await router.export_import(held["id"], _request(), format="turtle")
    graph = Graph().parse(data=bytes(response.body), format="turtle")
    assert (URIRef(REVENUE), URIRef(PREDICATE), Literal(METHOD)) in graph
    again = parse.parse("again.ttl", bytes(response.body))
    [item] = [i for i in again.items if i.iri == REVENUE]
    assert item.calculation_method == METHOD
    assert all(s.get("predicate") != PREDICATE for s in item.statements)
    assert terms["net revenue"]["calculation_method"] == METHOD


def _document(method: str | None) -> dict[str, Any]:
    snapshot = types.SimpleNamespace(
        terms_reached=lambda product: (
            [
                GroundedTerm(
                    term_id=1,
                    name="net revenue",
                    definition="Revenue kept after refunds.",
                    status="approved",
                    semantic_uri="provisa://default/terms/net revenue",
                    role="concept",
                    iri=REVENUE,
                    calculation_method=method,
                )
            ],
            [],
        )
    )
    product = types.SimpleNamespace(id="sales", name="Sales")
    return ossie_ontology(cast("Any", snapshot), cast("Any", product))


def test_the_ossie_export_carries_the_method_and_an_import_reads_it():
    document = _document(METHOD)
    assert document["ai_context"]["calculation_methods"] == [
        {"concept": "net revenue", "iri": REVENUE, "method": METHOD}
    ]
    [item] = parse.parse("sales.yaml", yaml.safe_dump(document).encode()).items
    assert item.calculation_method == METHOD
    # A term with no method writes no list.
    assert "ai_context" not in _document(None)


def test_an_ossie_method_list_of_another_shape_is_refused():
    document = _document(METHOD)
    document["ai_context"]["calculation_methods"] = [{"concept": "net revenue"}]
    with pytest.raises(parse.ImportRefused, match="calculation_methods"):
        parse.parse("sales.yaml", yaml.safe_dump(document).encode())
