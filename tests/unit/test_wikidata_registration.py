# Copyright (c) 2026 Kenneth Stott
# Canary: 4b2e9c71-8d3a-4f15-a6e0-c7d1f93b5a28
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Wikidata registration as amended 2026-11-20 (REQ-1923): columns read from a query's SELECT
variables, previews limited where the rows are produced, a class search that offers only
classes with instances, the browse view of top-level classes, and the SPARQL query timeout
setting. Wikidata is stubbed with recorded answers; the live service is never called."""

# Requirements: REQ-1923

from __future__ import annotations

from urllib.parse import parse_qs

import httpx
import pytest
import respx
from fastapi import FastAPI
from fastapi.testclient import TestClient

from provisa.api.admin.wikidata_router import router, sparql_router
from provisa.api_source.models import ApiColumnType, ApiEndpoint
from provisa.core import settings_registry
from provisa.sparql import wikidata
from provisa.sparql.source import _probe_limit, extract_variables, query_timeout

_E = "http://www.wikidata.org/entity/"
_INT = "http://www.w3.org/2001/XMLSchema#integer"


@pytest.fixture
def client() -> TestClient:
    app = FastAPI()
    app.include_router(router)
    app.include_router(sparql_router)
    return TestClient(app)


def _queries(route) -> list[str]:
    return [parse_qs(c.request.content.decode())["query"][0] for c in route.calls]


# ── Columns from the SELECT ──────────────────────────────────────────────────


def test_a_projected_expression_names_the_column_it_binds_not_what_it_reads():
    query = (
        "SELECT ?id (SAMPLE(?label) AS ?name) (COUNT(DISTINCT ?x) AS ?members) "
        "WHERE { ?id rdfs:label ?label . ?x wdt:P31 ?id }"
    )
    assert extract_variables(query) == ["id", "name", "members"]


def test_iris_strings_and_comments_do_not_hide_or_add_columns():
    query = (
        "PREFIX ex: <http://example.org/select?where>\n"
        "# SELECT ?ignored WHERE\n"
        'SELECT $a ?b { ?a ex:p "WHERE ?c" ; ex:q ?b }'
    )
    assert extract_variables(query) == ["a", "b"]


def test_columns_route_reads_a_hand_written_query_every_binding_text(client):
    answer = client.post(
        "/admin/sources/sparql/columns",
        json={"query": "SELECT ?item (SAMPLE(?l) AS ?label) WHERE { ?item rdfs:label ?l }"},
    )
    assert answer.status_code == 200
    assert answer.json() == {
        "columns": [
            {"name": "item", "data_type": "text"},
            {"name": "label", "data_type": "text"},
        ]
    }


@pytest.mark.parametrize(
    "query", ["SELECT * WHERE { ?s ?p ?o }", "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }"]
)
def test_columns_route_refuses_a_query_whose_columns_are_not_in_it(client, query):
    answer = client.post("/admin/sources/sparql/columns", json={"query": query})
    assert answer.status_code == 422
    assert "names no SELECT variables" in answer.json()["detail"]


# ── A preview's limit ────────────────────────────────────────────────────────


def test_a_plain_query_is_limited_on_its_answer():
    assert _probe_limit("SELECT ?x WHERE { ?x ?p ?o }", limit=5).endswith("\nLIMIT 5")


def test_an_aggregating_subquery_is_limited_at_the_pattern_producing_its_rows():
    query = (
        "SELECT ?id ?n WHERE { { SELECT ?id (COUNT(?x) AS ?n) WHERE {\n"
        "  { SELECT ?id WHERE { ?id wdt:P31 wd:Q6256 . } }\n"
        "  OPTIONAL { ?x wdt:P17 ?id }\n"
        "} GROUP BY ?id } }"
    )
    preview = _probe_limit(query, limit=5)
    assert "{ SELECT ?id WHERE { ?id wdt:P31 wd:Q6256 . } LIMIT 5 }" in preview
    assert preview.endswith("\nLIMIT 5")


def test_an_outer_limit_of_the_query_s_own_is_lowered_never_raised():
    assert _probe_limit("SELECT ?x WHERE { ?x ?p ?o } LIMIT 100", limit=5).endswith("LIMIT 5")
    assert _probe_limit("SELECT ?x WHERE { ?x ?p ?o } LIMIT 3", limit=5).endswith("LIMIT 3")


def test_a_class_table_s_preview_limits_its_instances_before_the_group():
    table = wikidata.class_table_query(
        "Q515",
        [wikidata.ClassProperty("P17", "country", ApiColumnType.string, item=True, uses=1)],
        row_filter=None,
        language="en",
    )
    preview = _probe_limit(table.query, limit=5)
    assert "{ SELECT ?item WHERE { ?item wdt:P31 wd:Q515 . } LIMIT 5 }" in preview
    assert preview.index("LIMIT 5") < preview.index("GROUP BY")


def test_the_full_class_table_reads_every_instance():
    table = wikidata.class_table_query("Q515", [], row_filter=None, language="en")
    assert "LIMIT" not in table.query


# ── Class search: only classes with instances ────────────────────────────────

# What wbsearchentities answered for "beer": a class, and two items that are no class -- Beers
# (a family name) and Inventors Digest (a magazine) -- each with no instances.
_BEER_SEARCH = {
    "search": [
        {"id": "Q44", "label": "beer", "description": "alcoholic drink"},
        {"id": "Q954104", "label": "Beers", "description": "family name"},
        {"id": "Q6059902", "label": "Inventors Digest", "description": "magazine"},
    ]
}


@respx.mock
async def test_class_search_offers_only_items_with_instances_in_one_query():
    respx.get(wikidata.SEARCH_API).mock(return_value=httpx.Response(200, json=_BEER_SEARCH))
    sparql = respx.post(wikidata.ENDPOINT).mock(
        return_value=httpx.Response(
            200,
            json={
                "head": {"vars": ["cls"]},
                "results": {"bindings": [{"cls": {"type": "uri", "value": _E + "Q44"}}]},
            },
        )
    )
    found = await wikidata.search_entities("beer", language="en", classes=True)
    assert found == [{"id": "Q44", "label": "beer", "description": "alcoholic drink"}]
    (query,) = _queries(sparql)
    assert "VALUES ?cls { wd:Q44 wd:Q954104 wd:Q6059902 }" in query
    assert "FILTER EXISTS { ?i wdt:P31 ?cls }" in query


@respx.mock
def test_class_search_route_asks_for_classes(client):
    respx.get(wikidata.SEARCH_API).mock(return_value=httpx.Response(200, json=_BEER_SEARCH))
    respx.post(wikidata.ENDPOINT).mock(
        return_value=httpx.Response(
            200, json={"head": {"vars": ["cls"]}, "results": {"bindings": []}}
        )
    )
    answer = client.get(
        "/admin/sources/sparql/wikidata/search",
        params={"q": "beer", "language": "en", "kind": "class"},
    )
    assert answer.status_code == 200
    assert answer.json() == []


@respx.mock
async def test_an_empty_search_page_asks_nothing_more():
    respx.get(wikidata.SEARCH_API).mock(return_value=httpx.Response(200, json={"search": []}))
    sparql = respx.post(wikidata.ENDPOINT)
    assert await wikidata.search_entities("zzzz", language="en", classes=True) == []
    assert not sparql.called


# ── The browse view ──────────────────────────────────────────────────────────


def _browse_row(qid: str, label: str | None, count: int) -> dict:
    row = {
        "cls": {"type": "uri", "value": _E + qid},
        "count": {"type": "literal", "value": str(count), "datatype": _INT},
    }
    if label is not None:
        row["label"] = {"type": "literal", "value": label, "xml:lang": "en"}
    return row


@respx.mock
def test_browse_lists_the_top_level_classes_by_member_count(client):
    sparql = respx.post(wikidata.ENDPOINT).mock(
        return_value=httpx.Response(
            200,
            json={
                "head": {"vars": ["cls", "label", "count"]},
                "results": {
                    "bindings": [
                        _browse_row("Q6256", "country", 200),
                        _browse_row("Q5", "human", 12_000_000),
                        _browse_row("Q515", None, 10_542),
                        _browse_row("Q3918", "university", 0),
                    ]
                },
            },
        )
    )
    answer = client.get("/admin/sources/sparql/wikidata/browse", params={"language": "en"})
    assert answer.status_code == 200
    assert answer.json() == [
        {"id": "Q5", "label": "human", "instance_count": 12_000_000},
        {"id": "Q515", "label": None, "instance_count": 10_542},
        {"id": "Q6256", "label": "country", "instance_count": 200},
    ]
    (query,) = _queries(sparql)
    for qid in wikidata.BROWSE_CLASSES:
        assert f"?i wdt:P31 wd:{qid} }}" in query
    assert 'FILTER(LANG(?label) = "en")' in query


# ── The SPARQL query timeout ─────────────────────────────────────────────────


def test_the_sparql_query_timeout_is_an_operator_setting():
    setting = settings_registry.setting("sparql.query_timeout")
    assert setting.default == 300.0
    assert setting.unit == "seconds"
    assert query_timeout() == 300.0


@respx.mock
async def test_wikidata_calls_wait_the_sparql_query_timeout(monkeypatch):
    monkeypatch.setenv("PROVISA_SPARQL_QUERY_TIMEOUT", "240")
    route = respx.post(wikidata.ENDPOINT).mock(
        return_value=httpx.Response(200, json={"head": {"vars": []}, "results": {"bindings": []}})
    )
    await wikidata.select("SELECT ?x WHERE { ?x ?p ?o }")
    assert route.calls.last.request.extensions["timeout"]["read"] == 240.0


def test_a_sparql_replica_load_waits_the_sparql_query_timeout(monkeypatch):
    from provisa.api_source.replica_read import _TIMEOUT, _timeout

    monkeypatch.setenv("PROVISA_SPARQL_QUERY_TIMEOUT", "600")
    sparql = ApiEndpoint(
        source_id="wd", path="/sparql", method="POST", table_name="countries", columns=[],
        response_normalizer="sparql_bindings",
    )  # fmt: skip
    rest = ApiEndpoint(source_id="api", path="/x", method="GET", table_name="x", columns=[])
    assert _timeout(sparql) == 600.0
    assert _timeout(rest) == _TIMEOUT
