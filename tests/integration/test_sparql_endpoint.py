# Copyright (c) 2026 Kenneth Stott
# Canary: 9a0e709f-b5c0-4a85-aac7-71c6daf45bf5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The SPARQL 1.1 Protocol endpoint, /data/sparql (REQ-2135), queried by standard clients:
SPARQLWrapper and curl, against a real listener serving the app over the isolated Postgres stack
and tests/fixtures/sample_config.yaml (customers and orders in sales-analytics, products in
product-catalog; org_admin, and an analyst who reads no product and no customer's email).

Covered: SELECT, ASK and CONSTRUCT, a property path, every content type the Protocol negotiates,
and a governed role answered only what it may read.
"""

# Requirements: REQ-2135

from __future__ import annotations

import csv
import io
import os
import subprocess
import threading
import time
from pathlib import Path

import httpx
import pytest
from rdflib import Graph, URIRef

pytestmark = [pytest.mark.e2e]

_FIXTURE_CONFIG = Path(__file__).parent.parent / "fixtures" / "sample_config.yaml"
BASE = "http://sparql.test"
CUSTOMER = f"{BASE}/vocab/sales-analytics/customers"
ORDER = f"{BASE}/vocab/sales-analytics/orders"
PRODUCT = f"{BASE}/vocab/product-catalog/products"
RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"


@pytest.fixture(scope="module")
def endpoint():
    """The app on a real HTTP listener, as a deployment serves it."""
    import uvicorn

    from tests.port_lease import lease_port

    saved = {k: os.environ.get(k) for k in ("PROVISA_CONFIG", "PROVISA_PUBLIC_URL")}
    os.environ.setdefault("PG_PASSWORD", "provisa")
    os.environ["PROVISA_CONFIG"] = str(_FIXTURE_CONFIG)
    os.environ["PROVISA_PUBLIC_URL"] = BASE

    from provisa.api.app import create_app

    port = lease_port()
    server = uvicorn.Server(
        uvicorn.Config(create_app(), host="127.0.0.1", port=port, log_level="warning")
    )
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    try:
        deadline = time.monotonic() + 300
        while not server.started and time.monotonic() < deadline:
            time.sleep(0.2)
        if not server.started:
            raise RuntimeError(f"the app did not start on {port} within 300s")
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        thread.join(timeout=30)
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


@pytest.fixture(scope="module")
def placed_by(endpoint) -> str:
    """The edge predicate of the orders-to-customers relationship, by its registered type."""
    schema = httpx.get(
        f"{endpoint}/data/graph-schema", headers={"X-Provisa-Role": "org_admin"}, timeout=60
    ).json()
    (rel,) = [
        r
        for r in schema["relationship_types"]
        if "Orders" in r["source"] and "Customers" in r["target"]
    ]
    return f"{BASE}/vocab/rel/{rel['type']}"


def _client(endpoint: str, role: str, query: str, fmt: str):
    from SPARQLWrapper import SPARQLWrapper

    sparql = SPARQLWrapper(f"{endpoint}/data/sparql")
    sparql.addCustomHttpHeader("X-Provisa-Role", role)
    sparql.setQuery(query)
    sparql.setReturnFormat(fmt)
    return sparql.query().convert()


def _select(endpoint: str, role: str, query: str) -> list[dict]:
    from SPARQLWrapper import JSON

    doc = _client(endpoint, role, query, JSON)
    return [{k: v["value"] for k, v in b.items()} for b in doc["results"]["bindings"]]


def test_select_reads_a_class_and_its_predicate(endpoint):
    rows = _select(
        endpoint,
        "org_admin",
        f"SELECT ?c ?name WHERE {{ ?c a <{CUSTOMER}> ; <{CUSTOMER}#name> ?name }} ORDER BY ?name",
    )
    assert rows and rows[0]["name"] == "Alice Johnson"
    assert all(r["c"].startswith(f"{BASE}/id/sales-analytics/customers/") for r in rows)


def test_ask(endpoint):
    from SPARQLWrapper import JSON

    yes = f'ASK {{ ?c <{CUSTOMER}#name> "Alice Johnson" }}'
    no = f'ASK {{ ?c <{CUSTOMER}#name> "Nobody At All" }}'
    assert _client(endpoint, "org_admin", yes, JSON)["boolean"] is True
    assert _client(endpoint, "org_admin", no, JSON)["boolean"] is False


def test_construct_returns_a_graph(endpoint, placed_by):
    from SPARQLWrapper import TURTLE

    data = _client(
        endpoint,
        "org_admin",
        f"CONSTRUCT {{ ?c <http://example.org/placed> ?o }} WHERE {{ ?o <{placed_by}> ?c }}",
        TURTLE,
    )
    graph = Graph().parse(data=data, format="turtle")
    assert len(graph) > 0
    for s, p, o in graph:
        assert str(s).startswith(f"{BASE}/id/sales-analytics/customers/")
        assert p == URIRef("http://example.org/placed")
        assert str(o).startswith(f"{BASE}/id/sales-analytics/orders/")


def test_a_property_path(endpoint, placed_by):
    # Each order paired with every order of its customer, itself included: an edge and its inverse.
    rows = _select(
        endpoint,
        "org_admin",
        f"SELECT ?a ?b WHERE {{ ?a <{placed_by}>/^<{placed_by}> ?b }}",
    )
    pairs = {(r["a"], r["b"]) for r in rows}
    assert pairs and all((a, a) in pairs for a, _ in pairs)
    assert all((b, a) in pairs for a, b in pairs)


def test_a_governed_role_is_answered_only_what_it_may_read(endpoint):
    alice = f'?c <{CUSTOMER}#name> "Alice Johnson" . ?c ?p ?o'
    admin = {r["p"] for r in _select(endpoint, "org_admin", f"SELECT ?p ?o WHERE {{ {alice} }}")}
    analyst = {r["p"] for r in _select(endpoint, "analyst", f"SELECT ?p ?o WHERE {{ {alice} }}")}
    assert f"{CUSTOMER}#email" in admin
    assert f"{CUSTOMER}#email" not in analyst and f"{CUSTOMER}#name" in analyst
    products = f"SELECT ?x WHERE {{ ?x a <{PRODUCT}> }} LIMIT 1"
    assert _select(endpoint, "org_admin", products)
    assert _select(endpoint, "analyst", products) == []


def _curl(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["curl", "-sS", "--fail-with-body", *args], capture_output=True, text=True
    )


def test_curl_get_as_csv(endpoint):
    out = _curl(
        "-G",
        f"{endpoint}/data/sparql",
        "-H",
        "X-Provisa-Role: org_admin",
        "-H",
        "Accept: text/csv",
        "--data-urlencode",
        f"query=SELECT ?name WHERE {{ ?c <{CUSTOMER}#name> ?name }} ORDER BY ?name LIMIT 2",
    )
    assert out.returncode == 0, out.stdout + out.stderr
    assert list(csv.reader(io.StringIO(out.stdout)))[:2] == [["name"], ["Alice Johnson"]]


def test_curl_post_as_a_sparql_query_body_with_n_triples(endpoint):
    out = _curl(
        f"{endpoint}/data/sparql",
        "-H",
        "X-Provisa-Role: org_admin",
        "-H",
        "Content-Type: application/sparql-query",
        "-H",
        "Accept: application/n-triples",
        "--data-binary",
        f"CONSTRUCT {{ ?c a <{CUSTOMER}> }} WHERE {{ ?c a <{CUSTOMER}> }}",
    )
    assert out.returncode == 0, out.stdout + out.stderr
    graph = Graph().parse(data=out.stdout, format="nt")
    assert len(graph) > 0 and all(p == URIRef(RDF_TYPE) for _, p, _ in graph)


def test_curl_post_a_form_as_xml_results(endpoint):
    out = _curl(
        f"{endpoint}/data/sparql",
        "-H",
        "X-Provisa-Role: org_admin",
        "-H",
        "Accept: application/sparql-results+xml",
        "--data-urlencode",
        f'query=ASK {{ ?c <{CUSTOMER}#name> "Alice Johnson" }}',
    )
    assert out.returncode == 0, out.stdout + out.stderr
    assert "<boolean>true</boolean>" in out.stdout


def test_an_aggregate_per_group(endpoint, placed_by):
    rows = _select(
        endpoint,
        "org_admin",
        f"SELECT ?c (COUNT(?o) AS ?n) WHERE {{ ?o <{placed_by}> ?c }} GROUP BY ?c "
        "HAVING (COUNT(?o) > 0) ORDER BY DESC(?n)",
    )
    counts = [int(r["n"]) for r in rows]
    assert counts and counts == sorted(counts, reverse=True) and all(n > 0 for n in counts)
    total = _select(
        endpoint, "org_admin", f"SELECT (COUNT(*) AS ?n) WHERE {{ ?o <{placed_by}> ?c }}"
    )
    assert int(total[0]["n"]) == sum(counts)


def test_the_vocabulary_offers_the_roles_classes_predicates_and_edges(endpoint, placed_by):
    """GET /data/sparql/vocabulary: what a client offers as a query is written (REQ-2135)."""
    doc = httpx.get(
        f"{endpoint}/data/sparql/vocabulary", headers={"X-Provisa-Role": "org_admin"}, timeout=60
    ).json()
    assert doc["base"] == BASE
    classes = {c["iri"]: c for c in doc["classes"]}
    assert classes[CUSTOMER]["kind"] == "table" and classes[ORDER]["kind"] == "table"
    assert any(e["iri"] == placed_by for e in doc["edges"])
    assert any(
        p["iri"].startswith(f"{CUSTOMER}#") and p["kind"] == "column" for p in doc["predicates"]
    )
