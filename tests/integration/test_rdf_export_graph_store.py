# Copyright (c) 2026 Kenneth Stott
# Canary: 0c6f3b84-7e2d-4a19-9d55-b1e8a3f62c07
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The bulk RDF export's direct load (REQ-2165): POST /data/sparql/export/load writes the role's
governed graph into a running Apache Jena Fuseki over the SPARQL 1.1 Graph Store HTTP Protocol and
answers the triples loaded and each failure. The app is served on a real listener over the
isolated Postgres stack and tests/fixtures/sample_config.yaml (customers in sales-analytics; an
analyst who reads no customer's email); Fuseki is the test stack's own container.

Covered: the triples the store then holds are the ones reported loaded, the ontology declarations
arrive with the instances, the analyst's load holds no email, the file export (Turtle and streamed
N-Triples) holds what the load does, and a store that refuses the requests is reported, not
raised.
"""

# Requirements: REQ-2165

from __future__ import annotations

import os
import threading
import time
import uuid
from pathlib import Path

import httpx
import pytest
from rdflib import Graph

pytestmark = [pytest.mark.integration, pytest.mark.requires_sparql]

_FIXTURE_CONFIG = Path(__file__).parent.parent / "fixtures" / "sample_config.yaml"
BASE = "http://sparql.test"
CUSTOMER = f"{BASE}/vocab/sales-analytics/customers"
OWL_CLASS = "http://www.w3.org/2002/07/owl#Class"
RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
RDFS_DOMAIN = "http://www.w3.org/2000/01/rdf-schema#domain"


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


def _fuseki() -> str:
    return f"http://localhost:{os.environ['FUSEKI_PORT']}/provisa"


@pytest.fixture
def named_graph():
    """A named graph of this test's own in the store, dropped afterwards."""
    graph = f"urn:provisa-test:rdf-export:{uuid.uuid4()}"
    yield graph
    resp = httpx.post(f"{_fuseki()}/update", data={"update": f"DROP SILENT GRAPH <{graph}>"})
    assert resp.status_code in (200, 204), resp.text


def _held(graph: str) -> Graph:
    """What the store holds in ``graph``, read back over its Graph Store endpoint."""
    resp = httpx.get(
        f"{_fuseki()}/data",
        params={"graph": graph},
        headers={"Accept": "application/n-triples"},
        timeout=60,
    )
    assert resp.status_code == 200, resp.text
    return Graph().parse(data=resp.text, format="nt")


def _lexical(graph: Graph) -> set[tuple[str, str, str]]:
    """The triples by their terms' text: a store writes an xsd:string literal plain (RDF 1.1)."""
    return {(str(s), str(p), str(o)) for s, p, o in graph}


def _load(endpoint: str, role: str, graph: str, store: str | None = None) -> httpx.Response:
    return httpx.post(
        f"{endpoint}/data/sparql/export/load",
        json={
            "scope": "class",
            "value": CUSTOMER,
            "endpoint": store or f"{_fuseki()}/data",
            "graph": graph,
        },
        headers={"X-Provisa-Role": role},
        timeout=300,
    )


def test_the_load_reports_the_triples_the_store_then_holds(endpoint, named_graph):
    resp = _load(endpoint, "org_admin", named_graph)
    assert resp.status_code == 200, resp.text
    report = resp.json()
    assert report["errors"] == []
    held = _held(named_graph)
    assert report["loaded"] >= len(held) > 0  # the store merges a triple sent twice into one
    assert any(
        str(s) == CUSTOMER and str(p) == RDF_TYPE and str(o) == OWL_CLASS for s, p, o in held
    )
    assert any(str(p) == RDFS_DOMAIN and str(o) == CUSTOMER for _, p, o in held)
    assert any(str(p) == f"{CUSTOMER}#email" for _, p, _o in held)
    instances = {s for s, p, o in held if str(p) == RDF_TYPE and str(o) == CUSTOMER}
    assert instances


def test_a_governed_roles_load_holds_only_what_it_reads(endpoint, named_graph):
    resp = _load(endpoint, "sales_analytics_analyst", named_graph)
    assert resp.status_code == 200, resp.text
    assert resp.json()["errors"] == []
    held = _held(named_graph)
    assert any(str(p) == f"{CUSTOMER}#name" for _, p, _o in held)
    assert not any(str(p) == f"{CUSTOMER}#email" for _, p, _o in held)


def test_the_file_export_holds_what_the_load_does(endpoint, named_graph):
    assert _load(endpoint, "org_admin", named_graph).json()["errors"] == []
    held = _lexical(_held(named_graph))
    for fmt, parse_as in (("turtle", "turtle"), ("ntriples", "nt")):
        resp = httpx.post(
            f"{endpoint}/data/sparql/export",
            json={"scope": "class", "value": CUSTOMER, "format": fmt},
            headers={"X-Provisa-Role": "org_admin"},
            timeout=300,
        )
        assert resp.status_code == 200, resp.text
        assert _lexical(Graph().parse(data=resp.text, format=parse_as)) == held, fmt


def test_a_store_that_refuses_is_reported_with_its_reason(endpoint, named_graph):
    # Fuseki's query service takes no Graph Store POST: each batch is refused, and said so.
    resp = _load(endpoint, "org_admin", named_graph, store=f"{_fuseki()}/query")
    assert resp.status_code == 200, resp.text
    report = resp.json()
    assert report["loaded"] == 0
    assert report["errors"] and all("triples refused: HTTP" in e for e in report["errors"])
