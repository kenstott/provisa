# Copyright (c) 2026 Kenneth Stott
# Canary: bc302c11-2c63-4d4d-9b11-b5478c76035f
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""BDD step definitions for REQ-296 — Neo4j query preview shape validation,
and REQ-298 — HTTP POST support for the API source caller (Neo4j & SPARQL)."""

from __future__ import annotations

import asyncio
import json
from urllib.parse import parse_qs

import httpx
import pytest
from pytest_bdd import given, scenarios, then, when

from provisa.api_source.caller import _request_with_retry
from provisa.api_source.models import ApiColumn, ApiColumnType
from provisa.neo4j.preview import (
    Neo4jNodeObjectError,
    validate_shape,
)
from provisa.neo4j.source import Neo4jSourceConfig
from provisa.neo4j.source import build_endpoint as build_neo4j_endpoint
from provisa.sparql.source import SparqlSourceConfig
from provisa.sparql.source import build_endpoint as build_sparql_endpoint

scenarios("../features/REQ-296.feature")
scenarios("../features/REQ-298.feature")


@pytest.fixture
def shared_data() -> dict:
    """Plain dict to pass state between Given/When/Then steps."""
    return {}


# ---------------------------------------------------------------------------
# REQ-296 — Neo4j preview shape validation
# ---------------------------------------------------------------------------


@given("a steward submitting a Cypher query for Neo4j table registration")
def steward_submits_cypher(shared_data: dict) -> None:
    """Record the steward's Cypher query for the registration preview step."""
    shared_data["cypher"] = "MATCH (n:Person) RETURN n"
    shared_data["database"] = "neo4j"
    assert shared_data["cypher"], "steward must supply a Cypher query"


@when("the query returns node or edge objects instead of flat scalar projections")
def query_returns_node_objects(shared_data: dict) -> None:
    """Simulate a preview response whose rows contain node/edge objects.

    A Cypher query like ``RETURN n`` yields whole node objects, which the
    neo4j_tabular normalizer surfaces as dict (or list) values per column.
    """
    # Preview rows as the normalizer would produce them for RETURN n
    preview_rows = [
        {
            "n": {
                "_id": "1",
                "_labels": ["Person"],
                "name": "Alice",
                "age": 30,
            }
        },
        {
            "n": {
                "_id": "2",
                "_labels": ["Person"],
                "name": "Bob",
                "age": 42,
            }
        },
    ]
    shared_data["preview_rows"] = preview_rows

    # Run the real shape validation and capture the resulting error.
    error: Neo4jNodeObjectError | None = None
    try:
        validate_shape(preview_rows)
    except Neo4jNodeObjectError as exc:
        error = exc
    shared_data["validation_error"] = error


@then(
    "registration is blocked with an error directing the steward to use explicit scalar RETURN aliases")
def registration_blocked_with_error(shared_data: dict) -> None:
    """Assert validation raised and the message guides toward scalar aliases."""
    error = shared_data.get("validation_error")
    assert error is not None, "expected registration to be blocked by validate_shape"
    assert isinstance(error, Neo4jNodeObjectError)

    message = str(error)
    # The error must name the offending column and indicate a node/list object.
    assert "n" in message
    assert "node object" in message.lower() or "list" in message.lower()

    # A scalar projection must pass cleanly, proving the block is shape-specific.
    scalar_rows = [
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 42},
    ]
    # Should not raise — explicit scalar RETURN aliases are accepted.
    validate_shape(scalar_rows)


# ---------------------------------------------------------------------------
# REQ-298 — HTTP POST support for the API source caller (Neo4j & SPARQL)
# ---------------------------------------------------------------------------


@given("a Neo4j or SPARQL source requiring POST requests with a request body")
def neo4j_sparql_post_source(shared_data: dict) -> None:
    """Build real Neo4j and SPARQL POST endpoints via the source builders."""
    neo4j_cfg = Neo4jSourceConfig(
        source_id="neo4j-1", host="localhost", port=7474, database="neo4j"
    )
    neo4j_ep = build_neo4j_endpoint(
        neo4j_cfg,
        "users",
        "MATCH (u) RETURN u.id AS id",
        [ApiColumn(name="id", type=ApiColumnType.integer)],
    )

    sparql_cfg = SparqlSourceConfig(
        source_id="sparql-1", endpoint_url="http://fuseki:3030/ds/sparql"
    )
    sparql_ep = build_sparql_endpoint(
        sparql_cfg,
        "things",
        "SELECT ?s WHERE { ?s ?p ?o }",
        [ApiColumn(name="s", type=ApiColumnType.string)],
    )

    # Both endpoints must declare POST with a body encoding for transmission.
    assert neo4j_ep.method == "POST"
    assert neo4j_ep.body_encoding == "json"
    assert sparql_ep.method == "POST"
    assert sparql_ep.body_encoding == "form"

    shared_data["neo4j_ep"] = neo4j_ep
    shared_data["sparql_ep"] = sparql_ep


@when("the API source caller executes a query")
def caller_executes_query(shared_data: dict) -> None:
    """Execute POST (Neo4j JSON, SPARQL form) and a GET via the real caller.

    A MockTransport captures the outgoing requests so the transmitted bodies
    can be asserted on, while a control GET request proves the GET path is
    unaffected by the POST-body changes.
    """
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        content_type = request.headers.get("content-type", "")
        if "x-www-form-urlencoded" in content_type or request.method == "GET":
            return httpx.Response(
                200, json={"head": {"vars": ["s"]}, "results": {"bindings": []}}
            )
        return httpx.Response(200, json={"results": [], "errors": []})

    transport = httpx.MockTransport(handler)

    async def run() -> tuple[httpx.Response, httpx.Response, httpx.Response]:
        async with httpx.AsyncClient(
            transport=transport, base_url="http://localhost:7474"
        ) as client:
            neo4j_resp = await _request_with_retry(
                client,
                "POST",
                "/db/neo4j/tx/commit",
                json_body={"statement": "MATCH (u) RETURN u.id AS id"},
            )
        async with httpx.AsyncClient(
            transport=transport, base_url="http://fuseki:3030"
        ) as client:
            sparql_resp = await _request_with_retry(
                client,
                "POST",
                "/ds/sparql",
                form_body={"query": "SELECT ?s WHERE { ?s ?p ?o }"},
            )
            get_resp = await _request_with_retry(
                client,
                "GET",
                "/ds/sparql",
                params={"q": "SELECT 1"},
            )
        return neo4j_resp, sparql_resp, get_resp

    neo4j_resp, sparql_resp, get_resp = asyncio.run(run())

    shared_data["captured"] = captured
    shared_data["neo4j_resp"] = neo4j_resp
    shared_data["sparql_resp"] = sparql_resp
    shared_data["get_resp"] = get_resp


@then(
    "the POST body is transmitted correctly and existing GET endpoints are unaffected"
)
def post_body_transmitted(shared_data: dict) -> None:
    """Assert POST bodies were transmitted and the GET request carried none."""
    captured = shared_data["captured"]
    assert len(captured) == 3, "expected Neo4j POST, SPARQL POST, and control GET"
    neo4j_req, sparql_req, get_req = captured

    # Neo4j: JSON body {"statement": "<cypher>"} with JSON content type.
    assert neo4j_req.method == "POST"
    assert neo4j_req.headers["content-type"].startswith("application/json")
    neo4j_body = json.loads(neo4j_req.content.decode())
    assert neo4j_body == {"statement": "MATCH (u) RETURN u.id AS id"}
    assert shared_data["neo4j_resp"].status_code == 200

    # SPARQL: form-encoded body query=<encoded-sparql> per SPARQL 1.1 protocol.
    assert sparql_req.method == "POST"
    assert "x-www-form-urlencoded" in sparql_req.headers["content-type"]
    form = parse_qs(sparql_req.content.decode())
    assert form["query"][0] == "SELECT ?s WHERE { ?s ?p ?o }"
    assert shared_data["sparql_resp"].status_code == 200

    # GET endpoint unaffected: no request body, params preserved on the URL.
    assert get_req.method == "GET"
    assert not get_req.content, "GET requests must not carry a request body"
    assert "q=SELECT" in str(get_req.url)
    assert shared_data["get_resp"].status_code == 200
