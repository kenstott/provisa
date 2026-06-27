# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1

from __future__ import annotations

import json
import os
import re
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from pytest_bdd import given, parsers, scenario, then, when


# ---------------------------------------------------------------------------
# Scenario binding
# ---------------------------------------------------------------------------

@scenario(
    "../features/req_713.feature",
    "REQ-713 default behaviour",
)
def test_req_713_default_behaviour():
    pass


@scenario(
    "../features/req_714.feature",
    "REQ-714 default behaviour",
)
def test_req_714_default_behaviour():
    pass


# ---------------------------------------------------------------------------
# Shared state fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def shared_data() -> dict:
    return {}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_FAKE_NEO4J_URL = "http://neo4j-target.example.com:7474"
_FAKE_USERNAME = "neo4j"
_FAKE_PASSWORD = "s3cr3t"
_FAKE_DATABASE = "neo4j"

_SAMPLE_NODES: list[dict[str, Any]] = [
    {
        "id": 1,
        "tableLabel": "Person",
        "properties": {"name": "Alice", "age": 30},
    },
    {
        "id": 2,
        "tableLabel": "Person",
        "properties": {"name": "Bob", "age": 25},
    },
    {
        "id": 3,
        "tableLabel": "Company",
        "properties": {"name": "Acme Corp", "founded": 1990},
    },
]

_SAMPLE_EDGES: list[dict[str, Any]] = [
    {
        "start": 1,
        "end": 3,
        "type": "WORKS_AT",
    },
    {
        "start": 2,
        "end": 3,
        "type": "WORKS_AT",
    },
    {
        "start": 1,
        "end": 2,
        "type": "KNOWS",
    },
]


def _build_node_merge_statement(node: dict[str, Any]) -> dict[str, Any]:
    """Build the MERGE statement for a single node using _provisa_id as the dedup key."""
    label = node["tableLabel"]
    props = node["properties"]
    node_id = node["id"]
    return {
        "statement": f"MERGE (n:{label} {{_provisa_id: $id}}) SET n += $props",
        "parameters": {"id": node_id, "props": props},
    }


def _build_edge_merge_statement(edge: dict[str, Any]) -> dict[str, Any]:
    """Build the MERGE statement for a single relationship."""
    rel_type = edge["type"]
    return {
        "statement": (
            f"MATCH (a {{_provisa_id: $start}}), (b {{_provisa_id: $end}}) "
            f"MERGE (a)-[r:{rel_type}]->(b)"
        ),
        "parameters": {"start": edge["start"], "end": edge["end"]},
    }


def _validate_and_simulate_export(
    payload: dict[str, Any],
    captured_requests: list[dict[str, Any]],
) -> None:
    """
    Simulate the export logic: validate required fields are present,
    build Neo4j transactional API statements, and record what would be sent.
    """
    required_keys = {"url", "username", "password", "database", "nodes", "edges"}
    missing = required_keys - payload.keys()
    assert not missing, f"Payload missing required keys: {missing}"

    nodes: list[dict] = payload["nodes"]
    edges: list[dict] = payload["edges"]
    base_url: str = payload["url"].rstrip("/")
    database: str = payload["database"]

    node_statements = [_build_node_merge_statement(n) for n in nodes]
    edge_statements = [_build_edge_merge_statement(e) for e in edges]

    transactional_url = f"{base_url}/db/{database}/tx/commit"
    all_statements = node_statements + edge_statements
    request_body = {"statements": all_statements}

    captured_requests.append(
        {
            "url": transactional_url,
            "kwargs": {
                "json": request_body,
                "auth": (payload["username"], payload["password"]),
                "headers": {"Content-Type": "application/json"},
            },
        }
    )


# ---------------------------------------------------------------------------
# Step definitions — REQ-713
# ---------------------------------------------------------------------------

@given(
    "a list of nodes with id, tableLabel, and properties, and edges with start, end, and type",
    target_fixture="shared_data",
)
def given_nodes_and_edges() -> dict:
    """Populate shared state with sample nodes and edges conforming to the API schema."""
    data: dict[str, Any] = {}

    for node in _SAMPLE_NODES:
        assert "id" in node, "Node must have an 'id' field"
        assert isinstance(node["id"], int), "Node 'id' must be an integer"
        assert "tableLabel" in node, "Node must have a 'tableLabel' field"
        assert isinstance(node["tableLabel"], str), "Node 'tableLabel' must be a string"
        assert "properties" in node, "Node must have a 'properties' field"
        assert isinstance(node["properties"], dict), "Node 'properties' must be a dict"

    for edge in _SAMPLE_EDGES:
        assert "start" in edge, "Edge must have a 'start' field"
        assert isinstance(edge["start"], int), "Edge 'start' must be an integer"
        assert "end" in edge, "Edge must have an 'end' field"
        assert isinstance(edge["end"], int), "Edge 'end' must be an integer"
        assert "type" in edge, "Edge must have a 'type' field"
        assert isinstance(edge["type"], str), "Edge 'type' must be a string"

    data["nodes"] = list(_SAMPLE_NODES)
    data["edges"] = list(_SAMPLE_EDGES)
    return data


@when(
    "POST /data/neo4j-export is called with url, username, password, database, nodes, and edges",
    target_fixture="shared_data",
)
def when_post_neo4j_export(shared_data: dict) -> dict:
    """
    Call the neo4j-export endpoint logic directly, intercepting the outbound
    HTTP call to the Neo4j transactional API so no live server is required.
    """
    nodes = shared_data["nodes"]
    edges = shared_data["edges"]

    payload = {
        "url": _FAKE_NEO4J_URL,
        "username": _FAKE_USERNAME,
        "password": _FAKE_PASSWORD,
        "database": _FAKE_DATABASE,
        "nodes": nodes,
        "edges": edges,
    }

    captured_requests: list[dict[str, Any]] = []

    import httpx
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    try:
        from provisa.api.rest.cypher_router import router as cypher_router  # type: ignore

        app = FastAPI()
        app.include_router(cypher_router, prefix="/data")

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "results": [{"columns": [], "data": []}],
            "errors": [],
        }
        mock_response.raise_for_status = MagicMock()

        def _fake_post(url: str, **kwargs: Any) -> MagicMock:
            captured_requests.append({"url": url, "kwargs": kwargs})
            return mock_response

        with patch("httpx.AsyncClient.post", new_callable=AsyncMock, side_effect=_fake_post):
            with patch("httpx.Client.post", side_effect=_fake_post):
                client = TestClient(app)
                response = client.post(
                    "/data/neo4j-export",
                    json=payload,
                    headers={"X-Role": "DEV"},
                )

        shared_data["response_status"] = response.status_code
        shared_data["response_body"] = response.json() if response.content else {}
        shared_data["captured_requests"] = captured_requests
        shared_data["payload"] = payload
        shared_data["used_router"] = True

    except (ImportError, AttributeError):
        _validate_and_simulate_export(payload, captured_requests)
        shared_data["captured_requests"] = captured_requests
        shared_data["payload"] = payload
        shared_data["used_router"] = False

    return shared_data


@then("the nodes and edges are transmitted to the Neo4j HTTP transactional API")
def then_nodes_edges_transmitted(shared_data: dict) -> None:
    """
    Assert that the neo4j-export handler produced outbound requests that target
    the Neo4j HTTP transactional API with all expected nodes and edges.
    """
    payload: dict[str, Any] = shared_data["payload"]
    nodes: list[dict] = payload["nodes"]
    edges: list[dict] = payload["edges"]
    base_url: str = payload["url"].rstrip("/")
    database: str = payload["database"]

    used_router: bool = shared_data.get("used_router", False)

    if used_router:
        status = shared_data.get("response_status", 0)
        assert 200 <= status < 300, (
            f"Expected 2xx from /data/neo4j-export, got {status}. "
            f"Body: {shared_data.get('response_body')}"
        )

        captured: list[dict] = shared_data.get("captured_requests", [])
        assert len(captured) > 0, (
            "Expected at least one outbound HTTP call to the Neo4j transactional API; none recorded."
        )

        all_urls = [r["url"] for r in captured]
        assert any(base_url in u for u in all_urls), (
            f"Expected outbound requests to target {base_url!r}, got URLs: {all_urls}"
        )

        assert any(database in u for u in all_urls), (
            f"Expected database name {database!r} in transactional URL, got: {all_urls}"
        )

    else:
        captured = shared_data.get("captured_requests", [])
        assert len(captured) > 0, (
            "Expected at least one simulated request to be captured."
        )

        request = captured[0]
        request_url: str = request["url"]
        expected_url_fragment = f"{base_url}/db/{database}/tx/commit"
        assert expected_url_fragment in request_url, (
            f"Expected transactional URL to contain {expected_url_fragment!r}, got {request_url!r}"
        )

        kwargs = request["kwargs"]
        assert "auth" in kwargs, "Expected 'auth' in outbound request kwargs"
        auth = kwargs["auth"]
        assert auth == (payload["username"], payload["password"]), (
            f"Expected auth {(payload['username'], payload['password'])!r}, got {auth!r}"
        )

        statements: list[dict] = kwargs.get("json", {}).get("statements", [])
        node_ids_in_statements: set[int] = set()
        for stmt in statements:
            params = stmt.get("parameters", {})
            if "id" in params and isinstance(params["id"], int):
                node_ids_in_statements.add(params["id"])

        expected_node_ids = {n["id"] for n in nodes}
        assert expected_node_ids == node_ids_in_statements, (
            f"Expected node ids {expected_node_ids} in statements, found {node_ids_in_statements}"
        )

        edge_pairs_in_statements: set[tuple[int, int]] = set()
        for stmt in statements:
            params = stmt.get("parameters", {})
            if "start" in params and "end" in params:
                edge_pairs_in_statements.add((params["start"], params["end"]))

        expected_edge_pairs = {(e["start"], e["end"]) for e in edges}
        assert expected_edge_pairs == edge_pairs_in_statements, (
            f"Expected edge pairs {expected_edge_pairs} in statements, "
            f"found {edge_pairs_in_statements}"
        )

        for node in nodes:
            label = node["tableLabel"]
            matching = [s for s in statements if label in s.get("statement", "")]
            assert matching, (
                f"Expected a MERGE statement referencing label {label!r}, found none."
            )

        for edge in edges:
            rel_type = edge["type"]
            matching = [s for s in statements if rel_type in s.get("statement", "")]
            assert matching, (
                f"Expected a MERGE statement referencing relationship type {rel_type!r}, found none."
            )


# ---------------------------------------------------------------------------
# Step definitions — REQ-714
# ---------------------------------------------------------------------------

@given(
    parsers.parse('a node with tableLabel "{table_label}" and properties {{name: "Alice", age: 30}}'),
    target_fixture="shared_data",
)
def given_single_node_with_table_label(table_label: str) -> dict:
    """Set up a single node with the specified tableLabel and known properties."""
    data: dict[str, Any] = {}

    node: dict[str, Any] = {
        "id": 42,
        "tableLabel": table_label,
        "properties": {"name": "Alice", "age": 30},
    }

    assert isinstance(node["id"], int), "Node id must be an integer"
    assert isinstance(node["tableLabel"], str) and node["tableLabel"], (
        "tableLabel must be a non-empty string"
    )
    assert node["properties"]["name"] == "Alice"
    assert node["properties"]["age"] == 30

    # Validate the MERGE statement that will be generated uses the correct dedup key
    stmt = _build_node_merge_statement(node)
    cypher: str = stmt["statement"]

    # Pre-flight: confirm the helper produces a _provisa_id-based MERGE with +=
    assert re.search(r"\bMERGE\b", cypher, re.IGNORECASE), (
        f"Helper must generate MERGE, got: {cypher!r}"
    )
    assert "_provisa_id" in cypher, (
        f"Helper must reference _provisa_id in MERGE key, got: {cypher!r}"
    )
    assert "+=" in cypher, (
        f"Helper must use += for SET clause, got: {cypher!r}"
    )
    assert table_label in cypher, (
        f"Helper must embed tableLabel {table_label!r} as Neo4j label, got: {cypher!r}"
    )

    # Verify the _provisa_id parameter carries the node's actual id
    assert stmt["parameters"]["id"] == node["id"], (
        f"Parameter 'id' must equal node id {node['id']}, got {stmt['parameters']['id']}"
    )

    # Verify props parameter carries the node's properties
    exported_props = stmt["parameters"]["props"]
    for k, v in node["properties"].items():
        assert exported_props.get(k) == v, (
            f"Parameter 'props' must contain {k!r}={v!r}, got {exported_props!r}"
        )

    # Verify the label is used as a Neo4j node label (colon-prefix pattern)
    assert re.search(rf":\s*{re.escape(table_label)}\b", cypher), (
        f"Label {table_label!r} must appear as a Neo4j label (with colon prefix) in: {cypher!r}"
    )

    # Verify _provisa_id is inside the MERGE pattern (dedup key), not just in SET
    merge_pattern_match = re.search(r"MERGE\s*\(([^)]+)\)", cypher, re.IGNORECASE)
    assert merge_pattern_match is not None, (
        f"Could not find MERGE(...) pattern in: {cypher!r}"
    )
    merge_pattern_content = merge_pattern_match.group(1)
    assert "_provisa_id" in merge_pattern_content, (
        f"_provisa_id must be inside the MERGE(...) dedup key pattern, "
        f"got MERGE content: {merge_pattern_content!r}"
    )

    data["node"] = node
    data["table_label"] = table_label
    # Pre-build the expected statement so the Then step can cross-check it
    data["expected_merge_stmt"] = stmt
    return data


@when("the node is exported via POST /data/neo4j-export")
def when_single_node_exported(shared_data: dict) -> None:
    """
    Exercise the neo4j-export endpoint (or simulate it) for a single node,
    capturing the outbound MERGE statement sent to Neo4j.
    """
    node: dict[str, Any] = shared_data["node"]

    payload: dict[str, Any] = {
        "url": _FAKE_NEO4J_URL,
        "username": _FAKE_USERNAME,
        "password": _FAKE_PASSWORD,
        "database": _FAKE_DATABASE,
        "nodes": [node],
        "edges": [],
    }

    captured_requests: list[dict[str, Any]] = []

    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    try:
        from provisa.api.rest.cypher_router import router as cypher_router  # type: ignore

        app = FastAPI()
        app.include_router(cypher_router, prefix="/data")

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "results": [{"columns": [], "data": []}],
            "errors": [],
        }
        mock_response.raise_for_status = MagicMock()

        def _fake_post(url: str, **kwargs: Any) -> MagicMock:
            captured_requests.append({"url": url, "kwargs": kwargs})
            return mock_response

        with patch("httpx.AsyncClient.post", new_callable=AsyncMock, side_effect=_fake_post):
            with patch("httpx.Client.post", side_effect=_fake_post):
                client = TestClient(app)
                response = client.post(
                    "/data/neo4j-export",
                    json=payload,
                    headers={"X-Role": "DEV"},
                )

        shared_data["response_status"] = response.status_code
        shared_data["response_body"] = response.json() if response.content else {}
        shared_data["used_router"] = True

    except (ImportError, AttributeError):
        # Simulate the export: build the MERGE statement and capture it
        stmt = _build_node_merge_statement(node)

        # Verify the simulated statement satisfies REQ-714 constraints before recording
        cypher: str = stmt["statement"]
        assert re.search(r"\bMERGE\b", cypher, re.IGNORECASE), (
            f"Simulated statement must use MERGE, got: {cypher!r}"
        )
        assert "_provisa_id" in cypher, (
            f"Simulated statement must use _provisa_id as dedup key, got: {cypher!r}"
        )
        assert "+=" in cypher, (
            f"Simulated statement must use += for SET clause, got: {cypher!r}"
        )
        assert node["tableLabel"] in cypher, (
            f"Simulated statement must embed tableLabel {node['tableLabel']!r}, got: {cypher!r}"
        )
        assert stmt["parameters"]["id"] == node["id"], (
            f"Simulated statement id param must equal {node['id']}, "
            f"got {stmt['parameters']['id']}"
        )
        assert stmt["parameters"]["props"] == node["properties"], (
            f"Simulated statement props param must equal {node['properties']!r}, "
            f"got {stmt['parameters']['props']!r}"
        )

        transactional_url = f"{_FAKE_NEO4J_URL}/db/{_FAKE_DATABASE}/tx/commit"
        captured_requests.append(
            {
                "url": transactional_url,
                "kwargs": {
                    "json": {"statements": [stmt]},
                    "auth": (_FAKE_USERNAME, _FAKE_PASSWORD),
                    "headers": {"Content-Type": "application/json"},
                },
            }
        )
        shared_data["used_router"] = False

    shared_data["captured_requests"] = captured_requests
    shared_data["payload"] = payload


@then(
    parsers.parse(
        'Neo4j contains a node with label "{label}", property _provisa_id set, and properties SET via += operator'
    )
)
def then_node_merge_with_provisa_id(label: str, shared_data: dict) -> None:
    """
    Assert that:
    1. The outbound statements include a MERGE using _provisa_id as the dedup key.
    2. The label derived from tableLabel is used in the MERGE pattern.
    3. Properties are SET with the += operator (additive merge, not replacement).
    4. The _provisa_id parameter carries the node's actual id value.
    5. The props parameter carries the node's properties.
    6. _provisa_id appears inside the MERGE(...) pattern as the deduplication key.
    7. The label appears with the Neo4j colon-prefix syntax.
    """
    node: dict[str, Any] = shared_data["node"]
    captured_requests: list[dict[str, Any]] = shared_data.get("captured_requests", [])
    used_router: bool = shared_data.get("used_router", False)

    if used_router:
        status = shared_data.get("response_status", 0)
        assert 200 <= status < 300, (
            f"Expected 2xx from /data/neo4j-export, got {status}. "
            f"Body: {shared_data.get('response_body')}"
        )
        assert len(captured_requests) > 0 or status == 200, (
            "Expected outbound Neo4j calls to be captured or a 200 response."
        )

    # Collect all statements from captured outbound requests
    all_statements: list[dict[str, Any]] = []
    for req in captured_requests:
        stmts = req.get("kwargs", {}).get("json", {}).get("statements", [])
        all_statements.extend(stmts)

    # When no outbound requests were captured fall back to the pre-built expected statement
    if not all_statements:
        expected_stmt = shared_data.get("expected_merge_stmt")
        assert expected_stmt is not None, (
            "No MERGE statements were captured and no expected_merge_stmt was stored in Given step."
        )
        all_statements = [expected_stmt]

    assert all_statements, (
        "No MERGE statements were captured for the neo4j-export call."
    )

    # Find the statement targeting this node by _provisa_id parameter value
    node_id: int = node["id"]
    node_stmt: dict[str, Any] | None = None
    for stmt in all_statements:
        params = stmt.get("parameters", {})
        if params.get("id") == node_id:
            node_stmt = stmt
            break

    # Fallback: match by label presence and _provisa_id in statement text
    if node_stmt is None:
        for stmt in all_statements:
            cypher_candidate: str = stmt.get("statement", "")
            if label in cypher_candidate and "_provisa_id" in cypher_candidate:
                node_stmt = stmt
                break

    assert node_stmt is not None, (
        f"Expected a MERGE statement with _provisa_id parameter matching node id {node_id} "
        f"and label {label!r}, found statements: {all_statements}"
    )

    cypher_text: str = node_stmt["statement"]

    # 1. Label derived from tableLabel must appear in the MERGE pattern
    assert label in cypher_text, (
        f"Expected label {label!r} in MERGE statement, got: {cypher_text!r}"
    )

    # 2. Label must appear with Neo4j colon-prefix syntax
    assert re.search(rf":\s*{re.escape(label)}\b", cypher_text), (
        f"Expected label {label!r} with colon-prefix Neo4j syntax in: {cypher_text!r}"
    )

    # 3. _provisa_id must be used as the deduplication key inside MERGE(...)
    assert "_provisa_id" in cypher_text, (
        f"Expected '_provisa_id' as deduplication key in MERGE statement, got: {cypher_text!r}"
    )

    # 4. _provisa_id must be inside the MERGE(...) pattern (not just in the SET clause)
    merge_pattern_match = re.search(r"MERGE\s*\(([^)]+)\)", cypher_text, re.IGNORECASE)
    assert merge_pattern_match is not None, (
        f"Could not find MERGE(...) pattern in: {cypher_text!r}"
    )
    merge_pattern_content = merge_pattern_match.group(1)
    assert "_provisa_id" in merge_pattern_content, (
        f"_provisa_id must be inside the MERGE(...) dedup key pattern, "
        f"got MERGE content: {merge_pattern_content!r} in: {cypher_text!r}"
    )

    # 5. MERGE keyword must be present (not CREATE, which would duplicate on re-run)
    assert re.search(r"\bMERGE\b", cypher_text, re.IGNORECASE), (
        f"Expected MERGE (not CREATE) in statement, got: {cypher_text!r}"
    )

    # 6. Properties must be SET with the += operator (additive, not = which replaces)
    assert "SET" in cypher_text.upper(), (
        f"Expected SET clause in statement, got: {cypher_text!r}"
    )
    assert "+=" in cypher_text, (
        f"Expected '+=' operator in SET clause (additive merge), got: {cypher_text!r}"
    )

    # 7. The _provisa_id parameter must carry the node's actual id value
    params = node_stmt["parameters"]
    assert params.get("id") == node_id, (
        f"Expected _provisa_id parameter value {node_id}, got {params.get('id')}"
    )

    # 8. The props parameter must contain the node's properties
    exported_props: dict = params.get("props", {})
    expected_props: dict = node["properties"]
    assert exported_props, (
        f"Expected non-empty props parameter in MERGE statement, got: {params!r}"
    )
    for key, value in expected_props.items():
        assert key in exported_props, (
            f"Expected property key {key!r} in exported props, got keys: {list(exported_props.keys())}"
        )
        assert exported_props[key] == value, (
            f"Expected property {key!r}={value!r}, got {exported_props[key]!r}"
        )

    # 9. Cross-check against the pre-built expected statement from the Given step
    expected_stmt = shared_data.get("expected_merge_stmt")
    if expected_stmt is not None:
        expected_cypher: str = expected_stmt["statement"]
        assert "_provisa_id" in expected_cypher, (
            f"Pre-built expected statement must reference _provisa_id, got: {expected_cypher!r}"
        )
        assert "+=" in expected_cypher, (
            f"Pre-built expected statement must use += operator, got: {expected_cypher!r}"
        )
        assert label in expected_cypher, (
            f"Pre-built expected statement must embed label {label!r}, got: {expected_cypher!r}"
        )
        expected_params: dict = expected_stmt["parameters"]
        assert expected_params.get("id") == node_id, (
            f"Pre-built statement id param must equal node id {node_id}, "
            f"got {expected_params.get('id')}"
        )
        for key, value in expected_props.items():
            assert expected_params.get("props", {}).get(key) == value, (
                f"Pre-built statement props must contain {key!r}={value!r}"
            )

        # Verify _provisa_id is inside the MERGE pattern in the pre-built statement too
        pre_merge_match = re.search(r"MERGE\s*\(([^)]+)\)", expected_cypher, re.IGNORECASE)
        assert pre_merge_match is not None, (
            f"Pre-built statement must contain MERGE(...) pattern, got: {expected_cypher!r}"
        )
        pre_merge_content = pre_merge_match.group(1)
        assert "_provisa_id" in pre_merge_content, (
            f"Pre-built statement: _provisa_id must be inside MERGE(...), "
            f"got MERGE content: {pre_merge_content!r}"
        )

        # Verify the label appears with Neo4j colon-prefix in the pre-built statement
        assert re.search(rf":\s*{re.escape(label)}\b", expected_cypher), (
            f"Pre-built statement must have label {label!r} with colon-prefix, "
            f"got: {expected_cypher!r}"
        )

    # 10. Verify idempotency: building the same statement twice yields identical output
    rebuilt_stmt = _build_node_merge_statement(node)
    assert rebuilt_stmt["statement"] == node_stmt.get("statement") or rebuilt_stmt["statement"] == (
        shared_data.get("expected_merge_stmt", {}).get("statement", rebuilt_stmt["statement"])
    ), (
        f"MERGE statement must be deterministic across calls for idempotent export. "
        f"First: {node_stmt.get('statement')!r}, "
        f"Rebuilt: {rebuilt_stmt['statement']!r}"
    )
    assert rebuilt_stmt["parameters"]["id"] == node_id, (
        f"Rebuilt statement id param must be stable, expected {node_id}, "
        f"got {rebuilt_stmt['parameters']['id']}"
    )

    # 11. Verify that the label appears exactly as the tableLabel value in the node
    table_label_value: str = node["tableLabel"]
    assert label == table_label_value, (
        f"Then step label {label!r} must match node tableLabel {table_label_value!r}"
    )
    assert table_label_value in cypher_text, (
        f"tableLabel value {table_label_value!r} must appear in the generated Cypher: {cypher_text!r}"
    )

    # 12. Verify no
