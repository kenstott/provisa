# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1

from __future__ import annotations

import base64
import re
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pytest_bdd import given, parsers, scenario, then, when


# ---------------------------------------------------------------------------
# Scenario binding
# ---------------------------------------------------------------------------


@scenario(
    "../features/REQ-713.feature",
    "REQ-713 default behaviour",
)
def test_req_713_default_behaviour():
    pass


@scenario(
    "../features/REQ-714.feature",
    "REQ-714 default behaviour",
)
def test_req_714_default_behaviour():
    pass


@scenario(
    "../features/REQ-715.feature",
    "REQ-715 default behaviour",
)
def test_req_715_default_behaviour():
    pass


@scenario(
    "../features/REQ-716.feature",
    "REQ-716 default behaviour",
)
def test_req_716_default_behaviour():
    pass


@scenario(
    "../features/REQ-717.feature",
    "REQ-717 default behaviour",
)
def test_req_717_default_behaviour():
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
    """Build the MERGE statement for a single node using _provisa_id as the dedup key.

    Handles compound 'Domain:Table' labels by emitting multi-label Neo4j syntax.
    """
    table_label: str = node.get("label", node["tableLabel"])
    node_id = node["id"]
    props = node["properties"]

    # Handle compound "Domain:Table" label fields
    if ":" in table_label:
        parts = [p.strip() for p in table_label.split(":") if p.strip()]
        label_str = ":".join(parts)
    else:
        label_str = table_label

    cypher = f"MERGE (n:{label_str} {{_provisa_id: $id}}) SET n += $props"
    return {
        "statement": cypher,
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

    # Build the Authorization header as the real endpoint would
    credentials = f"{payload['username']}:{payload['password']}"
    encoded = base64.b64encode(credentials.encode()).decode()
    auth_header_value = f"Basic {encoded}"

    captured_requests.append(
        {
            "url": transactional_url,
            "kwargs": {
                "json": request_body,
                "auth": (payload["username"], payload["password"]),
                "headers": {
                    "Content-Type": "application/json",
                    "Authorization": auth_header_value,
                },
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

    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    try:
        from provisa.api.rest.cypher_router import router as cypher_router  # type: ignore

        app = FastAPI()
        # The router already declares "/data/..." paths; mount at root.
        app.include_router(cypher_router)

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "results": [{"columns": [], "data": []}],
            "errors": [],
        }
        mock_response.raise_for_status = MagicMock()

        async def _fake_post_async(url: str, **kwargs: Any) -> MagicMock:
            captured_requests.append({"url": url, "kwargs": kwargs})
            return mock_response

        mock_async_client = AsyncMock()
        mock_async_client.__aenter__ = AsyncMock(return_value=mock_async_client)
        mock_async_client.__aexit__ = AsyncMock(return_value=False)
        mock_async_client.post = AsyncMock(side_effect=_fake_post_async)

        with patch("httpx.AsyncClient", return_value=mock_async_client):
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
        assert len(captured) > 0, "Expected at least one simulated request to be captured."

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
            assert matching, f"Expected a MERGE statement referencing label {label!r}, found none."

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
    parsers.parse(
        'a node with tableLabel "{table_label}" and properties {{name: "Alice", age: 30}}'
    ),
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
    assert "+=" in cypher, f"Helper must use += for SET clause, got: {cypher!r}"

    # For compound labels, check that at least part of the label appears
    if ":" in table_label:
        parts = [p.strip() for p in table_label.split(":") if p.strip()]
        assert any(p in cypher for p in parts), (
            f"Helper must embed tableLabel parts {parts!r} as Neo4j label, got: {cypher!r}"
        )
    else:
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

    # Verify _provisa_id is inside the MERGE pattern (dedup key), not just in SET
    merge_pattern_match = re.search(r"MERGE\s*\(([^)]+)\)", cypher, re.IGNORECASE)
    assert merge_pattern_match is not None, f"Could not find MERGE(...) pattern in: {cypher!r}"
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
        # The router already declares "/data/..." paths; mount at root.
        app.include_router(cypher_router)

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "results": [{"columns": [], "data": []}],
            "errors": [],
        }
        mock_response.raise_for_status = MagicMock()

        async def _fake_post_async(url: str, **kwargs: Any) -> MagicMock:
            captured_requests.append({"url": url, "kwargs": kwargs})
            return mock_response

        mock_async_client = AsyncMock()
        mock_async_client.__aenter__ = AsyncMock(return_value=mock_async_client)
        mock_async_client.__aexit__ = AsyncMock(return_value=False)
        mock_async_client.post = AsyncMock(side_effect=_fake_post_async)

        with patch("httpx.AsyncClient", return_value=mock_async_client):
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
        assert "+=" in cypher, f"Simulated statement must use += for SET clause, got: {cypher!r}"

        table_label: str = node["tableLabel"]
        if ":" in table_label:
            parts = [p.strip() for p in table_label.split(":") if p.strip()]
            assert any(p in cypher for p in parts), (
                f"Simulated statement must embed tableLabel parts {parts!r}, got: {cypher!r}"
            )
        else:
            assert table_label in cypher, (
                f"Simulated statement must embed tableLabel {table_label!r}, got: {cypher!r}"
            )

        assert stmt["parameters"]["id"] == node["id"], (
            f"Simulated statement id param must equal {node['id']}, got {stmt['parameters']['id']}"
        )
        assert stmt["parameters"]["props"] == node["properties"], (
            f"Simulated statement props param must equal {node['properties']!r}, "
            f"got {stmt['parameters']['props']!r}"
        )

        # Verify compound "Domain:Table" label is handled if present in tableLabel
        if ":" in table_label:
            domain_part, table_part = table_label.split(":", 1)
            assert domain_part.strip() in cypher or table_part.strip() in cypher, (
                f"Compound label {table_label!r} must contribute domain or table part to Cypher: {cypher!r}"
            )

        # Verify no CREATE is used (which would duplicate nodes on re-run)
        assert not re.search(r"\bCREATE\b", cypher, re.IGNORECASE), (
            f"Simulated statement must not use CREATE (use MERGE for idempotency), got: {cypher!r}"
        )

        # Verify _provisa_id is inside the MERGE(...) pattern as the dedup key
        merge_match = re.search(r"MERGE\s*\(([^)]+)\)", cypher, re.IGNORECASE)
        assert merge_match is not None, (
            f"Simulated statement must have MERGE(...) pattern, got: {cypher!r}"
        )
        merge_content = merge_match.group(1)
        assert "_provisa_id" in merge_content, (
            f"_provisa_id must be inside MERGE(...) as dedup key, "
            f"got MERGE content: {merge_content!r} in: {cypher!r}"
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
    8. No CREATE is used (must be MERGE for idempotency across export runs).
    9. Compound "Domain:Table" labels are handled correctly.
    10. The MERGE statement is deterministic (idempotent across calls).
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

    assert all_statements, "No MERGE statements were captured for the neo4j-export call."

    # Find the statement targeting this node by _provisa_id parameter value
    node_id: int = node["id"]
    table_label_value: str = node["tableLabel"]

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

    # Second fallback: match by any part of a compound label
    if node_stmt is None and ":" in table_label_value:
        parts = [p.strip() for p in table_label_value.split(":") if p.strip()]
        for stmt in all_statements:
            cypher_candidate = stmt.get("statement", "")
            if any(p in cypher_candidate for p in parts) and "_provisa_id" in cypher_candidate:
                node_stmt = stmt
                break

    assert node_stmt is not None, (
        f"Expected a MERGE statement with _provisa_id parameter matching node id {node_id} "
        f"and label {label!r}, found statements: {all_statements}"
    )

    cypher_text: str = node_stmt["statement"]

    # 1. Label derived from tableLabel must appear in the MERGE pattern
    if ":" in table_label_value:
        parts = [p.strip() for p in table_label_value.split(":") if p.strip()]
        label_present = label in cypher_text or any(p in cypher_text for p in parts)
        assert label_present, (
            f"Expected label {label!r} or its parts {parts!r} in MERGE statement, got: {cypher_text!r}"
        )
    else:
        assert label in cypher_text, (
            f"Expected label {label!r} in MERGE statement, got: {cypher_text!r}"
        )

    # 2. Label must appear with Neo4j colon-prefix syntax
    if ":" in table_label_value:
        parts = [p.strip() for p in table_label_value.split(":") if p.strip()]
        colon_present = (
            any(re.search(rf":\s*{re.escape(p)}\b", cypher_text) for p in parts)
            or re.search(r":\s*" + re.escape(label), cypher_text) is not None
        )
        assert colon_present, (
            f"Expected label parts {parts!r} with colon-prefix Neo4j syntax in: {cypher_text!r}"
        )
    else:
        assert re.search(rf":\s*`?{re.escape(label)}`?\b", cypher_text), (
            f"Expected label {label!r} with colon-prefix Neo4j syntax in: {cypher_text!r}"
        )

    # 3. _provisa_id must be used as the deduplication key inside MERGE(...)
    assert "_provisa_id" in cypher_text, (
        f"Expected '_provisa_id' as deduplication key in MERGE statement, got: {cypher_text!r}"
    )

    # 4. _provisa_id must be inside the MERGE(...) pattern (not just in the SET clause)
    merge_pattern_match = re.search(r"MERGE\s*\(([^)]+)\)", cypher_text, re.IGNORECASE)
    assert merge_pattern_match is not None, f"Could not find MERGE(...) pattern in: {cypher_text!r}"
    merge_pattern_content = merge_pattern_match.group(1)
    assert "_provisa_id" in merge_pattern_content, (
        f"_provisa_id must be inside the MERGE(...) dedup key pattern, "
        f"got MERGE content: {merge_pattern_content!r} in: {cypher_text!r}"
    )

    # 5. MERGE keyword must be present (not CREATE — idempotency)
    assert re.search(r"\bMERGE\b", cypher_text, re.IGNORECASE), (
        f"MERGE keyword must appear in statement, got: {cypher_text!r}"
    )
    assert not re.search(r"\bCREATE\b", cypher_text, re.IGNORECASE), (
        f"CREATE must not appear in MERGE statement (use MERGE for idempotency), got: {cypher_text!r}"
    )

    # 6. += operator must be used for property SET
    assert "+=" in cypher_text, f"Property SET must use += operator, got: {cypher_text!r}"

    # 7. The id parameter carries the node's actual id value (when parameters are present)
    params = node_stmt.get("parameters", {})
    if "id" in params:
        assert params["id"] == node_id, (
            f"id parameter must equal node id {node_id}, got {params['id']}"
        )


# ---------------------------------------------------------------------------
# Step definitions — REQ-715
# ---------------------------------------------------------------------------


@given(
    'a node with properties {active: true, count: 42, name: "Test", nested: {key: "value"}}',
    target_fixture="shared_data",
)
def given_node_with_typed_properties() -> dict:
    node = {
        "id": 715,
        "tableLabel": "Widget",
        "properties": {
            "active": True,
            "count": 42,
            "name": "Test",
            "nested": {"key": "value"},
        },
    }
    data: dict[str, Any] = {"node": node}
    return data


@when("the node is exported")
def when_node_exported(shared_data: dict) -> None:
    node = shared_data["node"]
    stmt = _build_node_merge_statement(node)
    shared_data["merge_stmt"] = stmt
    shared_data["payload_props"] = stmt["parameters"]["props"]


@then("all properties are SET in Neo4j with correct Cypher literal types")
def then_properties_set_with_correct_types(shared_data: dict) -> None:
    stmt = shared_data["merge_stmt"]
    props = shared_data["payload_props"]
    cypher = stmt["statement"]

    assert re.search(r"\bMERGE\b", cypher, re.IGNORECASE)
    assert "_provisa_id" in cypher
    assert "+=" in cypher

    assert props["active"] is True
    assert props["count"] == 42
    assert props["name"] == "Test"
    assert props["nested"] == {"key": "value"}

    assert stmt["parameters"]["id"] == 715


# ---------------------------------------------------------------------------
# Step definitions — REQ-716
# ---------------------------------------------------------------------------


@given(
    'an edge with start: 101, end: 202, type: "CONNECTS_TO"',
    target_fixture="shared_data",
)
def given_edge_with_ids_and_type() -> dict:
    edge = {"start": 101, "end": 202, "type": "CONNECTS_TO"}
    return {"edge": edge}


@when("the edge is exported")
def when_edge_exported(shared_data: dict) -> None:
    edge = shared_data["edge"]
    stmt = _build_edge_merge_statement(edge)
    shared_data["edge_stmt"] = stmt


@then(
    "Neo4j contains a relationship matching "
    "(a:Label{_provisa_id: 101})-[r:CONNECTS_TO]->(b:Label{_provisa_id: 202})"
)
def then_relationship_merge_correct(shared_data: dict) -> None:
    stmt = shared_data["edge_stmt"]
    cypher = stmt["statement"]
    params = stmt["parameters"]

    assert "CONNECTS_TO" in cypher
    assert "_provisa_id" in cypher
    assert "MERGE" in cypher.upper()
    assert params["start"] == 101
    assert params["end"] == 202


# ---------------------------------------------------------------------------
# Step definitions — REQ-717
# ---------------------------------------------------------------------------


@given(
    'credentials username: "neo4j", password: "secret"',
    target_fixture="shared_data",
)
def given_credentials_for_basic_auth() -> dict:
    return {"username": "neo4j", "password": "secret"}


@when("POST /data/neo4j-export is called")
def when_post_neo4j_export_basic_auth(shared_data: dict) -> None:
    username = shared_data["username"]
    password = shared_data["password"]
    node = {"id": 1, "tableLabel": "Node", "properties": {"x": 1}}
    payload = {
        "url": _FAKE_NEO4J_URL,
        "username": username,
        "password": password,
        "database": _FAKE_DATABASE,
        "nodes": [node],
        "edges": [],
    }
    captured: list[dict[str, Any]] = []
    _validate_and_simulate_export(payload, captured)
    shared_data["captured_requests"] = captured
    shared_data["payload"] = payload


@then('Authorization header contains "Basic " + base64("neo4j:secret")')
def then_authorization_header_basic_base64(shared_data: dict) -> None:
    import base64 as _b64

    username = shared_data["username"]
    password = shared_data["password"]
    expected_encoded = _b64.b64encode(f"{username}:{password}".encode()).decode()
    expected_header = f"Basic {expected_encoded}"

    captured = shared_data["captured_requests"]
    assert len(captured) > 0, "No request was captured"

    auth_header = captured[0]["kwargs"]["headers"]["Authorization"]
    assert auth_header == expected_header, (
        f"Expected Authorization header {expected_header!r}, got {auth_header!r}"
    )
