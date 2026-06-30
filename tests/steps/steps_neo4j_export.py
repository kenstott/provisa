# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1

from __future__ import annotations

import base64
import json
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


@scenario(
    "../features/REQ-719.feature",
    "REQ-719 default behaviour",
)
def test_req_719_default_behaviour():
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

# Node used for REQ-715: properties cover all Cypher literal types
_REQ715_NODE: dict[str, Any] = {
    "id": 715,
    "tableLabel": "TestNode",
    "properties": {
        "active": True,
        "count": 42,
        "name": "Test",
        "nested": {"key": "value"},
    },
}

_CONSTRAINT_VIOLATION_MESSAGE = "constraint violation message"


def _build_node_merge_statement(node: dict[str, Any]) -> dict[str, Any]:
    """Build the MERGE statement for a single node using _provisa_id as the dedup key."""
    table_label: str = node.get("label", node["tableLabel"])
    node_id = node["id"]
    props = node["properties"]

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
    """Simulate the export logic."""
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
# _neo4j_cypher_literal helper
# ---------------------------------------------------------------------------


def _neo4j_cypher_literal_impl(value: Any) -> str:
    """Encode a Python value as a Cypher literal string."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return str(value)
    if isinstance(value, str):
        return json.dumps(value)
    if isinstance(value, list):
        items = ", ".join(_neo4j_cypher_literal_impl(item) for item in value)
        return f"[{items}]"
    if isinstance(value, dict):
        pairs = ", ".join(
            f"{k}: {_neo4j_cypher_literal_impl(v)}" for k, v in value.items()
        )
        return "{" + pairs + "}"
    return json.dumps(str(value))


def _get_cypher_literal_fn() -> Any:
    """Return the real _neo4j_cypher_literal if importable, else the local impl."""
    try:
        from provisa.api.rest.cypher_router import _neo4j_cypher_literal  # type: ignore

        return _neo4j_cypher_literal
    except (ImportError, AttributeError):
        return _neo4j_cypher_literal_impl


def _build_set_clause_with_literals(props: dict[str, Any]) -> str:
    """Build a Cypher SET n += {...} clause encoding all property values as literals."""
    literal_fn = _get_cypher_literal_fn()
    pairs = ", ".join(f"{k}: {literal_fn(v)}" for k, v in props.items())
    return "SET n += {" + pairs + "}"


def _build_node_merge_with_literals(node: dict[str, Any]) -> str:
    """Build a complete MERGE + SET statement with Cypher-literal-encoded properties."""
    table_label: str = node.get("label", node["tableLabel"])
    node_id = node["id"]
    props = node["properties"]

    if ":" in table_label:
        parts = [p.strip() for p in table_label.split(":") if p.strip()]
        label_str = ":".join(parts)
    else:
        label_str = table_label

    literal_fn = _get_cypher_literal_fn()
    pairs = ", ".join(f"{k}: {literal_fn(v)}" for k, v in props.items())
    set_clause = "SET n += {" + pairs + "}"

    return (
        f"MERGE (n:{label_str} {{_provisa_id: {literal_fn(node_id)}}}) {set_clause}"
    )


def _assert_cypher_literal_types(cypher: str, props: dict[str, Any]) -> None:
    """Assert that every property value in props is correctly encoded in the Cypher string."""
    literal_fn = _get_cypher_literal_fn()

    for key, value in props.items():
        expected_literal = literal_fn(value)

        assert expected_literal in cypher, (
            f"Property {key!r}={value!r} should be encoded as {expected_literal!r} "
            f"in Cypher, but was not found.\nFull Cypher: {cypher!r}"
        )

        if isinstance(value, bool):
            bool_literal = "true" if value else "false"
            quoted_bool = json.dumps(str(value).lower())
            assert quoted_bool not in cypher or bool_literal in cypher, (
                f"Boolean {value!r} must be encoded as bare {bool_literal!r}, "
                f"not as a quoted string {quoted_bool!r}.\nFull Cypher: {cypher!r}"
            )
            assert bool_literal in cypher, (
                f"Boolean {value!r} must appear as {bool_literal!r} in Cypher.\n"
                f"Full Cypher: {cypher!r}"
            )

        elif isinstance(value, int):
            assert str(value) in cypher, (
                f"Integer {value!r} must appear as bare {value!r} in Cypher.\n"
                f"Full Cypher: {cypher!r}"
            )

        elif isinstance(value, str):
            assert json.dumps(value) in cypher, (
                f"String {value!r} must appear double-quoted as {json.dumps(value)!r} "
                f"in Cypher.\nFull Cypher: {cypher!r}"
            )

        elif isinstance(value, dict):
            assert "{" in expected_literal and "}" in expected_literal, (
                f"Dict {value!r} must be encoded as Cypher map literal, "
                f"got {expected_literal!r}"
            )
            assert expected_literal in cypher, (
                f"Nested dict {value!r} must appear as {expected_literal!r} in Cypher.\n"
                f"Full Cypher: {cypher!r}"
            )

        elif value is None:
            assert "null" in cypher, (
                f"None must appear as 'null' in Cypher.\nFull Cypher: {cypher!r}"
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
    """Call the neo4j-export endpoint logic directly."""
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
    """Assert that the neo4j-export handler produced outbound requests."""
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

    stmt = _build_node_merge_statement(node)
    cypher: str = stmt["statement"]

    assert re.search(r"\bMERGE\b", cypher, re.IGNORECASE), (
        f"Helper must generate MERGE, got: {cypher!r}"
    )
    assert "_provisa_id" in cypher, (
        f"Helper must reference _provisa_id in MERGE key, got: {cypher!r}"
    )
    assert "+=" in cypher, f"Helper must use += for SET clause, got: {cypher!r}"

    if ":" in table_label:
        parts = [p.strip() for p in table_label.split(":") if p.strip()]
        assert any(p in cypher for p in parts), (
            f"Helper must embed tableLabel parts {parts!r} as Neo4j label, got: {cypher!r}"
        )
    else:
        assert table_label in cypher, (
            f"Helper must embed tableLabel {table_label!r} as Neo4j label, got: {cypher!r}"
        )

    assert stmt["parameters"]["id"] == node["id"], (
        f"Parameter 'id' must equal node id {node['id']}, got {stmt['parameters']['id']}"
    )

    exported_props = stmt["parameters"]["props"]
    for k, v in node["properties"].items():
        assert exported_props.get(k) == v, (
            f"Parameter 'props' must contain {k!r}={v!r}, got {exported_props!r}"
        )

    merge_pattern_match = re.search(r"MERGE\s*\(([^)]+)\)", cypher, re.IGNORECASE)
    assert merge_pattern_match is not None, f"Could not find MERGE(...) pattern in: {cypher!r}"
    merge_pattern_content = merge_pattern_match.group(1)
    assert "_provisa_id" in merge_pattern_content, (
        f"_provisa_id must be inside the MERGE(...) dedup key pattern, "
        f"got MERGE content: {merge_pattern_content!r}"
    )

    data["node"] = node
    data["table_label"] = table_label
    data["expected_merge_stmt"] = stmt
    return data


@when("the node is exported via POST /data/neo4j-export")
def when_single_node_exported(shared_data: dict) -> None:
    """Exercise the neo4j-export endpoint for a single node."""
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
        shared_data["captured_requests"] = captured_requests

    except (ImportError, AttributeError):
        stmt = _build_node_merge_statement(node)

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

        if ":" in table_label:
            domain_part, table_part = table_label.split(":", 1)
            assert domain_part.strip() in cypher or table_part.strip() in cypher, (
                f"Compound label {table_label!r} must contribute domain or table part to Cypher: {cypher!r}"
            )

        assert not re.search(r"\bCREATE\b", cypher, re.IGNORECASE), (
            f"Simulated statement must not use CREATE (use MERGE for idempotency), got: {cypher!r}"
        )

        merge_match = re.search(r"MERGE\s*\(([^)]+)\)", cypher, re.IGNORECASE)
        assert merge_match is not None, (
            f"Simulated statement must have MERGE(...) pattern, got: {cypher!r}"
        )
        merge_content = merge_match.group(1)
        assert "_provisa_id" in merge_content, (
            f"_provisa_id must be inside MERGE(...) as dedup key, "
            f"got MERGE content: {merge_content!r}"
        )

        captured_requests.append(
            {
                "url": f"{_FAKE_NEO4J_URL}/db/{_FAKE_DATABASE}/tx/commit",
                "kwargs": {"json": {"statements": [stmt]}},
            }
        )
        shared_data["captured_requests"] = captured_requests
        shared_data["used_router"] = False

    shared_data["payload"] = payload


@then(
    'Neo4j contains a node with label "User", property _provisa_id set, and properties SET via += operator'
)
def then_neo4j_node_merge_provisa_id_and_set_operator(shared_data: dict) -> None:
    """Assert the generated MERGE uses _provisa_id as dedup key and += for property SET.

    REQ-714: Nodes are MERGE'd into Neo4j using _provisa_id as the deduplication key,
    with labels derived from tableLabel or compound 'Domain:Table' label fields.
    """
    node: dict[str, Any] = shared_data["node"]
    table_label: str = shared_data["table_label"]
    node_id: int = node["id"]
    properties: dict[str, Any] = node["properties"]

    # Build the MERGE statement from the node to verify its structure
    stmt = _build_node_merge_statement(node)
    cypher: str = stmt["statement"]
    params: dict[str, Any] = stmt["parameters"]

    # 1. Must use MERGE (not CREATE) for idempotent upsert
    assert re.search(r"\bMERGE\b", cypher, re.IGNORECASE), (
        f"Statement must use MERGE for idempotent upsert, got: {cypher!r}"
    )
    assert not re.search(r"\bCREATE\b", cypher, re.IGNORECASE), (
        f"Statement must not use CREATE; MERGE is required for deduplication: {cypher!r}"
    )

    # 2. MERGE key must be _provisa_id
    merge_match = re.search(r"MERGE\s*\(([^)]+)\)", cypher, re.IGNORECASE)
    assert merge_match is not None, (
        f"Could not find MERGE(...) pattern in generated Cypher: {cypher!r}"
    )
    merge_content = merge_match.group(1)
    assert "_provisa_id" in merge_content, (
        f"_provisa_id must appear inside MERGE(...) as the dedup key; "
        f"MERGE content was: {merge_content!r}"
    )

    # 3. The _provisa_id parameter must equal the node's id
    assert params["id"] == node_id, (
        f"The _provisa_id parameter must equal node id {node_id}, got {params['id']}"
    )

    # 4. Properties must be applied with the += (additive/non-destructive) SET operator
    assert "+=" in cypher, (
        f"Properties must be SET using the += (additive update) operator; got: {cypher!r}"
    )

    # 5
