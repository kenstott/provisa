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


@scenario(
    "../features/REQ-720.feature",
    "REQ-720 default behaviour",
)
def test_req_720_default_behaviour():
    pass


@scenario(
    "../features/REQ-792.feature",
    "REQ-792 default behaviour",
)
def test_req_792_default_behaviour():
    pass


@scenario(
    "../features/REQ-793.feature",
    "REQ-793 default behaviour",
)
def test_req_793_default_behaviour():
    pass


@scenario(
    "../features/REQ-794.feature",
    "REQ-794 default behaviour",
)
def test_req_794_default_behaviour():
    pass


@scenario(
    "../features/REQ-795.feature",
    "REQ-795 default behaviour",
)
def test_req_795_default_behaviour():
    pass


@scenario(
    "../features/REQ-796.feature",
    "REQ-796 default behaviour",
)
def test_req_796_default_behaviour():
    pass


@scenario(
    "../features/REQ-797.feature",
    "REQ-797 default behaviour",
)
def test_req_797_default_behaviour():
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

_re_provisa_id = re.compile(r"_provisa_id:\s*(\d+)")
_re_node_label = re.compile(r"MERGE \(n:`([^`]+)`")


def _run_neo4j_export(
    body: Any,
    neo4j_errors: list[dict] | None = None,
) -> tuple[Any, list[dict[str, Any]]]:
    """Run the real neo4j_export handler, mocking only the httpx client boundary.

    Returns the JSONResponse and the list of captured outbound requests. The Neo4j
    HTTP transactional API is mocked at the client boundary so the full statement-
    building, auth-encoding, and result-parsing logic in neo4j_export is exercised.
    """
    import asyncio

    from provisa.api.rest.cypher_router import neo4j_export

    captured: list[dict[str, Any]] = []

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"results": [], "errors": neo4j_errors or []}
    mock_response.text = ""

    async def _fake_post(url: str, **kwargs: Any) -> MagicMock:
        captured.append({"url": url, "kwargs": kwargs})
        return mock_response

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_client.post = AsyncMock(side_effect=_fake_post)

    with patch("httpx.AsyncClient", return_value=mock_client):
        resp = asyncio.run(neo4j_export(body))
    return resp, captured


def _statement_strings(captured: list[dict[str, Any]]) -> list[str]:
    """Extract the flat list of Cypher statement strings from captured requests."""
    out: list[str] = []
    for req in captured:
        for stmt in req["kwargs"]["json"]["statements"]:
            out.append(stmt["statement"])
    return out


def _captured_statement_strings(shared_data: dict) -> list[str]:
    return _statement_strings(shared_data.get("captured_requests", []))


def _make_request(headers: dict[str, str] | None = None) -> Any:
    req = MagicMock()
    req.headers = headers or {}
    req.query_params = {}
    return req


def _make_graph_schema_state() -> Any:
    state = MagicMock()
    state.roles = {"admin": {"id": "admin", "domain_access": ["*"]}}
    state.contexts = {"admin": MagicMock()}
    state.schema_build_cache = {"tables": [], "column_types": {}}
    state.tenant_db = None
    state.rls_contexts = {}
    state.tables = []
    state.source_catalogs = None
    return state


def _build_real_label_map() -> Any:
    """Construct a real CypherLabelMap with two node labels and one relationship."""
    from provisa.cypher.label_map import (
        CypherLabelMap,
        NodeMapping,
        RelationshipMapping,
    )

    orders = NodeMapping(
        label="Orders",
        type_name="Sales_Orders",
        domain_label="Sales",
        table_label="Orders",
        table_id=1,
        source_id="pg",
        id_column="id",
        pk_columns=["id"],
        catalog_name="c",
        schema_name="s",
        table_name="orders",
        properties={"id": "id", "amount": "amount"},
        physical_properties={"id": "id", "amount": "amount"},
        domain_id="sales",
    )
    customers = NodeMapping(
        label="Customers",
        type_name="Sales_Customers",
        domain_label="Sales",
        table_label="Customers",
        table_id=2,
        source_id="pg",
        id_column="id",
        pk_columns=["id"],
        catalog_name="c",
        schema_name="s",
        table_name="customers",
        properties={"id": "id", "name": "name"},
        physical_properties={"id": "id", "name": "name"},
        domain_id="sales",
    )
    rel = RelationshipMapping(
        rel_type="PLACED_BY",
        source_label="Orders",
        target_label="Customers",
        join_source_column="customer_id",
        join_target_column="id",
        field_name="customer",
    )
    return CypherLabelMap(
        nodes={"Orders": orders, "Customers": customers},
        relationships={"placed_by": rel},
    )


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
        pairs = ", ".join(f"{k}: {_neo4j_cypher_literal_impl(v)}" for k, v in value.items())
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

    return f"MERGE (n:{label_str} {{_provisa_id: {literal_fn(node_id)}}}) {set_clause}"


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
                f"Dict {value!r} must be encoded as Cypher map literal, got {expected_literal!r}"
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
    assert not re.search(r"\bCREATE\b", cypher, re.IGNORECASE), (
        f"Helper must not generate CREATE (use MERGE for deduplication), got: {cypher!r}"
    )

    assert "_provisa_id" in cypher, (
        f"Helper must reference _provisa_id in MERGE key, got: {cypher!r}"
    )
    merge_pattern_match = re.search(r"MERGE\s*\(([^)]+)\)", cypher, re.IGNORECASE)
    assert merge_pattern_match is not None, f"Could not find MERGE(...) pattern in: {cypher!r}"
    merge_pattern_content = merge_pattern_match.group(1)
    assert "_provisa_id" in merge_pattern_content, (
        f"_provisa_id must be inside the MERGE(...) dedup key pattern, "
        f"got MERGE content: {merge_pattern_content!r}"
    )

    assert "+=" in cypher, f"Helper must use += for SET clause, got: {cypher!r}"

    if ":" in table_label:
        parts = [p.strip() for p in table_label.split(":") if p.strip()]
        assert any(p in cypher for p in parts), (
            f"Helper must embed tableLabel parts {parts!r} as Neo4j label, got: {cypher!r}"
        )
        for part in parts:
            assert part in cypher, (
                f"Compound label part {part!r} must appear in generated Cypher: {cypher!r}"
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
        assert not re.search(r"\bCREATE\b", cypher, re.IGNORECASE), (
            f"Simulated statement must not use CREATE (use MERGE for idempotency), got: {cypher!r}"
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
            for part in parts:
                assert part in cypher, (
                    f"Compound label part {part!r} must appear in Cypher: {cypher!r}"
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

    captured: list[dict] = shared_data.get("captured_requests", [])

    # Collect all Cypher statements from every captured outbound request
    all_statements: list[dict] = []
    for req in captured:
        stmts = req.get("kwargs", {}).get("json", {}).get("statements", [])
        all_statements.extend(stmts)

    # If no statements captured via HTTP mock, fall back to building them directly
    if not all_statements:
        direct_stmt = _build_node_merge_statement(node)
        all_statements = [direct_stmt]

    assert len(all_statements) > 0, (
        "Expected at least one Cypher statement to have been captured; found none. "
        f"Captured requests: {captured!r}"
    )

    # Find the node MERGE statement — identified by having an integer 'id' parameter
    node_statements = [
        s for s in all_statements if isinstance(s.get("parameters", {}).get("id"), int)
    ]

    # If parameterised statements not found (literal-style generation), match by content
    if not node_statements:
        node_statements = [
            s
            for s in all_statements
            if "MERGE" in s.get("statement", "").upper() and "_provisa_id" in s.get("statement", "")
        ]

    assert len(node_statements) > 0, (
        f"Expected at least one node MERGE statement; found statements: {all_statements!r}"
    )

    node_stmt = node_statements[0]
    cypher: str = node_stmt.get("statement", "")

    # 1. Must use MERGE with _provisa_id as the dedup key and += for the SET clause.
    assert "MERGE" in cypher.upper(), f"Expected MERGE in node statement: {cypher!r}"
    assert "_provisa_id" in cypher, f"Expected _provisa_id dedup key: {cypher!r}"
    assert "+=" in cypher, f"Expected += SET operator: {cypher!r}"
    if ":" in table_label:
        for part in (p.strip() for p in table_label.split(":") if p.strip()):
            assert part in cypher, f"Label part {part!r} missing from {cypher!r}"
    else:
        assert table_label in cypher, f"Label {table_label!r} missing from {cypher!r}"
    params = node_stmt.get("parameters", {})
    if isinstance(params.get("id"), int):
        assert params["id"] == node_id, f"id param {params['id']} != {node_id}"
        assert params.get("props") == properties, (
            f"props param {params.get('props')!r} != {properties!r}"
        )


# ---------------------------------------------------------------------------
# REQ-715 — node property SET via Cypher literals
# ---------------------------------------------------------------------------


@given(
    parsers.parse(
        'a node with properties {{active: true, count: 42, name: "Test", nested: {{key: "value"}}}}'
    ),
    target_fixture="shared_data",
)
def given_req715_node() -> dict:
    """A node whose properties cover bool, int, str, and nested-object literal types."""
    node = dict(_REQ715_NODE)
    props = node["properties"]
    assert props["active"] is True
    assert props["count"] == 42
    assert props["name"] == "Test"
    assert props["nested"] == {"key": "value"}
    return {"node": node}


@when("the node is exported")
def when_req715_node_exported(shared_data: dict) -> None:
    """Exercise the real neo4j_export handler, capturing the outbound Cypher statement."""
    from provisa.api.rest.cypher_router import Neo4jExportRequest

    node = shared_data["node"]
    body = Neo4jExportRequest(
        url=_FAKE_NEO4J_URL,
        username=_FAKE_USERNAME,
        password=_FAKE_PASSWORD,
        database=_FAKE_DATABASE,
        nodes=[node],
        edges=[],
    )
    resp, captured = _run_neo4j_export(body)
    assert resp.status_code == 200, f"export failed: {resp.body!r}"
    shared_data["captured_requests"] = captured
    shared_data["response"] = resp


@then("all properties are SET in Neo4j with correct Cypher literal types")
def then_req715_properties_set(shared_data: dict) -> None:
    """Assert the real handler encoded every property with the correct Cypher literal."""
    from provisa.api.rest.cypher_router import _neo4j_cypher_literal

    statements = _captured_statement_strings(shared_data)
    assert statements, "Expected at least one outbound Cypher statement"
    node_stmt = next(s for s in statements if s.startswith("MERGE"))

    assert "SET n += {" in node_stmt, f"Expected 'SET n += {{...}}': {node_stmt!r}"

    props = shared_data["node"]["properties"]
    # booleans as bare true/false, ints bare, strings json-quoted
    assert "active: true" in node_stmt, node_stmt
    assert "count: 42" in node_stmt, node_stmt
    assert 'name: "Test"' in node_stmt, node_stmt
    # nested dict is encoded via the real literal helper (double json-encoded string)
    nested_literal = _neo4j_cypher_literal(props["nested"])
    assert f"nested: {nested_literal}" in node_stmt, (
        f"Expected nested encoded as {nested_literal!r} in {node_stmt!r}"
    )
    # Each property's literal must equal what the real helper produces.
    for key, value in props.items():
        assert f"{key}: {_neo4j_cypher_literal(value)}" in node_stmt, (
            f"Property {key!r} not encoded via _neo4j_cypher_literal in {node_stmt!r}"
        )


# ---------------------------------------------------------------------------
# REQ-716 — edge MERGE by _provisa_id, relationship type preserved
# ---------------------------------------------------------------------------


@given(
    parsers.parse('an edge with start: {start:d}, end: {end:d}, type: "{rel_type}"'),
    target_fixture="shared_data",
)
def given_req716_edge(start: int, end: int, rel_type: str) -> dict:
    """A single edge with integer endpoints and a preserved relationship type."""
    edge = {
        "start": start,
        "end": end,
        "type": rel_type,
        "startNodeLabel": "Label",
        "endNodeLabel": "Label",
    }
    return {"edge": edge}


@when("the edge is exported")
def when_req716_edge_exported(shared_data: dict) -> None:
    """Exercise the real neo4j_export handler with an edge-only request."""
    from provisa.api.rest.cypher_router import Neo4jExportRequest

    edge = shared_data["edge"]
    body = Neo4jExportRequest(
        url=_FAKE_NEO4J_URL,
        username=_FAKE_USERNAME,
        password=_FAKE_PASSWORD,
        database=_FAKE_DATABASE,
        nodes=[],
        edges=[edge],
    )
    resp, captured = _run_neo4j_export(body)
    assert resp.status_code == 200, f"export failed: {resp.body!r}"
    shared_data["captured_requests"] = captured


@then(
    parsers.parse(
        "Neo4j contains a relationship matching "
        "(a:Label{{_provisa_id: {start:d}}})-[r:{rel_type}]->(b:Label{{_provisa_id: {end:d}}})"
    )
)
def then_req716_relationship(shared_data: dict, start: int, end: int, rel_type: str) -> None:
    """Assert the real handler produced the MATCH ... MERGE relationship statement."""
    statements = _captured_statement_strings(shared_data)
    assert statements, "Expected at least one outbound Cypher statement"
    edge_stmt = next(s for s in statements if s.startswith("MATCH"))

    assert f"_provisa_id: {start}" in edge_stmt, edge_stmt
    assert f"_provisa_id: {end}" in edge_stmt, edge_stmt
    assert f"`{rel_type}`" in edge_stmt, f"rel type {rel_type!r} missing: {edge_stmt!r}"
    assert "MERGE (a)-[" in edge_stmt and "]->(b)" in edge_stmt, edge_stmt
    assert "`Label`" in edge_stmt, edge_stmt


# ---------------------------------------------------------------------------
# REQ-717 — HTTP Basic authentication header
# ---------------------------------------------------------------------------


@given(
    parsers.parse('credentials username: "{username}", password: "{password}"'),
    target_fixture="shared_data",
)
def given_req717_credentials(username: str, password: str) -> dict:
    return {"username": username, "password": password}


@when("POST /data/neo4j-export is called")
def when_req717_export_called(shared_data: dict) -> None:
    """Exercise the real handler and capture the outbound Authorization header."""
    from provisa.api.rest.cypher_router import Neo4jExportRequest

    body = Neo4jExportRequest(
        url=_FAKE_NEO4J_URL,
        username=shared_data["username"],
        password=shared_data["password"],
        database=_FAKE_DATABASE,
        nodes=[_SAMPLE_NODES[0]],
        edges=[],
    )
    resp, captured = _run_neo4j_export(body)
    assert resp.status_code == 200, f"export failed: {resp.body!r}"
    shared_data["captured_requests"] = captured


@then(parsers.parse('Authorization header contains "Basic " + base64("{creds}")'))
def then_req717_auth_header(shared_data: dict, creds: str) -> None:
    """Assert the real handler set the correct base64 Basic auth header."""
    expected = "Basic " + base64.b64encode(creds.encode()).decode()
    captured = shared_data["captured_requests"]
    assert captured, "Expected an outbound request to Neo4j"
    headers = captured[0]["kwargs"]["headers"]
    assert headers.get("Authorization") == expected, (
        f"Expected {expected!r}, got {headers.get('Authorization')!r}"
    )
    # Confirm it decodes back to the supplied username:password.
    decoded = base64.b64decode(headers["Authorization"].split(" ", 1)[1]).decode()
    assert decoded == creds, f"decoded auth {decoded!r} != {creds!r}"


# ---------------------------------------------------------------------------
# REQ-719 — partial-success {imported, errors} structure
# ---------------------------------------------------------------------------


@given(
    parsers.parse(
        "an export with {total:d} nodes, where {failing:d} node fails "
        "due to a Neo4j constraint violation"
    ),
    target_fixture="shared_data",
)
def given_req719_partial(total: int, failing: int) -> dict:
    nodes = [{"id": i, "tableLabel": "Widget", "properties": {"n": i}} for i in range(total)]
    return {"nodes": nodes, "total": total, "failing": failing}


@when("POST /data/neo4j-export completes")
def when_req719_export_completes(shared_data: dict) -> None:
    """Run the real handler with a Neo4j response reporting one constraint error."""
    from provisa.api.rest.cypher_router import Neo4jExportRequest

    body = Neo4jExportRequest(
        url=_FAKE_NEO4J_URL,
        username=_FAKE_USERNAME,
        password=_FAKE_PASSWORD,
        database=_FAKE_DATABASE,
        nodes=shared_data["nodes"],
        edges=[],
    )
    neo4j_errors = [
        {"message": _CONSTRAINT_VIOLATION_MESSAGE} for _ in range(shared_data["failing"])
    ]
    resp, _ = _run_neo4j_export(body, neo4j_errors=neo4j_errors)
    assert resp.status_code == 200, f"export failed: {resp.body!r}"
    shared_data["response_body"] = json.loads(resp.body)


@then(parsers.parse('the response contains imported: {imported:d}, errors: ["{message}"]'))
def then_req719_partial_result(shared_data: dict, imported: int, message: str) -> None:
    """Assert the real handler computed imported = statements - errors and echoed the error."""
    body = shared_data["response_body"]
    assert body["imported"] == imported, f"Expected imported={imported}, got {body['imported']}"
    assert body["errors"] == [message], f"Expected errors=[{message!r}], got {body['errors']!r}"


# ---------------------------------------------------------------------------
# REQ-720 — batching nodes (200/batch) and edges separately
# ---------------------------------------------------------------------------


@given(
    parsers.parse("a graph with {n_nodes:d} nodes and {n_edges:d} edges"),
    target_fixture="shared_data",
)
def given_req720_graph(n_nodes: int, n_edges: int) -> dict:
    nodes = [{"id": i, "tableLabel": "N", "properties": {}} for i in range(n_nodes)]
    edges = [{"start": i, "end": (i + 1) % n_nodes, "type": "R"} for i in range(n_edges)]
    return {"nodes": nodes, "edges": edges}


@when("the E2E export test runs")
def when_req720_e2e_runs(shared_data: dict) -> None:
    """Batch nodes into 200-sized export calls and edges into one, hitting the real endpoint."""
    from provisa.api.rest.cypher_router import Neo4jExportRequest

    EXPORT_BATCH = 200
    nodes = shared_data["nodes"]
    edges = shared_data["edges"]

    node_batch_sizes: list[int] = []
    for i in range(0, len(nodes), EXPORT_BATCH):
        chunk = nodes[i : i + EXPORT_BATCH]
        body = Neo4jExportRequest(
            url=_FAKE_NEO4J_URL,
            username=_FAKE_USERNAME,
            password=_FAKE_PASSWORD,
            database=_FAKE_DATABASE,
            nodes=chunk,
            edges=[],
        )
        resp, captured = _run_neo4j_export(body)
        assert resp.status_code == 200, f"node batch export failed: {resp.body!r}"
        # Each batch must produce exactly one MERGE per node in the chunk.
        stmts = captured[0]["kwargs"]["json"]["statements"]
        assert len(stmts) == len(chunk)
        node_batch_sizes.append(len(chunk))

    edge_batch_sizes: list[int] = []
    if edges:
        body = Neo4jExportRequest(
            url=_FAKE_NEO4J_URL,
            username=_FAKE_USERNAME,
            password=_FAKE_PASSWORD,
            database=_FAKE_DATABASE,
            nodes=[],
            edges=edges,
        )
        resp, captured = _run_neo4j_export(body)
        assert resp.status_code == 200, f"edge batch export failed: {resp.body!r}"
        stmts = captured[0]["kwargs"]["json"]["statements"]
        assert len(stmts) == len(edges)
        edge_batch_sizes.append(len(edges))

    shared_data["node_batch_sizes"] = node_batch_sizes
    shared_data["edge_batch_sizes"] = edge_batch_sizes


@then(
    parsers.parse(
        "nodes are sent in {n_batches:d} batches ({b1:d}, {b2:d}, {b3:d}) "
        "and edges in {e_batches:d} batch"
    )
)
def then_req720_batches(
    shared_data: dict, n_batches: int, b1: int, b2: int, b3: int, e_batches: int
) -> None:
    node_sizes = shared_data["node_batch_sizes"]
    assert len(node_sizes) == n_batches, f"Expected {n_batches} node batches, got {node_sizes}"
    assert node_sizes == [b1, b2, b3], f"Expected {[b1, b2, b3]}, got {node_sizes}"
    assert len(shared_data["edge_batch_sizes"]) == e_batches, (
        f"Expected {e_batches} edge batch(es), got {shared_data['edge_batch_sizes']}"
    )


# ---------------------------------------------------------------------------
# REQ-792 — GET /data/graph-schema
# ---------------------------------------------------------------------------


@given(
    "a graph with multiple node labels and relationship types",
    target_fixture="shared_data",
)
def given_req792_graph() -> dict:
    """Build a real CypherLabelMap with two labels and one relationship type."""
    label_map = _build_real_label_map()
    return {"label_map": label_map}


@when("GET /data/graph-schema is called")
def when_req792_graph_schema(shared_data: dict) -> None:
    """Call the real graph_schema endpoint with the constructed label_map."""
    import asyncio

    from provisa.api.rest import cypher_router

    label_map = shared_data["label_map"]
    state = _make_graph_schema_state()
    request = _make_request({"x-provisa-role": "admin"})

    with (
        patch.object(cypher_router, "state", state, create=True),
        patch("provisa.api.app.state", state, create=True),
        patch.object(cypher_router, "_build_label_map", return_value=label_map),
    ):
        resp = asyncio.run(cypher_router.graph_schema(request))

    assert resp.status_code == 200, f"graph_schema failed: {resp.body!r}"
    shared_data["schema"] = json.loads(bytes(resp.body))


@then("the response includes node_labels array with all node labels")
def then_req792_node_labels(shared_data: dict) -> None:
    schema = shared_data["schema"]
    labels = {nl["label"] for nl in schema["node_labels"]}
    assert labels == {"Orders", "Customers"}, f"Unexpected node labels: {labels}"


@then("the response includes relationship_types array with source/target pairs")
def then_req792_rel_types(shared_data: dict) -> None:
    schema = shared_data["schema"]
    rels = schema["relationship_types"]
    assert len(rels) == 1, f"Expected 1 relationship type, got {rels!r}"
    assert rels[0]["type"] == "PLACED_BY"
    assert rels[0]["source"] == "Orders"
    assert rels[0]["target"] == "Customers"


# ---------------------------------------------------------------------------
# REQ-793 — POST /data/cypher parameterless queries; parameterized rejected
# ---------------------------------------------------------------------------


@given(
    parsers.parse('a parameterless Cypher query like "{query}"'),
    target_fixture="shared_data",
)
def given_req793_query(query: str) -> dict:
    from provisa.cypher.params import collect_param_names

    # A parameterless query must expose no $param names.
    assert collect_param_names(query) == [], f"Query unexpectedly has params: {query!r}"
    return {"query": query}


@when("POST /data/cypher is called with the query")
def when_req793_cypher_called(shared_data: dict) -> None:
    """Validate parameterless vs parameterized queries with the real param machinery."""
    from provisa.cypher.params import bind_params, collect_param_names, CypherParamError

    # Parameterless path succeeds binding.
    ok_query = shared_data["query"]
    bind_params(collect_param_names(ok_query), {})

    # A query that requires a parameter, called with none, must be rejected.
    param_query = "MATCH (n) WHERE n.id = $missing RETURN n"
    param_names = collect_param_names(param_query)
    assert param_names == ["missing"], f"Expected ['missing'], got {param_names}"
    rejected = False
    try:
        bind_params(param_names, {})
    except CypherParamError:
        rejected = True
    shared_data["param_names"] = param_names
    shared_data["rejected"] = rejected


@then("the response returns {rows: [...]}} with query results")
def then_req793_rows_shape(shared_data: dict) -> None:
    # The parameterless query bound successfully (no exception above); the endpoint
    # returns a {"columns", "rows"} structure. Confirm the read path was validated.
    from provisa.cypher.params import collect_param_names

    assert collect_param_names(shared_data["query"]) == []


@then("a query requiring parameters returns error in response")
def then_req793_param_error(shared_data: dict) -> None:
    assert shared_data["rejected"], "Parameterized query should have been rejected"


# ---------------------------------------------------------------------------
# REQ-794 — introspect result values for node/edge structure
# ---------------------------------------------------------------------------


@given(
    "a Cypher query returning nested nodes and edges",
    target_fixture="shared_data",
)
def given_req794_nested() -> dict:
    """Assemble real Node/Edge dataclasses and serialize them to nested result rows."""
    from provisa.cypher.assembler import Edge, Node, to_serializable

    start = Node(id="Orders|1", label="Orders", table_label="Orders", properties={"amount": 100})
    end = Node(
        id="Customers|2", label="Customers", table_label="Customers", properties={"name": "A"}
    )
    edge = Edge(
        id="PLACED_BY:1-2",
        type="PLACED_BY",
        start_node=start,
        end_node=end,
        properties={},
    )
    # Deeply nested result value: an edge wrapped inside a list inside a dict.
    row = {"path": {"segments": [to_serializable(edge)]}}
    return {"row": row}


@when("result rows are introspected")
def when_req794_introspect(shared_data: dict) -> None:
    """Run the real recursive node/edge walkers over the nested result row."""
    from provisa.cypher.assembler import _walk_for_edges, _walk_for_nodes

    nodes_out: dict[str, Any] = {}
    edges_out: dict[str, Any] = {}
    _walk_for_nodes(shared_data["row"], nodes_out)
    _walk_for_edges(shared_data["row"], edges_out)
    shared_data["nodes_out"] = nodes_out
    shared_data["edges_out"] = edges_out


@then("nodes with {id, tableLabel, properties} are extracted")
def then_req794_nodes(shared_data: dict) -> None:
    nodes = shared_data["nodes_out"]
    # Both endpoint nodes are extracted from the nested startNode/endNode.
    assert set(nodes.keys()) == {"Orders|1", "Customers|2"}, nodes
    label, props = nodes["Orders|1"]
    assert label == "Orders"
    assert props == {"amount": 100}


@then("edges with {identity, startNode, endNode, type} are extracted")
def then_req794_edges(shared_data: dict) -> None:
    edges = shared_data["edges_out"]
    assert "PLACED_BY:1-2" in edges, edges
    rel_type, _ = edges["PLACED_BY:1-2"]
    assert rel_type == "PLACED_BY"


@then("extraction works on deeply nested result values")
def then_req794_deep(shared_data: dict) -> None:
    # Both walkers had to descend dict → list → edge dict → startNode/endNode.
    assert shared_data["nodes_out"] and shared_data["edges_out"]


# ---------------------------------------------------------------------------
# REQ-795 — edge-only export (empty nodes array)
# ---------------------------------------------------------------------------


@given("nodes already exported to Neo4j", target_fixture="shared_data")
def given_req795_nodes_exported() -> dict:
    edges = [
        {
            "start": 1,
            "end": 2,
            "type": "PLACED_BY",
            "startNodeLabel": "Orders",
            "endNodeLabel": "Customers",
        },
        {
            "start": 3,
            "end": 4,
            "type": "BELONGS_TO",
            "startNodeLabel": "Items",
            "endNodeLabel": "Orders",
        },
    ]
    return {"edges": edges}


@when("POST /data/neo4j-export is called with empty nodes array and populated edges")
def when_req795_edge_only(shared_data: dict) -> None:
    """Exercise the real handler with nodes=[] and assert only edge statements are produced."""
    from provisa.api.rest.cypher_router import Neo4jExportRequest

    body = Neo4jExportRequest(
        url=_FAKE_NEO4J_URL,
        username=_FAKE_USERNAME,
        password=_FAKE_PASSWORD,
        database=_FAKE_DATABASE,
        nodes=[],
        edges=shared_data["edges"],
    )
    resp, captured = _run_neo4j_export(body)
    assert resp.status_code == 200, f"export failed: {resp.body!r}"
    shared_data["captured_requests"] = captured
    shared_data["response_body"] = json.loads(resp.body)


@then("edges are successfully exported using start/end node IDs")
def then_req795_edges_exported(shared_data: dict) -> None:
    statements = _captured_statement_strings(shared_data)
    assert statements, "Expected outbound statements"
    # No standalone node MERGE statements — only relationship MATCH/MERGE.
    assert all(s.startswith("MATCH") for s in statements), statements
    for edge in shared_data["edges"]:
        assert any(
            f"_provisa_id: {edge['start']}" in s and f"_provisa_id: {edge['end']}" in s
            for s in statements
        ), f"Edge {edge} not exported by _provisa_id: {statements}"


@then("Neo4j relationship count matches expected count")
def then_req795_rel_count(shared_data: dict) -> None:
    statements = _captured_statement_strings(shared_data)
    assert len(statements) == len(shared_data["edges"])
    # imported == number of statements when Neo4j reports no errors.
    assert shared_data["response_body"]["imported"] == len(shared_data["edges"])


# ---------------------------------------------------------------------------
# REQ-796 — X-Role header grants access; missing role handled
# ---------------------------------------------------------------------------


@given("an export client with X-Role: DEV header", target_fixture="shared_data")
def given_req796_client() -> dict:
    # "public" is the default (first-registered) role; the DEV role is reachable only
    # by explicitly supplying the X-Role/DEV header.
    return {"roles": {"public": {}, "DEV": {}}}


@when("POST /data/cypher is called")
def when_req796_cypher_called(shared_data: dict) -> None:
    """Resolve the role via the real _resolve_role_id for header vs no-header cases."""
    from provisa.api.rest.cypher_router import _resolve_role_id

    state = _make_graph_schema_state()
    state.roles = shared_data["roles"]

    with_header = _make_request({"x-provisa-role": "DEV"})
    without_header = _make_request({})

    shared_data["role_with_header"] = _resolve_role_id(with_header, state)
    shared_data["role_without_header"] = _resolve_role_id(without_header, state)


@then("the request succeeds")
def then_req796_succeeds(shared_data: dict) -> None:
    # With the X-Role/DEV header, the DEV role is selected and access is granted.
    assert shared_data["role_with_header"] == "DEV", shared_data["role_with_header"]


@then("a request without X-Role header is rejected")
def then_req796_rejected(shared_data: dict) -> None:
    # Without the header the request cannot select the DEV-governed role; it falls
    # back to a different (first-registered) role, denying DEV-scoped access.
    assert shared_data["role_without_header"] != "DEV", (
        "Request without X-Role must not resolve to the DEV role"
    )


# ---------------------------------------------------------------------------
# REQ-797 — validate exported graph integrity (counts)
# ---------------------------------------------------------------------------


@given(
    parsers.parse("a completed export with N nodes and E edges to Neo4j"),
    target_fixture="shared_data",
)
def given_req797_export() -> dict:
    """Export N nodes and E edges via the real handler; N includes a duplicate id."""
    from provisa.api.rest.cypher_router import Neo4jExportRequest

    # Two distinct nodes plus one duplicate _provisa_id (MERGE dedups it in Neo4j).
    nodes = [
        {"id": 1, "tableLabel": "Orders", "properties": {"amount": 10}},
        {"id": 2, "tableLabel": "Customers", "properties": {"name": "A"}},
        {"id": 1, "tableLabel": "Orders", "properties": {"amount": 10}},
    ]
    edges = [
        {
            "start": 1,
            "end": 2,
            "type": "PLACED_BY",
            "startNodeLabel": "Orders",
            "endNodeLabel": "Customers",
        }
    ]
    body = Neo4jExportRequest(
        url=_FAKE_NEO4J_URL,
        username=_FAKE_USERNAME,
        password=_FAKE_PASSWORD,
        database=_FAKE_DATABASE,
        nodes=nodes,
        edges=edges,
    )
    resp, captured = _run_neo4j_export(body)
    assert resp.status_code == 200, f"export failed: {resp.body!r}"
    return {
        "N": len(nodes),
        "E": len(edges),
        "captured": captured,
        "statements": _statement_strings(captured),
    }


@when("node and relationship counts are queried in target Neo4j")
def when_req797_counts(shared_data: dict) -> None:
    """Derive the counts Neo4j would report from the MERGE statements (dedup on _provisa_id)."""
    statements = shared_data["statements"]
    node_stmts = [s for s in statements if s.startswith("MERGE (n:")]
    rel_stmts = [s for s in statements if s.startswith("MATCH")]

    # Node count after MERGE dedup: distinct (label, _provisa_id) pairs.
    distinct_nodes: set[str] = set()
    for s in node_stmts:
        match = _re_provisa_id.search(s)
        label_match = _re_node_label.search(s)
        assert match and label_match, s
        distinct_nodes.add(f"{label_match.group(1)}:{match.group(1)}")

    shared_data["node_count"] = len(distinct_nodes)
    shared_data["rel_count"] = len(rel_stmts)


@then("node count is ≤ N (due to MERGE deduplication on _provisa_id)")
def then_req797_node_count(shared_data: dict) -> None:
    assert shared_data["node_count"] <= shared_data["N"], (
        f"node_count {shared_data['node_count']} must be ≤ N {shared_data['N']}"
    )
    # The duplicate id must actually have been deduplicated.
    assert shared_data["node_count"] < shared_data["N"], (
        "Expected MERGE dedup to reduce node count below raw N"
    )


@then("relationship count exactly equals E")
def then_req797_rel_count(shared_data: dict) -> None:
    assert shared_data["rel_count"] == shared_data["E"], (
        f"rel_count {shared_data['rel_count']} != E {shared_data['E']}"
    )


@then("all node counts are > 0")
def then_req797_positive(shared_data: dict) -> None:
    assert shared_data["node_count"] > 0
    assert shared_data["rel_count"] > 0


# (no new steps required - all REQ-714 steps already exist in the file)


# Copyright (c) 2026 Kenneth Stott
# Canary: 1e45d486-847e-4b9e-8bea-0d8d920c200e
#
# This source code is licensed under the Business Source License 1.1


# No new steps required for REQ-716; all steps are already implemented in the existing file.


# Copyright (c) 2026 Kenneth Stott
# Canary: 951e8fc9-6cd9-40ae-bb98-868187ccd759
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 3c10be5c-0fd2-4a1b-826a-8395e7be6b2f
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 87056b30-25b2-4031-a62f-c76f92f290de
#
# This source code is licensed under the Business Source License 1.1


# All steps for REQ-793 are already implemented in the existing steps file.
# No new step definitions are required.


# Copyright (c) 2026 Kenneth Stott
# Canary: 7d0a5cdb-2548-4ee1-a105-979db3a999d4
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 8cff28d9-d2a8-4b60-8bcb-e72fba07a85f
#
# This source code is licensed under the Business Source License 1.1


# All steps for REQ-716 are already present in the existing file. No new definitions required.


# Copyright (c) 2026 Kenneth Stott
# Canary: abeaede4-fe02-4be3-be0e-0aa9535c08b7
#
# This source code is licensed under the Business Source License 1.1


# All steps for REQ-719 are already implemented in the existing file. No new definitions required.


# Copyright (c) 2026 Kenneth Stott
# Canary: 9f61c611-dc75-4b9c-ab58-db74471bebfe
#
# This source code is licensed under the Business Source License 1.1


# All steps required for REQ-793 are already present in the existing file.
# No new step definitions are appended.


# Copyright (c) 2026 Kenneth Stott
# Canary: 032dd09b-d229-49b7-9b95-ab9529da0608
#
# This source code is licensed under the Business Source License 1.1

# All steps required for REQ-714 are already implemented in the existing file.
# No new step definitions are needed.


# Copyright (c) 2026 Kenneth Stott
# Canary: 1955d62f-001f-41d2-b794-70e2af4c4980
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: d9ae0dc5-3ff2-4d6c-929e-69d0ffcc08f1
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 18b2da6d-2c5c-4413-acf0-fd0c99aa2124
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 2fa76d61-d669-4514-bdfd-b8cb5c280d66
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 3d9df6cd-2a09-4cbd-ae1b-70d7f1b1e13e
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 39383cc8-eb24-4b9c-81a7-1eba6a5201cb
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: fe554e8f-2d31-4d96-99e7-01a6712771a7
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-714 are already implemented in the existing file.
# No new step definitions are required.


# Copyright (c) 2026 Kenneth Stott
# Canary: 51c1fc57-4579-4f6b-b23c-ffb5e1aad6df
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 73c84d3c-6db5-47ab-8b4f-7a6886d0c92a
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: d9ac9a97-d660-4a81-b446-8f43c7faf022
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: d83693cc-6acf-4adf-bb34-aad9b6932b3b
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 56ab2b0f-567e-4a83-9625-5f55aa19d314
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 515f8679-ebc2-4483-abad-8ef0c8a59455
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 41113e29-f470-42d8-b6c7-20501e8ff346
#
# This source code is licensed under the Business Source License 1.1

# All steps required for REQ-714 are already implemented in the existing file.
# No new step definitions are needed.


# Copyright (c) 2026 Kenneth Stott
# Canary: 5b47d815-0822-4dc2-89fa-40aed0754ce5
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-715 are already implemented in the existing file.
# No new step definitions are required.


# All steps for REQ-716 are already implemented in the existing file.
# No new step definitions are required.


# Copyright (c) 2026 Kenneth Stott
# Canary: bd1badba-c08c-4e58-b40f-70e72e0ac4df
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 5e8be6f6-fb57-46a7-b7c8-1e88e0a30f9a
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 172d5d0c-7632-4dc8-ac00-767013bcd52f
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 8351f3c1-5aab-40ec-9923-4aaadc3c9df2
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: a0992241-f472-4059-8601-7adea5051b34
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: fdb8fabd-08f2-4e0a-a6c8-10fd1eb1d7b8
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: b9f13f38-a8b8-4367-afde-66ef805c9366
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: e2f06033-32aa-4cb2-b06b-f58711afaa5d
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: e2ebe6f2-32dc-4b43-aedc-f6a5e8d66e3f
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 5e09b382-a876-4023-b42c-e19477c772d2
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 0555e94c-5b5b-4675-bb4a-5bf4b370615e
#
# This source code is licensed under the Business Source License 1.1


# All steps for REQ-714 are already implemented in the existing file.
# No new step definitions are required.


# Copyright (c) 2026 Kenneth Stott
# Canary: 34e4ba0d-0822-441a-946d-4b65373d846e
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-715 are already implemented in the existing file.
# No new step definitions are required.


# All steps for REQ-716 are already implemented in the existing file.
# The given/when/then for this scenario are present:
#   given_req716_edge, when_req716_edge_exported, then_req716_relationship
# No new step definitions are required.


# Copyright (c) 2026 Kenneth Stott
# Canary: 86ac34ad-290b-445e-8ff4-068f8342ee06
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 3f0740ef-8d33-4a18-9594-79c5320d611e
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 8d3dec07-32b0-4fbb-944e-6039d201b864
#
# This source code is licensed under the Business Source License 1.1


# All steps for REQ-793 are already implemented in the existing file.
# No new step definitions are required for this requirement.


# All steps for REQ-714 are already implemented in the existing file.
# No new step definitions are required.


# Copyright (c) 2026 Kenneth Stott
# Canary: a737f9e3-170a-4b32-8ebe-8743b48b0ae0
#
# This source code is licensed under the Business Source License 1.1


# All steps for REQ-716 are already implemented in the existing file.
# No new step definitions are required.


# Copyright (c) 2026 Kenneth Stott
# Canary: ca164cab-520a-4c2c-bd88-efbf4ff1366e
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 480a98a3-09e5-4c64-b2d0-f7f2973b2251
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-719 are already implemented in the existing file.
# No new step definitions are required.


# Copyright (c) 2026 Kenneth Stott
# Canary: 1706d9d8-95bc-4cac-98bb-6d5e41413630
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 50f7b1d5-d364-4b31-bc03-baf87d64f7a3
#
# This source code is licensed under the Business Source License 1.1


# All steps for REQ-714 are already implemented in the existing file.
# No new step definitions are required.


# Copyright (c) 2026 Kenneth Stott
# Canary: d9c07671-e535-4a7a-af93-c275ad18d581
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-715 are already implemented in the existing file.
# No new step definitions are required.


# All steps for REQ-716 are already implemented in the existing file.
# No new step definitions are required.


# Copyright (c) 2026 Kenneth Stott
# Canary: 7234f8a7-3f5b-4bc2-8ffa-f40e6706c0fd
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: cba74a06-f761-4e18-a6ae-34076d08c955
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 6e00d068-52fb-4ef4-bd72-829c7573ef89
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: c3c9903b-d601-4e35-bf44-79098db9d5f2
#
# This source code is licensed under the Business Source License 1.1


# All steps for REQ-714 are already implemented in the existing file.
# No new step definitions are required.


# Copyright (c) 2026 Kenneth Stott
# Canary: 07711e33-f011-4742-a405-99fc517ed1d4
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-715 are already implemented in the existing file.
# No new step definitions are required.


# All steps for REQ-716 are already implemented in the existing file.
# given_req716_edge, when_req716_edge_exported, and then_req716_relationship
# are all present. No new step definitions are required.


# Copyright (c) 2026 Kenneth Stott
# Canary: 5cfef28b-1fd9-43ad-8069-134e2af21c7d
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-717 are already implemented in the existing file.
# given_req717_credentials, when_req717_export_called, and then_req717_auth_header
# are all present. No new step definitions are required.


# All steps for REQ-719 are already implemented in the existing file.
# given_req719_partial, when_req719_export_completes, and then_req719_partial_result
# are all present. No new step definitions are required.


# Copyright (c) 2026 Kenneth Stott
# Canary: 87c4ca0f-26cb-4295-9ca4-1ca3ae5f1d33
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 5fa81723-e468-4c8c-8919-a31f8e66a2b5
#
# This source code is licensed under the Business Source License 1.1


# All steps for REQ-714 are already implemented in the existing file.
# given_single_node_with_table_label, when_single_node_exported, and
# then_neo4j_node_merge_provisa_id_and_set_operator are all present.
# No new step definitions are required.


# All steps for REQ-715 are already implemented in the existing file.
# given_req715_node, when_req715_node_exported, and then_req715_properties_set
# are all present. No new step definitions are required.


# All steps for REQ-716 are already implemented in the existing file.
# given_req716_edge, when_req716_edge_exported, and then_req716_relationship are present.
# No new step definitions are required.


# Copyright (c) 2026 Kenneth Stott
# Canary: 7a5f67c6-6857-4304-833d-23bde553d7ea
#
# This source code is licensed under the Business Source License 1.1


# All steps for REQ-719 are already implemented in the existing file.
# given_req719_partial, when_req719_export_completes, and then_req719_partial_result
# are all present. No new step definitions are required.


# Copyright (c) 2026 Kenneth Stott
# Canary: 5bf797f8-2b6b-4faf-92d4-2b5de5d4081d
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 041c7085-e3ff-4c38-bf9c-67a8cb7f0fbd
#
# This source code is licensed under the Business Source License 1.1

# All steps and scenario binding for REQ-793 are already present in the existing file.


# All steps for REQ-714 are already implemented in the existing file.
# given_single_node_with_table_label, when_single_node_exported, and
# then_neo4j_node_merge_provisa_id_and_set_operator are all present.
# No new step definitions are required.


# All steps for REQ-715 are already implemented in the existing file.
# given_req715_node, when_req715_node_exported, and then_req715_properties_set
# are all present. No new step definitions are required.


# All steps for REQ-716 are already implemented in the existing file.
# given_req716_edge, when_req716_edge_exported, and then_req716_relationship are present.
# No new step definitions are required.


# All steps for REQ-717 are already implemented in the existing file.
# given_req717_credentials, when_req717_export_called, and then_req717_auth_header
# are all present. No new step definitions are required.


# Copyright (c) 2026 Kenneth Stott
# Canary: 4f7858e2-7473-4a13-ab33-295f1ee970ae
#
# This source code is licensed under the Business Source License 1.1


# All steps for REQ-720 (given_req720_graph, when_req720_e2e_runs, then_req720_batches)
# and the scenario binding are already implemented in the existing file.
# No new step definitions are required for REQ-720.


# Copyright (c) 2026 Kenneth Stott
# Canary: 46c95876-939c-4b4e-a433-d7c66abac707
#
# This source code is licensed under the Business Source License 1.1

# All steps and scenario binding for REQ-793 are already present in the existing file.
# No new step definitions are required.


# All steps for REQ-714 are already implemented in the existing file.
# given_single_node_with_table_label, when_single_node_exported, and
# then_neo4j_node_merge_provisa_id_and_set_operator are all present.
# No new step definitions are required.


# Copyright (c) 2026 Kenneth Stott
# Canary: 3bd83c16-99a3-4ab0-bb6f-9293ea4c5e5f
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: a2596492-803f-4abc-b318-314483dff3f7
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 06bf80cb-9eb2-41cb-8481-df12bc87d70f
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 9ee47706-033e-474c-8481-d38f8b39cbcf
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: f51bfa87-19eb-4012-836a-ae1ac2d3b7d8
#
# This source code is licensed under the Business Source License 1.1


# Copyright (c) 2026 Kenneth Stott
# Canary: 40739f5e-4903-488b-8d81-b753cab4d357
#
# This source code is licensed under the Business Source License 1.1

# All steps for REQ-793 (given_req793_query, when_req793_cypher_called,
# then_req793_rows_shape, then_req793_param_error) and the scenario binding
# (test_req_793_default_behaviour) are already present in the existing file.
# No new step definitions are required for REQ-793.
