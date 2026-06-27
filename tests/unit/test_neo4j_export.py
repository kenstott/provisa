# Copyright (c) 2026 Kenneth Stott
# Canary: a1b2c3d4-e5f6-7890-abcd-ef1234567890
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for Neo4j export endpoint (REQ-713–REQ-720)."""

from __future__ import annotations

import base64
import json
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from provisa.api.rest.cypher_router import (
    Neo4jExportRequest,
    _neo4j_cypher_literal,
    neo4j_export,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_request(
    url: str = "http://neo4j:7474",
    username: str = "neo4j",
    password: str = "secret",
    database: str = "neo4j",
    nodes: list[dict] | None = None,
    edges: list[dict] | None = None,
) -> Neo4jExportRequest:
    return Neo4jExportRequest(
        url=url,
        username=username,
        password=password,
        database=database,
        nodes=nodes or [],
        edges=edges or [],
    )


def _mock_neo4j_response(errors: list | None = None, status_code: int = 200) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = {"errors": errors or [], "results": []}
    resp.text = ""
    return resp


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestNeo4jExportRequest:
    def test_model_accepts_required_fields(self):
        """REQ-713: Neo4jExportRequest accepts url, credentials, database, nodes, edges."""
        req = Neo4jExportRequest(
            url="bolt://localhost:7687",
            username="neo4j",
            password="pass",
            database="mydb",
            nodes=[{"id": 1, "label": "Person", "properties": {"name": "Alice"}}],
            edges=[{"start": 1, "end": 2, "type": "KNOWS"}],
        )
        assert req.url == "bolt://localhost:7687"
        assert req.username == "neo4j"
        assert req.password == "pass"
        assert req.database == "mydb"
        assert len(req.nodes) == 1
        assert len(req.edges) == 1

    def test_database_defaults_to_neo4j(self):
        """REQ-713: database field defaults to 'neo4j' when omitted."""
        req = Neo4jExportRequest(url="http://x", username="u", password="p", nodes=[], edges=[])
        assert req.database == "neo4j"


class TestNeo4jCypherLiteral:
    def test_none_renders_null(self):
        assert _neo4j_cypher_literal(None) == "null"

    def test_bool_true(self):
        assert _neo4j_cypher_literal(True) == "true"

    def test_bool_false(self):
        assert _neo4j_cypher_literal(False) == "false"

    def test_integer(self):
        assert _neo4j_cypher_literal(42) == "42"

    def test_float(self):
        assert _neo4j_cypher_literal(3.14) == "3.14"

    def test_string_json_escaped(self):
        result = _neo4j_cypher_literal("hello world")
        assert result == '"hello world"'

    def test_string_with_quotes(self):
        result = _neo4j_cypher_literal('say "hi"')
        assert '\\"' in result or result == '"say \\"hi\\""'


class TestNodeMergeStatements:
    """REQ-714: MERGE on _provisa_id, labels from tableLabel or compound 'Domain:Table'."""

    @pytest.mark.asyncio
    async def test_node_merge_uses_provisa_id(self):
        """REQ-714: MERGE statement uses _provisa_id as dedup key."""
        body = _make_request(
            nodes=[{"id": 99, "tableLabel": "Person", "label": "Org:Person", "properties": {}}],
        )
        captured: list[dict] = []

        async def _fake_post(url, *, json, headers, timeout):
            captured.append(json)
            return _mock_neo4j_response()

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock(side_effect=_fake_post)

        with patch("httpx.AsyncClient", return_value=mock_client):
            await neo4j_export(body)

        stmts = [s["statement"] for s in captured[0]["statements"]]
        assert any("_provisa_id: 99" in s for s in stmts)

    @pytest.mark.asyncio
    async def test_node_label_uses_table_label(self):
        """REQ-714: tableLabel field drives the node label in MERGE when present."""
        body = _make_request(
            nodes=[{"id": 1, "tableLabel": "Order", "label": "Sales:Order", "properties": {}}],
        )
        captured: list[dict] = []

        async def _fake_post(url, *, json, headers, timeout):
            captured.append(json)
            return _mock_neo4j_response()

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock(side_effect=_fake_post)

        with patch("httpx.AsyncClient", return_value=mock_client):
            await neo4j_export(body)

        stmts = [s["statement"] for s in captured[0]["statements"]]
        assert any("`Order`" in s for s in stmts)

    @pytest.mark.asyncio
    async def test_node_compound_label_from_full_label(self):
        """REQ-714: When tableLabel absent, compound label parsed from 'Domain:Table'."""
        body = _make_request(
            nodes=[{"id": 2, "tableLabel": "", "label": "Finance:Invoice", "properties": {}}],
        )
        captured: list[dict] = []

        async def _fake_post(url, *, json, headers, timeout):
            captured.append(json)
            return _mock_neo4j_response()

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock(side_effect=_fake_post)

        with patch("httpx.AsyncClient", return_value=mock_client):
            await neo4j_export(body)

        stmts = [s["statement"] for s in captured[0]["statements"]]
        assert any("`Invoice`" in s and "`Finance`" in s for s in stmts)


class TestNodeProperties:
    """REQ-715: Node properties written via SET n += {...}."""

    @pytest.mark.asyncio
    async def test_set_block_contains_properties(self):
        """REQ-715: SET n += block includes all node properties."""
        props = {"name": "Alice", "age": 30}
        body = _make_request(
            nodes=[{"id": 5, "tableLabel": "Person", "label": "Person", "properties": props}],
        )
        captured: list[dict] = []

        async def _fake_post(url, *, json, headers, timeout):
            captured.append(json)
            return _mock_neo4j_response()

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock(side_effect=_fake_post)

        with patch("httpx.AsyncClient", return_value=mock_client):
            await neo4j_export(body)

        stmts = [s["statement"] for s in captured[0]["statements"]]
        assert any("SET n +=" in s and "name" in s and "age" in s for s in stmts)

    @pytest.mark.asyncio
    async def test_no_set_block_when_no_properties(self):
        """REQ-715: SET block omitted when node has no properties."""
        body = _make_request(
            nodes=[{"id": 6, "tableLabel": "Tag", "label": "Tag", "properties": {}}],
        )
        captured: list[dict] = []

        async def _fake_post(url, *, json, headers, timeout):
            captured.append(json)
            return _mock_neo4j_response()

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock(side_effect=_fake_post)

        with patch("httpx.AsyncClient", return_value=mock_client):
            await neo4j_export(body)

        stmts = [s["statement"] for s in captured[0]["statements"]]
        assert any("SET" not in s for s in stmts)


class TestEdgeMergeStatements:
    """REQ-716: Edges MERGE'd by matching source/target nodes on _provisa_id."""

    @pytest.mark.asyncio
    async def test_edge_merge_matches_on_provisa_id(self):
        """REQ-716: Edge MERGE uses _provisa_id to match both endpoint nodes."""
        body = _make_request(
            edges=[
                {
                    "start": 10,
                    "end": 20,
                    "type": "KNOWS",
                    "startNodeLabel": "Person",
                    "endNodeLabel": "Person",
                }
            ],
        )
        captured: list[dict] = []

        async def _fake_post(url, *, json, headers, timeout):
            captured.append(json)
            return _mock_neo4j_response()

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock(side_effect=_fake_post)

        with patch("httpx.AsyncClient", return_value=mock_client):
            await neo4j_export(body)

        stmts = [s["statement"] for s in captured[0]["statements"]]
        assert any(
            "_provisa_id: 10" in s and "_provisa_id: 20" in s and "KNOWS" in s for s in stmts
        )

    @pytest.mark.asyncio
    async def test_edge_uses_start_end_node_labels(self):
        """REQ-716: Edge MATCH uses startNodeLabel and endNodeLabel."""
        body = _make_request(
            edges=[
                {
                    "start": 1,
                    "end": 2,
                    "type": "OWNS",
                    "startNodeLabel": "Customer",
                    "endNodeLabel": "Product",
                }
            ],
        )
        captured: list[dict] = []

        async def _fake_post(url, *, json, headers, timeout):
            captured.append(json)
            return _mock_neo4j_response()

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock(side_effect=_fake_post)

        with patch("httpx.AsyncClient", return_value=mock_client):
            await neo4j_export(body)

        stmts = [s["statement"] for s in captured[0]["statements"]]
        assert any("`Customer`" in s and "`Product`" in s for s in stmts)


class TestHttpBasicAuth:
    """REQ-717: Uses HTTP Basic auth via Authorization header."""

    @pytest.mark.asyncio
    async def test_authorization_header_is_basic(self):
        """REQ-717: Authorization header is Basic base64(username:password)."""
        body = _make_request(username="admin", password="letmein")
        captured_headers: list[dict] = []

        async def _fake_post(url, *, json, headers, timeout):
            captured_headers.append(headers)
            return _mock_neo4j_response()

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock(side_effect=_fake_post)

        with patch("httpx.AsyncClient", return_value=mock_client):
            await neo4j_export(body)

        assert captured_headers, "No POST call captured"
        auth_header = captured_headers[0].get("Authorization", "")
        assert auth_header.startswith("Basic ")
        decoded = base64.b64decode(auth_header[len("Basic ") :]).decode()
        assert decoded == "admin:letmein"

    @pytest.mark.asyncio
    async def test_url_uses_database_path(self):
        """REQ-717: POST target URL includes /db/<database>/tx/commit."""
        body = _make_request(url="http://neo4j:7474", database="mydb")
        captured_urls: list[str] = []

        async def _fake_post(url, *, json, headers, timeout):
            captured_urls.append(url)
            return _mock_neo4j_response()

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock(side_effect=_fake_post)

        with patch("httpx.AsyncClient", return_value=mock_client):
            await neo4j_export(body)

        assert captured_urls
        assert "/db/mydb/tx/commit" in captured_urls[0]


class TestConnectionErrors:
    """REQ-718: Connection errors → 502; timeouts → 504."""

    @pytest.mark.asyncio
    async def test_connect_error_returns_502(self):
        """REQ-718: httpx.ConnectError maps to HTTP 502."""
        body = _make_request()

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock(side_effect=httpx.ConnectError("refused"))

        with patch("httpx.AsyncClient", return_value=mock_client):
            response = await neo4j_export(body)

        assert response.status_code == 502

    @pytest.mark.asyncio
    async def test_timeout_returns_504(self):
        """REQ-718: httpx.TimeoutException maps to HTTP 504."""
        body = _make_request()

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock(side_effect=httpx.TimeoutException("timed out"))

        with patch("httpx.AsyncClient", return_value=mock_client):
            response = await neo4j_export(body)

        assert response.status_code == 504

    @pytest.mark.asyncio
    async def test_401_from_neo4j_returns_401(self):
        """REQ-717/718: Neo4j 401 response propagates as 401."""
        body = _make_request()

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock(return_value=_mock_neo4j_response(status_code=401))

        with patch("httpx.AsyncClient", return_value=mock_client):
            response = await neo4j_export(body)

        assert response.status_code == 401


class TestImportedResponse:
    """REQ-719: Returns {imported: N, errors: [...]} structure."""

    @pytest.mark.asyncio
    async def test_returns_imported_count_and_empty_errors(self):
        """REQ-719: Successful export returns imported count equal to statement count."""
        body = _make_request(
            nodes=[
                {"id": 1, "tableLabel": "A", "label": "A", "properties": {}},
                {"id": 2, "tableLabel": "B", "label": "B", "properties": {}},
            ],
            edges=[
                {"start": 1, "end": 2, "type": "LINKS", "startNodeLabel": "A", "endNodeLabel": "B"}
            ],
        )

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock(return_value=_mock_neo4j_response(errors=[]))

        with patch("httpx.AsyncClient", return_value=mock_client):
            response = await neo4j_export(body)

        assert response.status_code == 200
        data = json.loads(bytes(response.body))
        assert "imported" in data
        assert "errors" in data
        assert data["imported"] == 3
        assert data["errors"] == []

    @pytest.mark.asyncio
    async def test_errors_from_neo4j_included_in_response(self):
        """REQ-719: Neo4j error messages are included in the errors list."""
        body = _make_request(
            nodes=[{"id": 1, "tableLabel": "X", "label": "X", "properties": {}}],
        )
        neo4j_errors = [{"message": "Constraint violation on node"}]

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock(return_value=_mock_neo4j_response(errors=neo4j_errors))

        with patch("httpx.AsyncClient", return_value=mock_client):
            response = await neo4j_export(body)

        data = json.loads(bytes(response.body))
        assert len(data["errors"]) == 1
        assert "Constraint violation" in data["errors"][0]

    @pytest.mark.asyncio
    async def test_imported_reduced_by_error_count(self):
        """REQ-719: imported = total statements - error count."""
        body = _make_request(
            nodes=[
                {"id": 1, "tableLabel": "N", "label": "N", "properties": {}},
                {"id": 2, "tableLabel": "N", "label": "N", "properties": {}},
            ],
        )
        neo4j_errors = [{"message": "error on second"}]

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock(return_value=_mock_neo4j_response(errors=neo4j_errors))

        with patch("httpx.AsyncClient", return_value=mock_client):
            response = await neo4j_export(body)

        data = json.loads(bytes(response.body))
        assert data["imported"] == 1


class TestBatchingBehavior:
    """REQ-720: E2E export batches nodes and edges separately."""

    @pytest.mark.asyncio
    async def test_nodes_and_edges_sent_in_single_transaction(self):
        """REQ-720: All node and edge statements sent as one batch to /tx/commit."""
        body = _make_request(
            nodes=[
                {"id": 1, "tableLabel": "A", "label": "A", "properties": {"x": 1}},
                {"id": 2, "tableLabel": "B", "label": "B", "properties": {"y": 2}},
            ],
            edges=[
                {"start": 1, "end": 2, "type": "REL", "startNodeLabel": "A", "endNodeLabel": "B"}
            ],
        )
        captured: list[dict] = []

        async def _fake_post(url, *, json, headers, timeout):
            captured.append(json)
            return _mock_neo4j_response()

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock(side_effect=_fake_post)

        with patch("httpx.AsyncClient", return_value=mock_client):
            await neo4j_export(body)

        assert len(captured) == 1, "Expected exactly one HTTP call to Neo4j"
        stmts = captured[0]["statements"]
        assert len(stmts) == 3

    @pytest.mark.asyncio
    async def test_node_statements_precede_edge_statements(self):
        """REQ-720: Node MERGE statements appear before edge MERGE statements."""
        body = _make_request(
            nodes=[{"id": 1, "tableLabel": "A", "label": "A", "properties": {}}],
            edges=[
                {"start": 1, "end": 2, "type": "HAS", "startNodeLabel": "A", "endNodeLabel": "B"}
            ],
        )
        captured: list[dict] = []

        async def _fake_post(url, *, json, headers, timeout):
            captured.append(json)
            return _mock_neo4j_response()

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock(side_effect=_fake_post)

        with patch("httpx.AsyncClient", return_value=mock_client):
            await neo4j_export(body)

        stmts = [s["statement"] for s in captured[0]["statements"]]
        node_idx = next(i for i, s in enumerate(stmts) if "MERGE (n:" in s)
        edge_idx = next(i for i, s in enumerate(stmts) if "MATCH (a:" in s)
        assert node_idx < edge_idx

    @pytest.mark.asyncio
    async def test_empty_nodes_and_edges_returns_zero_imported(self):
        """REQ-720: Export with no nodes or edges returns imported=0."""
        body = _make_request(nodes=[], edges=[])

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock(return_value=_mock_neo4j_response())

        with patch("httpx.AsyncClient", return_value=mock_client):
            response = await neo4j_export(body)

        data = json.loads(bytes(response.body))
        assert data["imported"] == 0
        assert data["errors"] == []
