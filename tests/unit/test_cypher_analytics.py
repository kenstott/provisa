# Copyright (c) 2026 Kenneth Stott
# Canary: a1b2c3d4-e5f6-7890-abcd-ef1234567890
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for impute-relationships endpoint (REQ-784, REQ-785, REQ-786)."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import ValidationError

from provisa.api.rest.cypher_router import ImputeRequest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_pg_row(id_: int, label: str, composite_id: str) -> dict:
    """Simulate an asyncpg Record for node_ids rows."""
    return {"id": id_, "label": label, "composite_id": composite_id}


def _make_serialized_node(label: str, node_id: Any, table_label: str = "") -> dict:
    return {
        "id": node_id,
        "label": label,
        "tableLabel": table_label or label,
        "properties": {},
    }


def _make_serialized_edge(
    identity: str,
    start_node: dict,
    end_node: dict,
    rel_type: str = "RELATES_TO",
) -> dict:
    return {
        "identity": identity,
        "start": start_node["id"],
        "end": end_node["id"],
        "type": rel_type,
        "properties": {},
        "startNode": start_node,
        "endNode": end_node,
    }


# ---------------------------------------------------------------------------
# REQ-786: ImputeRequest model
# ---------------------------------------------------------------------------


class TestImputeRequestModel:
    """REQ-786: endpoint accepts visible node set with optional relationship hints."""

    def test_accepts_nodes_list(self):
        """ImputeRequest accepts a list of node dicts."""
        req = ImputeRequest(nodes=[{"label": "Person", "id": "1"}])
        assert len(req.nodes) == 1
        assert req.nodes[0]["label"] == "Person"

    def test_empty_nodes_list_allowed(self):
        """ImputeRequest allows an empty nodes list."""
        req = ImputeRequest(nodes=[])
        assert req.nodes == []

    def test_nodes_with_extra_fields_allowed(self):
        """ImputeRequest nodes may carry optional relationship hints as extra fields."""
        req = ImputeRequest(
            nodes=[
                {"label": "Person", "id": "42", "hint": "KNOWS", "properties": {"name": "Alice"}}
            ]
        )
        assert req.nodes[0].get("hint") == "KNOWS"

    def test_nodes_required_field(self):
        """ImputeRequest raises ValidationError when nodes field is missing."""
        with pytest.raises(ValidationError):
            ImputeRequest()  # type: ignore[call-arg]

    def test_multiple_nodes_different_labels(self):
        """ImputeRequest accepts nodes of different labels in one request."""
        req = ImputeRequest(
            nodes=[
                {"label": "Person", "id": "1"},
                {"label": "Company", "id": "2"},
                {"label": "Person", "id": "3"},
            ]
        )
        assert len(req.nodes) == 3
        labels = {n["label"] for n in req.nodes}
        assert labels == {"Person", "Company"}

    def test_node_id_as_string_integer(self):
        """ImputeRequest preserves string-encoded integer ids."""
        req = ImputeRequest(nodes=[{"label": "Person", "id": "999"}])
        assert req.nodes[0]["id"] == "999"

    def test_node_id_as_integer(self):
        """ImputeRequest preserves integer ids as-is."""
        req = ImputeRequest(nodes=[{"label": "Person", "id": 999}])
        assert req.nodes[0]["id"] == 999


# ---------------------------------------------------------------------------
# REQ-784: endpoint generates relationship edges for visible node set
# ---------------------------------------------------------------------------


class TestImputeRelationshipsEdgeGeneration:
    """REQ-784: auto-impute generates relationship edges for visible node set."""

    def _make_mock_state(self, pg_rows: list[dict]) -> MagicMock:
        """Build a minimal AppState mock."""
        state = MagicMock()
        state.pg_pool = MagicMock()

        conn_ctx = AsyncMock()
        conn_ctx.fetch = AsyncMock(return_value=pg_rows)
        acquire_ctx = MagicMock()
        acquire_ctx.__aenter__ = AsyncMock(return_value=conn_ctx)
        acquire_ctx.__aexit__ = AsyncMock(return_value=False)
        state.pg_pool.acquire = MagicMock(return_value=acquire_ctx)

        state.contexts = {"default": MagicMock()}
        state.roles = {"default": {}}
        state.schema_build_cache = {}
        state.source_catalogs = None
        return state

    @pytest.mark.asyncio
    async def test_returns_empty_when_no_nodes(self):
        """Empty node set produces empty columns+rows response."""

        # Direct unit test of logic: no nodes → no queries → empty response
        req = ImputeRequest(nodes=[])
        # No pg_pool calls needed for empty request
        state = self._make_mock_state([])
        state.contexts = {"default": MagicMock()}

        import provisa.api.app as _app_mod

        with (
            patch.object(_app_mod, "state", state),
            patch("provisa.api.rest.cypher_router._build_label_map") as mock_lm,
        ):
            mock_lm.return_value = MagicMock(nodes={}, relationships={})
            from fastapi import Request as FastAPIRequest

            mock_request = MagicMock(spec=FastAPIRequest)
            mock_request.headers = {}
            from provisa.api.rest.cypher_router import impute_relationships

            resp = await impute_relationships(mock_request, req)
            data = resp.body
            import json

            parsed = json.loads(data)
            assert parsed["columns"] == []
            assert parsed["rows"] == []

    @pytest.mark.asyncio
    async def test_generates_cypher_for_visible_pairs(self):
        """For two visible labels with a known relationship, one query is built per rel."""
        from fastapi import Request as FastAPIRequest
        from provisa.api.rest.cypher_router import impute_relationships

        pg_rows = [
            _make_pg_row(1, "Person", "Person|1"),
            _make_pg_row(2, "Company", "Company|2"),
        ]
        state = self._make_mock_state(pg_rows)

        person_nm = MagicMock()
        person_nm.label = "Person"
        person_nm.id_column = "person_id"
        person_nm.domain_id = "hr"

        company_nm = MagicMock()
        company_nm.label = "Company"
        company_nm.id_column = "company_id"
        company_nm.domain_id = "hr"

        rel = MagicMock()
        rel.source_label = "person_key"
        rel.target_label = "company_key"
        rel.rel_type = "WORKS_AT"

        label_map = MagicMock()
        label_map.nodes = {
            "person_key": person_nm,
            "company_key": company_nm,
        }
        label_map.relationships = {"works_at": rel}

        captured_queries: list[str] = []

        async def _fake_execute_call_body(ast, lmap, params, st, ctx, role_id="default"):
            return [], {}

        import provisa.api.app as _app_mod

        def _capture_parse(q):
            captured_queries.append(q)
            return MagicMock()

        with (
            patch.object(_app_mod, "state", state),
            patch("provisa.api.rest.cypher_router._build_label_map", return_value=label_map),
            patch(
                "provisa.api.rest.cypher_router._execute_call_body",
                side_effect=_fake_execute_call_body,
            ),
            patch("provisa.cypher.assembler.register_node_ids", new_callable=AsyncMock),
            patch("provisa.cypher.assembler.assemble_rows", return_value=[]),
            patch("provisa.cypher.parser.parse_cypher", side_effect=_capture_parse),
        ):
            mock_request = MagicMock(spec=FastAPIRequest)
            mock_request.headers = {}

            body = ImputeRequest(
                nodes=[
                    {"label": "Person", "id": "1"},
                    {"label": "Company", "id": "2"},
                ]
            )
            await impute_relationships(mock_request, body)

        assert len(captured_queries) == 1
        q = captured_queries[0]
        assert "WORKS_AT" in q
        assert "Person" in q
        assert "Company" in q

    @pytest.mark.asyncio
    async def test_no_queries_when_one_label_missing(self):
        """If only one of the two relationship endpoints is visible, no query is built."""
        from fastapi import Request as FastAPIRequest
        from provisa.api.rest.cypher_router import impute_relationships

        # Only Person nodes in pg, no Company
        pg_rows = [_make_pg_row(1, "Person", "Person|1")]
        state = self._make_mock_state(pg_rows)

        person_nm = MagicMock()
        person_nm.label = "Person"
        person_nm.id_column = "person_id"

        company_nm = MagicMock()
        company_nm.label = "Company"
        company_nm.id_column = "company_id"

        rel = MagicMock()
        rel.source_label = "person_key"
        rel.target_label = "company_key"
        rel.rel_type = "WORKS_AT"

        label_map = MagicMock()
        label_map.nodes = {"person_key": person_nm, "company_key": company_nm}
        label_map.relationships = {"works_at": rel}

        import provisa.api.app as _app_mod

        with (
            patch.object(_app_mod, "state", state),
            patch("provisa.api.rest.cypher_router._build_label_map", return_value=label_map),
        ):
            mock_request = MagicMock(spec=FastAPIRequest)
            mock_request.headers = {}

            body = ImputeRequest(nodes=[{"label": "Person", "id": "1"}])
            import json

            resp = await impute_relationships(mock_request, body)
            parsed = json.loads(resp.body)
            assert parsed["columns"] == []
            assert parsed["rows"] == []

    @pytest.mark.asyncio
    async def test_response_columns_include_node(self):
        """Response columns field equals ['node'] when edges are returned."""
        from fastapi import Request as FastAPIRequest
        from provisa.api.rest.cypher_router import impute_relationships
        import json

        pg_rows = [
            _make_pg_row(10, "Person", "Person|10"),
            _make_pg_row(20, "Company", "Company|20"),
        ]
        state = self._make_mock_state(pg_rows)

        person_nm = MagicMock()
        person_nm.label = "Person"
        person_nm.id_column = "person_id"

        company_nm = MagicMock()
        company_nm.label = "Company"
        company_nm.id_column = "company_id"

        rel = MagicMock()
        rel.source_label = "person_key"
        rel.target_label = "company_key"
        rel.rel_type = "WORKS_AT"

        label_map = MagicMock()
        label_map.nodes = {"person_key": person_nm, "company_key": company_nm}
        label_map.relationships = {"works_at": rel}

        node_a = _make_serialized_node("Person", "Person|10")
        node_b = _make_serialized_node("Company", "Company|20")
        edge = _make_serialized_edge("edge|1", node_a, node_b, "WORKS_AT")
        assembled_rows = [{"a": node_a, "r": edge, "b": node_b}]

        async def _fake_execute_call_body(ast, lmap, params, st, ctx, role_id="default"):
            return [{}], {}

        import provisa.api.app as _app_mod

        with (
            patch.object(_app_mod, "state", state),
            patch("provisa.api.rest.cypher_router._build_label_map", return_value=label_map),
            patch(
                "provisa.api.rest.cypher_router._execute_call_body",
                side_effect=_fake_execute_call_body,
            ),
            patch("provisa.cypher.assembler.register_node_ids", new_callable=AsyncMock),
            patch("provisa.cypher.parser.parse_cypher", return_value=MagicMock()),
            patch("provisa.cypher.assembler.assemble_rows", return_value=assembled_rows),
        ):
            mock_request = MagicMock(spec=FastAPIRequest)
            mock_request.headers = {}

            body = ImputeRequest(
                nodes=[
                    {"label": "Person", "id": "10"},
                    {"label": "Company", "id": "20"},
                ]
            )
            resp = await impute_relationships(mock_request, body)
            parsed = json.loads(resp.body)
            assert "columns" in parsed
            assert parsed["columns"] == ["node"]


# ---------------------------------------------------------------------------
# REQ-785: startNode/endNode ids must be stable integers
# ---------------------------------------------------------------------------


class TestImputeStableIds:
    """REQ-785: imputed edge startNode/endNode ids are stable integers, not random/sequential."""

    def test_register_node_ids_replaces_composite_with_integer(self):
        """register_node_ids replaces composite string IDs with DB-assigned integers."""
        from provisa.cypher.assembler import _apply_id_map

        id_map = {"Person|1": 101, "Company|2": 202}
        node = {"id": "Person|1", "label": "Person", "tableLabel": "Person", "properties": {}}
        result = _apply_id_map(node, id_map)
        assert result["id"] == 101

    def test_apply_id_map_replaces_edge_start_end(self):
        """_apply_id_map updates edge start/end and nested startNode/endNode ids."""
        from provisa.cypher.assembler import _apply_id_map

        id_map = {"Person|1": 101, "Company|2": 202}
        edge = {
            "identity": "edge|1",
            "start": "Person|1",
            "end": "Company|2",
            "type": "WORKS_AT",
            "properties": {},
            "startNode": {"id": "Person|1", "label": "Person", "properties": {}},
            "endNode": {"id": "Company|2", "label": "Company", "properties": {}},
        }
        result = _apply_id_map(edge, id_map)
        assert result["start"] == 101
        assert result["end"] == 202
        assert result["startNode"]["id"] == 101
        assert result["endNode"]["id"] == 202

    def test_same_input_produces_same_id(self):
        """Same composite_id always maps to the same integer (deterministic, not random)."""
        from provisa.cypher.assembler import _apply_id_map

        id_map = {"Person|42": 9999}
        node_a = {"id": "Person|42", "label": "Person", "properties": {}}
        node_b = {"id": "Person|42", "label": "Person", "properties": {}}

        result_a = _apply_id_map(node_a, id_map)
        result_b = _apply_id_map(node_b, id_map)

        assert result_a["id"] == result_b["id"] == 9999

    def test_walk_for_nodes_collects_composite_ids(self):
        """_walk_for_nodes collects nodes whose id contains '|' (composite format)."""
        from provisa.cypher.assembler import _walk_for_nodes

        out: dict = {}
        node = {
            "id": "Person|7",
            "label": "Person",
            "properties": {"name": "Alice"},
        }
        _walk_for_nodes(node, out)
        assert "Person|7" in out
        assert out["Person|7"] == ("Person", {"name": "Alice"})

    def test_walk_for_nodes_skips_non_composite_ids(self):
        """_walk_for_nodes does not collect nodes whose id is already an integer."""
        from provisa.cypher.assembler import _walk_for_nodes

        out: dict = {}
        node = {
            "id": 7,
            "label": "Person",
            "properties": {"name": "Alice"},
        }
        _walk_for_nodes(node, out)
        assert out == {}

    def test_walk_for_nodes_descends_into_edge_endpoints(self):
        """_walk_for_nodes extracts startNode and endNode ids from edge dicts."""
        from provisa.cypher.assembler import _walk_for_nodes

        out: dict = {}
        edge = {
            "identity": "edge|1",
            "start": "Person|1",
            "end": "Company|2",
            "type": "WORKS_AT",
            "properties": {},
            "startNode": {"id": "Person|1", "label": "Person", "properties": {"name": "Bob"}},
            "endNode": {"id": "Company|2", "label": "Company", "properties": {"name": "Acme"}},
        }
        _walk_for_nodes(edge, out)
        assert "Person|1" in out
        assert "Company|2" in out

    @pytest.mark.asyncio
    async def test_register_node_ids_calls_upsert_then_replaces(self):
        """register_node_ids upserts to DB and mutates rows in place with returned ids."""
        from provisa.cypher.assembler import register_node_ids

        db_rows = [{"composite_id": "Person|5", "id": 555}]

        conn = AsyncMock()
        conn.fetch = AsyncMock(return_value=db_rows)

        acquire_ctx = MagicMock()
        acquire_ctx.__aenter__ = AsyncMock(return_value=conn)
        acquire_ctx.__aexit__ = AsyncMock(return_value=False)

        pg_pool = MagicMock()
        pg_pool.acquire = MagicMock(return_value=acquire_ctx)

        rows = [{"node": {"id": "Person|5", "label": "Person", "properties": {}}}]
        await register_node_ids(rows, pg_pool)

        conn.fetch.assert_called_once()
        assert rows[0]["node"]["id"] == 555

    @pytest.mark.asyncio
    async def test_register_node_ids_noop_without_pg_pool(self):
        """register_node_ids is a no-op when pg_pool is None."""
        from provisa.cypher.assembler import register_node_ids

        rows = [{"node": {"id": "Person|5", "label": "Person", "properties": {}}}]
        original_id = rows[0]["node"]["id"]
        await register_node_ids(rows, None)
        assert rows[0]["node"]["id"] == original_id

    def test_id_stability_across_multiple_apply_calls(self):
        """Applying the same id_map twice yields the same result (idempotent)."""
        from provisa.cypher.assembler import _apply_id_map

        id_map = {"Person|3": 300, "Company|4": 400}
        edge = {
            "identity": "e|1",
            "start": "Person|3",
            "end": "Company|4",
            "type": "REL",
            "properties": {},
            "startNode": {"id": "Person|3", "label": "Person", "properties": {}},
            "endNode": {"id": "Company|4", "label": "Company", "properties": {}},
        }
        result1 = _apply_id_map(edge, id_map)
        result2 = _apply_id_map(result1, id_map)
        assert result1["start"] == result2["start"] == 300
        assert result1["end"] == result2["end"] == 400
