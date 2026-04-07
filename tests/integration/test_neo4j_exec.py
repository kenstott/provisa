# Copyright (c) 2026 Kenneth Stott
# Canary: 9f1e3b7d-c2a4-5086-8d9e-1f2a3c4b5d6e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for Neo4j source builder (Phase AO, REQ-295–299).

Pure-Python tests (no live Neo4j required) cover build_api_source, build_endpoint,
and infer_columns. Live-service tests (pytest.mark.requires_neo4j) verify actual
Cypher query execution through the API source pipeline.

To run live tests:
    docker compose up neo4j   # adds neo4j to the compose stack
    pytest tests/integration/test_neo4j_exec.py -m requires_neo4j
"""

from __future__ import annotations

import pytest

from provisa.api_source.models import (
    ApiColumnType,
    ApiEndpoint,
    ApiSource,
    ApiSourceType,
)
from provisa.neo4j.source import (
    Neo4jSourceConfig,
    build_api_source,
    build_endpoint,
    infer_columns,
)

pytestmark = [pytest.mark.integration]


# ---------------------------------------------------------------------------
# build_api_source — no live service required
# ---------------------------------------------------------------------------

class TestBuildApiSource:
    def test_builds_http_base_url(self):
        cfg = Neo4jSourceConfig(source_id="neo4j-1", host="localhost", port=7474)
        source = build_api_source(cfg)
        assert isinstance(source, ApiSource)
        assert source.base_url == "http://localhost:7474"

    def test_builds_https_when_flag_set(self):
        cfg = Neo4jSourceConfig(source_id="neo4j-2", host="db.example.com", port=7473, use_https=True)
        source = build_api_source(cfg)
        assert source.base_url.startswith("https://")

    def test_source_id_set(self):
        cfg = Neo4jSourceConfig(source_id="my-neo4j", host="localhost")
        source = build_api_source(cfg)
        assert source.id == "my-neo4j"

    def test_source_type_is_openapi(self):
        cfg = Neo4jSourceConfig(source_id="neo4j-x", host="host")
        source = build_api_source(cfg)
        assert source.type == ApiSourceType.openapi


# ---------------------------------------------------------------------------
# build_endpoint — no live service required
# ---------------------------------------------------------------------------

class TestBuildEndpoint:
    def _cfg(self):
        return Neo4jSourceConfig(source_id="neo4j-1", host="localhost", port=7474, database="neo4j")

    def test_builds_endpoint(self):
        from provisa.api_source.models import ApiColumn, ApiColumnType
        cfg = self._cfg()
        cols = [ApiColumn(name="id", type=ApiColumnType.integer)]
        ep = build_endpoint(cfg, "users", "MATCH (u:User) RETURN u.id AS id", cols)
        assert isinstance(ep, ApiEndpoint)

    def test_uses_cypher_http_path(self):
        from provisa.api_source.models import ApiColumn
        cfg = self._cfg()
        cols = [ApiColumn(name="id", type=ApiColumnType.integer)]
        ep = build_endpoint(cfg, "users", "MATCH (u:User) RETURN u.id AS id", cols)
        assert "/tx/commit" in ep.path
        assert "neo4j" in ep.path

    def test_response_normalizer_is_neo4j_tabular(self):
        from provisa.api_source.models import ApiColumn
        cfg = self._cfg()
        ep = build_endpoint(
            cfg, "users", "MATCH (u) RETURN u.id",
            [ApiColumn(name="id", type=ApiColumnType.integer)],
        )
        assert ep.response_normalizer == "neo4j_tabular"

    def test_body_encoding_is_json(self):
        from provisa.api_source.models import ApiColumn
        cfg = self._cfg()
        ep = build_endpoint(
            cfg, "users", "MATCH (u) RETURN u.id",
            [ApiColumn(name="id", type=ApiColumnType.integer)],
        )
        assert ep.body_encoding == "json"

    def test_query_template_stored(self):
        from provisa.api_source.models import ApiColumn
        cypher = "MATCH (u:User) WHERE u.active = true RETURN u.id, u.name"
        cfg = self._cfg()
        ep = build_endpoint(
            cfg, "active_users", cypher,
            [ApiColumn(name="id", type=ApiColumnType.integer)],
        )
        assert ep.query_template == cypher

    def test_method_is_post(self):
        from provisa.api_source.models import ApiColumn
        cfg = self._cfg()
        ep = build_endpoint(
            cfg, "nodes", "MATCH (n) RETURN n",
            [ApiColumn(name="n", type=ApiColumnType.jsonb)],
        )
        assert ep.method == "POST"

    def test_custom_database(self):
        cfg = Neo4jSourceConfig(source_id="neo4j-1", host="localhost", database="movies")
        from provisa.api_source.models import ApiColumn
        ep = build_endpoint(
            cfg, "films", "MATCH (m:Movie) RETURN m.title",
            [ApiColumn(name="title", type=ApiColumnType.string)],
        )
        assert "movies" in ep.path

    def test_ttl_default(self):
        from provisa.api_source.models import ApiColumn
        cfg = self._cfg()
        ep = build_endpoint(cfg, "t", "MATCH (n) RETURN n", [ApiColumn(name="n", type=ApiColumnType.string)])
        assert ep.ttl == 300

    def test_custom_ttl(self):
        from provisa.api_source.models import ApiColumn
        cfg = self._cfg()
        ep = build_endpoint(cfg, "t", "MATCH (n) RETURN n",
                            [ApiColumn(name="n", type=ApiColumnType.string)], ttl=60)
        assert ep.ttl == 60


# ---------------------------------------------------------------------------
# infer_columns — pure Python type inference
# ---------------------------------------------------------------------------

class TestInferColumns:
    def test_empty_returns_empty(self):
        assert infer_columns([]) == []

    def test_string_field(self):
        cols = infer_columns([{"name": "Alice"}, {"name": "Bob"}])
        assert len(cols) == 1
        assert cols[0].name == "name"
        assert cols[0].type == ApiColumnType.string

    def test_integer_field(self):
        cols = infer_columns([{"id": 1, "age": 30}])
        id_col = next(c for c in cols if c.name == "id")
        age_col = next(c for c in cols if c.name == "age")
        assert id_col.type == ApiColumnType.integer
        assert age_col.type == ApiColumnType.integer

    def test_float_field(self):
        cols = infer_columns([{"score": 9.5}])
        assert cols[0].type == ApiColumnType.number

    def test_boolean_field(self):
        cols = infer_columns([{"active": True}])
        assert cols[0].type == ApiColumnType.boolean

    def test_dict_field_is_jsonb(self):
        cols = infer_columns([{"metadata": {"key": "val"}}])
        assert cols[0].type == ApiColumnType.jsonb

    def test_list_field_is_jsonb(self):
        cols = infer_columns([{"tags": ["a", "b"]}])
        assert cols[0].type == ApiColumnType.jsonb

    def test_none_field_defaults_to_string(self):
        """A field where all rows are None defaults to string."""
        cols = infer_columns([{"notes": None}, {"notes": None}])
        assert cols[0].type == ApiColumnType.string

    def test_skips_none_to_find_first_value(self):
        """infer_columns inspects first non-None value to guess type."""
        cols = infer_columns([{"score": None}, {"score": 42}])
        assert cols[0].type == ApiColumnType.integer

    def test_column_order_matches_first_row(self):
        cols = infer_columns([{"a": 1, "b": "x", "c": True}])
        names = [c.name for c in cols]
        assert names == ["a", "b", "c"]

    def test_multiple_rows_uses_first_row_keys(self):
        """Column names come from the first row."""
        cols = infer_columns([{"x": 1}, {"x": 2, "y": 3}])
        assert [c.name for c in cols] == ["x"]


# ---------------------------------------------------------------------------
# Live-service tests (requires a running Neo4j instance)
# ---------------------------------------------------------------------------

@pytest.mark.requires_neo4j
class TestLiveNeo4jExecution:
    """Require Docker Compose neo4j service:
        docker compose up neo4j
    These tests execute a Cypher query through the API source pipeline
    and verify the response normalizer produces flat row dicts.
    """

    NEO4J_HOST = "localhost"
    NEO4J_PORT = 7474

    @pytest.fixture
    def cfg(self):
        return Neo4jSourceConfig(
            source_id="neo4j-live",
            host=self.NEO4J_HOST,
            port=self.NEO4J_PORT,
            database="neo4j",
        )

    async def test_returns_rows_list(self, cfg):
        """A simple UNWIND returns rows via the HTTP transaction API."""
        import httpx
        from provisa.api_source.normalizers import neo4j_tabular

        cypher = "UNWIND range(1, 3) AS n RETURN n"
        endpoint = build_endpoint(
            cfg, "numbers", cypher,
            [__import__("provisa.api_source.models", fromlist=["ApiColumn"]).ApiColumn(
                name="n", type=ApiColumnType.integer)],
        )

        payload = {"statements": [{"statement": cypher}]}
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"http://{self.NEO4J_HOST}:{self.NEO4J_PORT}/db/neo4j/tx/commit",
                json=payload,
                headers={"Content-Type": "application/json", "Accept": "application/json"},
                timeout=10.0,
            )
            resp.raise_for_status()

        rows = neo4j_tabular(resp.json())
        assert len(rows) == 3
        ns = [r["n"] for r in rows]
        assert ns == [1, 2, 3]

    async def test_column_names_from_return_clause(self, cfg):
        """Returned column names match RETURN aliases."""
        import httpx
        from provisa.api_source.normalizers import neo4j_tabular

        cypher = "RETURN 'hello' AS greeting, 42 AS answer"
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"http://{self.NEO4J_HOST}:{self.NEO4J_PORT}/db/neo4j/tx/commit",
                json={"statements": [{"statement": cypher}]},
                headers={"Content-Type": "application/json", "Accept": "application/json"},
                timeout=10.0,
            )
            resp.raise_for_status()

        rows = neo4j_tabular(resp.json())
        assert len(rows) == 1
        assert rows[0]["greeting"] == "hello"
        assert rows[0]["answer"] == 42
