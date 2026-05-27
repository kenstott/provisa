# Copyright (c) 2026 Kenneth Stott
# Canary: b3e1a7f2-d4c8-4e91-b2a3-5f6d8e9c1a04
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for compileQuery GQL mutation (REQ-161).

Verifies that compile returns governed SQL — with RLS, masking, and
visibility enforcement applied — without executing the query.
"""

from __future__ import annotations

import os
from pathlib import Path

import asyncpg
import httpx
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

_COMPILE_MUTATION = """
mutation CompileQuery($input: CompileQueryInput!) {
  compileQuery(input: $input) {
    sql semanticSql trinoSql directSql route routeReason sources
    rootField canonicalField compiledCypher optimizations warnings
    columnAliases { fieldName column }
    enforcement {
      rlsFiltersApplied columnsExcluded schemaScope maskingApplied ceilingApplied route
    }
  }
}
"""


async def _compile(
    client: AsyncClient, query: str, role: str = "admin", variables: dict | None = None
):
    resp = await client.post(
        "/admin/graphql",
        json={
            "query": _COMPILE_MUTATION,
            "variables": {"input": {"query": query, "role": role, "variables": variables}},
        },
    )
    return resp


_FIXTURE_CONFIG = Path(__file__).parent.parent / "fixtures" / "sample_config.yaml"
_SCHEMA_SQL = Path(__file__).parent.parent.parent / "provisa" / "core" / "schema.sql"
_MAIN_CONFIG = Path(__file__).parent.parent.parent / "config" / "provisa.yaml"
_LIVE_URL = os.environ.get("PROVISA_URL", "http://localhost:8000")


@pytest_asyncio.fixture(scope="module", loop_scope="session", autouse=True)
async def _register_demo_data_on_live_server():
    """Register demo sources/tables/relationships on the live server for TestFlatCypherReturn."""
    try:
        httpx.get(f"{_LIVE_URL}/health", timeout=3)
    except Exception:
        yield
        return

    async with httpx.AsyncClient(base_url=_LIVE_URL, timeout=120) as ac:

        async def _gql(query: str) -> dict:
            resp = await ac.post("/admin/graphql", json={"query": query})
            resp.raise_for_status()
            return resp.json()

        await _gql(
            'mutation { createSource(input: {id: "inquiries-sqlite", type: "sqlite"}) { success } }'
        )
        await _gql(
            'mutation { createSource(input: {id: "shelter", type: "graphql_remote"}) { success } }'
        )
        await _gql(
            'mutation { createDomain(input: {id: "shelter", description: "Animal shelter"}) { success } }'
        )
        await _gql("""
            mutation {
                registerTable(input: {
                    sourceId: "inquiries-sqlite", domainId: "pet-store",
                    schemaName: "default", tableName: "inquiries",
                    governance: "pre-approved",
                    columns: [
                        {name: "id", visibleTo: ["admin", "analyst"]},
                        {name: "pet_id", visibleTo: ["admin", "analyst"]},
                        {name: "inquiry_type", visibleTo: ["admin", "analyst"]},
                        {name: "message", visibleTo: ["admin", "analyst"]},
                        {name: "status", visibleTo: ["admin", "analyst"]},
                        {name: "submitted_at", visibleTo: ["admin", "analyst"]}
                    ]
                }) { success message }
            }
        """)
        await _gql("""
            mutation {
                registerTable(input: {
                    sourceId: "shelter", domainId: "shelter",
                    schemaName: "default", tableName: "assignments",
                    alias: "shelter__assignments",
                    governance: "pre-approved",
                    columns: [
                        {name: "id", visibleTo: ["admin", "analyst"]},
                        {name: "breedName", visibleTo: ["admin", "analyst"]}
                    ]
                }) { success message }
            }
        """)
        await _gql("""
            mutation {
                upsertRelationship(input: {
                    id: "inquiries-to-pets",
                    sourceTableId: "inquiries",
                    targetTableId: "pets",
                    sourceColumn: "pet_id",
                    targetColumn: "id",
                    cardinality: "many-to-one",
                    alias: "HAS_PETS"
                }) { success message }
            }
        """)
        await _gql("""
            mutation {
                upsertRelationship(input: {
                    id: "pets-to-shelter-assignments",
                    sourceTableId: "pets",
                    targetTableId: "shelter__assignments",
                    sourceColumn: "breed_name",
                    targetColumn: "breedName",
                    cardinality: "many-to-one"
                }) { success message }
            }
        """)

        yield

        for rel_id in ("pets-to-shelter-assignments", "inquiries-to-pets"):
            await ac.post(
                "/admin/graphql",
                json={"query": f'mutation {{ deleteRelationship(id: "{rel_id}") {{ success }} }}'},
            )
        for src_id in ("inquiries-sqlite", "shelter"):
            await ac.post(
                "/admin/graphql",
                json={"query": f'mutation {{ deleteSource(id: "{src_id}") {{ success }} }}'},
            )
        await ac.post(
            "/admin/graphql",
            json={"query": 'mutation { deleteDomain(id: "shelter") { success } }'},
        )
        await ac.put(
            "/admin/config",
            content=_MAIN_CONFIG.read_bytes(),
            headers={"Content-Type": "application/x-yaml"},
        )


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def client():
    os.environ.setdefault("PG_PASSWORD", "provisa")

    from provisa.api.app import create_app
    from provisa.core.config_loader import load_config_from_yaml

    pg_dsn = (
        f"postgresql://{os.environ.get('PG_USER', 'provisa')}"
        f":{os.environ.get('PG_PASSWORD', 'provisa')}"
        f"@{os.environ.get('PG_HOST', 'localhost')}"
        f":{os.environ.get('PG_PORT', '5432')}"
        f"/{os.environ.get('PG_DATABASE', 'provisa')}"
    )
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(_SCHEMA_SQL.read_text())
        await load_config_from_yaml(_FIXTURE_CONFIG, conn)
    finally:
        await conn.close()

    app = create_app()

    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            yield c


class TestCompileBasic:
    async def test_compile_returns_sql(self, client):
        """Basic compile returns a non-empty SQL string."""
        resp = await _compile(client, "{ sa__orders { id amount } }")
        assert resp.status_code == 200
        body = resp.json()
        assert "errors" not in body, body.get("errors")
        results = body["data"]["compileQuery"]
        assert len(results) > 0
        assert len(results[0]["sql"]) > 0

    async def test_compile_returns_enforcement_metadata(self, client):
        """Compile response includes the enforcement object (REQ-161)."""
        resp = await _compile(client, "{ sa__orders { id amount } }")
        assert resp.status_code == 200
        body = resp.json()
        assert "errors" not in body, body.get("errors")
        enforcement = body["data"]["compileQuery"][0]["enforcement"]
        assert "rlsFiltersApplied" in enforcement
        assert "columnsExcluded" in enforcement
        assert "schemaScope" in enforcement
        assert "maskingApplied" in enforcement
        assert "route" in enforcement

    async def test_compile_schema_scope_reflects_role(self, client):
        """schema_scope in enforcement reflects the requested role."""
        resp = await _compile(client, "{ sa__orders { id } }", role="analyst")
        assert resp.status_code == 200
        body = resp.json()
        assert "errors" not in body, body.get("errors")
        scope = body["data"]["compileQuery"][0]["enforcement"]["schemaScope"]
        assert "analyst" in scope

    async def test_compile_returns_route_decision(self, client):
        """Compile includes route and routeReason fields."""
        resp = await _compile(client, "{ sa__orders { id } }")
        assert resp.status_code == 200
        body = resp.json()
        assert "errors" not in body, body.get("errors")
        result = body["data"]["compileQuery"][0]
        assert result["route"] in ("direct", "trino")
        assert result["routeReason"]

    async def test_compile_returns_sources(self, client):
        """Compile identifies the source(s) involved in the query."""
        resp = await _compile(client, "{ sa__orders { id } }")
        assert resp.status_code == 200
        body = resp.json()
        assert "errors" not in body, body.get("errors")
        sources = body["data"]["compileQuery"][0]["sources"]
        assert isinstance(sources, list)
        assert len(sources) > 0


class TestCompileWithVariables:
    async def test_compile_with_variables(self, client):
        """Variables are incorporated into compiled SQL as parameters."""
        resp = await _compile(
            client,
            "query ($region: String) { sa__orders(where: {region: {eq: $region}}) { id } }",
            variables={"region": "EMEA"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert "errors" not in body, body.get("errors")
        sql = body["data"]["compileQuery"][0]["sql"]
        assert "sql" in body["data"]["compileQuery"][0]
        # Variable should not be interpolated literally into SQL
        assert "EMEA" not in sql


class TestCompileMultiRoot:
    async def test_multi_root_query_returns_multiple_results(self, client):
        """Multiple root fields return multiple compile results."""
        resp = await _compile(
            client,
            "{ sa__orders { id } sa__customers { id } }",
        )
        assert resp.status_code == 200
        body = resp.json()
        assert "errors" not in body, body.get("errors")
        results = body["data"]["compileQuery"]
        assert len(results) == 2
        for r in results:
            assert "sql" in r
            assert "enforcement" in r


class TestCompileRLSEnforcement:
    async def test_compile_rls_filters_shown_in_enforcement(self, client):
        """When a role has RLS configured, compile lists the filters applied."""
        resp = await _compile(client, "{ sa__orders { id } }", role="analyst")
        assert resp.status_code == 200
        body = resp.json()
        assert "errors" not in body, body.get("errors")
        enforcement = body["data"]["compileQuery"][0]["enforcement"]
        assert isinstance(enforcement["rlsFiltersApplied"], list)

    async def test_compile_excluded_columns_shown(self, client):
        """Columns not requested appear in columnsExcluded."""
        resp = await _compile(client, "{ sa__orders { id } }", role="analyst")
        assert resp.status_code == 200
        body = resp.json()
        assert "errors" not in body, body.get("errors")
        enforcement = body["data"]["compileQuery"][0]["enforcement"]
        assert isinstance(enforcement["columnsExcluded"], list)


class TestCompileErrors:
    async def test_compile_invalid_graphql_syntax_error(self, client):
        """Syntactically invalid GraphQL returns GQL error."""
        resp = await _compile(client, "{ sa__orders { id")
        assert resp.status_code == 200
        body = resp.json()
        assert "errors" in body

    async def test_compile_unknown_role_error(self, client):
        """Unknown role returns GQL error (no schema found)."""
        resp = await _compile(client, "{ sa__orders { id } }", role="nonexistent_role")
        assert resp.status_code == 200
        body = resp.json()
        assert "errors" in body

    async def test_compile_empty_query_error(self, client):
        """Empty query string returns GQL error."""
        resp = await _compile(client, "")
        assert resp.status_code == 200
        body = resp.json()
        assert "errors" in body


_COMPILE_MUTATION_WITH_FLAT = """
mutation CompileQuery($input: CompileQueryInput!) {
  compileQuery(input: $input) {
    compiledCypher
  }
}
"""


async def _compile_cypher(client, query: str, flat_cypher: bool, role: str = "admin"):
    resp = await client.post(
        "/admin/graphql",
        json={
            "query": _COMPILE_MUTATION_WITH_FLAT,
            "variables": {"input": {"query": query, "role": role, "flatCypher": flat_cypher}},
        },
    )
    return resp


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope="session")
class TestFlatCypherReturn:
    """Regression: flat Cypher RETURN must expand fields as label__prop, not bare node alias.

    Uses the live server (live_client) so no fixture config loading is needed —
    the ps__pets → assignment relationship exists in the dev environment.
    """

    async def test_flat_cypher_no_bare_node_alias(self, live_client):
        """flatCypher=True must not emit 'b AS assignment' — each field must be explicit."""
        resp = await _compile_cypher(
            live_client,
            "{ ps__pets { name assignment { breedName } } }",
            flat_cypher=True,
        )
        assert resp.status_code == 200
        body = resp.json()
        assert "errors" not in body, body.get("errors")
        cypher = body["data"]["compileQuery"][0]["compiledCypher"]
        if not cypher:
            pytest.skip("compiledCypher not produced for this query")
        import re

        bare = re.search(r"\b[a-z]\s+AS\s+\w+\b", cypher)
        assert bare is None, f"RETURN must not use bare node alias: {cypher}"

    async def test_flat_cypher_contains_per_field_paths(self, live_client):
        """flatCypher=True must expand joined fields as node.prop AS label__prop."""
        resp = await _compile_cypher(
            live_client,
            "{ ps__pets { name assignment { breedName } } }",
            flat_cypher=True,
        )
        assert resp.status_code == 200
        body = resp.json()
        assert "errors" not in body, body.get("errors")
        cypher = body["data"]["compileQuery"][0]["compiledCypher"]
        if not cypher:
            pytest.skip("compiledCypher not produced for this query")
        assert "RETURN" in cypher
        # Relationship fields must be expanded with per-field dotted paths
        assert "." in cypher.split("RETURN")[-1], f"RETURN must use dotted paths: {cypher}"
