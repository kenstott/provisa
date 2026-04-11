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


async def _compile(client: AsyncClient, query: str, role: str = "admin", variables: dict | None = None):
    resp = await client.post(
        "/admin/graphql",
        json={"query": _COMPILE_MUTATION, "variables": {"input": {"query": query, "role": role, "variables": variables}}},
    )
    return resp


@pytest_asyncio.fixture
async def client():
    os.environ.setdefault("PG_PASSWORD", "provisa")

    from provisa.api.app import create_app

    app = create_app()

    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            yield c


class TestCompileBasic:
    async def test_compile_returns_sql(self, client):
        """Basic compile returns a non-empty SQL string."""
        resp = await _compile(client, "{ sales_analytics__orders { id amount } }")
        assert resp.status_code == 200
        body = resp.json()
        assert "errors" not in body, body.get("errors")
        results = body["data"]["compileQuery"]
        assert len(results) > 0
        assert len(results[0]["sql"]) > 0

    async def test_compile_returns_enforcement_metadata(self, client):
        """Compile response includes the enforcement object (REQ-161)."""
        resp = await _compile(client, "{ sales_analytics__orders { id amount } }")
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
        resp = await _compile(client, "{ sales_analytics__orders { id } }", role="analyst")
        assert resp.status_code == 200
        body = resp.json()
        assert "errors" not in body, body.get("errors")
        scope = body["data"]["compileQuery"][0]["enforcement"]["schemaScope"]
        assert "analyst" in scope

    async def test_compile_returns_route_decision(self, client):
        """Compile includes route and routeReason fields."""
        resp = await _compile(client, "{ sales_analytics__orders { id } }")
        assert resp.status_code == 200
        body = resp.json()
        assert "errors" not in body, body.get("errors")
        result = body["data"]["compileQuery"][0]
        assert result["route"] in ("direct", "trino")
        assert result["routeReason"]

    async def test_compile_returns_sources(self, client):
        """Compile identifies the source(s) involved in the query."""
        resp = await _compile(client, "{ sales_analytics__orders { id } }")
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
            "query ($region: String) { sales_analytics__orders(where: {region: {eq: $region}}) { id } }",
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
            "{ sales_analytics__orders { id } sales_analytics__customers { id } }",
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
        resp = await _compile(client, "{ sales_analytics__orders { id } }", role="analyst")
        assert resp.status_code == 200
        body = resp.json()
        assert "errors" not in body, body.get("errors")
        enforcement = body["data"]["compileQuery"][0]["enforcement"]
        assert isinstance(enforcement["rlsFiltersApplied"], list)

    async def test_compile_excluded_columns_shown(self, client):
        """Columns not requested appear in columnsExcluded."""
        resp = await _compile(client, "{ sales_analytics__orders { id } }", role="analyst")
        assert resp.status_code == 200
        body = resp.json()
        assert "errors" not in body, body.get("errors")
        enforcement = body["data"]["compileQuery"][0]["enforcement"]
        assert isinstance(enforcement["columnsExcluded"], list)


class TestCompileErrors:
    async def test_compile_invalid_graphql_syntax_error(self, client):
        """Syntactically invalid GraphQL returns GQL error."""
        resp = await _compile(client, "{ sales_analytics__orders { id")
        assert resp.status_code == 200
        body = resp.json()
        assert "errors" in body

    async def test_compile_unknown_role_error(self, client):
        """Unknown role returns GQL error (no schema found)."""
        resp = await _compile(client, "{ sales_analytics__orders { id } }", role="nonexistent_role")
        assert resp.status_code == 200
        body = resp.json()
        assert "errors" in body

    async def test_compile_empty_query_error(self, client):
        """Empty query string returns GQL error."""
        resp = await _compile(client, "")
        assert resp.status_code == 200
        body = resp.json()
        assert "errors" in body
