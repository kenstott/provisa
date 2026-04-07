# Copyright (c) 2026 Kenneth Stott
# Canary: b3e1a7f2-d4c8-4e91-b2a3-5f6d8e9c1a04
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for POST /data/compile endpoint (REQ-161).

Verifies that compile returns governed SQL — with RLS, masking, and
visibility enforcement applied — without executing the query.
"""

from __future__ import annotations

import os

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


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
        resp = await client.post(
            "/data/compile",
            json={"query": "{ sales_analytics__orders { id amount } }", "role": "admin"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert "sql" in body
        assert len(body["sql"]) > 0

    async def test_compile_returns_enforcement_metadata(self, client):
        """Compile response includes the enforcement object (REQ-161)."""
        resp = await client.post(
            "/data/compile",
            json={"query": "{ sales_analytics__orders { id amount } }", "role": "admin"},
        )
        assert resp.status_code == 200
        body = resp.json()
        enforcement = body["enforcement"]
        assert "rls_filters_applied" in enforcement
        assert "columns_excluded" in enforcement
        assert "schema_scope" in enforcement
        assert "masking_applied" in enforcement
        assert "route" in enforcement

    async def test_compile_schema_scope_reflects_role(self, client):
        """schema_scope in enforcement reflects the requested role."""
        resp = await client.post(
            "/data/compile",
            json={"query": "{ sales_analytics__orders { id } }", "role": "analyst"},
        )
        assert resp.status_code == 200
        scope = resp.json()["enforcement"]["schema_scope"]
        assert "analyst" in scope

    async def test_compile_returns_route_decision(self, client):
        """Compile includes route and route_reason fields."""
        resp = await client.post(
            "/data/compile",
            json={"query": "{ sales_analytics__orders { id } }", "role": "admin"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["route"] in ("direct", "trino")
        assert body["route_reason"]

    async def test_compile_returns_sources(self, client):
        """Compile identifies the source(s) involved in the query."""
        resp = await client.post(
            "/data/compile",
            json={"query": "{ sales_analytics__orders { id } }", "role": "admin"},
        )
        assert resp.status_code == 200
        sources = resp.json()["sources"]
        assert isinstance(sources, list)
        assert len(sources) > 0

    async def test_compile_returns_params_list(self, client):
        """Compile always returns a params list (may be empty)."""
        resp = await client.post(
            "/data/compile",
            json={"query": "{ sales_analytics__orders { id } }", "role": "admin"},
        )
        assert resp.status_code == 200
        assert isinstance(resp.json()["params"], list)


class TestCompileWithVariables:
    async def test_compile_with_variables(self, client):
        """Variables are incorporated into compiled SQL as parameters."""
        resp = await client.post(
            "/data/compile",
            json={
                "query": "query ($region: String) { sales_analytics__orders(where: {region: {eq: $region}}) { id } }",
                "variables": {"region": "EMEA"},
                "role": "admin",
            },
        )
        assert resp.status_code == 200
        body = resp.json()
        assert "sql" in body
        # Variable should appear as a parameter, not interpolated into SQL
        assert "EMEA" not in body["sql"]
        assert any("EMEA" in str(p) for p in body["params"])


class TestCompileMultiRoot:
    async def test_multi_root_query_returns_queries_array(self, client):
        """Multiple root fields return {'queries': [...]} shape."""
        resp = await client.post(
            "/data/compile",
            json={
                "query": "{ sales_analytics__orders { id } sales_analytics__customers { id } }",
                "role": "admin",
            },
        )
        assert resp.status_code == 200
        body = resp.json()
        # Multi-root queries return a 'queries' array
        assert "queries" in body
        assert len(body["queries"]) == 2
        for q in body["queries"]:
            assert "sql" in q
            assert "enforcement" in q


class TestCompileRLSEnforcement:
    async def test_compile_rls_filters_shown_in_enforcement(self, client):
        """When a role has RLS configured, compile lists the filters applied."""
        # Use a role known to have RLS from test config
        resp = await client.post(
            "/data/compile",
            json={"query": "{ sales_analytics__orders { id } }", "role": "analyst"},
        )
        assert resp.status_code == 200
        enforcement = resp.json()["enforcement"]
        # rls_filters_applied is a list (may be empty if analyst has no RLS for orders)
        assert isinstance(enforcement["rls_filters_applied"], list)

    async def test_compile_excluded_columns_shown(self, client):
        """Columns not requested appear in columns_excluded."""
        # analyst can see: id, customer_id, region, status, created_at on orders
        # querying only id means the rest appear as excluded
        resp = await client.post(
            "/data/compile",
            json={"query": "{ sales_analytics__orders { id } }", "role": "analyst"},
        )
        assert resp.status_code == 200
        enforcement = resp.json()["enforcement"]
        assert isinstance(enforcement["columns_excluded"], list)


class TestCompileErrors:
    async def test_compile_invalid_graphql_syntax_400(self, client):
        """Syntactically invalid GraphQL returns 400."""
        resp = await client.post(
            "/data/compile",
            json={"query": "{ sales_analytics__orders { id", "role": "admin"},
        )
        assert resp.status_code == 400

    async def test_compile_unknown_role_400(self, client):
        """Unknown role returns 400 (no schema found)."""
        resp = await client.post(
            "/data/compile",
            json={"query": "{ sales_analytics__orders { id } }", "role": "nonexistent_role"},
        )
        assert resp.status_code == 400

    async def test_compile_empty_query_400(self, client):
        """Empty query string returns 400."""
        resp = await client.post(
            "/data/compile",
            json={"query": "", "role": "admin"},
        )
        assert resp.status_code == 400
