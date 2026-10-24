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

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

_COMPILE_MUTATION = """
mutation CompileQuery($input: CompileQueryInput!) {
  compileQuery(input: $input) {
    sql semanticSql engineSql directSql route routeReason sources
    rootField canonicalField compiledCypher optimizations warnings
    columnAliases { fieldName column }
    enforcement {
      rlsFiltersApplied columnsExcluded schemaScope maskingApplied ceilingApplied route
    }
  }
}
"""


async def _compile(
    client: AsyncClient, query: str, role: str = "org_admin", variables: dict | None = None
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


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def client():
    os.environ.setdefault("PG_PASSWORD", "provisa")

    from provisa.api.app import create_app

    # Point the app at the fixture config so _load_and_build uses it instead of
    # the production config (which has a different analyst role domain_access).
    _prev_config = os.environ.get("PROVISA_CONFIG")
    os.environ["PROVISA_CONFIG"] = str(_FIXTURE_CONFIG)

    app = create_app()

    try:
        async with app.router.lifespan_context(app):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as c:
                yield c
    finally:
        if _prev_config is None:
            os.environ.pop("PROVISA_CONFIG", None)
        else:
            os.environ["PROVISA_CONFIG"] = _prev_config


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


class TestCompileSparqlExport:
    async def test_with_no_public_address_the_compile_answers_with_the_reason_it_has_no_sparql(
        self, client, monkeypatch
    ):
        """REQ-2136: the SPARQL export is one more view of the compiled read. With no
        server.public_url its IRIs have no base, so it has none, and says why; the SQL and the
        Cypher are answered as before."""
        from provisa.core import settings_registry

        value = settings_registry.value
        monkeypatch.setattr(
            settings_registry,
            "value",
            lambda key, *a, **k: None if key == "server.public_url" else value(key, *a, **k),
        )
        query = _COMPILE_MUTATION.replace(
            "compiledCypher", "compiledCypher compiledSparql sparqlError"
        )
        resp = await client.post(
            "/admin/graphql",
            json={
                "query": query,
                "variables": {
                    "input": {"query": "{ sa__orders { id amount } }", "role": "org_admin"}
                },
            },
        )
        assert resp.status_code == 200
        body = resp.json()
        assert "errors" not in body, body.get("errors")
        result = body["data"]["compileQuery"][0]
        assert result["sql"] and result["compiledCypher"]
        assert result["compiledSparql"] is None
        assert "public_url" in result["sparqlError"]


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


async def _compile_cypher(client, query: str, flat_cypher: bool, role: str = "org_admin"):
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

    Uses the in-process ``client`` (sample_config) so the sa__orders → customer relationship is
    always present. The shared live server is started by whichever requires_provisa_server test
    runs first and may not carry this fixture config, so live_client would be order-dependent.
    """

    async def test_flat_cypher_no_bare_node_alias(self, client):
        """flatCypher=True must not emit 'b AS assignment' — each field must be explicit."""
        resp = await _compile_cypher(
            client,
            "{ sa__orders { amount customer { name } } }",
            flat_cypher=True,
        )
        assert resp.status_code == 200
        body = resp.json()
        assert "errors" not in body, body.get("errors")
        cypher = body["data"]["compileQuery"][0]["compiledCypher"]
        assert cypher, "compiledCypher must be produced for flatCypher query"
        import re

        bare = re.search(r"\b[a-z]\s+AS\s+\w+\b", cypher)
        assert bare is None, f"RETURN must not use bare node alias: {cypher}"

    async def test_flat_cypher_contains_per_field_paths(self, client):
        """flatCypher=True must expand joined fields as node.prop AS label__prop."""
        resp = await _compile_cypher(
            client,
            "{ sa__orders { amount customer { name } } }",
            flat_cypher=True,
        )
        assert resp.status_code == 200
        body = resp.json()
        assert "errors" not in body, body.get("errors")
        cypher = body["data"]["compileQuery"][0]["compiledCypher"]
        assert cypher, "compiledCypher must be produced for flatCypher query"
        assert "RETURN" in cypher
        # Relationship fields must be expanded with per-field dotted paths
        assert "." in cypher.split("RETURN")[-1], f"RETURN must use dotted paths: {cypher}"
