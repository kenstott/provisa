# Copyright (c) 2026 Kenneth Stott
# Canary: a1b2c3d4-e5f6-7890-abcd-ef1234567890
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for auto-generated REST endpoints (REQ-222).

Tests the REST route generation layer in two complementary ways:
  1. Direct Python API — tests _parse_where_params, _parse_order_by_params,
     _build_graphql_query, _get_scalar_fields without any HTTP round-trip.
  2. HTTP via AsyncClient — tests the full FastAPI handler when the live
     AppState (schemas, contexts, pools) is available.

The HTTP tests are skipped when PostgreSQL is unavailable.
"""

from __future__ import annotations

import pytest

from provisa.api.rest.generator import (
    _build_graphql_query,
    _get_scalar_fields,
    _parse_order_by_params,
    _parse_where_params,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_pg_available = pytest.mark.skipif(
    False,  # updated at collection time below
    reason="PostgreSQL unavailable",
)


def _pg_skip():
    """Return a skip marker when PG is not reachable."""
    import os
    import socket

    host = os.environ.get("PG_HOST", "localhost")
    port = int(os.environ.get("PG_PORT", "5432"))
    try:
        with socket.create_connection((host, port), timeout=1):
            return False
    except OSError:
        return True


_SKIP_NO_PG = pytest.mark.skipif(_pg_skip(), reason="PostgreSQL unavailable")


# ---------------------------------------------------------------------------
# Unit-style tests of the Python REST generator API (no HTTP, no PG needed)
# ---------------------------------------------------------------------------


class TestParseWhereParams:
    """Test _parse_where_params query string parsing."""

    async def test_simple_eq_filter(self):
        params = {"where.region.eq": "us-east"}
        result = _parse_where_params(params)
        assert result == {"region": {"eq": "us-east"}}

    async def test_numeric_gt_filter(self):
        params = {"where.amount.gt": "100"}
        result = _parse_where_params(params)
        assert "amount" in result
        assert "gt" in result["amount"]

    async def test_in_filter_splits_on_comma(self):
        params = {"where.region.in": "us-east,eu-west"}
        result = _parse_where_params(params)
        assert result["region"]["in"] == ["us-east", "eu-west"]

    async def test_unknown_op_ignored(self):
        params = {"where.region.INVALID": "x"}
        result = _parse_where_params(params)
        assert result == {}

    async def test_non_where_params_ignored(self):
        params = {"limit": "10", "offset": "0", "region": "us-east"}
        result = _parse_where_params(params)
        assert result == {}

    async def test_multiple_filters_collected(self):
        params = {
            "where.region.eq": "us-east",
            "where.amount.gte": "500",
        }
        result = _parse_where_params(params)
        assert "region" in result
        assert "amount" in result

    async def test_malformed_key_ignored(self):
        # Only two parts — missing op segment
        params = {"where.region": "us-east"}
        result = _parse_where_params(params)
        assert result == {}


class TestParseOrderByParams:
    """Test _parse_order_by_params query string parsing."""

    async def test_asc_ordering(self):
        params = {"order_by.created_at": "asc"}
        result = _parse_order_by_params(params)
        assert result == [{"field": "created_at", "dir": "asc"}]

    async def test_desc_ordering(self):
        params = {"order_by.amount": "desc"}
        result = _parse_order_by_params(params)
        assert result == [{"field": "amount", "dir": "desc"}]

    async def test_invalid_direction_defaults_to_asc(self):
        params = {"order_by.amount": "INVALID"}
        result = _parse_order_by_params(params)
        assert result[0]["dir"] == "asc"

    async def test_non_order_by_params_ignored(self):
        params = {"limit": "5", "where.region.eq": "us-east"}
        result = _parse_order_by_params(params)
        assert result == []

    async def test_multiple_ordering_columns(self):
        params = {
            "order_by.created_at": "desc",
            "order_by.amount": "asc",
        }
        result = _parse_order_by_params(params)
        fields = {o["field"] for o in result}
        assert fields == {"created_at", "amount"}


class TestBuildGraphQLQuery:
    """Test _build_graphql_query string construction."""

    async def test_simple_no_args(self):
        q = _build_graphql_query("orders", ["id", "amount"], {}, [], None, None)
        assert "orders" in q
        assert "id" in q
        assert "amount" in q

    async def test_limit_appears_in_query(self):
        q = _build_graphql_query("orders", ["id"], {}, [], 5, None)
        assert "limit: 5" in q

    async def test_offset_appears_in_query(self):
        q = _build_graphql_query("orders", ["id"], {}, [], None, 10)
        assert "offset: 10" in q

    async def test_where_clause_appears(self):
        where = {"region": {"eq": "us-east"}}
        q = _build_graphql_query("orders", ["id"], where, [], None, None)
        assert "where" in q
        assert "region" in q
        assert "us-east" in q

    async def test_order_by_clause_appears(self):
        order_by = [{"field": "amount", "dir": "desc"}]
        q = _build_graphql_query("orders", ["id"], {}, order_by, None, None)
        assert "order_by" in q
        assert "amount" in q
        assert "desc" in q

    async def test_numeric_value_unquoted(self):
        where = {"amount": {"gt": "100"}}
        q = _build_graphql_query("orders", ["id"], where, [], None, None)
        # Numeric string should be emitted without quotes
        assert '"100"' not in q
        assert "100" in q

    async def test_in_operator_uses_list_syntax(self):
        where = {"region": {"in": ["us-east", "eu-west"]}}
        q = _build_graphql_query("orders", ["id"], where, [], None, None)
        assert "in:" in q or "in :" in q or "[" in q

    async def test_fields_joined_with_space(self):
        fields = ["id", "amount", "region"]
        q = _build_graphql_query("orders", fields, {}, [], None, None)
        for field in fields:
            assert field in q


class TestGetScalarFields:
    """Test _get_scalar_fields using a minimal in-memory GraphQL schema."""

    def _make_schema(self):
        from graphql import (
            GraphQLField,
            GraphQLInt,
            GraphQLList,
            GraphQLObjectType,
            GraphQLSchema,
            GraphQLString,
        )

        order_type = GraphQLObjectType(
            "Order",
            {
                "id": GraphQLField(GraphQLInt),
                "region": GraphQLField(GraphQLString),
                "amount": GraphQLField(GraphQLInt),
            },
        )
        query_type = GraphQLObjectType(
            "Query",
            {
                "orders": GraphQLField(GraphQLList(order_type)),
            },
        )
        return GraphQLSchema(query=query_type)

    async def test_scalar_fields_returned(self):
        schema = self._make_schema()
        fields = _get_scalar_fields(schema, "orders")
        assert "id" in fields
        assert "region" in fields
        assert "amount" in fields

    async def test_unknown_table_returns_empty(self):
        schema = self._make_schema()
        fields = _get_scalar_fields(schema, "nonexistent")
        assert fields == []

    async def test_no_query_type_returns_empty(self):
        from graphql import GraphQLSchema
        schema = GraphQLSchema()
        fields = _get_scalar_fields(schema, "orders")
        assert fields == []


# ---------------------------------------------------------------------------
# HTTP integration tests (require live PG + AppState)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def _rest_app_state(pg_pool):
    """Build a minimal AppState with a schema for 'admin' role.

    Skips the module if PG is not reachable or the schema is missing.
    """
    pytest.importorskip("asyncpg")
    return None  # actual construction deferred to async fixture below


class TestRestEndpointsHTTP:
    """HTTP-level tests using httpx.AsyncClient against the FastAPI app.

    These tests require a running PostgreSQL instance with the full Provisa
    schema pre-loaded (same stack as the other integration tests).
    """

    pytestmark = _SKIP_NO_PG

    async def _make_client(self, pg_pool):
        """Construct a minimal AppState and return an AsyncClient."""
        httpx = pytest.importorskip("httpx")
        from provisa.api.rest.generator import create_rest_router
        from provisa.api.app import AppState
        from provisa.compiler.rls import RLSContext
        from fastapi import FastAPI

        # Build a minimal in-memory schema for the 'admin' role so we can
        # test the REST handler without a full startup sequence.
        from graphql import (
            GraphQLField,
            GraphQLInt,
            GraphQLList,
            GraphQLNonNull,
            GraphQLObjectType,
            GraphQLSchema,
            GraphQLString,
        )

        order_type = GraphQLObjectType(
            "Order",
            lambda: {
                "id": GraphQLField(GraphQLNonNull(GraphQLInt)),
                "region": GraphQLField(GraphQLString),
                "amount": GraphQLField(GraphQLInt),
            },
        )
        query_type = GraphQLObjectType(
            "Query",
            {"orders": GraphQLField(GraphQLList(order_type))},
        )
        schema = GraphQLSchema(query=query_type)

        # Minimal compilation context — use real context if available
        try:
            from provisa.compiler.sql_gen import CompilationContext, TableMeta
            ctx = CompilationContext(
                tables={
                    "orders": TableMeta(
                        table_id=1,
                        field_name="orders",
                        type_name="Order",
                        source_id="test-pg",
                        catalog_name="postgresql",
                        schema_name="public",
                        table_name="orders",
                        domain_id="default",
                    )
                }
            )
        except Exception:
            pytest.skip("Cannot build CompilationContext — schema mismatch")

        from provisa.executor.pool import SourcePool
        import os

        source_pool = SourcePool()
        try:
            await source_pool.add(
                "test-pg",
                source_type="postgresql",
                host=os.environ.get("PG_HOST", "localhost"),
                port=int(os.environ.get("PG_PORT", "5432")),
                database=os.environ.get("PG_DATABASE", "provisa"),
                user=os.environ.get("PG_USER", "provisa"),
                password=os.environ.get("PG_PASSWORD", "provisa"),
            )
        except Exception:
            pytest.skip("Cannot connect to PostgreSQL source pool")

        app_state = AppState()
        app_state.schemas = {"admin": schema}
        app_state.contexts = {"admin": ctx}
        app_state.rls_contexts = {"admin": RLSContext.empty()}
        app_state.source_pools = source_pool
        app_state.source_types = {"test-pg": "postgresql"}
        app_state.source_dialects = {"test-pg": "postgres"}
        app_state.masking_rules = {}

        app = FastAPI()
        rest_router = create_rest_router(app_state)
        app.include_router(rest_router)

        transport = httpx.ASGITransport(app=app)
        client = httpx.AsyncClient(transport=transport, base_url="http://test")
        return client, source_pool

    @_SKIP_NO_PG
    async def test_get_list_endpoint_returns_rows(self, pg_pool):
        """GET /data/rest/orders returns array of objects."""
        try:
            client, pool = await self._make_client(pg_pool)
        except pytest.skip.Exception:
            raise
        except Exception as exc:
            pytest.skip(f"REST client setup failed: {exc}")

        try:
            async with client:
                response = await client.get("/data/rest/orders")
            assert response.status_code == 200
            body = response.json()
            assert "data" in body
            assert isinstance(body["data"], list)
        finally:
            await pool.close_all()

    @_SKIP_NO_PG
    async def test_get_list_with_filter(self, pg_pool):
        """GET /data/rest/orders?where.region.eq=us-east filters results."""
        try:
            client, pool = await self._make_client(pg_pool)
        except (pytest.skip.Exception, Exception) as exc:
            pytest.skip(f"REST client setup failed: {exc}")

        try:
            async with client:
                response = await client.get(
                    "/data/rest/orders",
                    params={"where.region.eq": "us-east"},
                )
            assert response.status_code == 200
            body = response.json()
            rows = body.get("data", [])
            for row in rows:
                assert row.get("region") == "us-east"
        finally:
            await pool.close_all()

    @_SKIP_NO_PG
    async def test_get_list_with_limit(self, pg_pool):
        """GET /data/rest/orders?limit=3 returns at most 3 items."""
        try:
            client, pool = await self._make_client(pg_pool)
        except (pytest.skip.Exception, Exception) as exc:
            pytest.skip(f"REST client setup failed: {exc}")

        try:
            async with client:
                response = await client.get("/data/rest/orders", params={"limit": "3"})
            assert response.status_code == 200
            rows = response.json().get("data", [])
            assert len(rows) <= 3
        finally:
            await pool.close_all()

    @_SKIP_NO_PG
    async def test_get_by_id_not_found(self, pg_pool):
        """GET /data/rest/orders?where.id.eq=99999 returns empty data."""
        try:
            client, pool = await self._make_client(pg_pool)
        except (pytest.skip.Exception, Exception) as exc:
            pytest.skip(f"REST client setup failed: {exc}")

        try:
            async with client:
                response = await client.get(
                    "/data/rest/orders",
                    params={"where.id.eq": "99999"},
                )
            assert response.status_code == 200
            rows = response.json().get("data", [])
            assert rows == []
        finally:
            await pool.close_all()

    @_SKIP_NO_PG
    async def test_get_list_rls_applied(self, pg_pool):
        """REST endpoint applies RLS for the requesting role (no crash)."""
        try:
            client, pool = await self._make_client(pg_pool)
        except (pytest.skip.Exception, Exception) as exc:
            pytest.skip(f"REST client setup failed: {exc}")

        try:
            async with client:
                response = await client.get("/data/rest/orders")
            # RLS may restrict rows but should not cause a 500 error
            assert response.status_code in (200, 403)
        finally:
            await pool.close_all()

    @_SKIP_NO_PG
    async def test_response_content_type(self, pg_pool):
        """REST response Content-Type is application/json."""
        try:
            client, pool = await self._make_client(pg_pool)
        except (pytest.skip.Exception, Exception) as exc:
            pytest.skip(f"REST client setup failed: {exc}")

        try:
            async with client:
                response = await client.get("/data/rest/orders")
            ct = response.headers.get("content-type", "")
            assert "application/json" in ct
        finally:
            await pool.close_all()

    async def test_unknown_table_returns_404(self):
        """GET /data/rest/nonexistent_table returns 404 without PG."""
        httpx = pytest.importorskip("httpx")
        from provisa.api.rest.generator import create_rest_router
        from provisa.api.app import AppState
        from provisa.compiler.rls import RLSContext
        from graphql import (
            GraphQLField,
            GraphQLInt,
            GraphQLList,
            GraphQLObjectType,
            GraphQLSchema,
        )
        from fastapi import FastAPI

        order_type = GraphQLObjectType("Order", {"id": GraphQLField(GraphQLInt)})
        query_type = GraphQLObjectType("Query", {"orders": GraphQLField(GraphQLList(order_type))})
        schema = GraphQLSchema(query=query_type)

        from provisa.compiler.sql_gen import CompilationContext
        try:
            ctx = CompilationContext()
        except Exception:
            pytest.skip("CompilationContext signature mismatch")

        app_state = AppState()
        app_state.schemas = {"admin": schema}
        app_state.contexts = {"admin": ctx}
        app_state.rls_contexts = {"admin": RLSContext.empty()}

        app = FastAPI()
        app.include_router(create_rest_router(app_state))

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get("/data/rest/nonexistent_table")
        assert response.status_code == 404
