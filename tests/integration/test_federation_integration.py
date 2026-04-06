# Copyright (c) 2026 Kenneth Stott
# Canary: b07c8d9e-0f1a-2345-6789-012345678901
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for Apollo Federation v2 subgraph endpoint."""

from __future__ import annotations

import asyncio
import os
from unittest.mock import MagicMock

import pytest
from graphql import (
    GraphQLField,
    GraphQLInt,
    GraphQLList,
    GraphQLNonNull,
    GraphQLObjectType,
    GraphQLSchema,
    GraphQLString,
    build_schema,
)

from provisa.compiler.federation import (
    FEDERATION_LINK_URL,
    build_federation_schema,
    extract_pk_columns,
    generate_federation_sdl,
)
from provisa.api.data.federation import (
    EntityResolutionPlan,
    compile_entity_query,
    group_representations,
    resolve_entities,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


# ---------------------------------------------------------------------------
# Helpers — build a minimal base schema for testing
# ---------------------------------------------------------------------------

def _make_base_schema() -> GraphQLSchema:
    """Build a minimal GraphQL schema with Order and Customer types."""
    order_type = GraphQLObjectType(
        "Order",
        lambda: {
            "id": GraphQLField(GraphQLNonNull(GraphQLInt)),
            "amount": GraphQLField(GraphQLString),
            "region": GraphQLField(GraphQLString),
        },
    )
    customer_type = GraphQLObjectType(
        "Customer",
        lambda: {
            "id": GraphQLField(GraphQLNonNull(GraphQLInt)),
            "name": GraphQLField(GraphQLString),
        },
    )
    query_type = GraphQLObjectType(
        "Query",
        {
            "orders": GraphQLField(GraphQLNonNull(GraphQLList(GraphQLNonNull(order_type)))),
            "customers": GraphQLField(GraphQLNonNull(GraphQLList(GraphQLNonNull(customer_type)))),
        },
    )
    return GraphQLSchema(query=query_type, types=[order_type, customer_type])


def _make_tables_with_type_names() -> list[dict]:
    return [
        {"id": 1, "table_name": "orders", "_type_name": "Order", "primary_key": "id"},
        {"id": 2, "table_name": "customers", "_type_name": "Customer", "primary_key": "id"},
    ]


def _make_pk_columns() -> dict[int, list[str]]:
    return {1: ["id"], 2: ["id"]}


# ---------------------------------------------------------------------------
# Federation compiler tests
# ---------------------------------------------------------------------------

class TestFederationSDL:
    async def test_service_query_returns_sdl(self):
        """generate_federation_sdl returns a non-empty SDL string."""
        base_schema = _make_base_schema()
        sdl = generate_federation_sdl(base_schema, {"Order": "id"})
        assert isinstance(sdl, str)
        assert len(sdl) > 0

    async def test_sdl_contains_key_directives(self):
        """SDL contains @key directive on entity types with PKs."""
        base_schema = _make_base_schema()
        sdl = generate_federation_sdl(
            base_schema,
            {"Order": "id", "Customer": "id"},
        )
        assert '@key(fields: "id")' in sdl
        # Verify both types are annotated
        assert "Order" in sdl
        assert "Customer" in sdl

    async def test_sdl_contains_federation_link(self):
        """Federation SDL header includes the @link extension declaration."""
        base_schema = _make_base_schema()
        sdl = generate_federation_sdl(base_schema, key_directives=None)
        assert FEDERATION_LINK_URL in sdl
        assert "extend schema" in sdl

    async def test_subgraph_schema_valid(self):
        """SDL produced by generate_federation_sdl is parseable as valid GraphQL SDL."""
        base_schema = _make_base_schema()
        sdl = generate_federation_sdl(base_schema, {"Order": "id"})

        # The header 'extend schema' block is not a standalone SDL —
        # extract just the type definitions portion for strict parsing
        type_lines = [
            line for line in sdl.splitlines()
            if not line.startswith("extend schema")
        ]
        type_sdl = "\n".join(type_lines).strip()

        if type_sdl:
            # Should be parseable without errors
            from graphql import parse as gql_parse  # noqa: PLC0415
            doc = gql_parse(type_sdl)
            assert doc is not None

    async def test_build_federation_schema_adds_service_field(self):
        """build_federation_schema wraps base schema with _service and _entities."""
        base_schema = _make_base_schema()
        tables = _make_tables_with_type_names()
        pk_columns = _make_pk_columns()

        fed_schema = build_federation_schema(base_schema, tables, pk_columns)

        query_fields = fed_schema.query_type.fields
        assert "_service" in query_fields
        assert "_entities" in query_fields
        # Original fields preserved
        assert "orders" in query_fields
        assert "customers" in query_fields

    async def test_build_federation_schema_service_resolves_sdl(self):
        """_service field resolver returns the federation SDL string."""
        base_schema = _make_base_schema()
        tables = _make_tables_with_type_names()
        pk_columns = _make_pk_columns()

        fed_schema = build_federation_schema(base_schema, tables, pk_columns)
        service_field = fed_schema.query_type.fields["_service"]

        # Call the resolver
        result = service_field.resolve(None, None)
        assert isinstance(result, dict)
        assert "sdl" in result
        assert "@key" in result["sdl"] or "Order" in result["sdl"]


# ---------------------------------------------------------------------------
# extract_pk_columns tests
# ---------------------------------------------------------------------------

class TestExtractPKColumns:
    async def test_extract_explicit_primary_key(self):
        """Tables with 'primary_key' field use it directly."""
        tables = [
            {"id": 10, "table_name": "t1", "primary_key": "order_id"},
            {"id": 11, "table_name": "t2", "primary_key": ["col_a", "col_b"]},
        ]
        pk_map = extract_pk_columns(tables, column_types={})
        assert pk_map[10] == ["order_id"]
        assert pk_map[11] == ["col_a", "col_b"]

    async def test_extract_fallback_to_id_column(self):
        """Tables without explicit PK fall back to 'id' column."""
        from provisa.compiler.introspect import ColumnMetadata  # noqa: PLC0415

        tables = [{"id": 20, "table_name": "items"}]
        col_meta = {
            20: [
                ColumnMetadata(column_name="id", data_type="integer", is_nullable=False),
                ColumnMetadata(column_name="name", data_type="varchar", is_nullable=True),
            ]
        }
        pk_map = extract_pk_columns(tables, column_types=col_meta)
        assert pk_map[20] == ["id"]

    async def test_extract_fallback_to_first_nonnullable(self):
        """Without 'id', falls back to first non-nullable column."""
        from provisa.compiler.introspect import ColumnMetadata  # noqa: PLC0415

        tables = [{"id": 30, "table_name": "logs"}]
        col_meta = {
            30: [
                ColumnMetadata(column_name="ts", data_type="timestamp", is_nullable=False),
                ColumnMetadata(column_name="msg", data_type="varchar", is_nullable=True),
            ]
        }
        pk_map = extract_pk_columns(tables, column_types=col_meta)
        assert pk_map[30] == ["ts"]


# ---------------------------------------------------------------------------
# Entity resolution tests
# ---------------------------------------------------------------------------

class TestEntityResolution:
    async def test_group_representations_single_type(self):
        """group_representations groups by __typename correctly."""
        representations = [
            {"__typename": "Order", "id": 1},
            {"__typename": "Order", "id": 2},
        ]
        type_to_table = {"Order": 1}
        type_to_keys = {"Order": ["id"]}

        plans = group_representations(representations, type_to_table, type_to_keys)
        assert len(plans) == 1
        plan = plans[0]
        assert plan.type_name == "Order"
        assert plan.table_id == 1
        assert len(plan.key_values) == 2

    async def test_group_representations_multiple_types(self):
        """Representations for different types produce separate plans."""
        representations = [
            {"__typename": "Order", "id": 10},
            {"__typename": "Customer", "id": 99},
            {"__typename": "Order", "id": 11},
        ]
        type_to_table = {"Order": 1, "Customer": 2}
        type_to_keys = {"Order": ["id"], "Customer": ["id"]}

        plans = group_representations(representations, type_to_table, type_to_keys)
        plan_types = {p.type_name for p in plans}
        assert "Order" in plan_types
        assert "Customer" in plan_types

        order_plan = next(p for p in plans if p.type_name == "Order")
        assert len(order_plan.key_values) == 2
        assert order_plan.positions == [0, 2]

    async def test_group_representations_missing_typename_raises(self):
        """Missing __typename raises ValueError."""
        representations = [{"id": 1}]
        with pytest.raises(ValueError, match="__typename"):
            group_representations(representations, {"Order": 1}, {"Order": ["id"]})

    async def test_group_representations_unknown_type_raises(self):
        """Unknown __typename raises ValueError."""
        representations = [{"__typename": "Unknown", "id": 1}]
        with pytest.raises(ValueError, match="Unknown entity type"):
            group_representations(representations, {"Order": 1}, {"Order": ["id"]})

    async def test_compile_entity_query_single_pk(self):
        """Single-column PK generates an IN clause."""
        sql, params = compile_entity_query(
            catalog="pg",
            schema_name="public",
            table_name="orders",
            key_columns=["id"],
            key_values=[{"id": 1}, {"id": 2}, {"id": 3}],
        )
        assert "IN" in sql
        assert "$1" in sql
        assert len(params) == 3
        assert 1 in params

    async def test_compile_entity_query_composite_pk(self):
        """Composite PK generates a tuple IN clause."""
        sql, params = compile_entity_query(
            catalog="pg",
            schema_name="public",
            table_name="order_items",
            key_columns=["order_id", "item_id"],
            key_values=[
                {"order_id": 1, "item_id": 10},
                {"order_id": 2, "item_id": 20},
            ],
        )
        assert "IN" in sql
        assert len(params) == 4  # 2 rows x 2 columns

    async def test_entities_query_resolves_objects(self):
        """resolve_entities returns ordered list matching input representations."""
        async def mock_execute(sql, params, rls_context):
            # Return rows keyed by id
            return [{"id": p, "__data": f"row_{p}"} for p in params]

        representations = [
            {"__typename": "Order", "id": 5},
            {"__typename": "Order", "id": 3},
        ]
        type_to_table = {"Order": 1}
        type_to_keys = {"Order": ["id"]}
        table_meta = {
            1: {"catalog": "pg", "schema_name": "public", "table_name": "orders"}
        }

        results = await resolve_entities(
            representations=representations,
            type_to_table=type_to_table,
            type_to_keys=type_to_keys,
            table_meta=table_meta,
            execute_fn=mock_execute,
        )

        assert len(results) == 2
        assert results[0]["id"] == 5
        assert results[1]["id"] == 3
        for r in results:
            assert r["__typename"] == "Order"


# ---------------------------------------------------------------------------
# Federation endpoint HTTP tests (skip if PG unavailable)
# ---------------------------------------------------------------------------

class TestFederationEndpoint:
    """HTTP-level tests require a running Provisa server.

    These tests are skipped unless PROVISA_TEST_URL is set.
    """

    def _base_url(self) -> str:
        return os.environ.get("PROVISA_TEST_URL", "")

    async def test_federation_endpoint_reachable(self):
        """POST to the federation endpoint returns 200 for a valid query."""
        base_url = self._base_url()
        if not base_url:
            pytest.skip("PROVISA_TEST_URL not set")

        try:
            import httpx  # noqa: PLC0415
        except ImportError:
            pytest.skip("httpx not installed")

        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{base_url}/graphql/federation",
                json={"query": "{ _service { sdl } }"},
                timeout=10.0,
            )
        assert resp.status_code == 200

    async def test_service_query_returns_sdl_via_http(self):
        """{ _service { sdl } } returns a non-empty SDL string over HTTP."""
        base_url = self._base_url()
        if not base_url:
            pytest.skip("PROVISA_TEST_URL not set")

        try:
            import httpx  # noqa: PLC0415
        except ImportError:
            pytest.skip("httpx not installed")

        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{base_url}/graphql/federation",
                json={"query": "{ _service { sdl } }"},
                timeout=10.0,
            )
        data = resp.json()
        assert "data" in data
        assert "_service" in data["data"]
        sdl = data["data"]["_service"]["sdl"]
        assert isinstance(sdl, str)
        assert len(sdl) > 0
