# Copyright (c) 2026 Kenneth Stott
# Canary: 18748b0c-3619-4abb-ae24-28e81c8f6f54
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for Apollo Federation v2 subgraph support (Phase AJ)."""

import pytest

from graphql import (
    GraphQLField,
    GraphQLInt,
    GraphQLList,
    GraphQLNonNull,
    GraphQLObjectType,
    GraphQLSchema,
    GraphQLString,
    graphql_sync,
)

from provisa.compiler.federation import (
    FederationConfig,
    build_federation_schema,
    extract_pk_columns,
    generate_federation_sdl,
)
from provisa.compiler.introspect import ColumnMetadata
from provisa.api.data.federation import (
    compile_entity_query,
    group_representations,
    resolve_entities,
)


# --- Helpers ---


def _make_base_schema(type_names: list[str] | None = None) -> GraphQLSchema:
    """Build a minimal base schema with Orders and Customers types."""
    orders_type = GraphQLObjectType(
        "Orders",
        {
            "id": GraphQLField(GraphQLNonNull(GraphQLInt)),
            "customer_id": GraphQLField(GraphQLNonNull(GraphQLInt)),
            "amount": GraphQLField(GraphQLNonNull(GraphQLInt)),
        },
    )
    customers_type = GraphQLObjectType(
        "Customers",
        {
            "id": GraphQLField(GraphQLNonNull(GraphQLInt)),
            "name": GraphQLField(GraphQLNonNull(GraphQLString)),
        },
    )
    query = GraphQLObjectType(
        "Query",
        {
            "orders": GraphQLField(
                GraphQLList(GraphQLNonNull(orders_type))
            ),
            "customers": GraphQLField(
                GraphQLList(GraphQLNonNull(customers_type))
            ),
        },
    )
    return GraphQLSchema(query=query)


def _make_tables():
    return [
        {"id": 1, "table_name": "orders", "_type_name": "Orders"},
        {"id": 2, "table_name": "customers", "_type_name": "Customers"},
    ]


def _make_pk_columns():
    return {1: ["id"], 2: ["id"]}


# --- FederationConfig ---


class TestFederationConfig:
    def test_defaults(self):
        cfg = FederationConfig()
        assert cfg.enabled is False
        assert cfg.version == 2
        assert cfg.service_name == "provisa"

    def test_disabled_by_default(self):
        """Federation features absent when federation.enabled is false."""
        cfg = FederationConfig()
        assert not cfg.enabled


# --- extract_pk_columns ---


class TestExtractPKColumns:
    def test_explicit_primary_key_string(self):
        tables = [{"id": 1, "primary_key": "order_id"}]
        column_types: dict[int, list[ColumnMetadata]] = {}
        result = extract_pk_columns(tables, column_types)
        assert result == {1: ["order_id"]}

    def test_explicit_primary_key_list(self):
        tables = [{"id": 1, "primary_key": ["col1", "col2"]}]
        result = extract_pk_columns(tables, {})
        assert result == {1: ["col1", "col2"]}

    def test_fallback_to_id_column(self):
        tables = [{"id": 1}]
        column_types = {
            1: [
                ColumnMetadata("name", "varchar", True),
                ColumnMetadata("id", "integer", False),
            ]
        }
        result = extract_pk_columns(tables, column_types)
        assert result == {1: ["id"]}

    def test_fallback_to_first_non_nullable(self):
        tables = [{"id": 1}]
        column_types = {
            1: [
                ColumnMetadata("order_num", "integer", False),
                ColumnMetadata("name", "varchar", True),
            ]
        }
        result = extract_pk_columns(tables, column_types)
        assert result == {1: ["order_num"]}


# --- build_federation_schema ---


class TestBuildFederationSchema:
    def test_adds_service_field(self):
        schema = _make_base_schema()
        tables = _make_tables()
        pk = _make_pk_columns()
        fed = build_federation_schema(schema, tables, pk)
        assert "_service" in fed.query_type.fields

    def test_adds_entities_field(self):
        schema = _make_base_schema()
        tables = _make_tables()
        pk = _make_pk_columns()
        fed = build_federation_schema(schema, tables, pk)
        assert "_entities" in fed.query_type.fields

    def test_service_returns_sdl(self):
        schema = _make_base_schema()
        tables = _make_tables()
        pk = _make_pk_columns()
        fed = build_federation_schema(schema, tables, pk)
        result = graphql_sync(fed, "{ _service { sdl } }")
        assert result.errors is None
        sdl = result.data["_service"]["sdl"]
        assert "@key" in sdl
        assert "extend schema @link" in sdl

    def test_key_directive_on_entity_types(self):
        schema = _make_base_schema()
        tables = _make_tables()
        pk = _make_pk_columns()
        fed = build_federation_schema(schema, tables, pk)
        result = graphql_sync(fed, "{ _service { sdl } }")
        sdl = result.data["_service"]["sdl"]
        assert '@key(fields: "id")' in sdl

    def test_composite_pk_key_directive(self):
        """Composite PK generates @key(fields: "col1 col2")."""
        schema = _make_base_schema()
        tables = [
            {"id": 1, "table_name": "orders", "_type_name": "Orders"},
        ]
        pk = {1: ["region", "order_id"]}
        fed = build_federation_schema(schema, tables, pk)
        result = graphql_sync(fed, "{ _service { sdl } }")
        sdl = result.data["_service"]["sdl"]
        assert '@key(fields: "region order_id")' in sdl

    def test_entity_union_contains_types(self):
        schema = _make_base_schema()
        tables = _make_tables()
        pk = _make_pk_columns()
        fed = build_federation_schema(schema, tables, pk)
        entity_type = fed.type_map.get("_Entity")
        assert entity_type is not None

    def test_any_scalar_exists(self):
        schema = _make_base_schema()
        tables = _make_tables()
        pk = _make_pk_columns()
        fed = build_federation_schema(schema, tables, pk)
        assert "_Any" in fed.type_map

    def test_preserves_existing_query_fields(self):
        schema = _make_base_schema()
        tables = _make_tables()
        pk = _make_pk_columns()
        fed = build_federation_schema(schema, tables, pk)
        assert "orders" in fed.query_type.fields
        assert "customers" in fed.query_type.fields

    def test_preserves_mutation_type(self):
        mut_type = GraphQLObjectType(
            "Mutation",
            {"noop": GraphQLField(GraphQLString)},
        )
        orders_type = GraphQLObjectType(
            "Orders",
            {"id": GraphQLField(GraphQLNonNull(GraphQLInt))},
        )
        query = GraphQLObjectType(
            "Query",
            {"orders": GraphQLField(GraphQLList(GraphQLNonNull(orders_type)))},
        )
        schema = GraphQLSchema(query=query, mutation=mut_type)
        tables = [{"id": 1, "table_name": "orders", "_type_name": "Orders"}]
        pk = {1: ["id"]}
        fed = build_federation_schema(schema, tables, pk)
        assert fed.mutation_type is not None


# --- Federation disabled by default ---


class TestFederationDisabled:
    def test_no_federation_fields_in_base_schema(self):
        """When federation is not applied, no _service or _entities fields."""
        schema = _make_base_schema()
        assert "_service" not in schema.query_type.fields
        assert "_entities" not in schema.query_type.fields


# --- generate_federation_sdl ---


class TestGenerateFederationSDL:
    def test_includes_link_directive(self):
        schema = _make_base_schema()
        sdl = generate_federation_sdl(schema, {"Orders": "id"})
        assert "extend schema @link" in sdl
        assert "federation/v2.3" in sdl

    def test_includes_key_directive(self):
        schema = _make_base_schema()
        sdl = generate_federation_sdl(schema, {"Orders": "id"})
        assert '@key(fields: "id")' in sdl

    def test_no_key_directives_when_none(self):
        schema = _make_base_schema()
        sdl = generate_federation_sdl(schema, None)
        assert "@key(fields:" not in sdl


# --- group_representations ---


class TestGroupRepresentations:
    def test_groups_by_type(self):
        reps = [
            {"__typename": "Orders", "id": 1},
            {"__typename": "Customers", "id": 10},
            {"__typename": "Orders", "id": 2},
        ]
        type_to_table = {"Orders": 1, "Customers": 2}
        type_to_keys = {"Orders": ["id"], "Customers": ["id"]}
        plans = group_representations(reps, type_to_table, type_to_keys)
        assert len(plans) == 2
        orders_plan = next(p for p in plans if p.type_name == "Orders")
        assert len(orders_plan.key_values) == 2
        assert orders_plan.positions == [0, 2]

    def test_missing_typename_raises(self):
        with pytest.raises(ValueError, match="missing __typename"):
            group_representations(
                [{"id": 1}], {"Orders": 1}, {"Orders": ["id"]}
            )

    def test_unknown_type_raises(self):
        with pytest.raises(ValueError, match="Unknown entity type"):
            group_representations(
                [{"__typename": "Unknown", "id": 1}],
                {"Orders": 1},
                {"Orders": ["id"]},
            )

    def test_missing_key_field_raises(self):
        with pytest.raises(ValueError, match="missing key field"):
            group_representations(
                [{"__typename": "Orders"}],
                {"Orders": 1},
                {"Orders": ["id"]},
            )


# --- compile_entity_query ---


class TestCompileEntityQuery:
    def test_single_pk(self):
        sql, params = compile_entity_query(
            "pg", "public", "orders", ["id"], [{"id": 1}, {"id": 2}]
        )
        assert '"id" IN ($1, $2)' in sql
        assert params == [1, 2]
        assert '"pg"."public"."orders"' in sql

    def test_composite_pk(self):
        sql, params = compile_entity_query(
            "pg",
            "public",
            "orders",
            ["region", "order_id"],
            [{"region": "us", "order_id": 1}],
        )
        assert '("region", "order_id") IN' in sql
        assert params == ["us", 1]


# --- resolve_entities ---


class TestResolveEntities:
    @pytest.mark.asyncio
    async def test_resolves_by_pk(self):
        reps = [{"__typename": "Orders", "id": 1}]
        type_to_table = {"Orders": 1}
        type_to_keys = {"Orders": ["id"]}
        table_meta = {
            1: {"catalog": "pg", "schema_name": "public", "table_name": "orders"}
        }

        async def mock_execute(sql, params, rls_ctx):
            return [{"id": 1, "amount": 100}]

        results = await resolve_entities(
            reps, type_to_table, type_to_keys, table_meta, mock_execute
        )
        assert len(results) == 1
        assert results[0]["id"] == 1
        assert results[0]["__typename"] == "Orders"

    @pytest.mark.asyncio
    async def test_batch_resolution_groups_by_type(self):
        """Multiple representations resolved with single query per type."""
        reps = [
            {"__typename": "Orders", "id": 1},
            {"__typename": "Customers", "id": 10},
            {"__typename": "Orders", "id": 2},
        ]
        type_to_table = {"Orders": 1, "Customers": 2}
        type_to_keys = {"Orders": ["id"], "Customers": ["id"]}
        table_meta = {
            1: {"catalog": "pg", "schema_name": "public", "table_name": "orders"},
            2: {"catalog": "pg", "schema_name": "public", "table_name": "customers"},
        }

        call_count = {"n": 0}

        async def mock_execute(sql, params, rls_ctx):
            call_count["n"] += 1
            if "orders" in sql:
                return [
                    {"id": 1, "amount": 100},
                    {"id": 2, "amount": 200},
                ]
            return [{"id": 10, "name": "Alice"}]

        results = await resolve_entities(
            reps, type_to_table, type_to_keys, table_meta, mock_execute
        )
        # Should be 2 queries: one for Orders, one for Customers
        assert call_count["n"] == 2
        assert results[0]["id"] == 1
        assert results[1]["id"] == 10
        assert results[2]["id"] == 2

    @pytest.mark.asyncio
    async def test_rls_context_passed_to_executor(self):
        """RLS context is forwarded to the execution function."""
        reps = [{"__typename": "Orders", "id": 1}]
        type_to_table = {"Orders": 1}
        type_to_keys = {"Orders": ["id"]}
        table_meta = {
            1: {"catalog": "pg", "schema_name": "public", "table_name": "orders"}
        }

        captured_rls = {}

        async def mock_execute(sql, params, rls_ctx):
            captured_rls["ctx"] = rls_ctx
            return [{"id": 1, "amount": 50}]

        rls_sentinel = {"role": "analyst", "filter": "region = 'us'"}
        await resolve_entities(
            reps,
            type_to_table,
            type_to_keys,
            table_meta,
            mock_execute,
            rls_context=rls_sentinel,
        )
        assert captured_rls["ctx"] is rls_sentinel

    @pytest.mark.asyncio
    async def test_unresolved_returns_none(self):
        reps = [{"__typename": "Orders", "id": 999}]
        type_to_table = {"Orders": 1}
        type_to_keys = {"Orders": ["id"]}
        table_meta = {
            1: {"catalog": "pg", "schema_name": "public", "table_name": "orders"}
        }

        async def mock_execute(sql, params, rls_ctx):
            return []  # no rows found

        results = await resolve_entities(
            reps, type_to_table, type_to_keys, table_meta, mock_execute
        )
        assert results == [None]
