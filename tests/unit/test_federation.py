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
    build_federation_schema,
    generate_federation_sdl,
)
from provisa.core.models import FederationConfig, ProvisaConfig, Role


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
            "orders": GraphQLField(GraphQLList(GraphQLNonNull(orders_type))),
            "customers": GraphQLField(GraphQLList(GraphQLNonNull(customers_type))),
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
        assert cfg.subgraph_name == "provisa"
        assert cfg.role is None

    def test_disabled_by_default(self):
        """Federation features absent when federation.enabled is false."""
        cfg = FederationConfig()
        assert not cfg.enabled

    def test_enabled_names_its_role(self):
        """A subgraph is an endpoint with a declared role: enabling it without one is refused."""
        with pytest.raises(ValueError, match="no role"):
            FederationConfig(enabled=True)

    def test_role_is_one_of_the_config(self):
        role = Role(id="partner", description="d", capabilities=[], domain_access=["*"])
        ok = ProvisaConfig(
            sources=[],
            domains=[],
            tables=[],
            roles=[role],
            federation=FederationConfig(enabled=True, role="partner"),
        )
        assert ok.federation.role == "partner"
        with pytest.raises(ValueError, match="not a role of this configuration"):
            ProvisaConfig(
                sources=[],
                domains=[],
                tables=[],
                roles=[role],
                federation=FederationConfig(enabled=True, role="nobody"),
            )


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


# --- Operation directives ---


def test_the_subgraph_sdl_leaves_out_provisas_operation_directives():
    """A router composes the subgraph SDL into its supergraph, where @join is the federation join
    spec's own name: a subgraph that defined Provisa's @join (an operation directive) failed
    composition with a syntax error. A router passes no client's operation directive on, so the
    subgraph publishes none; a directive on the schema is kept."""
    from graphql import DirectiveLocation, GraphQLArgument, GraphQLDirective

    base = _make_base_schema()
    join = GraphQLDirective(
        "join",
        [DirectiveLocation.QUERY, DirectiveLocation.SUBSCRIPTION],
        {"strategy": GraphQLArgument(GraphQLNonNull(GraphQLString))},
    )
    watermark = GraphQLDirective("watermark", [DirectiveLocation.FIELD])
    on_schema = GraphQLDirective("label", [DirectiveLocation.FIELD_DEFINITION])
    schema = GraphQLSchema(
        **{**base.to_kwargs(), "directives": [*base.directives, join, watermark, on_schema]}
    )
    sdl = generate_federation_sdl(schema, {"Orders": "id"})
    assert "directive @join" not in sdl and "directive @watermark" not in sdl
    assert "directive @label on FIELD_DEFINITION" in sdl
    assert 'type Orders @key(fields: "id") {' in sdl
