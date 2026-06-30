# Copyright (c) 2026 Kenneth Stott
# Canary: a9b3c7d1-e2f4-5678-9abc-def012345678
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Unit tests for REST OpenAPI 3.1 spec generator (REQ-804)."""

from __future__ import annotations

from types import SimpleNamespace

from graphql import (
    GraphQLField,
    GraphQLFloat,
    GraphQLInt,
    GraphQLList,
    GraphQLObjectType,
    GraphQLSchema,
    GraphQLString,
)

from provisa.api.rest.openapi_spec import generate_rest_openapi_spec


def _make_state(role_id: str = "admin") -> SimpleNamespace:
    order_type = GraphQLObjectType(
        "Order",
        lambda: {
            "id": GraphQLField(GraphQLInt),  # type: ignore[arg-type]
            "region": GraphQLField(GraphQLString),  # type: ignore[arg-type]
            "amount": GraphQLField(GraphQLFloat),  # type: ignore[arg-type]
        },
    )
    query_type = GraphQLObjectType(
        "Query",
        {"orders": GraphQLField(GraphQLList(order_type))},  # type: ignore[arg-type]
    )
    schema = GraphQLSchema(query=query_type)  # type: ignore[arg-type]
    path_map = {
        "orders": {
            "schema_name": "public",
            "table_name": "orders",
            "domain_id": "default",
            "table_description": "Sales orders",
            "domain_description": "Default domain",
        }
    }
    state = SimpleNamespace(
        schemas={role_id: schema},
        table_path_maps={role_id: path_map},
    )
    return state


class TestEmptySpec:
    def test_unknown_role_returns_empty_spec(self):
        state = SimpleNamespace(schemas={}, table_path_maps={})
        spec = generate_rest_openapi_spec(state, "nobody")
        assert spec["openapi"] == "3.1.0"
        assert spec["paths"] == {}

    def test_no_query_type_returns_empty_spec(self):
        schema = GraphQLSchema()
        state = SimpleNamespace(schemas={"r": schema}, table_path_maps={"r": {}})
        spec = generate_rest_openapi_spec(state, "r")
        assert spec["paths"] == {}


class TestSpecStructure:
    def test_openapi_version(self):
        state = _make_state()
        spec = generate_rest_openapi_spec(state, "admin")
        assert spec["openapi"] == "3.1.0"

    def test_paths_include_table(self):
        state = _make_state()
        spec = generate_rest_openapi_spec(state, "admin")
        assert "/default/orders" in spec["paths"]

    def test_get_operation_present(self):
        state = _make_state()
        spec = generate_rest_openapi_spec(state, "admin")
        op = spec["paths"]["/default/orders"]["get"]
        assert op["operationId"] == "get_orders"

    def test_tags_reflect_domain(self):
        state = _make_state()
        spec = generate_rest_openapi_spec(state, "admin")
        op = spec["paths"]["/default/orders"]["get"]
        assert "default" in op["tags"]

    def test_top_level_tags_list(self):
        state = _make_state()
        spec = generate_rest_openapi_spec(state, "admin")
        tag_names = {t["name"] for t in spec.get("tags", [])}
        assert "default" in tag_names


class TestParameters:
    def _params(self) -> list[dict]:
        state = _make_state()
        spec = generate_rest_openapi_spec(state, "admin")
        return spec["paths"]["/default/orders"]["get"]["parameters"]

    def test_limit_param_present(self):
        names = {p["name"] for p in self._params()}
        assert "limit" in names

    def test_offset_param_present(self):
        names = {p["name"] for p in self._params()}
        assert "offset" in names

    def test_fields_param_present(self):
        names = {p["name"] for p in self._params()}
        assert "fields" in names

    def test_filter_param_present(self):
        names = {p["name"] for p in self._params()}
        assert "filter" in names

    def test_order_by_param_present(self):
        names = {p["name"] for p in self._params()}
        assert "orderBy" in names

    def test_limit_has_minimum(self):
        params = {p["name"]: p for p in self._params()}
        assert params["limit"]["schema"]["minimum"] == 1

    def test_offset_has_minimum(self):
        params = {p["name"]: p for p in self._params()}
        assert params["offset"]["schema"]["minimum"] == 0


class TestComponentSchemas:
    def _components(self) -> dict:
        state = _make_state()
        spec = generate_rest_openapi_spec(state, "admin")
        return spec["components"]["schemas"]

    def test_row_schema_registered(self):
        assert "Order" in self._components()

    def test_row_schema_has_columns(self):
        schema = self._components()["Order"]
        assert "id" in schema["properties"]
        assert "region" in schema["properties"]

    def test_comparator_schema_registered(self):
        assert "Comparator" in self._components()

    def test_direction_schema_registered(self):
        assert "Direction" in self._components()

    def test_filter_type_registered(self):
        assert "OrderFilter" in self._components()

    def test_order_by_type_registered(self):
        assert "OrderOrderBy" in self._components()


class TestDomainFilter:
    def test_domain_filter_restricts_paths(self):
        order_type = GraphQLObjectType("Order", lambda: {"id": GraphQLField(GraphQLInt)})  # type: ignore[arg-type]
        product_type = GraphQLObjectType("Product", lambda: {"sku": GraphQLField(GraphQLString)})  # type: ignore[arg-type]
        query_type = GraphQLObjectType(
            "Query",
            {
                "orders": GraphQLField(GraphQLList(order_type)),  # type: ignore[arg-type]
                "products": GraphQLField(GraphQLList(product_type)),  # type: ignore[arg-type]
            },
        )
        schema = GraphQLSchema(query=query_type)  # type: ignore[arg-type]
        path_map = {
            "orders": {
                "schema_name": "public",
                "table_name": "orders",
                "domain_id": "sales",
                "table_description": None,
                "domain_description": None,
            },
            "products": {
                "schema_name": "public",
                "table_name": "products",
                "domain_id": "catalog",
                "table_description": None,
                "domain_description": None,
            },
        }
        state = SimpleNamespace(schemas={"admin": schema}, table_path_maps={"admin": path_map})

        spec = generate_rest_openapi_spec(state, "admin", domains=["sales"])
        assert "/sales/orders" in spec["paths"]
        assert "/catalog/products" not in spec["paths"]
