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


class TestCommandPaths:
    """REQ-1155: registered commands appear as POST paths in the OpenAPI surface."""

    def _state_with_commands(self):
        state = _make_state()
        grpc_fn = {
            "name": "random_grpc_set",
            "domain_id": "default",
            "visible_to": ["admin"],
            "description": "demo grpc command",
            "arguments": [],
        }
        py_fn = {
            "name": "random_python_set",
            "domain_id": "default",
            "visible_to": ["admin"],
            "arguments": [{"name": "rows", "type": "Int"}, {"name": "seed", "type": "Int"}],
        }
        hidden = {
            "name": "secret_cmd",
            "domain_id": "default",
            "visible_to": ["ops"],
            "arguments": [],
        }
        state.tracked_functions = {
            "random_grpc_set": grpc_fn,
            "ps__random_grpc_set": grpc_fn,  # prefixed alias must not double-emit
            "random_python_set": py_fn,
            "secret_cmd": hidden,
        }
        return state

    def test_command_post_path_present(self):
        spec = generate_rest_openapi_spec(self._state_with_commands(), "admin")
        assert "post" in spec["paths"]["/default/commands/random_grpc_set"]

    def test_command_not_deduped_alias(self):
        spec = generate_rest_openapi_spec(self._state_with_commands(), "admin")
        # exactly one path for the command despite the prefixed alias key
        cmd_paths = [p for p in spec["paths"] if p.endswith("/commands/random_grpc_set")]
        assert len(cmd_paths) == 1

    def test_command_hidden_from_unauthorized_role(self):
        spec = generate_rest_openapi_spec(self._state_with_commands(), "admin")
        assert "/default/commands/secret_cmd" not in spec["paths"]

    def test_command_request_body_from_arguments(self):
        spec = generate_rest_openapi_spec(self._state_with_commands(), "admin")
        op = spec["paths"]["/default/commands/random_python_set"]["post"]
        props = op["requestBody"]["content"]["application/json"]["schema"]["properties"]
        assert props == {"rows": {"type": "integer"}, "seed": {"type": "integer"}}
        assert op["operationId"] == "call_random_python_set"

    def test_command_domain_filter_excludes(self):
        spec = generate_rest_openapi_spec(
            self._state_with_commands(), "admin", domains=["other"]
        )
        assert not any("/commands/" in p for p in spec["paths"])


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


class TestAggregateGroupByParams:
    """REQ-1359: aggregate/groupBy query params + component schemas are gated on schema
    presence of {field}_aggregate / {field}_group_by root fields."""

    def _make_agg_state(self, enable_aggregates: bool = True, enable_group_by: bool = True):
        from provisa.compiler import naming as _naming
        from provisa.compiler.context import build_context
        from provisa.compiler.introspect import ColumnMetadata
        from provisa.compiler.schema_gen import SchemaInput, generate_schema

        def _col(name, data_type="varchar(100)", nullable=False):
            return ColumnMetadata(column_name=name, data_type=data_type, is_nullable=nullable)

        _naming.configure(gql="snake")
        tables = [
            {
                "id": 1,
                "source_id": "sales-pg",
                "domain_id": "sales",
                "schema_name": "public",
                "table_name": "orders",
                "enable_aggregates": enable_aggregates,
                "enable_group_by": enable_group_by,
                "columns": [
                    {"column_name": "id", "visible_to": ["admin"]},
                    {"column_name": "amount", "visible_to": ["admin"]},
                    {"column_name": "region", "visible_to": ["admin"]},
                ],
            },
        ]
        column_types = {
            1: [
                _col("id", "integer"),
                _col("amount", "decimal(10,2)"),
                _col("region", "varchar(20)"),
            ],
        }
        role = {"id": "admin", "capabilities": [], "domain_access": ["*"]}
        domains = [{"id": "sales", "description": "Sales"}]
        si = SchemaInput(
            tables=tables,
            relationships=[],
            column_types=column_types,
            naming_rules=[],
            role=role,
            domains=domains,
        )
        schema = generate_schema(si)
        ctx = build_context(si)
        path_map = {"orders": {"domain_id": "sales", "table_name": "orders"}}
        state = SimpleNamespace(
            schemas={"admin": schema}, contexts={"admin": ctx}, table_path_maps={"admin": path_map}
        )
        return state

    def test_aggregate_param_present_when_enabled(self):
        state = self._make_agg_state(enable_aggregates=True, enable_group_by=False)
        spec = generate_rest_openapi_spec(state, "admin")
        names = {p["name"] for p in spec["paths"]["/sales/orders"]["get"]["parameters"]}
        assert "aggregate" in names

    def test_group_by_param_present_when_enabled(self):
        state = self._make_agg_state(enable_aggregates=False, enable_group_by=True)
        spec = generate_rest_openapi_spec(state, "admin")
        names = {p["name"] for p in spec["paths"]["/sales/orders"]["get"]["parameters"]}
        assert "groupBy" in names
        # groupBy implies aggregate results too
        assert "aggregate" in names

    def test_params_absent_when_flags_off(self):
        state = self._make_agg_state(enable_aggregates=False, enable_group_by=False)
        spec = generate_rest_openapi_spec(state, "admin")
        names = {p["name"] for p in spec["paths"]["/sales/orders"]["get"]["parameters"]}
        assert "aggregate" not in names
        assert "groupBy" not in names

    def test_aggregate_result_component_registered_when_enabled(self):
        state = self._make_agg_state(enable_aggregates=True, enable_group_by=False)
        spec = generate_rest_openapi_spec(state, "admin")
        assert "OrdersAggregateResult" in spec["components"]["schemas"]

    def test_group_by_row_component_registered_when_enabled(self):
        state = self._make_agg_state(enable_aggregates=False, enable_group_by=True)
        spec = generate_rest_openapi_spec(state, "admin")
        assert "OrdersGroupByRow" in spec["components"]["schemas"]

    def test_components_absent_when_flags_off(self):
        state = self._make_agg_state(enable_aggregates=False, enable_group_by=False)
        spec = generate_rest_openapi_spec(state, "admin")
        schemas = spec["components"]["schemas"]
        assert "OrdersAggregateResult" not in schemas
        assert "OrdersGroupByRow" not in schemas


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
