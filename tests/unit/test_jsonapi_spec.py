# Copyright (c) 2026 Kenneth Stott
# Canary: b1c4d8e2-f3a5-6789-0bcd-ef1234567890
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Unit tests for JSON:API OpenAPI 3.1 spec generator (REQ-805)."""

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

from provisa.api.jsonapi.spec import generate_jsonapi_openapi_spec


def _make_state(role_id: str = "admin") -> SimpleNamespace:
    pet_type = GraphQLObjectType(
        "Pet",
        lambda: {
            "id": GraphQLField(GraphQLInt),  # type: ignore[arg-type]
            "name": GraphQLField(GraphQLString),  # type: ignore[arg-type]
            "age": GraphQLField(GraphQLInt),  # type: ignore[arg-type]
            "weight": GraphQLField(GraphQLFloat),  # type: ignore[arg-type]
        },
    )
    query_type = GraphQLObjectType(
        "Query",
        {"pets": GraphQLField(GraphQLList(pet_type))},  # type: ignore[arg-type]
    )
    schema = GraphQLSchema(query=query_type)  # type: ignore[arg-type]
    path_map = {
        "pets": {
            "schema_name": "public",
            "table_name": "Pets",
            "domain_id": "store",
            "table_description": "Pet records",
            "domain_description": "Pet store domain",
        }
    }
    state = SimpleNamespace(
        schemas={role_id: schema},
        table_path_maps={role_id: path_map},
        schema_build_cache={"domains": [{"id": "store", "description": "Pet store domain"}]},
    )
    return state


class TestEmptySpec:
    def test_unknown_role_returns_empty(self):
        state = SimpleNamespace(schemas={}, table_path_maps={})
        spec = generate_jsonapi_openapi_spec(state, "nobody")
        assert spec["openapi"] == "3.1.0"
        assert spec["paths"] == {}

    def test_no_query_type_returns_empty(self):
        schema = GraphQLSchema()
        state = SimpleNamespace(
            schemas={"r": schema},
            table_path_maps={"r": {}},
            schema_build_cache={},
        )
        spec = generate_jsonapi_openapi_spec(state, "r")
        assert spec["paths"] == {}


class TestSpecStructure:
    def test_openapi_version(self):
        spec = generate_jsonapi_openapi_spec(_make_state(), "admin")
        assert spec["openapi"] == "3.1.0"

    def test_path_per_table(self):
        spec = generate_jsonapi_openapi_spec(_make_state(), "admin")
        assert "/store/Pets" in spec["paths"]

    def test_get_operation_present(self):
        spec = generate_jsonapi_openapi_spec(_make_state(), "admin")
        assert "get" in spec["paths"]["/store/Pets"]

    def test_operation_id_includes_field_name(self):
        spec = generate_jsonapi_openapi_spec(_make_state(), "admin")
        op = spec["paths"]["/store/Pets"]["get"]
        assert "pets" in op["operationId"]

    def test_tags_reflect_domain(self):
        spec = generate_jsonapi_openapi_spec(_make_state(), "admin")
        op = spec["paths"]["/store/Pets"]["get"]
        assert "store" in op["tags"]

    def test_top_level_tags(self):
        spec = generate_jsonapi_openapi_spec(_make_state(), "admin")
        tag_names = {t["name"] for t in spec.get("tags", [])}
        assert "store" in tag_names


class TestParameters:
    def _params(self) -> dict[str, dict]:
        spec = generate_jsonapi_openapi_spec(_make_state(), "admin")
        params = spec["paths"]["/store/Pets"]["get"]["parameters"]
        return {p["name"]: p for p in params}

    def test_page_size_present(self):
        assert "page[size]" in self._params()

    def test_page_number_present(self):
        assert "page[number]" in self._params()

    def test_fields_sparse_fieldset_present(self):
        assert "fields[Pets]" in self._params()

    def test_sort_param_present(self):
        assert "sort" in self._params()

    def test_include_param_present(self):
        assert "include" in self._params()

    def test_filter_eq_per_column(self):
        params = self._params()
        assert "filter[name]" in params
        assert "filter[age]" in params

    def test_filter_operators_present(self):
        params = self._params()
        assert "filter[name][neq]" in params
        assert "filter[age][gt]" in params
        assert "filter[age][gte]" in params
        assert "filter[age][lt]" in params
        assert "filter[age][lte]" in params

    def test_page_size_has_minimum(self):
        assert self._params()["page[size]"]["schema"]["minimum"] == 1

    def test_page_number_has_minimum(self):
        assert self._params()["page[number]"]["schema"]["minimum"] == 1


class TestResponseSchema:
    def test_200_response_present(self):
        spec = generate_jsonapi_openapi_spec(_make_state(), "admin")
        responses = spec["paths"]["/store/Pets"]["get"]["responses"]
        assert "200" in responses

    def test_response_content_type_jsonapi(self):
        spec = generate_jsonapi_openapi_spec(_make_state(), "admin")
        content = spec["paths"]["/store/Pets"]["get"]["responses"]["200"]["content"]
        assert "application/vnd.api+json" in content

    def test_response_schema_has_data_array(self):
        spec = generate_jsonapi_openapi_spec(_make_state(), "admin")
        schema = spec["paths"]["/store/Pets"]["get"]["responses"]["200"]["content"][
            "application/vnd.api+json"
        ]["schema"]
        assert schema["properties"]["data"]["type"] == "array"

    def test_response_schema_has_links(self):
        spec = generate_jsonapi_openapi_spec(_make_state(), "admin")
        schema = spec["paths"]["/store/Pets"]["get"]["responses"]["200"]["content"][
            "application/vnd.api+json"
        ]["schema"]
        assert "links" in schema["properties"]

    def test_403_response_present(self):
        spec = generate_jsonapi_openapi_spec(_make_state(), "admin")
        responses = spec["paths"]["/store/Pets"]["get"]["responses"]
        assert "403" in responses


class TestDomainFilter:
    def test_domain_filter_restricts_paths(self):
        pet_type = GraphQLObjectType("Pet", lambda: {"id": GraphQLField(GraphQLInt)})  # type: ignore[arg-type]
        order_type = GraphQLObjectType("Order", lambda: {"id": GraphQLField(GraphQLInt)})  # type: ignore[arg-type]
        query_type = GraphQLObjectType(
            "Query",
            {
                "pets": GraphQLField(GraphQLList(pet_type)),  # type: ignore[arg-type]
                "orders": GraphQLField(GraphQLList(order_type)),  # type: ignore[arg-type]
            },
        )
        schema = GraphQLSchema(query=query_type)  # type: ignore[arg-type]
        path_map = {
            "pets": {
                "schema_name": "public",
                "table_name": "Pets",
                "domain_id": "store",
                "table_description": None,
                "domain_description": None,
            },
            "orders": {
                "schema_name": "public",
                "table_name": "Orders",
                "domain_id": "sales",
                "table_description": None,
                "domain_description": None,
            },
        }
        state = SimpleNamespace(
            schemas={"admin": schema},
            table_path_maps={"admin": path_map},
            schema_build_cache={"domains": []},
        )
        spec = generate_jsonapi_openapi_spec(state, "admin", domains=["store"])
        assert "/store/Pets" in spec["paths"]
        assert "/sales/Orders" not in spec["paths"]
