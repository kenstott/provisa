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
from tests.role_state import RoleState


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
            "write_ops": ["delete", "insert", "update"],
            "write_returns_rows": True,
            "write_refused_forms": [],
        }
    }
    state = RoleState(
        schemas={role_id: schema},
        table_path_maps={role_id: path_map},
    )
    return state


class TestEmptySpec:
    def test_unknown_role_returns_empty_spec(self):
        state = RoleState(schemas={}, table_path_maps={})
        spec = generate_rest_openapi_spec(state, "nobody")
        assert spec["openapi"] == "3.1.0"
        assert spec["paths"] == {}

    def test_no_query_type_returns_empty_spec(self):
        schema = GraphQLSchema()
        state = RoleState(schemas={"r": schema}, table_path_maps={"r": {}})
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
        spec = generate_rest_openapi_spec(self._state_with_commands(), "admin", domains=["other"])
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
                "write_ops": ["delete", "insert", "update"],
                "write_returns_rows": True,
                "write_refused_forms": [],
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
        path_map = {
            "orders": {
                "domain_id": "sales",
                "table_name": "orders",
                "write_ops": ["delete", "insert", "update"],
                "write_returns_rows": True,
                "write_refused_forms": [],
            }
        }
        state = RoleState(
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

    def test_include_nodes_param_present_when_group_by_enabled(self):
        # Regression: generator.py's raw_params lookup honors ?includeNodes=true regardless of
        # whether the spec declares it, but Swagger UI's Try-it-out only renders an input for
        # params the spec lists. Without this declaration, the NL page's "open in openapi" replay
        # (provisa-ui/src/pages/OpenApiPage.tsx) finds no includeNodes input row, silently skips
        # setting it, and Executes without the param — so the real endpoint correctly (per its own
        # contract) omits `nodes`, even though the NL preview and the replayed URL both show it.
        state = self._make_agg_state(enable_aggregates=False, enable_group_by=True)
        spec = generate_rest_openapi_spec(state, "admin")
        names = {p["name"] for p in spec["paths"]["/sales/orders"]["get"]["parameters"]}
        assert "includeNodes" in names

    def test_include_nodes_param_absent_when_group_by_disabled(self):
        state = self._make_agg_state(enable_aggregates=True, enable_group_by=False)
        spec = generate_rest_openapi_spec(state, "admin")
        names = {p["name"] for p in spec["paths"]["/sales/orders"]["get"]["parameters"]}
        assert "includeNodes" not in names

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
                "write_ops": ["delete", "insert", "update"],
                "write_returns_rows": True,
                "write_refused_forms": [],
            },
            "products": {
                "schema_name": "public",
                "table_name": "products",
                "domain_id": "catalog",
                "table_description": None,
                "domain_description": None,
                "write_ops": ["delete", "insert", "update"],
                "write_returns_rows": True,
                "write_refused_forms": [],
            },
        }
        state = RoleState(schemas={"admin": schema}, table_path_maps={"admin": path_map})

        spec = generate_rest_openapi_spec(state, "admin", domains=["sales"])
        assert "/sales/orders" in spec["paths"]
        assert "/catalog/products" not in spec["paths"]


def _two_table_state() -> RoleState:
    """``analyst`` is served orders without its amount, and no customers; ``admin`` is served both."""

    def meta(table: str) -> dict:
        return {
            "schema_name": "public",
            "table_name": table,
            "domain_id": "sales",
            "table_description": None,
            "domain_description": None,
            "write_ops": [],
            "write_returns_rows": False,
            "write_refused_forms": [],
        }

    def schema(order_fields: dict, *customers: str) -> GraphQLSchema:
        fields = {
            "orders": GraphQLField(GraphQLList(GraphQLObjectType("Order", order_fields))),  # type: ignore[arg-type]
        }
        if customers:
            customer = GraphQLObjectType("Customer", {"id": GraphQLField(GraphQLInt)})  # type: ignore[arg-type]
            fields["customers"] = GraphQLField(GraphQLList(customer))  # type: ignore[arg-type]
        return GraphQLSchema(query=GraphQLObjectType("Query", fields))  # type: ignore[arg-type]

    return RoleState(
        schemas={
            "admin": schema(
                {"id": GraphQLField(GraphQLInt), "amount": GraphQLField(GraphQLFloat)},  # type: ignore[arg-type]
                "customers",
            ),
            "analyst": schema({"id": GraphQLField(GraphQLInt)}),  # type: ignore[arg-type]
        },
        table_path_maps={
            "admin": {"orders": meta("orders"), "customers": meta("customers")},
            "analyst": {"orders": meta("orders")},
        },
    )


class TestTableFilter:
    """REQ-2158: a contract's address is the description narrowed to its one table."""

    def test_the_description_holds_only_the_named_table(self):
        spec = generate_rest_openapi_spec(_two_table_state(), "admin", tables=["orders"])
        assert list(spec["paths"]) == ["/sales/orders"]
        assert "Customer" not in spec["components"]["schemas"]
        assert "Order" in spec["components"]["schemas"]

    def test_it_narrows_within_the_named_domain(self):
        state = _two_table_state()
        narrowed = generate_rest_openapi_spec(state, "admin", domains=["sales"], tables=["orders"])
        assert list(narrowed["paths"]) == ["/sales/orders"]
        assert (
            generate_rest_openapi_spec(state, "admin", domains=["hr"], tables=["orders"])["paths"]
            == {}
        )

    def test_a_table_the_role_is_not_served_is_not_described(self):
        spec = generate_rest_openapi_spec(_two_table_state(), "analyst", tables=["customers"])
        assert spec["paths"] == {}
        assert "Customer" not in spec["components"]["schemas"]

    def test_the_named_table_keeps_the_rights_of_the_whole_description(self):
        state = _two_table_state()
        whole = generate_rest_openapi_spec(state, "analyst")
        narrowed = generate_rest_openapi_spec(state, "analyst", tables=["orders"])
        assert narrowed["paths"] == whole["paths"]
        assert narrowed["components"]["schemas"]["Order"] == whole["components"]["schemas"]["Order"]
        assert "amount" not in narrowed["components"]["schemas"]["Order"]["properties"]

    def test_a_named_table_leaves_the_commands_out(self):
        state = _two_table_state()
        state.tracked_functions = {
            "add": {"name": "add", "domain_id": "sales", "visible_to": ["admin"], "arguments": []}
        }
        assert "/sales/commands/add" in generate_rest_openapi_spec(state, "admin")["paths"]
        narrowed = generate_rest_openapi_spec(state, "admin", tables=["orders"])
        assert list(narrowed["paths"]) == ["/sales/orders"]


class TestSwaggerUiTheme:
    """REQ-1434: the docs page carries the app's colour scheme in its markup.

    The UI loads this page into a ``srcDoc`` iframe (so the request can carry the bearer), and a
    srcdoc document's location is ``about:srcdoc`` — it has no query string. Reading ?theme in the
    browser therefore always missed, and the frame rendered dark inside a light app.
    """

    def test_light_renders_without_the_dark_class(self):
        from provisa.api.rest.openapi_spec import swagger_ui_html

        assert '<html lang="en" class="">' in swagger_ui_html("light")

    def test_dark_is_stamped_on_the_root_element(self):
        from provisa.api.rest.openapi_spec import swagger_ui_html

        assert '<html lang="en" class="dark-mode">' in swagger_ui_html("dark")

    def test_a_direct_visit_with_no_theme_is_dark(self):
        from provisa.api.rest.openapi_spec import swagger_ui_html

        assert '<html lang="en" class="dark-mode">' in swagger_ui_html(None)

    def test_the_page_never_reads_the_theme_from_the_url(self):
        from provisa.api.rest.openapi_spec import swagger_ui_html

        html = swagger_ui_html("light")
        assert "__PROVISA_THEME_CLASS__" not in html
        assert 'get("theme")' not in html


class TestSwaggerSpecDownload:
    """The docs page downloads the spec as it fetches it: with the role and the credential."""

    def test_the_download_is_not_a_bare_link(self):
        from provisa.api.rest.openapi_spec import swagger_ui_html

        html = swagger_ui_html("light")
        # A bare link carried no role header and no credential.
        assert "<a href=\"' + dlUrl" not in html
        assert 'download="openapi.json">' not in html

    def test_the_download_carries_the_identity_every_other_request_carries(self):
        from provisa.api.rest.openapi_spec import swagger_ui_html

        html = swagger_ui_html("light")
        assert "fetch(dlUrl, { headers: withIdentity({}) })" in html
        assert "withIdentity(req.headers);" in html  # the spec and try-it-out requests
        identity = html[html.index("const withIdentity") : html.index("const bar =")]
        assert 'headers["x-provisa-role"] = role' in identity
        assert '"Bearer " + token' in identity
        assert 'headers["X-Org-Provisa"] = orgId' in identity

    def test_a_demo_visitors_identity_is_read_where_the_app_keeps_it(self):
        """A demo sandbox's visitor holds the tab's token, org and role in sessionStorage under
        ``visitor:`` (provisa-ui browserStorage.ts); read from localStorage they were absent, and
        the spec request was refused 401."""
        from provisa.api.rest.openapi_spec import swagger_ui_html

        html = swagger_ui_html("light")
        identity = html[html.index("const visitor") : html.index("const bar =")]
        assert 'sessionStorage.getItem("provisa_visitor") === "true"' in identity
        assert 'sessionStorage.getItem("visitor:" + key)' in identity
        # Every read goes through identity().
        assert 'localStorage.getItem("provisa_' not in identity
        for key in ("provisa_role", "provisa_su_token", "provisa_token", "provisa_org"):
            assert f'identity("{key}")' in identity


class TestSwaggerUiScope:
    """The docs page describes the role and domains the app selected.

    Read from ``location.search``, they were empty in the app's ``srcDoc`` frame, so the spec
    request named no domains and Swagger listed every domain's endpoints (meta among them) with
    only "ontology" selected in the navbar.
    """

    def _scope(self, html: str) -> dict:
        import json

        start = html.index("const scope = ") + len("const scope = ")
        return json.loads(html[start : html.index(";\n", start)])

    def test_the_selected_domains_are_stamped_into_the_page(self):
        from provisa.api.rest.openapi_spec import swagger_ui_html

        html = swagger_ui_html("light", "analyst", "ontology")
        assert self._scope(html) == {"role": "analyst", "domains": "ontology"}

    def test_the_spec_request_carries_the_stamped_domains(self):
        from provisa.api.rest.openapi_spec import swagger_ui_html

        html = swagger_ui_html("light", "analyst", "ontology,sales")
        assert 'const urlDomains = scope.domains || "";' in html
        assert 'specParams.set("domains", urlDomains)' in html

    def test_the_page_never_reads_its_scope_from_the_url(self):
        from provisa.api.rest.openapi_spec import swagger_ui_html

        html = swagger_ui_html("light", "analyst", "ontology")
        assert "__PROVISA_DOC_SCOPE__" not in html
        assert "window.location.search" not in html

    def test_no_selection_is_an_empty_scope(self):
        from provisa.api.rest.openapi_spec import swagger_ui_html

        assert self._scope(swagger_ui_html(None)) == {"role": "", "domains": ""}

    def test_a_value_cannot_close_the_script(self):
        from provisa.api.rest.openapi_spec import swagger_ui_html

        html = swagger_ui_html("light", "</script><b>", None)
        assert "</script><b>" not in html
        assert self._scope(html)["role"] == "</script><b>"
