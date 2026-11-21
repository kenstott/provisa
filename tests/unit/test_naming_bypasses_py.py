# Copyright (c) 2026 Kenneth Stott
# Canary: 3c9e51a4-7f20-4d1b-9a66-0be2f4c8d915
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Client-facing names built by the naming authority, not by hand (REQ-194).

One test per row of the 2026-11-07 naming audit whose site shaped a name the authority
(provisa/compiler/naming.py) already had a function for. A row whose local helper produced
the authority's output for every input is pinned by routing: the site's name is the
authority's, so replacing the authority's answer replaces the site's.
"""

import inspect
from typing import cast

import pytest
from graphql import (
    GraphQLField,
    GraphQLList,
    GraphQLNonNull,
    GraphQLObjectType,
    GraphQLScalarType,
    GraphQLString,
)

from provisa.compiler import naming
from provisa.compiler.schema_types import SchemaInput, _TableInfo


@pytest.fixture(autouse=True)
def _convention():
    naming.configure(gql="apollo_graphql", sql="snake")
    yield
    naming.configure(gql="apollo_graphql", sql="snake")


# ── naming.to_pascal_case: the public name of the authority's pascal casing ────────────


def test_to_pascal_case_keeps_inner_capitals():
    assert naming.to_pascal_case("activeUsers") == "ActiveUsers"
    assert naming.to_pascal_case("active_users") == "ActiveUsers"


# ── B5: gRPC command RPC names ─────────────────────────────────────────────────────────


def test_command_rpc_name_keeps_inner_capitals():
    from provisa.grpc.proto_gen import command_rpc_name

    assert command_rpc_name("activeUsers") == "ActiveUsers"
    assert command_rpc_name("active_users") == "ActiveUsers"
    assert command_rpc_name("sales__activeUsers") == "SalesActiveUsers"


# ── B6: a command's {Name}ReturnType ───────────────────────────────────────────────────


def _command_input(functions: list[dict], webhooks: list[dict]) -> SchemaInput:
    return SchemaInput(
        tables=[],
        relationships=[],
        column_types={},
        naming_rules=[],
        role={"id": "admin", "capabilities": ["command_execution"], "domain_access": ["*"]},
        domains=[{"id": "pets", "description": "Pets"}],
        domain_prefix=False,
        functions=functions,
        webhooks=webhooks,
    )


def _named_type(gql_type) -> str:
    while isinstance(gql_type, (GraphQLList, GraphQLNonNull)):
        gql_type = gql_type.of_type
    assert isinstance(gql_type, GraphQLObjectType)
    return gql_type.name


def test_function_return_type_name_keeps_inner_capitals():
    from provisa.compiler.actions_schema import _build_action_fields

    func = {
        "name": "activeUsers",
        "arguments": [],
        "returns": "",
        "return_schema": {"type": "object", "properties": {"id": {"type": "integer"}}},
        "domain_id": "pets",
        "kind": "query",
        "visible_to": ["admin"],
    }
    query_fields, _ = _build_action_fields(_command_input([func], []), {}, [])
    assert _named_type(query_fields["activeUsers"].type) == "ActiveUsersReturnType"


def test_webhook_return_type_name_keeps_inner_capitals():
    from provisa.compiler.actions_schema import _build_action_fields

    webhook = {
        "name": "notifyOwners",
        "url": "http://x",
        "arguments": [],
        "inline_return_type": [{"name": "ok", "type": "Boolean"}],
        "domain_id": "pets",
        "kind": "mutation",
        "visible_to": ["admin"],
    }
    _, mutation_fields = _build_action_fields(_command_input([], [webhook]), {}, [])
    assert _named_type(mutation_fields["notifyOwners"].type) == "NotifyOwnersReturnType"


# ── B3: a computed relationship's GraphQL field ────────────────────────────────────────


def _table(table_id: int, table_name: str, field_name: str) -> _TableInfo:
    return _TableInfo(
        table_id=table_id,
        field_name=field_name,
        type_name=naming.to_type_name(field_name),
        domain_id="sales",
        source_id="pg",
        schema_name="public",
        table_name=table_name,
        visible_columns=[],
        column_metadata={},
    )


def _computed_fields(rel: dict) -> dict:
    from provisa.compiler.schema_gen import _add_computed_relationship_field

    orders = _table(1, "orders", "orders")
    line_items = _table(2, "line_items", "lineItems")
    ret_type = cast(
        GraphQLObjectType,
        GraphQLObjectType(
            "LineItems", {"id": GraphQLField(cast(GraphQLScalarType, GraphQLString))}
        ),
    )
    gql_types: dict[int, GraphQLObjectType] = {2: ret_type}
    fields: dict = {}
    _add_computed_relationship_field(
        fields,
        rel,
        "order_lines",
        [{"name": "order_lines", "returns": "public.line_items"}],
        [orders, line_items],
        gql_types,
    )
    return fields


def test_computed_relationship_field_is_named_for_its_target():
    rel = {
        "id": "orders-to-order_lines",
        "source_table_id": 1,
        "target_function_name": "order_lines",
        "source_column": "id",
        "cardinality": "one-to-many",
    }
    assert (
        list(_computed_fields(rel))
        == [naming.rel_field_name("lineItems", "one-to-many")]
        == ["lineItems"]
    )


def test_computed_relationship_field_takes_its_graphql_alias():
    rel = {
        "id": "orders-to-order_lines",
        "source_table_id": 1,
        "target_function_name": "order_lines",
        "source_column": "id",
        "cardinality": "one-to-many",
        "graphql_alias": "lines",
    }
    assert list(_computed_fields(rel)) == ["lines"]


# ── B14: a new file source's column alias ──────────────────────────────────────────────


def test_file_source_column_alias_is_the_authoritys_snake_case():
    from provisa.api.admin import schema_mutation

    assert schema_mutation._column_alias("OrderTotal") == naming.apply_convention(
        "OrderTotal", "snake"
    )
    assert schema_mutation._column_alias("order_total") is None


def test_file_source_column_alias_routes_through_the_authority(monkeypatch):
    from provisa.api.admin import schema_mutation

    monkeypatch.setattr(schema_mutation, "apply_convention", lambda name, conv: f"<{conv}:{name}>")
    assert schema_mutation._column_alias("OrderTotal") == "<snake:OrderTotal>"


# ── B18: a fact measure's governed metric name ─────────────────────────────────────────


def test_fact_metric_name_splits_an_acronym_as_the_authority_does():
    from provisa.mv.modeling import Fact, Measure, fact_registration

    reg = fact_registration(
        Fact(name="HTTPLogs", source="raw.logs", grain=("id",), measures=(Measure("bytes"),))
    )
    assert [m["name"] for m in reg["metrics"]] == ["http_logs_bytes_sum"]


def test_fact_metric_name_is_an_identifier():
    from provisa.mv.modeling import Fact, Measure, fact_registration

    reg = fact_registration(
        Fact(name="Sales", source="raw.orders", grain=("id",), measures=(Measure("net amt"),))
    )
    assert [m["name"] for m in reg["metrics"]] == ["sales_net_amt_sum"]


# ── D6: a landed graphql_remote table's remote field name ──────────────────────────────


def test_graphql_remote_field_name_inverts_the_sql_name():
    from provisa.api.app_loaders import _graphql_remote_field_name

    # The landed table is named apply_sql_name(remote field); the remote field is its inverse.
    for remote_field in ("orderLineItems", "orders_2024", "orders"):
        table = "sales__" + naming.apply_sql_name(remote_field)
        assert _graphql_remote_field_name(table) == remote_field


# ── D10, D11: connector column names in snake case ─────────────────────────────────────


def test_file_source_column_names_route_through_the_authority(monkeypatch):
    from provisa.file_source import source

    monkeypatch.setattr(
        source, "_discover_for_path", lambda config, path: [{"name": "TotalAmt", "type": "INT"}]
    )
    monkeypatch.setattr(source, "apply_convention", lambda name, conv: f"<{conv}:{name}>")
    config = source.FileSourceConfig(id="f", source_type="csv", path="/x.csv")
    table = source.TableDefinition(table_name="t", source_id="f", path="/x.csv")
    columns = source.discover_schema(config, table)
    assert [c.column_name for c in columns] == ["<snake:TotalAmt>"]


def test_quickbooks_names_route_through_the_authority(monkeypatch):
    from provisa.quickbooks import entities, rows

    for module in (entities, rows):
        monkeypatch.setattr(module, "apply_convention", lambda name, conv: f"<{conv}:{name}>")
    assert entities.table_name("JournalEntry") == "<snake:JournalEntry>"
    assert list(rows.row({"TotalAmt": 1})) == ["<snake:TotalAmt>"]


# ── D12: GraphQL scalar names for engine subtypes ──────────────────────────────────────


def test_scalar_name_routes_through_the_authority(monkeypatch):
    from provisa.compiler import type_map

    monkeypatch.setattr(type_map, "to_pascal_case", lambda name: f"<{name}>")
    assert type_map._scalar_name("tsrange") == "<ts_range>"


def test_scalar_names_of_known_subtypes():
    from provisa.compiler.type_map import _scalar_name

    assert _scalar_name("int4multirange") == "Int4MultiRange"
    assert _scalar_name("tsrange") == "TsRange"


# ── D8, D9: dead name helpers are gone ─────────────────────────────────────────────────


def test_grpc_query_rpc_derives_no_field_name():
    from provisa.grpc import server

    assert not hasattr(server, "_pascal_to_snake")
    params = inspect.signature(server.ProvisaServicer._handle_query).parameters
    assert "field_name" not in params


def test_openapi_register_has_no_private_alias_derivation():
    from provisa.openapi import register

    assert not hasattr(register, "_operation_id_to_alias")
    assert not hasattr(register, "_singularize")


def test_computed_relationship_field_taken_by_a_table_relationship_is_refused():
    """REQ-194: a computed relationship named for the field a table relationship holds is refused,
    naming both; neither replaces the other in the schema."""
    from provisa.compiler.context import RelationshipFieldTaken
    from provisa.compiler.schema_gen import _make_object_type_fields

    orders = _table(1, "orders", "orders")
    line_items = _table(2, "line_items", "lineItems")
    gql_types = {
        t.table_id: cast(
            GraphQLObjectType,
            GraphQLObjectType(
                t.type_name, {"id": GraphQLField(cast(GraphQLScalarType, GraphQLString))}
            ),
        )
        for t in (orders, line_items)
    }
    fk = {
        "id": "orders-line_items",
        "source_table_id": 1,
        "target_table_id": 2,
        "source_column": "id",
        "target_column": "order_id",
        "cardinality": "one-to-many",
    }
    computed = {
        "id": "orders-order_lines",
        "source_table_id": 1,
        "target_function_name": "order_lines",
        "source_column": "id",
        "cardinality": "one-to-many",
    }
    for rels in ([fk, computed], [computed, fk]):
        with pytest.raises(RelationshipFieldTaken) as err:
            _make_object_type_fields(
                1,
                {1: orders, 2: line_items},
                gql_types,
                None,
                [],
                rels,
                [{"name": "order_lines", "returns": "public.line_items"}],
                [orders, line_items],
                {},
                {},
                {},
            )
        assert err.value.field == "lineItems"
        assert {err.value.held_by, err.value.refused} == {"orders-line_items", "orders-order_lines"}


def test_computed_relationship_with_its_own_alias_beside_a_table_relationship():
    from provisa.compiler.schema_gen import _make_object_type_fields

    orders = _table(1, "orders", "orders")
    line_items = _table(2, "line_items", "lineItems")
    gql_types = {
        t.table_id: cast(
            GraphQLObjectType,
            GraphQLObjectType(
                t.type_name, {"id": GraphQLField(cast(GraphQLScalarType, GraphQLString))}
            ),
        )
        for t in (orders, line_items)
    }
    rels = [
        {
            "id": "orders-line_items",
            "source_table_id": 1,
            "target_table_id": 2,
            "source_column": "id",
            "target_column": "order_id",
            "cardinality": "one-to-many",
        },
        {
            "id": "orders-order_lines",
            "source_table_id": 1,
            "target_function_name": "order_lines",
            "source_column": "id",
            "cardinality": "one-to-many",
            "graphql_alias": "orderLines",
        },
    ]
    fields = _make_object_type_fields(
        1,
        {1: orders, 2: line_items},
        gql_types,
        None,
        [],
        rels,
        [{"name": "order_lines", "returns": "public.line_items"}],
        [orders, line_items],
        {},
        {},
        {},
    )
    assert {"lineItems", "orderLines"} <= set(fields)
