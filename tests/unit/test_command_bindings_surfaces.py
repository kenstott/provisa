# Copyright (c) 2026 Kenneth Stott
# Canary: 1f6b8d27-3c94-4a5e-b071-6e2d9c4a8b35
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2168: each surface offers a command by its command binding in its language, and not at
all where the command is excluded: SQL and Cypher recognize the bound name, GraphQL's field and
the gRPC proto follow the GraphQL binding, REST's path the REST binding, MCP lists only what its
binding offers."""

# Requirements: REQ-2168

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.api.data.action_exec import command_named, list_visible_commands, usable_commands
from provisa.api.errors import ApiError
from provisa.compiler import naming
from provisa.compiler.actions_schema import _build_action_fields
from provisa.compiler.schema_types import SchemaInput

pytestmark = pytest.mark.unit

_SCHEMA = {
    "name": "meta_schema",
    "domain_id": "sales",
    "kind": "query",
    "visible_to": ["seller"],
    "arguments": [],
    "returns": "",
    "language_bindings": {
        "cypher": {"name": "apoc.meta.schema"},
        "graphql": {"exclude": True},
        "rest": {"exclude": True},
        "mcp": {"exclude": True},
    },
}
_ENRICH = {
    "name": "enrich",
    "domain_id": "sales",
    "kind": "query",
    "visible_to": ["seller"],
    "arguments": [],
    "returns": "",
    "language_bindings": {"sql": {"name": "enrich_rows"}, "graphql": {"name": "enrichRows"}},
}


def _state() -> SimpleNamespace:
    return SimpleNamespace(
        roles={"seller": {"id": "seller", "capabilities": [], "domain_access": ["sales"]}},
        tracked_functions={"meta_schema": _SCHEMA, "enrich": _ENRICH},
        tracked_webhooks={},
    )


def test_sql_and_cypher_recognize_a_command_by_its_binding_there():
    state = _state()
    assert set(usable_commands(state, "seller", language="sql")) == {"meta_schema", "enrich_rows"}
    assert set(usable_commands(state, "seller", language="cypher")) == {
        "apoc.meta.schema",
        "enrich",
    }
    assert command_named(state, "seller", "cypher", "apoc.meta.schema") == "meta_schema"
    assert command_named(state, "seller", "sql", "enrich_rows") == "enrich"


def test_a_name_not_bound_in_the_language_answers_as_an_unknown_command():
    for language, name in (("cypher", "meta_schema"), ("sql", "enrich"), ("graphql", "x")):
        with pytest.raises(ApiError) as refused:
            command_named(_state(), "seller", language, name)
        assert refused.value.code == "functions.unknown_command"


def test_a_listing_names_each_command_by_its_binding_and_leaves_out_an_excluded_one():
    state = _state()
    cypher = {c["name"]: c["command"] for c in list_visible_commands(state, "seller", "cypher")}
    assert cypher == {"apoc.meta.schema": "meta_schema", "enrich": "enrich"}
    graphql = [c["name"] for c in list_visible_commands(state, "seller", "graphql")]
    assert graphql == ["enrichRows"]


def test_mcp_lists_only_what_its_binding_offers_by_the_sql_name_it_runs():
    from provisa.api.mcp.tools import list_commands

    state = _state()
    state.contexts = {"seller": object()}
    listed = [c["name"] for c in list_commands(state, "seller")]
    assert listed == ["enrich_rows"]


def _schema_input(functions: list[dict]) -> SchemaInput:
    return SchemaInput(
        tables=[],
        relationships=[],
        column_types={},
        naming_rules=[],
        role={"id": "seller", "capabilities": [], "domain_access": ["sales"]},
        domains=[{"id": "sales", "description": "Sales"}],
        domain_prefix=False,
        functions=functions,
        webhooks=[],
    )


def test_the_graphql_field_is_the_graphql_binding_and_an_excluded_command_has_none():
    naming.configure(gql="apollo_graphql", sql="snake")
    query, mutation = _build_action_fields(_schema_input([_SCHEMA, _ENRICH]), {}, [])
    assert set(query) == {"enrichRows"}
    assert query["enrichRows"].extensions["provisa_command"] == "enrich"
    assert mutation == {}


def test_the_grpc_proto_follows_the_graphql_binding():
    from provisa.grpc.proto_gen import _visible_commands

    assert [f["name"] for f in _visible_commands(_schema_input([_SCHEMA, _ENRICH]))] == ["enrich"]


def test_every_role_reaches_the_meta_domain_for_commands_but_not_for_data():
    from provisa.security.mutation_authz import command_reachable
    from provisa.security.rights import reaches_domain, reaches_implicitly

    assert reaches_implicitly(["shelter"], "meta")
    assert reaches_implicitly([], "meta")
    assert not reaches_implicitly(["shelter"], "ops")
    assert not reaches_implicitly(["shelter"], "hr")
    # Direct SQL over the catalog still needs it listed.
    assert not reaches_domain(["shelter"], "meta")
    apoc = {"name": "apoc_meta_data", "domain_id": "meta", "visible_to": ["reader"]}
    assert command_reachable(apoc, {"id": "reader", "domain_access": ["shelter"]})
    # Grants still decide: reach alone assigns nothing.
    assert not command_reachable(apoc, {"id": "outsider", "domain_access": ["shelter"]})
