# Copyright (c) 2026 Kenneth Stott
# Canary: 7d3a1e94-5b2c-4f68-9e10-a4c2b8d6f351
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1929: every surface offers a role what the role's GraphQL schema offers, and nothing more.

Three rules the requirement's scenarios state: a column hidden from a role is absent from its
.proto, OpenAPI spec and JSON:API attributes; a query-kind command is offered on every surface
under its own conventions to a role that reaches it and on none to a role that does not; and a
gRPC table read lowers to the same semantic SQL as the equivalent GraphQL request."""

# Requirements: REQ-1929

from __future__ import annotations

import json
from typing import Any

import pytest

from provisa.api.jsonapi.spec import generate_jsonapi_openapi_spec
from provisa.api.rest.openapi_spec import generate_rest_openapi_spec
from provisa.compiler import naming
from provisa.compiler.context import build_context
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.parser import parse_query
from provisa.compiler.schema_gen import SchemaInput, build_table_path_map, generate_schema
from provisa.compiler.sql_gen import compile_query
from provisa.grpc.proto_gen import generate_proto
from provisa.compiler.rls import RLSContext
from tests.role_state import RoleState

pytestmark = pytest.mark.unit

_DOMAINS = [{"id": "petstore", "description": "Pet store"}]
_COLUMNS = [
    {"column_name": "id", "visible_to": ["analyst", "auditor"]},
    {"column_name": "name", "visible_to": ["analyst", "auditor"]},
    {"column_name": "secret_note", "visible_to": ["auditor"]},
]
_TYPES = {
    1: [
        ColumnMetadata(column_name="id", data_type="integer", is_nullable=False),
        ColumnMetadata(column_name="name", data_type="varchar", is_nullable=True),
        ColumnMetadata(column_name="secret_note", data_type="varchar", is_nullable=True),
    ]
}
_FIND_PETS = {
    "name": "find_pets",
    "domain_id": "petstore",
    "kind": "query",
    "visible_to": ["analyst"],
    "arguments": [{"name": "status", "type": "String"}],
    "returns": "",
}
_ADD_PET = {
    "name": "add_pet",
    "domain_id": "petstore",
    "kind": "mutation",
    "visible_to": ["analyst"],
    "arguments": [{"name": "name", "type": "String"}],
    "returns": "",
}
_ROLES = {
    "analyst": {
        "id": "analyst",
        "capabilities": ["usage", "command_execution"],
        "domain_access": ["petstore"],
    },
    "auditor": {"id": "auditor", "capabilities": ["usage"], "domain_access": ["petstore"]},
    "outsider": {
        "id": "outsider",
        "capabilities": ["usage", "command_execution"],
        "domain_access": ["elsewhere"],
    },
}


def _si(role_id: str, functions: list[dict] | None = None) -> SchemaInput:
    return SchemaInput(
        tables=[
            {
                "id": 1,
                "source_id": "pg1",
                "domain_id": "petstore",
                "schema_name": "public",
                "table_name": "pets",
                "columns": _COLUMNS,
                "write_ops": [],
                "write_returns_rows": False,
                "write_refused_forms": [],
            }
        ],
        relationships=[],
        column_types=_TYPES,
        naming_rules=[],
        role=_ROLES[role_id],
        domains=_DOMAINS,
        functions=functions or [],
        webhooks=[],
    )


def _state(role_id: str, functions: list[dict] | None = None) -> RoleState:
    """The state serving ``role_id``: the surface built from its SchemaInput, and the registered
    commands as the loader holds them."""
    naming.configure(gql="apollo_graphql", sql="snake")
    si = _si(role_id, functions)
    ctx = build_context(si)
    return RoleState(
        schemas={role_id: generate_schema(si)},
        contexts={role_id: ctx},
        rls_contexts={role_id: RLSContext(rules={})},
        table_path_maps={role_id: build_table_path_map(si)},
        roles=_ROLES,
        tracked_functions={f["name"]: f for f in functions or []},
        tracked_webhooks={},
        schema_build_cache={"domains": _DOMAINS},
    )


_SECRET = ("secret_note", "secretNote", "secret-note")


def _mentions(text: str) -> bool:
    """The column under any of the surfaces' name conventions."""
    return any(name in text for name in _SECRET)


def _schema_text(state: RoleState, role_id: str) -> str:
    from graphql import print_schema

    return print_schema(state.schemas[role_id])


def _walk(node: Any):
    if isinstance(node, dict):
        yield node
        for value in node.values():
            yield from _walk(value)
    elif isinstance(node, list):
        for value in node:
            yield from _walk(value)


# --- a hidden column is absent from every surface ------------------------------------------------


def test_a_column_hidden_in_graphql_is_absent_from_the_proto_openapi_and_jsonapi():
    hidden = _state("analyst")
    assert not _mentions(_schema_text(hidden, "analyst"))  # the decision, made once
    assert not _mentions(generate_proto(_si("analyst")))
    rest = json.dumps(generate_rest_openapi_spec(hidden, "analyst"))
    jsonapi = json.dumps(generate_jsonapi_openapi_spec(hidden, "analyst"))
    assert not _mentions(rest)
    assert not _mentions(jsonapi)
    # The same spec still describes the visible columns: the check is not vacuous.
    assert '"name"' in rest and '"name"' in jsonapi


def test_the_same_column_is_offered_on_every_surface_to_a_role_that_may_read_it():
    shown = _state("auditor")
    assert "secretNote" in _schema_text(shown, "auditor")
    assert "secret_note" in generate_proto(_si("auditor"))
    assert _mentions(json.dumps(generate_rest_openapi_spec(shown, "auditor")))
    assert _mentions(json.dumps(generate_jsonapi_openapi_spec(shown, "auditor")))


# --- a command is offered on every surface to the role GraphQL offers it to ----------------------


def _rest_ops(state: RoleState, role_id: str) -> set[tuple[str, str]]:
    spec = generate_rest_openapi_spec(state, role_id)
    return {
        (method.upper(), path)
        for path, item in spec["paths"].items()
        if "/commands/" in path
        for method in item
    }


def _jsonapi_command_paths(state: RoleState, role_id: str) -> list[str]:
    spec = generate_jsonapi_openapi_spec(state, role_id)
    return [p for p in spec["paths"] if "find_pets" in p or "find-pets" in p or "findPets" in p]


def test_a_query_kind_command_is_offered_on_every_surface_to_a_role_that_reaches_it():
    functions = [_FIND_PETS, _ADD_PET]
    state = _state("analyst", functions)
    graphql = state.schemas["analyst"]
    assert "findPets" in graphql.query_type.fields
    assert "addPet" in graphql.mutation_type.fields
    rest = _rest_ops(state, "analyst")
    assert ("GET", "/petstore/commands/find_pets") in rest  # a Query field is a read
    assert ("POST", "/petstore/commands/add_pet") in rest  # a Mutation field is a write
    assert ("POST", "/petstore/commands/find_pets") not in rest
    proto = generate_proto(_si("analyst", functions))
    assert "rpc CallFindPets(FindPetsRequest) returns (CommandResponse);" in proto
    assert "rpc CallAddPet(AddPetRequest) returns (MutationResponse);" in proto
    assert _jsonapi_command_paths(state, "analyst")


def test_the_get_a_query_command_is_given_takes_the_parameters_a_table_takes():
    state = _state("analyst", [_FIND_PETS])
    spec = generate_rest_openapi_spec(state, "analyst")
    operation = spec["paths"]["/petstore/commands/find_pets"]["get"]
    names = {p["name"] for p in operation["parameters"]}
    assert {"filter", "orderBy", "limit", "offset", "status"} <= names


@pytest.mark.parametrize("role_id", ["outsider", "auditor"])
def test_a_role_that_does_not_reach_the_command_has_none_on_any_surface(role_id):
    functions = [_FIND_PETS, _ADD_PET]
    state = _state(role_id, functions)
    graphql = state.schemas[role_id]
    assert "findPets" not in graphql.query_type.fields if graphql.query_type else True
    assert graphql.mutation_type is None or "addPet" not in graphql.mutation_type.fields
    assert _rest_ops(state, role_id) == set()
    proto = generate_proto(_si(role_id, functions))
    assert "FindPets" not in proto and "AddPet" not in proto
    assert _jsonapi_command_paths(state, role_id) == []
    assert "find_pets" not in json.dumps(generate_jsonapi_openapi_spec(state, role_id))


# --- a gRPC table read lowers to the SQL of the equivalent GraphQL request -----------------------


def _grpc_sql(model: tuple[Any, Any], limit: int, offset: int | None, filter_msg: Any):
    from tests.grpc_model import compiled_read

    lowered = compiled_read(model, "Pets", limit, filter_msg, None, None, offset)
    assert lowered is not None
    return lowered


def _graphql_sql(model: tuple[Any, Any], text: str, variables: dict | None = None):
    schema, ctx = model
    (compiled,) = compile_query(parse_query(schema, text, variables, ctx=ctx), ctx, variables)
    return compiled.sql, list(compiled.params)


@pytest.mark.parametrize(
    ("limit", "offset", "filter_msg", "text"),
    [
        (10, None, None, "{ pets(limit: 10) { id name } }"),
        (10, 20, None, "{ pets(limit: 10, offset: 20) { id name } }"),
        (5, None, {"name": "Rex"}, '{ pets(limit: 5, where: {name: {eq: "Rex"}}) { id name } }'),
    ],
)
def test_a_grpc_table_read_compiles_to_the_sql_of_the_graphql_request_it_is_equivalent_to(
    limit, offset, filter_msg, text
):
    naming.configure(gql="apollo_graphql", sql="snake")
    si = _si("analyst")
    model = (generate_schema(si), build_context(si))
    grpc_sql, grpc_params = _grpc_sql(model, limit, offset, filter_msg)
    gql_sql, gql_params = _graphql_sql(model, text)
    assert grpc_sql == gql_sql
    assert grpc_params == gql_params
