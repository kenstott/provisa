# Copyright (c) 2026 Kenneth Stott
# Canary: 94414f9a-ff21-4e49-a809-d27c93fee411
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1929: a command over REST and JSON:API is the role's GraphQL command field.

A query-kind command is read with GET: the read is lowered to the GraphQL field and executed as
GraphQL executes it, its filter, ordering and paging the field's own where/order_by/limit/offset.
A mutation-kind command is run with POST. JSON:API serves both at /<domain>/commands/<name>. The
real routers, schema and GraphQL action-field execution run; only the command executor is
replaced by a recorder that answers fixed rows."""

# Requirements: REQ-1929

from __future__ import annotations

import json
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI, Request

from provisa.api.data import action_exec, mutations
from provisa.api.jsonapi import generator as jsonapi_generator
from provisa.api.rest import generator as rest_generator
from provisa.compiler import naming
from provisa.compiler.context import build_context
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.rls import RLSContext
from provisa.compiler.schema_gen import SchemaInput, build_table_path_map, generate_schema
from tests.role_state import RoleState

pytestmark = [pytest.mark.anyio]

_DOMAINS = [{"id": "petstore", "description": "Pet store"}]
_FIND_PETS = {
    "name": "find_pets",
    "domain_id": "petstore",
    "kind": "query",
    "visible_to": ["analyst"],
    "arguments": [{"name": "status", "type": "String"}],
    "returns": "public.pets",
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
    "outsider": {
        "id": "outsider",
        "capabilities": ["usage", "command_execution"],
        "domain_access": ["elsewhere"],
    },
}
_ROWS = [
    {"id": 1, "name": "Rex"},
    {"id": 2, "name": "Tom"},
    {"id": 3, "name": "Rex"},
]
_JSONAPI = {"accept": "application/vnd.api+json"}


@pytest.fixture
def anyio_backend():
    return "asyncio"


def _si(role_id: str) -> SchemaInput:
    return SchemaInput(
        tables=[
            {
                "id": 1,
                "source_id": "pg1",
                "domain_id": "petstore",
                "schema_name": "public",
                "table_name": "pets",
                "columns": [
                    {"column_name": "id", "visible_to": ["analyst"]},
                    {"column_name": "name", "visible_to": ["analyst"]},
                ],
                "write_ops": [],
                "write_returns_rows": False,
                "write_refused_forms": [],
            }
        ],
        relationships=[],
        column_types={
            1: [
                ColumnMetadata(column_name="id", data_type="integer", is_nullable=False),
                ColumnMetadata(column_name="name", data_type="varchar", is_nullable=True),
            ]
        },
        naming_rules=[],
        role=_ROLES[role_id],
        domains=_DOMAINS,
        functions=[_FIND_PETS, _ADD_PET],
        webhooks=[],
    )


@pytest.fixture
def api(monkeypatch):
    naming.configure(gql="apollo_graphql", sql="snake")
    built = {role: _si(role) for role in _ROLES}
    state = RoleState(
        schemas={role: generate_schema(si) for role, si in built.items()},
        contexts={role: build_context(si) for role, si in built.items()},
        rls_contexts={role: RLSContext(rules={}) for role in built},
        table_path_maps={role: build_table_path_map(si) for role, si in built.items()},
        roles=_ROLES,
        tracked_functions={"find_pets": _FIND_PETS, "add_pet": _ADD_PET},
        tracked_webhooks={},
        schema_build_cache={"domains": _DOMAINS},
    )
    calls: list[tuple[str, dict, str]] = []

    async def _invoke(name, args, _state, role_id):
        calls.append((name, dict(args), role_id))
        return [dict(r) for r in _ROWS]

    monkeypatch.setattr(mutations, "invoke_tracked_function", _invoke)
    monkeypatch.setattr(action_exec, "invoke_tracked_function", _invoke)

    app = FastAPI()

    @app.middleware("http")
    async def _role(request: Request, call_next):  # pyright: ignore[reportUnusedFunction]
        request.state.role = request.headers.get("x-provisa-role")
        return await call_next(request)

    app.include_router(rest_generator.create_rest_router(state))
    app.include_router(jsonapi_generator.create_jsonapi_router(state))
    client = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")
    return SimpleNamespace(client=client, calls=calls)


def _as(role: str, extra: dict | None = None) -> dict:
    return {"X-Provisa-Role": role, **(extra or {})}


# --- REST ---------------------------------------------------------------------------------------


async def test_a_rest_get_reads_a_query_command_with_the_table_read_parameters(api):
    resp = await api.client.get(
        "/data/rest/petstore/commands/find_pets",
        params={
            "status": "available",
            "filter": json.dumps([{"field": "name", "comparator": "eq", "value": "Rex"}]),
            "orderBy": json.dumps([{"field": "id", "direction": "desc"}]),
            "limit": "1",
        },
        headers=_as("analyst"),
    )
    assert resp.status_code == 200, resp.text
    assert resp.json() == {"data": [{"id": 3, "name": "Rex"}]}
    assert api.calls == [("find_pets", {"status": "available"}, "analyst")]


async def test_a_rest_get_pages_with_offset(api):
    resp = await api.client.get(
        "/data/rest/petstore/commands/find_pets",
        params={"status": "available", "offset": "1", "limit": "1"},
        headers=_as("analyst"),
    )
    assert resp.status_code == 200, resp.text
    assert resp.json() == {"data": [{"id": 2, "name": "Tom"}]}


async def test_a_rest_filter_on_a_column_the_command_does_not_return_is_refused(api):
    resp = await api.client.get(
        "/data/rest/petstore/commands/find_pets",
        params={
            "status": "available",
            "filter": json.dumps([{"field": "owner", "comparator": "eq", "value": "x"}]),
        },
        headers=_as("analyst"),
    )
    assert resp.status_code == 400
    assert "owner" in resp.text


async def test_a_rest_filter_with_an_unknown_comparator_is_refused(api):
    resp = await api.client.get(
        "/data/rest/petstore/commands/find_pets",
        params={
            "status": "available",
            "filter": json.dumps([{"field": "id", "comparator": "near", "value": 1}]),
        },
        headers=_as("analyst"),
    )
    assert resp.status_code == 400
    assert "near" in resp.text
    assert api.calls == []


async def test_a_declared_argument_is_read_from_the_query_string(api):
    resp = await api.client.get(
        "/data/rest/petstore/commands/find_pets",
        params={"status": "sold"},
        headers=_as("analyst"),
    )
    assert resp.status_code == 200, resp.text
    assert api.calls == [("find_pets", {"status": "sold"}, "analyst")]


async def test_a_query_command_is_not_run_with_post(api):
    resp = await api.client.post(
        "/data/rest/petstore/commands/find_pets", json={}, headers=_as("analyst")
    )
    assert resp.status_code == 405
    assert resp.headers["allow"] == "GET"
    assert api.calls == []


async def test_a_mutation_command_is_run_with_post_and_not_read_with_get(api):
    resp = await api.client.post(
        "/data/rest/petstore/commands/add_pet", json={"name": "Fido"}, headers=_as("analyst")
    )
    assert resp.status_code == 200, resp.text
    assert api.calls == [("add_pet", {"name": "Fido"}, "analyst")]
    resp = await api.client.get("/data/rest/petstore/commands/add_pet", headers=_as("analyst"))
    assert resp.status_code == 405
    assert resp.headers["allow"] == "POST"


@pytest.mark.parametrize(("method", "command"), [("get", "find_pets"), ("post", "add_pet")])
async def test_a_role_whose_schema_lacks_the_command_is_told_it_is_unknown(api, method, command):
    resp = await api.client.request(
        method.upper(), f"/data/rest/petstore/commands/{command}", headers=_as("outsider")
    )
    assert resp.status_code == 404
    assert api.calls == []


# --- JSON:API -----------------------------------------------------------------------------------


async def test_a_jsonapi_get_reads_a_query_command_under_collection_conventions(api):
    resp = await api.client.get(
        "/data/jsonapi/petstore/commands/find_pets",
        params={"status": "available", "filter[id][gt]": "1", "sort": "-id", "page[size]": "1"},
        headers=_as("analyst", _JSONAPI),
    )
    assert resp.status_code == 200, resp.text
    assert resp.headers["content-type"].startswith("application/vnd.api+json")
    assert resp.json() == {"meta": {"rows": [{"id": 3, "name": "Rex"}]}}
    assert api.calls == [("find_pets", {"status": "available"}, "analyst")]


async def test_a_jsonapi_filter_value_is_read_as_its_column_type(api):
    resp = await api.client.get(
        "/data/jsonapi/petstore/commands/find_pets",
        params={"status": "available", "filter[id]": "two"},
        headers=_as("analyst", _JSONAPI),
    )
    assert resp.status_code == 400
    assert "Int" in resp.text


async def test_a_jsonapi_post_runs_a_mutation_command_from_the_body_meta(api):
    resp = await api.client.post(
        "/data/jsonapi/petstore/commands/add_pet",
        json={"meta": {"name": "Fido"}},
        headers=_as("analyst", _JSONAPI),
    )
    assert resp.status_code == 200, resp.text
    assert api.calls == [("add_pet", {"name": "Fido"}, "analyst")]


async def test_jsonapi_refuses_the_wrong_method_and_an_unoffered_command(api):
    wrong = await api.client.post(
        "/data/jsonapi/petstore/commands/find_pets", json={}, headers=_as("analyst", _JSONAPI)
    )
    assert wrong.status_code == 405
    unknown = await api.client.get(
        "/data/jsonapi/petstore/commands/find_pets", headers=_as("outsider", _JSONAPI)
    )
    assert unknown.status_code == 404
    assert "errors" in unknown.json()
    assert api.calls == []


# --- the JSON:API description -------------------------------------------------------------------


def test_the_jsonapi_description_offers_a_read_and_a_write_at_the_command_paths():
    from provisa.api.jsonapi.spec import generate_jsonapi_openapi_spec

    naming.configure(gql="apollo_graphql", sql="snake")
    si = _si("analyst")
    state = RoleState(
        schemas={"analyst": generate_schema(si)},
        contexts={"analyst": build_context(si)},
        rls_contexts={"analyst": RLSContext(rules={})},
        table_path_maps={"analyst": build_table_path_map(si)},
        roles=_ROLES,
        schema_build_cache={"domains": _DOMAINS},
    )
    paths = generate_jsonapi_openapi_spec(state, "analyst")["paths"]
    read = paths["/petstore/commands/find_pets"]
    assert set(read) == {"get"}
    names = {p["name"] for p in read["get"]["parameters"]}
    assert {"filter[id]", "filter[name]", "sort", "page[size]", "page[number]", "status"} <= names
    assert set(paths["/petstore/commands/add_pet"]) == {"post"}
