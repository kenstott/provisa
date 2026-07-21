# Copyright (c) 2026 Kenneth Stott
# Canary: 7c1e9a04-2b58-4d63-9f10-3a6d5f2c8b71
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-872 / REQ-1156: a webhook is a governed command, reachable beyond GraphQL.

Two behaviors:
  1. Callable (function/webhook) mutation fields apply the active GraphQL naming convention, exactly
     like table fields — ``add_pet`` -> ``addPet`` under apollo_graphql (previously left snake_case).
  2. Webhooks project into the shared command catalog and route through the shared
     ``invoke_tracked_function`` executor, so they are discoverable and invocable on the non-GraphQL
     surfaces (SQL/pgwire, Cypher/Bolt), not only as a GraphQL mutation.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.api.data import action_exec
from provisa.api.data.action_exec import (
    _webhook_body,
    invoke_tracked_function,
    list_visible_commands,
)


def _webhook() -> dict:
    return {
        "name": "add_pet",
        "url": "http://petstore/api/v3/pet",
        "method": "POST",
        "timeout_ms": 5000,
        "returns": None,
        "inline_return_type": [{"name": "id", "type": "Int"}],
        "arguments": [{"name": "name", "type": "String"}, {"name": "status", "type": "String"}],
        "visible_to": ["admin"],
        "domain_id": "pet-store",
        "description": "Add a pet",
        "kind": "mutation",
    }


def _state() -> SimpleNamespace:
    wh = _webhook()
    return SimpleNamespace(
        tracked_functions={},
        tracked_webhooks={"add_pet": wh, "ps__addPet": wh},  # raw + gql-field-name alias
        roles={
            "admin": {"id": "admin", "domain_access": ["*"], "capabilities": ["admin"]},
            "guest": {"id": "guest", "domain_access": ["*"], "capabilities": []},
        },
    )


# ------------------------------------------------------------------ discovery


def test_webhook_appears_in_command_catalog():
    out = list_visible_commands(_state(), "admin")
    by_name = {c["name"]: c for c in out}
    assert "add_pet" in by_name  # a webhook is a discoverable command
    assert [c["name"] for c in out].count("add_pet") == 1  # prefixed alias collapsed
    assert by_name["add_pet"]["kind"] == "mutation"
    # inline_return_type makes it set-returning even without returns/return_schema
    assert by_name["add_pet"]["set_returning"] is True
    assert by_name["add_pet"]["arguments"] == [
        {"name": "name", "type": "String"},
        {"name": "status", "type": "String"},
    ]


def test_webhook_visibility_filtered_by_role():
    names = {c["name"] for c in list_visible_commands(_state(), "guest")}
    assert "add_pet" not in names  # visible_to = ["admin"] excludes guest


# ------------------------------------------------------------------ arg binding


def test_webhook_body_maps_positional_sql_args_to_names():
    # SQL surfaces pass a0/a1 positionally — they must bind to the declared names for the POST body
    body = _webhook_body(_webhook(), {"a0": "Rex", "a1": "available"})
    assert body == {"name": "Rex", "status": "available"}


def test_webhook_body_passes_named_graphql_args_through():
    body = _webhook_body(_webhook(), {"name": "Rex", "status": "available"})
    assert body == {"name": "Rex", "status": "available"}


# ------------------------------------------------------------------ invocation


@pytest.mark.asyncio
async def test_invoke_tracked_function_routes_webhook_over_http(monkeypatch):
    captured: dict = {}

    class _Resp:
        def json(self):
            return {"id": 7, "name": "Rex"}

    class _Client:
        def __init__(self, *a, **k):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def request(self, method, url, json):
            captured.update(method=method, url=url, json=json)
            return _Resp()

    monkeypatch.setattr(action_exec.httpx, "AsyncClient", _Client)

    # positional args, as pgwire/bolt pass them — the shared executor resolves the webhook
    rows = await invoke_tracked_function("add_pet", {"a0": "Rex", "a1": "available"}, _state(), "admin")

    assert rows == [{"id": 7, "name": "Rex"}]  # dict response normalized to one row
    assert captured["method"] == "POST"
    assert captured["url"] == "http://petstore/api/v3/pet"
    assert captured["json"] == {"name": "Rex", "status": "available"}


@pytest.mark.asyncio
async def test_invoke_tracked_function_unknown_name_still_raises():
    from fastapi import HTTPException

    with pytest.raises(HTTPException):
        await invoke_tracked_function("nope", {}, _state(), "admin")


# ------------------------------------------------------------------ pgwire detection


def test_pgwire_detects_webhook_call():
    from provisa.pgwire.function_call import detect_sql_function_call

    hit = detect_sql_function_call("SELECT * FROM add_pet('Rex', 'available')", _state())
    assert hit is not None
    name, values = hit
    assert name == "add_pet"
    assert values == ["Rex", "available"]


# ------------------------------------------------------------------ gRPC


def test_grpc_proto_gen_emits_webhook_rpc():
    from provisa.grpc.proto_gen import _visible_commands

    si = SimpleNamespace(
        role={"id": "admin", "domain_access": ["*"]},
        functions=[],
        webhooks=[_webhook()],
    )
    assert {c["name"] for c in _visible_commands(si)} == {"add_pet"}


def test_grpc_servicer_resolves_webhook_rpc():
    from provisa.grpc.server import ProvisaServicer

    servicer = ProvisaServicer(_state(), pb2_module=object(), pb2_grpc_module=object())
    assert servicer._resolve_command_rpc("AddPet") == "add_pet"


# ------------------------------------------------------------------ camelCase field naming


@pytest.mark.parametrize(
    ("convention", "expected"),
    [("apollo_graphql", "ps__addPet"), ("hasura_graphql", "ps__add_pet")],
)
def test_callable_field_applies_naming_convention(convention, expected):
    from provisa.compiler import naming
    from provisa.compiler.actions_schema import _build_action_fields

    prev = naming.active_gql_convention()
    naming.configure(gql=convention)
    try:
        si = SimpleNamespace(
            role={"id": "admin", "domain_access": ["*"]},
            functions=[],
            webhooks=[_webhook()],
            domain_prefix=True,
        )
        _query, mutation = _build_action_fields(
            si, {}, [], domain_alias_map={"pet-store": "ps"}
        )
        assert expected in mutation
        assert "ps__add_pet" not in mutation or convention == "hasura_graphql"
    finally:
        naming.configure(gql=prev)
