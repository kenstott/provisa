# Copyright (c) 2026 Kenneth Stott
# Canary: f12aed3d-e9dd-49e5-bc6d-e26182a041c3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A role named in the path is one the caller holds (REQ-273).

Five data routes address a role by id in the URL, and the MCP tools take one as an argument.
Naming a role there is the same act as naming one in ``X-Provisa-Role`` and follows the same
rule: a role the authenticated caller is assigned is honoured; any other is refused with the
message the middleware gives for the header. A deployment with no auth provider takes the role
at face value, as it does for the header.
"""

from __future__ import annotations

import types
from typing import Any

import pytest

import provisa.api.app as appmod
from provisa.api.acting_role import header_role, held_role
from provisa.api.data import endpoint_dev, endpoint_grpc_proxy
from provisa.api.errors import ApiError
from provisa.auth.models import RoleAssignment

A, B = "role_a", "role_b"


def _request(*held: str, user_id: str | None = "u1", body: dict | None = None) -> Any:
    """A request as the auth middleware leaves it: an identity and the roles it is assigned.
    ``user_id=None`` is a router mounted without the middleware (no identity at all)."""
    state = types.SimpleNamespace()
    if user_id is not None:
        state.identity = types.SimpleNamespace(user_id=user_id, roles=list(held))
        state.assignments = [RoleAssignment(role_id=r, domain_id="*") for r in held]

    async def _json():
        return body or {}

    return types.SimpleNamespace(state=state, json=_json)


@pytest.fixture(autouse=True)
def _two_roles(monkeypatch):
    for name, value in (
        ("proto_files", {A: 'syntax = "proto3"; // a', B: 'syntax = "proto3"; // b'}),
        ("roles", {r: {"id": r, "capabilities": [], "domain_access": ["*"]} for r in (A, B)}),
        ("schemas", {}),
        ("contexts", {}),
        ("tracked_functions", {"do_it": {"name": "do_it"}}),
        ("schema_build_cache", {}),
    ):
        monkeypatch.setattr(appmod.state, name, value, raising=False)


def _refused(err: pytest.ExceptionInfo[ApiError], role_id: str) -> None:
    assert (err.value.status_code, err.value.code) == (403, "auth.role_not_assigned")
    # The same sentence the middleware answers an unassigned X-Provisa-Role with.
    assert err.value.detail == f"Role {role_id!r} is not assigned to this user"
    assert err.value.params == {"role_id": role_id}


# --- the rule ------------------------------------------------------------------------------------


def test_a_held_role_is_honoured_and_any_other_is_refused():
    assert held_role(_request(A), A) == A
    assert held_role(_request(A, B), B) == B
    with pytest.raises(ApiError) as err:
        held_role(_request(A), B)
    _refused(err, B)
    with pytest.raises(ApiError) as err:
        held_role(_request(), A)  # an authenticated caller holding nothing
    _refused(err, A)


def test_no_auth_provider_takes_the_role_at_face_value():
    assert held_role(_request(user_id="anonymous"), B) == B
    assert held_role(_request(user_id=None), B) == B


# --- /data/proto/{role_id} -----------------------------------------------------------------------


async def test_a_caller_holding_a_fetches_a_proto_and_is_refused_b():
    resp = await endpoint_dev.proto_endpoint(A, _request(A))
    assert resp.status_code == 200
    assert resp.body == b'syntax = "proto3"; // a'

    with pytest.raises(ApiError) as err:
        await endpoint_dev.proto_endpoint(B, _request(A))
    _refused(err, B)
    # ...and naming domains does not get around it.
    with pytest.raises(ApiError) as err:
        await endpoint_dev.proto_endpoint(B, _request(A), domains="sales")
    _refused(err, B)


async def test_the_dev_identity_fetches_any_roles_proto():
    resp = await endpoint_dev.proto_endpoint(B, _request(user_id="anonymous"))
    assert resp.body == b'syntax = "proto3"; // b'


# --- the gRPC Explorer's routes ------------------------------------------------------------------


async def test_the_command_catalog_is_listed_only_for_a_held_role():
    listed = await endpoint_grpc_proxy.grpc_commands(A, _request(A))
    assert [c["name"] for c in listed] == ["do_it"]
    with pytest.raises(ApiError) as err:
        await endpoint_grpc_proxy.grpc_commands(B, _request(A))
    _refused(err, B)


async def test_a_command_is_never_run_as_a_role_the_caller_does_not_hold(monkeypatch):
    ran: list[tuple] = []

    async def _invoke(name, args, state, role_id):
        ran.append((name, role_id))
        return []

    monkeypatch.setattr("provisa.api.data.action_exec.invoke_tracked_function", _invoke)
    body = {"name": "do_it", "args_json": "{}"}

    with pytest.raises(ApiError) as err:
        await endpoint_grpc_proxy.grpc_command(B, _request(A, body=body))
    _refused(err, B)
    assert ran == [], "the command must not run"

    await endpoint_grpc_proxy.grpc_command(A, _request(A, body=body))
    assert ran == [("do_it", A)]


@pytest.mark.parametrize(
    "call",
    [
        lambda req: endpoint_grpc_proxy.grpc_group_by_columns(B, "OrdersGroupBy", req),
        lambda req: endpoint_grpc_proxy.jsonapi_group_by_columns(B, "sales", "orders", req),
    ],
)
async def test_the_group_by_pickers_answer_only_for_a_held_role(call):
    with pytest.raises(ApiError) as err:
        await call(_request(A))
    _refused(err, B)
    # A held role gets past the rule (and, having no schema in this fixture, the ordinary 404).
    with pytest.raises(ApiError) as err:
        await call(_request(B))
    assert (err.value.status_code, err.value.code) == (404, "data.no_schema_for_role")


# --- MCP: a role named in a tool call ------------------------------------------------------------


def test_a_remote_mcp_caller_may_name_only_a_role_it_holds():
    from provisa.api.mcp import server

    r1 = server._request_role.set(A)  # the role the bearer token maps to
    r2 = server._request_identity.set(types.SimpleNamespace(user_id="u1", roles=[A, "role_c"]))
    try:
        assert server.named_role(A) == A
        assert server.named_role("role_c") == "role_c"  # another role the principal holds
        with pytest.raises(PermissionError, match=f"Role '{B}' is not assigned to this user"):
            server.named_role(B)
    finally:
        server._request_role.reset(r1)
        server._request_identity.reset(r2)


def test_the_tokens_own_mapped_role_qualifies_even_when_no_claim_names_it():
    from provisa.api.mcp import server

    # A mapping rule can resolve a token to a role its roles claim does not list.
    r1 = server._request_role.set(A)
    r2 = server._request_identity.set(types.SimpleNamespace(user_id="u1", roles=[]))
    try:
        assert server.named_role(A) == A
        with pytest.raises(PermissionError):
            server.named_role(B)
    finally:
        server._request_role.reset(r1)
        server._request_identity.reset(r2)


def test_a_local_stdio_mcp_client_names_roles_as_before():
    from provisa.api.mcp import server

    assert server._request_identity.get() is None
    assert server.named_role(B) == B


# --- X-Role names no role: /data/sdl, /data/domains, /data/introspection -------------------------


def _acting(role: str | None) -> Any:
    return types.SimpleNamespace(state=types.SimpleNamespace(role=role))


def test_the_acting_role_decides_and_a_differing_x_role_is_refused():
    assert header_role(_acting(A), None, None) == A
    assert header_role(_acting(A), None, A) == A  # agreeing: accepted, does nothing
    with pytest.raises(ApiError) as err:
        header_role(_acting(A), None, B)
    assert (err.value.status_code, err.value.code) == (400, "data.role_mismatch")
    assert err.value.params == {"body_role": B, "acting_role": A}


def test_x_role_never_establishes_a_role_by_itself():
    # No acting role and no X-Provisa-Role: an X-Role is not a way to name one.
    with pytest.raises(ApiError) as err:
        header_role(_acting(None), None, B)
    assert err.value.code == "data.role_mismatch"
    assert header_role(_acting(None), None, None) is None
    assert header_role(_acting(None), A, None) == A


@pytest.mark.parametrize("endpoint", ["get_domains", "get_sdl", "get_introspection"])
async def test_the_schema_endpoints_refuse_an_x_role_that_is_not_the_acting_role(endpoint):
    from provisa.api.data import sdl

    handler = getattr(sdl, endpoint)
    none: Any = None
    kwargs = {"x_provisa_role": none, "x_role": B}
    if endpoint != "get_domains":
        kwargs["domain"] = None
    with pytest.raises(ApiError) as err:
        await handler(_acting(A), **kwargs)
    assert (err.value.status_code, err.value.code) == (400, "data.role_mismatch")


async def test_the_domain_list_is_the_acting_roles(monkeypatch):
    from provisa.api.data import sdl

    monkeypatch.setattr(
        appmod.state,
        "roles",
        {
            A: {"id": A, "capabilities": [], "domain_access": ["sales"]},
            B: {"id": B, "capabilities": [], "domain_access": ["*"]},
        },
        raising=False,
    )
    monkeypatch.setattr(
        appmod.state,
        "schema_build_cache",
        {"domains": [{"id": "sales"}, {"id": "finance"}]},
        raising=False,
    )
    monkeypatch.setattr("provisa.core.domain_policy.single_domain", lambda: False)
    none: Any = None
    resp = await sdl.get_domains(_acting(A), x_provisa_role=none, x_role=none)
    assert resp.body == b'["sales"]'
    with pytest.raises(ApiError) as err:
        await sdl.get_domains(_acting(None), x_provisa_role=none, x_role=none)
    assert (err.value.status_code, err.value.code) == (422, "data.missing_x_provisa_role_header")
