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
from provisa.api.acting_role import header_role, held_role, named_role
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

    return types.SimpleNamespace(state=state, json=_json, headers={})


@pytest.fixture(autouse=True)
def _two_roles(monkeypatch):
    for name, value in (
        ("proto_files", {A: 'syntax = "proto3"; // a', B: 'syntax = "proto3"; // b'}),
        ("roles", {r: {"id": r, "capabilities": [], "domain_access": ["*"]} for r in (A, B)}),
        ("schemas", {}),
        ("contexts", {}),
        (
            "tracked_functions",
            {"do_it": {"name": "do_it", "kind": "query", "domain_id": "sales"}},
        ),
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


# --- a set of held roles named in the path (REQ-1620) --------------------------------------------

META = "meta:role_a+role_b"


@pytest.fixture
def built(monkeypatch):
    """The sets whose meta-role was asked for, with the meta-role's surface in place of a build."""
    asked: list[tuple[str, ...]] = []

    def _ensure(state, members):
        asked.append(tuple(sorted(set(members))))
        return "meta:" + "+".join(sorted(set(members)))

    monkeypatch.setattr("provisa.security.meta_role.ensure_meta_role", _ensure)
    appmod.state.proto_files[META] = 'syntax = "proto3"; // a+b'
    return asked


def test_one_named_role_is_that_role_and_several_act_as_their_meta_role(built):
    assert named_role(_request(A, B), A) == A
    assert built == []
    assert named_role(_request(A, B), f"{B}, {A}") == META
    assert named_role(_request(A, B), f"{A},{A}") == A  # the same role twice is one role
    assert built == [(A, B)]


def test_every_member_of_a_named_set_is_held(built):
    with pytest.raises(ApiError) as err:
        named_role(_request(A), f"{A},{B}")
    _refused(err, B)
    assert built == [], "no meta-role is made for a set the caller does not hold"


def test_a_meta_role_is_never_named_directly(built):
    for request in (_request(A, B), _request(user_id="anonymous"), _request(user_id=None)):
        for named in (META, f"{A},{META}"):
            with pytest.raises(ApiError) as err:
                named_role(request, named)
            assert (err.value.status_code, err.value.code) == (403, "auth.meta_role_named")
    assert built == []


def test_no_auth_provider_takes_a_named_set_at_face_value(built):
    assert named_role(_request(user_id="anonymous"), f"{A},{B}") == META


def test_a_set_naming_a_role_that_does_not_exist_is_refused_by_name(built):
    with pytest.raises(ApiError) as err:
        named_role(_request(user_id="anonymous"), f"{A},ghost")
    assert (err.value.status_code, err.value.code) == (404, "data.no_role")
    assert err.value.params == {"role_id": "ghost"}
    assert built == []


def test_a_control_plane_role_adds_nothing_to_a_named_set(built, monkeypatch):
    """REQ-1327: it confers no data rights, so the set acts as its data-plane members."""
    roles = dict(appmod.state.roles)
    roles["platform"] = {"id": "platform", "capabilities": ["cross_org"], "domain_access": ["*"]}
    monkeypatch.setattr(appmod.state, "roles", roles, raising=False)
    assert named_role(_request(A, B, "platform"), f"{A},platform") == A
    assert named_role(_request(A, B, "platform"), f"{A},platform,{B}") == META
    assert named_role(_request("platform"), "platform") == "platform"  # alone: the role named


def test_naming_no_role_is_refused():
    with pytest.raises(ApiError) as err:
        named_role(_request(A), " , ")
    assert (err.value.status_code, err.value.code) == (400, "data.missing_role_id")


async def test_the_explorers_routes_answer_for_the_named_set(built, monkeypatch):
    both = f"{A},{B}"
    resp = await endpoint_dev.proto_endpoint(both, _request(A, B))
    assert resp.body == b'syntax = "proto3"; // a+b'

    seen: list[str] = []

    def _listed(state, role_id):
        seen.append(role_id)
        return []

    monkeypatch.setattr("provisa.api.data.action_exec.list_visible_commands", _listed)
    roles = {**appmod.state.roles, META: {"id": META, "capabilities": [], "domain_access": ["*"]}}
    monkeypatch.setattr(appmod.state, "roles", roles, raising=False)
    await endpoint_grpc_proxy.grpc_commands(both, _request(A, B))
    assert seen == [META]

    ran: list[str] = []

    async def _invoke(name, args, state, role_id):
        ran.append(role_id)
        return []

    monkeypatch.setattr("provisa.api.data.action_exec.invoke_tracked_function", _invoke)
    monkeypatch.setattr(
        "provisa.api.data.action_exec.bind_named_args", lambda name, args, state, role_id: args
    )
    body = {"name": "do_it", "args_json": "{}"}
    await endpoint_grpc_proxy.grpc_command(both, _request(A, B, body=body))
    assert ran == [META]
    with pytest.raises(ApiError) as err:
        await endpoint_grpc_proxy.grpc_command(both, _request(A, body=body))
    _refused(err, B)
    assert ran == [META], "the command must not run"


# --- why a role has no proto ----------------------------------------------------------------------


async def test_each_reason_a_role_has_no_proto_is_its_own_answer(monkeypatch):
    monkeypatch.setattr(appmod.state, "proto_files", {}, raising=False)
    # The model has not been built: nothing can be said about the role yet.
    monkeypatch.setattr(appmod.state, "role_build_inputs", {}, raising=False)
    with pytest.raises(ApiError) as err:
        await endpoint_dev.proto_endpoint(A, _request(A))
    assert (err.value.status_code, err.value.code) == (503, "data.schema_cache_not_ready")

    monkeypatch.setattr(appmod.state, "role_build_inputs", {"tables": []}, raising=False)
    # Built, and the role exists with no data surface.
    with pytest.raises(ApiError) as err:
        await endpoint_dev.proto_endpoint(A, _request(A))
    assert (err.value.status_code, err.value.code) == (404, "data.no_proto_for_role")
    assert err.value.params == {"role_id": A}
    # Built, and there is no such role.
    with pytest.raises(ApiError) as err:
        await endpoint_dev.proto_endpoint("ghost", _request(user_id="anonymous"))
    assert (err.value.status_code, err.value.code) == (404, "data.no_role")


# --- /data/grpc/{type}: the role the request runs as ---------------------------------------------


async def test_the_proxy_never_runs_as_a_body_role_the_request_does_not_run_as():
    request = _request(A, body={"role_id": B})
    request.state.role = A  # established by the auth layer
    with pytest.raises(ApiError) as err:
        await endpoint_grpc_proxy.grpc_proxy("Orders", request)
    assert (err.value.status_code, err.value.code) == (400, "data.role_mismatch")
    # The acting role is the one served: with no schema in this fixture, the ordinary 404 for A.
    for body in ({}, {"role_id": A}):
        request = _request(A, body=body)
        request.state.role = A
        with pytest.raises(ApiError) as err:
            await endpoint_grpc_proxy.grpc_proxy("Orders", request)
        assert (err.value.status_code, err.value.code) == (404, "data.no_schema_for_role")
        assert err.value.params == {"role_id": A}


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
        assert server.named_role(A, None) == A
        assert server.named_role("role_c", None) == "role_c"  # another role the principal holds
        with pytest.raises(PermissionError, match=f"'{B}' is not assigned"):
            server.named_role(B, None)
    finally:
        server._request_role.reset(r1)
        server._request_identity.reset(r2)


def test_the_tokens_own_mapped_role_qualifies_even_when_no_claim_names_it():
    from provisa.api.mcp import server

    # A mapping rule can resolve a token to a role its roles claim does not list.
    r1 = server._request_role.set(A)
    r2 = server._request_identity.set(types.SimpleNamespace(user_id="u1", roles=[]))
    try:
        assert server.named_role(A, None) == A
        with pytest.raises(PermissionError):
            server.named_role(B, None)
    finally:
        server._request_role.reset(r1)
        server._request_identity.reset(r2)


def test_a_local_stdio_mcp_client_names_roles_as_before():
    from provisa.api.mcp import server

    assert server._request_identity.get() is None
    assert server.named_role(B, None) == B


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


# --- the admin compile: a role named in the input -------------------------------------------------

DEVELOPER, INSPECTOR = "developer", "inspector"


@pytest.fixture
def compiled_as(monkeypatch):
    """Roles for the compile cases, and the role the compiler was asked to compile as."""
    from provisa.api.admin import dev_queries

    roles = dict(appmod.state.roles)
    roles[DEVELOPER] = {
        "id": DEVELOPER,
        "capabilities": ["query_development"],
        "domain_access": ["*"],
    }
    roles[INSPECTOR] = {
        "id": INSPECTOR,
        "capabilities": ["query_development", "access_config"],
        "domain_access": ["*"],
    }
    monkeypatch.setattr(appmod.state, "roles", roles, raising=False)
    seen: list[str] = []

    async def _compile(role_id, *_args, **_kwargs):
        seen.append(role_id)
        return []

    monkeypatch.setattr(dev_queries, "compile_query", _compile)
    return seen


async def _compile_as(role_id: str, *held: str) -> None:
    from provisa.api.admin import schema_mutation
    from provisa.api.admin.types import CompileQueryInput

    info = types.SimpleNamespace(context={"request": _request(*held)})
    await schema_mutation.Mutation().compile_query(
        info,  # type: ignore[arg-type]
        CompileQueryInput(query="{ x }", role=role_id),
    )


async def test_a_compile_as_a_held_role_runs(compiled_as):
    await _compile_as(A, DEVELOPER, A)
    assert compiled_as == [A]


async def test_a_compile_as_a_role_the_caller_does_not_hold_is_refused(compiled_as):
    with pytest.raises(ApiError) as err:
        await _compile_as(B, DEVELOPER, A)
    _refused(err, B)
    assert compiled_as == []


async def test_the_holder_of_access_config_compiles_as_any_role(compiled_as):
    await _compile_as(B, INSPECTOR)
    assert compiled_as == [B]


def test_a_remote_mcp_caller_names_a_set_of_held_roles(monkeypatch):
    from provisa.api.mcp import server
    import provisa.security.meta_role as meta_role

    monkeypatch.setattr(meta_role, "ensure_meta_role", lambda st, m: meta_role.meta_role_id(m))
    r1 = server._request_role.set(A)
    r2 = server._request_identity.set(types.SimpleNamespace(user_id="u1", roles=["role_c"]))
    try:
        assert server.named_role(f"role_c,{A}", None) == meta_role.meta_role_id([A, "role_c"])
        with pytest.raises(PermissionError, match=f"'{B}' is not assigned"):
            server.named_role(f"{A},{B}", None)
        with pytest.raises(PermissionError, match="is not a role"):
            server.named_role(meta_role.meta_role_id([A, "role_c"]), None)
    finally:
        server._request_role.reset(r1)
        server._request_identity.reset(r2)


def test_a_local_stdio_mcp_client_names_a_set_at_will(monkeypatch):
    from provisa.api.mcp import server
    import provisa.security.meta_role as meta_role

    monkeypatch.setattr(meta_role, "ensure_meta_role", lambda st, m: meta_role.meta_role_id(m))
    assert server.named_role(f"{B},{A}", None) == meta_role.meta_role_id([A, B])
    with pytest.raises(PermissionError, match="is not a role"):
        server.named_role(meta_role.meta_role_id([A, B]), None)
