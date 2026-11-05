# Copyright (c) 2026 Kenneth Stott
# Canary: 3b8e6f14-2a7d-4c59-9e10-6d4f1c7a82b5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An authenticated gRPC call is served in its org (REQ-273, REQ-1263, REQ-1266).

The principal's role and session variables are org-owned, so the call's org is bound before they
are read, as on Flight. These run a real listener over the real ``AppState``: a stand-in state
answers a per-org read with no org bound, which is how every RPC on a secured deployment failed
with "No active org bound" while the suites stayed green."""

# Requirements: REQ-273, REQ-1263, REQ-1266

from __future__ import annotations

import grpc
import jwt
import pytest

from provisa.api.app import AppState
from provisa.api.org_runtime import OrgRuntime
from provisa.core.request_context import (
    current_org,
    current_session_vars,
    reset_current_org,
    set_current_org,
)
from provisa.grpc import auth as grpc_auth
from provisa.grpc import server as grpc_server
from provisa.grpc.descriptor import OrgDescriptor
from provisa.grpc.rpc_scope import rpc_scope
from tests.port_lease import lease_port

pytestmark = pytest.mark.unit

_SECRET = "secured-grpc-org-binding-signing-secret-32b!"
_PROTO = """syntax = "proto3";
package provisa.v1;
message Orders { int32 id = 1; }
message OrdersRequest { int32 limit = 1; }
service ProvisaService { rpc QueryOrders (OrdersRequest) returns (stream Orders); }
"""
_AUTH = {
    "provider": "simple",
    "allow_simple_auth": True,
    "jwt_secret": _SECRET,
    "simple": {"users": []},
    "default_role": "analyst",
    "role_mapping": [],
}


def _token(user_id: str = "u-1") -> str:
    return jwt.encode({"sub": user_id, "username": "alice"}, _SECRET, algorithm="HS256")


class _Rows:
    def __init__(self, org_ids: list[str]) -> None:
        self._org_ids = org_ids

    def fetchall(self) -> list:
        return [type("Row", (), {"_mapping": {"org_id": o}})() for o in self._org_ids]


class _Conn:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return False

    async def execute_core(self, _stmt):
        return _Rows(["acme"])


class _PlatformDb:
    """The platform plane: the user belongs to acme."""

    def acquire(self):
        return _Conn()


def _serve_org(state: AppState, org_id: str) -> None:
    """The org's runtime as its build leaves it: the service it is served, an empty model."""
    token = set_current_org(org_id)
    try:
        state.wire_proto = _PROTO
        state.grpc_descriptor = None
        state.proto_files = {}
        state.user_attributes = {"u-1": {"region": "emea"}}
        state.settings_overrides = {"derived_roles": []}
    finally:
        reset_current_org(token)


def _state(*, multitenancy: bool = False) -> AppState:
    state = AppState()
    state.org_id = "root"
    state.multitenancy = multitenancy
    state.auth_config = _AUTH
    state.auth_middleware_active = True
    state.admin_db = _PlatformDb()
    state.org_registry.set("acme", OrgRuntime(org_id="acme"))
    _serve_org(state, "root")
    _serve_org(state, "acme")
    return state


@pytest.fixture
def serve(monkeypatch):
    """A real server over ``state``. The query body is stood in for: it answers one row carrying
    the request's limit, and records what the call was bound to when it ran."""
    served: list[tuple[str | None, dict]] = []
    stops = []

    async def _rows(self, request, _context, type_name, _role_id):
        served.append((current_org.get(), dict(current_session_vars.get())))
        yield getattr(self._pb2, type_name)(id=request.limit)

    async def _built(_org_id):
        return None

    monkeypatch.setattr(grpc_server.ProvisaServicer, "_handle_query_bound", _rows)
    monkeypatch.setattr(grpc_server.ProvisaServicer, "_meter_msg", lambda self, msg: msg)
    monkeypatch.setattr("provisa.api.app.ensure_serving_runtime", _built)

    def _serve(state: AppState):
        port = lease_port()
        server = grpc_server.start_grpc_server(port, state)
        channel = grpc.insecure_channel(f"127.0.0.1:{port}")
        stops.append((server, channel))
        return channel, served

    yield _serve
    for server, channel in stops:
        channel.close()
        server.stop(grace=0).wait()


def _query(channel, metadata: tuple, limit: int = 7) -> list[int]:
    messages = OrgDescriptor(_PROTO)
    call = channel.unary_stream(
        "/provisa.v1.ProvisaService/QueryOrders",
        request_serializer=lambda raw: raw,
        response_deserializer=messages.Orders.FromString,
    )
    return [
        row.id
        for row in call(
            messages.OrdersRequest(limit=limit).SerializeToString(), metadata=metadata, timeout=30
        )
    ]


def _bearer(token: str | None = None) -> tuple:
    return (("authorization", f"Bearer {token or _token()}"),)


class TestSingleOrgDeployment:
    def test_an_authenticated_query_answers_in_the_deployments_org(self, serve):
        channel, served = serve(_state())
        assert _query(channel, _bearer()) == [7]
        ((org_id, session_vars),) = served
        assert org_id == "root"
        # The org's own attribute of the caller: read from the runtime the call was bound to.
        assert session_vars["region"] == "emea"
        assert session_vars["user_id"] == "u-1"
        assert session_vars["role"] == "analyst"

    def test_a_call_with_no_credential_is_refused(self, serve):
        channel, served = serve(_state())
        with pytest.raises(grpc.RpcError) as refused:
            _query(channel, ())
        assert refused.value.code() == grpc.StatusCode.UNAUTHENTICATED
        assert served == []

    def test_a_rejected_credential_is_refused(self, serve):
        channel, served = serve(_state())
        with pytest.raises(grpc.RpcError) as refused:
            _query(channel, _bearer("not-a-credential"))
        assert refused.value.code() == grpc.StatusCode.UNAUTHENTICATED
        assert refused.value.details() == "credential rejected"
        assert served == []


class TestMultitenantDeployment:
    def test_a_member_is_served_in_the_org_it_names(self, serve):
        channel, served = serve(_state(multitenancy=True))
        assert _query(channel, (*_bearer(), ("x-provisa-org", "acme"))) == [7]
        ((org_id, session_vars),) = served
        assert org_id == "acme"
        assert session_vars["region"] == "emea"

    def test_an_org_the_principal_is_not_a_member_of_is_refused(self, serve):
        channel, served = serve(_state(multitenancy=True))
        with pytest.raises(grpc.RpcError) as refused:
            _query(channel, (*_bearer(), ("x-provisa-org", "root")))
        assert refused.value.code() == grpc.StatusCode.PERMISSION_DENIED
        assert served == []

    def test_a_call_naming_no_org_is_refused_and_says_what_to_send(self, serve):
        channel, served = serve(_state(multitenancy=True))
        with pytest.raises(grpc.RpcError) as refused:
            _query(channel, _bearer())
        assert refused.value.code() == grpc.StatusCode.PERMISSION_DENIED
        assert "x-provisa-org" in (refused.value.details() or "")
        assert served == []


class _Details:
    method = "/provisa.v1.ProvisaService/QueryOrders"
    invocation_metadata = (("authorization", f"Bearer {_token()}"),)


class _Context:
    def peer(self) -> str:
        return "ipv4:127.0.0.1:50000"


@pytest.mark.unbound
def test_the_role_is_authorized_with_the_org_bound_and_the_binding_stays_with_the_rpc(monkeypatch):
    """``authorize_role_or_probe`` reads the org on the handler thread, outside the RPC's context,
    and a pool thread serves the next RPC: bound for that read, and gone from the thread after it."""
    seen: dict[str, str | None] = {}

    def _authorize(_state, _identity, _requested, **_kwargs) -> str:
        seen["authorizing"] = current_org.get()
        return "analyst"

    def behavior(_request, _context):
        async def _read() -> str | None:
            return current_org.get()

        with rpc_scope() as rpc:
            seen["rpc"] = rpc.run(_read())
        seen["thread"] = current_org.get()
        return "ok"

    monkeypatch.setattr(grpc_auth, "authorize_role_or_probe", _authorize)
    interceptor = grpc_auth.AuthInterceptor(_state())
    handler = interceptor.intercept_service(
        lambda _details: grpc.unary_unary_rpc_method_handler(behavior), _Details()
    )
    assert handler.unary_unary(None, _Context()) == "ok"
    assert seen == {"authorizing": "root", "rpc": "root", "thread": None}
    assert current_org.get() is None
