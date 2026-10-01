# Copyright (c) 2026 Kenneth Stott
# Canary: 8b4e1f26-3d7a-4c95-a0e2-5f9c6d1b3a78
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every transport is audited with or without authentication (REQ-074/REQ-1386, amended
2026-10-01).

On a deployment with no auth provider, HTTP attributed its statements to the ``anonymous`` dev
principal and pgwire to the startup user name — but Flight, gRPC and a Bolt connection that named
no principal bound no audit identity at all, and a statement with no identity writes no row. They
now bind the same ``anonymous`` principal, under their own surface.
"""

# Requirements: REQ-074, REQ-1386

from __future__ import annotations

import inspect
from types import SimpleNamespace

import grpc

from provisa.audit.context import ANONYMOUS_USER, AuditIdentity, current_audit_identity


def test_the_anonymous_principal_is_the_one_http_uses():
    import provisa.auth.middleware as middleware

    assert ANONYMOUS_USER == "anonymous"
    assert f'user_id="{ANONYMOUS_USER}"' in inspect.getsource(middleware)


def _unsecured_state():
    # No auth_config and no auth middleware: the deployment authenticates nobody.
    return SimpleNamespace(auth_config=None, auth_middleware_active=False, config=None)


def _identity_in_the_rpc() -> AuditIdentity | None:
    """The audit identity as the RPC's own coroutines see it (they run in the RPC's context)."""
    from provisa.grpc.rpc_scope import rpc_scope

    async def _read() -> AuditIdentity | None:
        return current_audit_identity()

    with rpc_scope() as rpc:
        return rpc.run(_read())


def _intercepted(handler):
    from provisa.grpc.auth import AuthInterceptor

    details = SimpleNamespace(
        method="/provisa.v1.ProvisaService/QueryOrders", invocation_metadata=()
    )
    return AuthInterceptor(_unsecured_state()).intercept_service(lambda _details: handler, details)


def test_an_unsecured_grpc_unary_rpc_runs_under_the_anonymous_principal():
    seen: list[AuditIdentity | None] = []

    def behavior(request, context):
        seen.append(_identity_in_the_rpc())
        return "ok"

    handler = _intercepted(grpc.unary_unary_rpc_method_handler(behavior))
    assert handler.unary_unary("req", None) == "ok"
    assert seen == [AuditIdentity(user_id="anonymous", surface="grpc")]
    assert current_audit_identity() is None  # bound in the RPC's context, not on this thread


def test_an_unsecured_grpc_streaming_rpc_runs_under_the_anonymous_principal():
    seen: list[AuditIdentity | None] = []

    def behavior(request, context):
        seen.append(_identity_in_the_rpc())
        yield "a"
        seen.append(_identity_in_the_rpc())
        yield "b"

    handler = _intercepted(grpc.unary_stream_rpc_method_handler(behavior))
    assert list(handler.unary_stream("req", None)) == ["a", "b"]
    assert seen == [AuditIdentity(user_id="anonymous", surface="grpc")] * 2


def test_an_unknown_grpc_method_is_still_unknown():
    assert _intercepted(None) is None


def test_flight_binds_a_principal_for_every_do_get():
    """do_get scopes the request to the validated identity, or to the anonymous principal when
    the deployment authenticates nobody — there is no unaudited branch."""
    import provisa.api.flight.server as flight_server

    source = inspect.getsource(flight_server.ProvisaFlightServer._do_get_on_loop)
    assert "identity.user_id if identity is not None else ANONYMOUS_USER" in source
    assert source.count("return self._do_get_inner(request, ticket)") == 1
    before = source[: source.index("return self._do_get_inner(request, ticket)")]
    assert before.rstrip().endswith('with audit_identity_scope(user_id, "flight"):')


def test_bolt_binds_the_anonymous_principal_when_the_client_named_none():
    import provisa.bolt.session as session

    source = inspect.getsource(session)
    assert 'audit_identity_scope(self.user_id or ANONYMOUS_USER, "bolt")' in source
