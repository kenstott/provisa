# Copyright (c) 2026 Kenneth Stott
# Canary: 98c66b6f-b504-4cbd-a48f-faf11b4d50cf
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every entrypoint binds the org its work is for, by itself (REQ-1266).

The state refuses a per-org read with no org bound, so an entrypoint that forgot to bind would
fail rather than be served some org's runtime. These run with NOTHING bound (the unit harness's
deployment-org binding is off), so each proves its entrypoint binds without help.
"""

# Requirements: REQ-1266

from __future__ import annotations

import inspect
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest

from provisa.core.request_context import current_org

pytestmark = pytest.mark.unbound


@pytest.fixture()
def pgwire_loop():
    """This test thread's connection loop, bound as ProvisaHandler.handle binds one (REQ-1882)."""
    from provisa.core.connection_loop import connection_loop

    with connection_loop() as cl:
        yield cl


def test_nothing_is_bound_here():
    assert current_org.get() is None


# --- HTTP ----------------------------------------------------------------------------------------


def _routing_middleware():
    from provisa.api import app as app_mod

    held = app_mod.state.auth_config
    app_mod.state.auth_config = None
    try:
        the_app = app_mod.create_app()
    finally:
        app_mod.state.auth_config = held
    (cls,) = [
        m.cls
        for m in the_app.user_middleware
        if getattr(m.cls, "__name__", "") == "_OrgRoutingMiddleware"
    ]
    return cast("Any", cls)


def _with_runtime(registry, org_id: str):
    """A copy of ``registry`` that also holds a built runtime for ``org_id``."""
    from provisa.api.org_runtime import OrgRegistry, OrgRuntime

    copy = OrgRegistry()
    for key in registry.all_org_ids():
        rt = registry.get(key)
        assert rt is not None
        copy.set(key, rt)
    copy.set(org_id, OrgRuntime(org_id=org_id))
    return copy


async def _route(monkeypatch, active_org: str | None) -> list[str | None]:
    from provisa.api.app import state
    from provisa.core.environments import PROD

    if active_org is not None and state.org_registry.get(active_org) is None:
        # The named org's runtime, as ensure_org_runtime builds it (stubbed below).
        monkeypatch.setattr(state, "org_registry", _with_runtime(state.org_registry, active_org))

    async def _prod(*_a, **_k):
        return PROD

    async def _built(_org_id, _env=None):
        return None

    monkeypatch.setattr("provisa.api.env_routing.resolve_selected_env", _prod)
    monkeypatch.setattr("provisa.api.app.ensure_org_runtime", _built)
    seen: list[str | None] = []

    async def _inner(_scope, _receive, _send):
        seen.append(current_org.get())

    scope = {"type": "http", "path": "/data/graphql", "headers": [], "state": {}}
    if active_org is not None:
        scope["state"]["active_org_id"] = active_org
    await _routing_middleware()(_inner)(scope, None, None)
    return seen


async def test_http_routing_binds_the_deployments_own_org(monkeypatch):
    from provisa.api.app import state

    assert await _route(monkeypatch, state.org_id) == [state.org_id]
    assert current_org.get() is None  # unbound again once the request is served


async def test_http_routing_binds_a_tenant_org(monkeypatch):
    assert await _route(monkeypatch, "acme") == ["acme"]


async def test_http_routing_serves_a_request_with_no_org_unbound(monkeypatch):
    assert await _route(monkeypatch, None) == [None]


async def test_auth_middleware_reads_the_platform_plane_in_the_deployment_org(monkeypatch):
    from starlette.requests import Request

    from provisa.auth.middleware import AuthMiddleware

    mw = AuthMiddleware(None, default_org_id="root")
    seen: list[str | None] = []

    async def _resolve(_self, _request):
        seen.append(current_org.get())

    monkeypatch.setattr(AuthMiddleware, "_resolve_identity", _resolve)
    await mw._process(Request({"type": "http", "path": "/data/graphql", "headers": []}))
    assert seen == ["root"]
    assert current_org.get() is None


# --- lifespan ------------------------------------------------------------------------------------


def test_the_boot_binds_the_deployment_org_before_it_builds_and_unbinds_at_shutdown():
    from provisa.api import app as app_mod

    src = inspect.getsource(app_mod.lifespan)
    bind = src.index("_boot_org_token = set_current_org(_scope)")
    assert bind < src.index("await _once_per_launch()")
    assert bind < src.index("await _load_and_build(apply=False)")
    assert bind < src.index("await _start_background_tasks(_log)")
    assert src.rstrip().endswith("reset_current_org(_boot_org_token)")


# --- pgwire --------------------------------------------------------------------------------------


def _pg_handler():
    from provisa.pgwire.server import ProvisaHandler

    handler = object.__new__(ProvisaHandler)
    handler.wfile = MagicMock()
    handler.send_authentication_ok = MagicMock()
    handler.handle_post_auth = MagicMock()
    handler._send_pg_error = MagicMock()
    handler._sasl_offered = False
    handler._meter = MagicMock()
    return handler


def _pg_ctx(user: str):
    from provisa.pgwire.server import ProvisaSession

    ctx = MagicMock()
    ctx.params = {"user": user}
    ctx.session = ProvisaSession()
    return ctx


def test_an_unsecured_pgwire_session_binds_the_deployment_org():
    state = MagicMock(org_id="root", multitenancy=False, auth_middleware_active=False)
    state.auth_config = {"provider": "none"}
    ctx = _pg_ctx("analyst")
    with patch("provisa.pgwire.server.state", state):
        _pg_handler().handle_md5_password(ctx, b"x\x00")
    assert ctx.session.org_id == "root"
    assert ctx.session._with_org(current_org.get) == "root"
    assert current_org.get() is None


def test_an_authenticated_single_tenant_pgwire_session_binds_the_deployment_org(pgwire_loop):
    from provisa.auth.models import AuthIdentity

    class _Provider:
        async def validate_token(self, _token):
            return AuthIdentity(
                user_id="alice", email=None, display_name="alice", roles=[], raw_claims={}
            )

    state = MagicMock(org_id="root", multitenancy=False, auth_middleware_active=True)
    state.auth_config = {"provider": "oidc", "role_mapping": [], "default_role": "viewer"}
    ctx = _pg_ctx("alice")
    with (
        patch("provisa.pgwire.server.state", state),
        patch("provisa.auth.wiring.build_auth_provider", return_value=_Provider()),
    ):
        _pg_handler().handle_md5_password(ctx, b"token\x00")
    assert ctx.session.org_id == "root"


async def test_pgwire_runs_a_sessions_coroutine_in_its_org():
    from provisa.pgwire.server import _run_with_org

    async def _org():
        return current_org.get()

    assert await _run_with_org("acme", _org()) == "acme"


# --- bolt ----------------------------------------------------------------------------------------


def _bolt_session(org_id: str | None):
    from provisa.bolt.session import BoltSession

    session = BoltSession.__new__(BoltSession)
    session.org_id = org_id
    session.roles = ["analyst", "steward"]
    return session


async def test_bolt_authenticates_in_the_deployment_org(monkeypatch):
    from provisa.bolt.session import BoltSession

    seen: list[str | None] = []

    async def _platform(_self, _scheme, _principal, _credentials):
        seen.append(current_org.get())

    monkeypatch.setattr(BoltSession, "_resolve_platform_user", _platform)
    monkeypatch.setattr("provisa.api.app.state", SimpleNamespace(org_id="root"), raising=False)
    await _bolt_session(None)._resolve_user("basic", "u", "p")
    assert seen == ["root"]
    assert current_org.get() is None


def test_bolt_builds_a_meta_role_in_the_sessions_org(monkeypatch):
    seen: list[str | None] = []

    def _resolve(_state, _held, named):
        seen.append(current_org.get())
        return named

    monkeypatch.setattr("provisa.security.meta_role.resolve_requested_role", _resolve)
    _bolt_session("acme")._meta_role(object(), "analyst,steward")
    assert seen == ["acme"]


async def test_a_single_tenant_bolt_session_resolves_the_deployment_org(monkeypatch):
    session = _bolt_session(None)
    session._org_resolved = False
    session.user_id = "u1"
    session._credential_org = None
    monkeypatch.setattr(session, "_requested_org", lambda: None)
    monkeypatch.setattr(
        "provisa.api.app.state",
        SimpleNamespace(org_id="root", multitenancy=False, platform_roles={}),
        raising=False,
    )
    await session._ensure_org()
    assert session.org_id == "root"


# --- flight --------------------------------------------------------------------------------------


def _flight(multitenancy: bool):
    from provisa.api.flight.server import ProvisaFlightServer

    server = ProvisaFlightServer.__new__(ProvisaFlightServer)
    server._state = cast("Any", SimpleNamespace(org_id="root", multitenancy=multitenancy))
    return server


def test_a_single_tenant_flight_ticket_binds_the_deployment_org():
    from provisa.core.request_context import reset_current_org

    token = _flight(False)._resolve_and_bind_org({})
    try:
        assert current_org.get() == "root"
    finally:
        reset_current_org(token)


def test_flight_catalog_metadata_is_answered_in_the_one_org_and_refused_under_multitenancy():
    import pyarrow.flight as flight

    assert _flight(False)._in_catalog_org(current_org.get) == "root"
    assert current_org.get() is None
    with pytest.raises(flight.FlightServerError, match="names no org under multitenancy"):
        _flight(True)._in_catalog_org(current_org.get)


# --- gRPC ----------------------------------------------------------------------------------------


async def test_a_single_tenant_rpc_binds_the_deployment_org():
    from provisa.core.request_context import reset_current_org
    from provisa.grpc.server import ProvisaServicer

    servicer = ProvisaServicer(SimpleNamespace(org_id="root", multitenancy=False), None, None)
    token = await servicer._bind_org({})
    try:
        assert current_org.get() == "root"
    finally:
        reset_current_org(token)


# --- MCP -----------------------------------------------------------------------------------------


async def test_a_loopback_mcp_request_binds_the_deployment_org():
    from provisa.api.mcp.server import _wrap_role_auth

    seen: list[str | None] = []

    async def _app(_scope, _receive, _send):
        seen.append(current_org.get())

    wrapped = _wrap_role_auth(_app, SimpleNamespace(org_id="root"), require_token=False)
    await wrapped({"type": "http", "headers": []}, None, None)
    assert seen == ["root"]
    assert current_org.get() is None


async def test_a_single_tenant_mcp_identity_resolves_the_deployment_org():
    from provisa.api.mcp.server import _org_for_identity

    assert await _org_for_identity(object(), SimpleNamespace(org_id="root")) == "root"


# --- engine wake ---------------------------------------------------------------------------------


async def test_the_query_path_wake_refuses_work_bound_to_no_org(monkeypatch):
    from provisa.federation import engine_wake

    monkeypatch.setattr(engine_wake.k8s, "provisioning_available", lambda: True)
    with pytest.raises(RuntimeError, match="No active org bound"):
        await engine_wake.ensure_engine_awake(SimpleNamespace(org_id="root"))


async def test_a_prewarm_binds_the_org_it_was_given(monkeypatch):
    import asyncio

    from provisa.federation import engine_wake

    seen: list[str | None] = []

    async def _wake(_state):
        seen.append(current_org.get())

    monkeypatch.setattr(engine_wake.k8s, "provisioning_available", lambda: True)
    monkeypatch.setattr(engine_wake, "ensure_engine_awake", _wake)
    monkeypatch.setattr(engine_wake, "_prewarm_tasks", {})
    fut = engine_wake.prewarm_engine(SimpleNamespace(org_id="root"), "acme")
    assert fut is not None
    await asyncio.wrap_future(fut)
    assert seen == ["acme"]


# --- replica runner and event wiring -------------------------------------------------------------


def test_the_build_runner_refuses_to_wire_with_no_org_bound(tmp_path):
    from provisa.core import process_mode
    from provisa.federation import replica_builds

    process_mode.set_mode(process_mode.EVERY)
    with pytest.raises(RuntimeError, match="No active org bound"):
        replica_builds.wire_replica_runner(
            SimpleNamespace(add_job=lambda *a, **k: None),
            state=object(),
            platform_url=f"sqlite+pysqlite:///{tmp_path / 'cp.db'}",
        )


async def test_each_build_pass_binds_the_org_it_was_wired_for(monkeypatch, tmp_path):
    from provisa.core import process_mode
    from provisa.core.request_context import reset_current_org, set_current_org
    from provisa.federation import replica_builds

    seen: list[str | None] = []

    class _Runner:
        async def run_pass(self):
            seen.append(current_org.get())

    jobs: dict = {}
    monkeypatch.setattr(replica_builds, "make_runner", lambda *_a: _Runner())
    monkeypatch.setattr(replica_builds, "_runners", {})
    process_mode.set_mode(process_mode.EVERY)
    token = set_current_org("acme")
    try:
        replica_builds.wire_replica_runner(
            SimpleNamespace(add_job=lambda func, **kw: jobs.update({kw["id"]: func})),
            state=object(),
            platform_url=f"sqlite+pysqlite:///{tmp_path / 'cp.db'}",
        )
    finally:
        reset_current_org(token)
    await jobs["replica:builds:org_acme"]()  # the scheduler fires it with nothing bound
    assert seen == ["acme"]


def test_the_delta_lock_wiring_refuses_with_no_org_bound():
    from provisa.events.app_wiring import replica_write_lock_factory

    with pytest.raises(RuntimeError, match="No active org bound"):
        replica_write_lock_factory(SimpleNamespace(org_id="root"))
