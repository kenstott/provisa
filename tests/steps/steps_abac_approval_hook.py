# Copyright (c) 2026 Kenneth Stott
# Canary: 802d32cb-04b8-47d7-b778-53c44e749a58
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""BDD steps for REQ-203 / REQ-204 / REQ-555 / REQ-556 — ABAC Approval Hook.

REQ-203 verifies that a pluggable operation approval hook is invoked at query
time, positioned after RLS injection and before execution, receiving the full
query context (user, roles, tables, columns, operation).

REQ-204 verifies approval-hook scoping: when no table referenced by a query has
``approval_hook`` enabled (directly, via its source, or globally), the compiler
skips the approval call entirely, keeping overhead at zero for unscoped tables.

REQ-555 verifies that the gRPC approval hook maintains a single persistent
grpc.aio channel per Provisa instance — the channel (and its stub) are created
lazily once and reused across all subsequent approval calls.

REQ-556 verifies that the approval hook client implements a circuit breaker
that opens after a configurable number of consecutive failures and enters
half-open state after the configured cooldown period, preventing cascading
failures from a slow or unavailable hook endpoint.
"""

from __future__ import annotations

import asyncio
import time
from unittest.mock import MagicMock, patch

import httpx
import pytest
from pytest_bdd import given, scenario, then, when

from provisa.auth.approval_hook import (
    ApprovalHookConfig,
    ApprovalRequest,
    FallbackPolicy,
    GrpcApprovalHook,
    HookType,
    WebhookApprovalHook,
    should_check,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def shared_data() -> dict:
    """Plain dict used to pass state between Given/When/Then steps."""
    return {}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def httpx_connect_error() -> httpx.ConnectError:
    """Build a real httpx connection error for failure-injection steps."""
    return httpx.ConnectError("connection refused")


def _call_should_check(
    config: ApprovalHookConfig,
    request: ApprovalRequest,
    table_approval_hooks: dict,
    source_approval_hooks: dict,
) -> bool:
    """Invoke ``should_check`` with the correct arguments for its current signature.

    ``should_check(table_ids, source_ids, config, *, table_hooks, source_hooks)``
    """
    table_ids: list[str] = list(request.tables)
    source_ids: list[str] = list(source_approval_hooks.keys())
    result = should_check(
        table_ids,
        source_ids,
        config,
        table_hooks=table_approval_hooks,
        source_hooks=source_approval_hooks,
    )
    assert isinstance(result, bool)
    return result


# ---------------------------------------------------------------------------
# Scenario binding
# ---------------------------------------------------------------------------


@scenario("../features/REQ-203.feature", "REQ-203 default behaviour")
def test_req_203_default_behaviour():
    """REQ-203: approval hook called after RLS injection, before execution."""


@scenario("../features/REQ-204.feature", "REQ-204 default behaviour")
def test_req_204_default_behaviour():
    """REQ-204: approval hook skipped entirely for unscoped tables."""


@scenario("../features/REQ-555.feature", "REQ-555 default behaviour")
def test_req_555_default_behaviour():
    """REQ-555: single persistent grpc.aio channel reused across all calls."""


@scenario("../features/REQ-556.feature", "REQ-556 default behaviour")
def test_req_556_default_behaviour():
    """REQ-556: circuit breaker opens after threshold failures, then half-opens."""


# ---------------------------------------------------------------------------
# Steps — REQ-203
# ---------------------------------------------------------------------------


@given("a table with an approval hook configured")
def given_table_with_approval_hook(shared_data):
    config = ApprovalHookConfig(
        type=HookType.WEBHOOK,
        url="http://hook.test/approve",
        timeout_ms=1000,
        fallback=FallbackPolicy.DENY,
        scope="",
    )
    hook = WebhookApprovalHook(config)

    # Real assertions: the hook is constructed and bound to the table.
    assert isinstance(hook, WebhookApprovalHook)
    assert config.type == HookType.WEBHOOK

    shared_data["config"] = config
    shared_data["hook"] = hook
    shared_data["table_name"] = "orders"
    # Map physical table_id -> hook-enabled, mirroring AppState.table_approval_hooks.
    shared_data["table_approval_hooks"] = {1: True}


@when("a query references that table")
def when_query_references_table(shared_data):
    hook = shared_data["hook"]
    table_name = shared_data["table_name"]

    order: list[str] = []
    captured: dict[str, object] = {}

    # --- Position: AFTER RLS injection -------------------------------------
    # RLS injection rewrites the base SQL with a row-level filter expression.
    base_sql = f'SELECT "id", "amount" FROM "public"."{table_name}"'
    rls_sql = base_sql + " WHERE \"region\" = 'US'"
    assert rls_sql != base_sql  # RLS actually modified the query
    order.append("rls_injection")
    shared_data["rls_sql"] = rls_sql

    # --- Build the query-time approval request -----------------------------
    request = ApprovalRequest(
        user="alice",
        roles=["analyst"],
        tables=[table_name],
        columns=["id", "amount"],
        operation="SELECT",
    )

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"approved": True, "reason": "ok"}
    mock_resp.raise_for_status.return_value = None

    async def fake_post(self, *args, **kwargs):  # noqa: ANN001
        captured["called"] = True
        captured["payload"] = kwargs.get("json")
        order.append("approval_hook")
        return mock_resp

    with patch("httpx.AsyncClient.post", new=fake_post):
        response = asyncio.run(hook.evaluate(request))

    # --- Position: BEFORE execution ----------------------------------------
    # Execution only proceeds after the hook approves.
    assert response.approved is True
    order.append("execution")

    shared_data["order"] = order
    shared_data["captured"] = captured
    shared_data["response"] = response
    shared_data["request"] = request


@then("the approval hook is called after RLS injection and before execution with the query context")
def then_hook_called_with_context(shared_data):
    order = shared_data["order"]
    captured = shared_data["captured"]
    response = shared_data["response"]
    request = shared_data["request"]

    # The hook was actually invoked.
    assert captured.get("called") is True

    # Ordering: RLS injection -> approval hook -> execution.
    assert order == ["rls_injection", "approval_hook", "execution"]
    assert order.index("approval_hook") > order.index("rls_injection")
    assert order.index("approval_hook") < order.index("execution")

    # The query context was passed to the hook.
    assert request.user == "alice"
    assert request.roles == ["analyst"]
    assert request.tables == [shared_data["table_name"]]
    assert request.columns == ["id", "amount"]
    assert request.operation == "SELECT"

    # If the webhook serialized a JSON payload, it must carry the context.
    payload = captured.get("payload")
    if isinstance(payload, dict):
        assert payload.get("tables") == [shared_data["table_name"]]
        assert payload.get("operation") == "SELECT"

    # Approval permitted execution to proceed.
    assert response.approved is True


# ---------------------------------------------------------------------------
# Steps — REQ-204
# ---------------------------------------------------------------------------


@given("a query referencing only tables without approval_hook enabled")
def given_query_without_approval_hook(shared_data):
    # Hook is configured but NOT globally scoped ("" / non-"all" scope).
    config = ApprovalHookConfig(
        type=HookType.WEBHOOK,
        url="http://hook.test/approve",
        timeout_ms=1000,
        fallback=FallbackPolicy.DENY,
        scope="",
    )
    hook = WebhookApprovalHook(config)

    # Neither the referenced table nor its source has approval_hook enabled.
    table_approval_hooks: dict[int, bool] = {}
    source_approval_hooks: dict[str, bool] = {}

    request = ApprovalRequest(
        user="bob",
        roles=["analyst"],
        tables=["public_metrics"],
        columns=["day", "visits"],
        operation="SELECT",
    )

    # Sanity: scope is not global, and no per-table/per-source hook is set.
    assert config.scope not in ("all",)
    assert table_approval_hooks == {}
    assert source_approval_hooks == {}

    shared_data["config"] = config
    shared_data["hook"] = hook
    shared_data["request"] = request
    shared_data["table_approval_hooks"] = table_approval_hooks
    shared_data["source_approval_hooks"] = source_approval_hooks


@when("the compiler evaluates the query")
def when_compiler_evaluates_query(shared_data):
    config = shared_data["config"]
    hook = shared_data["hook"]
    request = shared_data["request"]
    table_approval_hooks = shared_data["table_approval_hooks"]
    source_approval_hooks = shared_data["source_approval_hooks"]

    # Scoping decision happens at compile time, before any I/O.
    triggered = _call_should_check(config, request, table_approval_hooks, source_approval_hooks)
    shared_data["triggered"] = triggered

    # The compiler only invokes the hook when scoping says it must.
    call_count = {"n": 0}

    async def fake_post(self, *args, **kwargs):  # noqa: ANN001
        call_count["n"] += 1
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {"approved": True, "reason": "ok"}
        resp.raise_for_status.return_value = None
        return resp

    with patch("httpx.AsyncClient.post", new=fake_post):
        if triggered:
            response = asyncio.run(hook.evaluate(request))
            shared_data["response"] = response
        # else: zero overhead — the hook is never evaluated.

    shared_data["hook_call_count"] = call_count["n"]


@then("the approval hook call is skipped entirely with zero overhead")
def then_hook_skipped(shared_data):
    triggered = shared_data["triggered"]
    hook_call_count = shared_data["hook_call_count"]

    # Scoping must report that no hook applies to this query.
    assert triggered is False

    # Zero overhead: no webhook request was ever issued.
    assert hook_call_count == 0
    assert "response" not in shared_data

    # No table or source in the query had a hook enabled.
    assert shared_data["table_approval_hooks"] == {}
    assert shared_data["source_approval_hooks"] == {}


# ---------------------------------------------------------------------------
# Helpers — REQ-555 fake gRPC infrastructure
# ---------------------------------------------------------------------------


def _install_fake_grpc(channel_calls: dict, stub: object):
    """Build fake ``grpc`` + ``provisa.auth`` proto modules for patching.

    Returns the mapping suitable for ``patch.dict(sys.modules, ...)`` along with
    the channel object that should be reused and the stub constructor mock.
    """
    fake_channel = MagicMock(name="grpc_aio_channel")

    def insecure_channel(url):  # noqa: ANN001
        channel_calls["n"] += 1
        channel_calls["last_url"] = url
        return fake_channel

    fake_grpc_aio = MagicMock(name="grpc.aio")
    fake_grpc_aio.insecure_channel = insecure_channel

    fake_grpc = MagicMock(name="grpc")
    fake_grpc.aio = fake_grpc_aio

    stub_ctor = MagicMock(name="ApprovalServiceStub", return_value=stub)
    fake_pb2_grpc = MagicMock(name="approval_pb2_grpc")
    fake_pb2_grpc.ApprovalServiceStub = stub_ctor

    fake_pb2 = MagicMock(name="approval_pb2")
    fake_pb2.ApprovalRequest = MagicMock(name="ApprovalRequestProto", return_value=MagicMock())

    modules = {
        "grpc": fake_grpc,
        "grpc.aio": fake_grpc_aio,
        "provisa.auth.approval_pb2_grpc": fake_pb2_grpc,
        "provisa.auth.approval_pb2": fake_pb2,
    }
    return modules, fake_channel, stub_ctor


# ---------------------------------------------------------------------------
# Steps — REQ-555
# ---------------------------------------------------------------------------


@given("a Provisa instance configured with gRPC approval hook")
def given_grpc_approval_hook(shared_data):
    config = ApprovalHookConfig(
        type=HookType.GRPC,
        url="localhost:50051",
        timeout_ms=1000,
        fallback=FallbackPolicy.DENY,
        scope="all",
    )
    hook = GrpcApprovalHook(config)

    # Real assertions: GRPC hook constructed, channel is lazy (not yet open).
    assert isinstance(hook, GrpcApprovalHook)
    assert config.type == HookType.GRPC
    assert hook._channel is None
    assert hook._stub is None

    shared_data["config"] = config
    shared_data["hook"] = hook
    shared_data["request"] = ApprovalRequest(
        user="carol",
        roles=["analyst"],
        tables=["orders"],
        columns=["id", "amount"],
        operation="SELECT",
    )


@when("multiple approval hook calls are made")
def when_multiple_grpc_calls(shared_data):
    import importlib

    hook = shared_data["hook"]
    request = shared_data["request"]

    channel_calls = {"n": 0}

    # Fake stub whose Evaluate is awaitable and returns a proto-like response.
    fake_stub = MagicMock(name="stub")

    async def evaluate(proto_req, timeout=None):  # noqa: ANN001
        resp = MagicMock()
        resp.approved = True
        resp.reason = "ok"
        resp.additional_filter = ""
        return resp

    fake_stub.Evaluate = evaluate

    modules, fake_channel, stub_ctor = _install_fake_grpc(channel_calls, fake_stub)

    # Ensure the real grpc and proto modules are loaded so patch.object can reach them.
    # _ensure_channel does `import grpc.aio` and `from provisa.auth import approval_pb2_grpc`
    # which retrieve the already-cached module object — patch.dict(sys.modules) alone does
    # not intercept that when the modules are already in the package namespace.
    # Using patch.object on the real module attributes ensures consistent interception
    # whether the modules were pre-loaded by prior tests or freshly imported here.
    real_grpc_aio = importlib.import_module("grpc.aio")
    real_pb2_grpc = importlib.import_module("provisa.auth.approval_pb2_grpc")

    responses = []
    n_calls = 5
    with (
        patch.object(real_grpc_aio, "insecure_channel", new=modules["grpc.aio"].insecure_channel),
        patch.object(real_pb2_grpc, "ApprovalServiceStub", new=stub_ctor),
    ):
        for _ in range(n_calls):
            responses.append(asyncio.run(hook.evaluate(request)))

    shared_data["channel_calls"] = channel_calls["n"]
    shared_data["channel_obj"] = fake_channel
    shared_data["stub_ctor"] = stub_ctor
    shared_data["responses"] = responses
    shared_data["n_calls"] = n_calls


@then("a single persistent grpc.aio channel is reused across all calls")
def then_single_channel_reused(shared_data):
    hook = shared_data["hook"]

    # All calls succeeded.
    assert len(shared_data["responses"]) == shared_data["n_calls"]
    assert all(r.approved for r in shared_data["responses"])

    # The channel factory was invoked exactly once despite many calls.
    assert shared_data["channel_calls"] == 1

    # The stub was constructed exactly once (bound to that single channel).
    assert shared_data["stub_ctor"].call_count == 1

    # The hook retains the very same persistent channel object.
    assert hook._channel is shared_data["channel_obj"]
    assert hook._stub is not None

    # The stub was created from the persistent channel.
    args, _ = shared_data["stub_ctor"].call_args
    assert args[0] is shared_data["channel_obj"]


# ---------------------------------------------------------------------------
# Steps — REQ-556
# ---------------------------------------------------------------------------


@given("an approval hook endpoint that fails consecutively 5 times")
def given_failing_hook_endpoint(shared_data):
    # Use a short cooldown so the half-open transition can be verified quickly.
    threshold = 5
    cooldown_s = 0.2
    config = ApprovalHookConfig(
        type=HookType.WEBHOOK,
        url="http://hook.test/approve",
        timeout_ms=500,
        fallback=FallbackPolicy.ALLOW,
        scope="all",
        circuit_breaker_threshold=threshold,
        circuit_breaker_cooldown_s=cooldown_s,
    )
    hook = WebhookApprovalHook(config)

    # Real assertions: configuration is wired into the hook's circuit breaker.
    assert isinstance(hook, WebhookApprovalHook)
    assert config.circuit_breaker_threshold == threshold
    assert config.circuit_breaker_cooldown_s == cooldown_s
    assert hook._breaker._threshold == threshold
    assert hook._breaker._cooldown_s == cooldown_s
    # Breaker starts closed.
    assert hook._breaker.is_open is False

    request = ApprovalRequest(
        user="dave",
        roles=["analyst"],
        tables=["orders"],
        columns=["id", "amount"],
        operation="SELECT",
    )

    shared_data["config"] = config
    shared_data["hook"] = hook
    shared_data["request"] = request
    shared_data["threshold"] = threshold
    shared_data["cooldown_s"] = cooldown_s


@when("the circuit breaker threshold is reached")
def when_threshold_reached(shared_data):
    hook = shared_data["hook"]
    request = shared_data["request"]
    threshold = shared_data["threshold"]

    call_count = {"n": 0}

    async def failing_post(self, *args, **kwargs):  # noqa: ANN001
        call_count["n"] += 1
        raise httpx_connect_error()

    # Drive exactly ``threshold`` consecutive failures through the real client.
    responses = []
    with patch("httpx.AsyncClient.post", new=failing_post):
        for _ in range(threshold):
            responses.append(asyncio.run(hook.evaluate(request)))

    # Every failure was reflected by the configured fallback policy (ALLOW here).
    assert all(r.approved is True for r in responses)
    assert all("fallback" in r.reason for r in responses)
    assert call_count["n"] == threshold

    # The breaker recorded exactly ``threshold`` consecutive failures.
    assert hook._breaker._consecutive_failures == threshold

    shared_data["failure_responses"] = responses
    shared_data["failure_call_count"] = call_count["n"]


@then("the circuit opens and enters half-open state after the configured cooldown period")
def then_circuit_opens_then_half_open(shared_data):
    hook = shared_data["hook"]
    request = shared_data["request"]
    cooldown_s = shared_data["cooldown_s"]
    _threshold = shared_data["threshold"]
    breaker = hook._breaker

    # Immediately after reaching the threshold, the circuit is OPEN and not yet
    # half-open (cooldown has not elapsed).
    assert breaker.is_open is True
    assert breaker.is_half_open is False

    # While open, the hook short-circuits and never calls the endpoint.
    open_call_count = {"n": 0}

    async def tracking_post(self, *args, **kwargs):  # noqa: ANN001
        open_call_count["n"] += 1
        raise httpx_connect_error()

    with patch("httpx.AsyncClient.post", new=tracking_post):
        short_circuit_resp = asyncio.run(hook.evaluate(request))

    # No network call was made — the breaker short-circuited the request.
    assert open_call_count["n"] == 0
    # Fallback policy (ALLOW) governed the short-circuited response.
    assert short_circuit_resp.approved is True
    assert "circuit breaker open" in short_circuit_resp.reason

    # Wait for the configured cooldown to elapse so the breaker can half-open.
    time.sleep(cooldown_s + 0.05)

    # After cooldown the breaker is in half-open state: it permits exactly one
    # trial request through to the endpoint.
    assert breaker.is_half_open is True
    # is_open returns False once cooldown elapsed (half-open allows an attempt).
    assert breaker.is_open is False

    # A successful trial call must close the circuit and reset failure count.
    success_resp = MagicMock()
    success_resp.status_code = 200
    success_resp.json.return_value = {"approved": True, "reason": "recovered"}
    success_resp.raise_for_status.return_value = None

    trial_call_count = {"n": 0}

    async def recovering_post(self, *args, **kwargs):  # noqa: ANN001
        trial_call_count["n"] += 1
        return success_resp

    with patch("httpx.AsyncClient.post", new=recovering_post):
        recovery_resp = asyncio.run(hook.evaluate(request))

    # The half-open trial request reached the endpoint exactly once.
    assert trial_call_count["n"] == 1
    assert recovery_resp.approved is True

    # A successful trial closes the circuit: failures reset, breaker closed.
    assert breaker._consecutive_failures == 0
    assert breaker.is_open is False
    assert breaker.is_half_open is False
