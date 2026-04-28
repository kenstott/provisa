# Copyright (c) 2026 Kenneth Stott
# Canary: f1a2b3c4-d5e6-7890-abcd-ef1234567890
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Extended ABAC approval hook tests: gRPC transport, unix_socket transport,
timeout/circuit-breaker half-open recovery, and create_hook factory coverage.

These tests do NOT duplicate the webhook-only scenarios already in
tests/unit/test_abac_hook.py.  They focus on:
  - GrpcApprovalHook approve / deny / fallback paths
  - UnixSocketApprovalHook approve / deny / connection-error fallback
  - Timeout treated as a failure (fallback applied)
  - CircuitBreaker half-open allows one probe, then resets on success
  - create_hook factory for GRPC and UNIX_SOCKET types
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from provisa.auth.approval_hook import (
    ApprovalHookConfig,
    ApprovalRequest,
    ApprovalResponse,
    CircuitBreaker,
    FallbackPolicy,
    GrpcApprovalHook,
    HookType,
    UnixSocketApprovalHook,
    WebhookApprovalHook,
    create_hook,
)

pytestmark = [pytest.mark.asyncio(loop_scope="session")]

# ---------------------------------------------------------------------------
# Helpers shared across the module
# ---------------------------------------------------------------------------


def _grpc_config(
    url: str = "localhost:50051",
    fallback: FallbackPolicy = FallbackPolicy.DENY,
    threshold: int = 5,
    cooldown: float = 9999.0,
) -> ApprovalHookConfig:
    return ApprovalHookConfig(
        type=HookType.GRPC,
        url=url,
        timeout_ms=1000,
        fallback=fallback,
        circuit_breaker_threshold=threshold,
        circuit_breaker_cooldown_s=cooldown,
    )


def _unix_config(
    socket_path: str = "/var/run/opa.sock",
    url: str = "http://localhost/evaluate",
    fallback: FallbackPolicy = FallbackPolicy.DENY,
    threshold: int = 5,
    cooldown: float = 9999.0,
) -> ApprovalHookConfig:
    return ApprovalHookConfig(
        type=HookType.UNIX_SOCKET,
        socket_path=socket_path,
        url=url,
        timeout_ms=1000,
        fallback=fallback,
        circuit_breaker_threshold=threshold,
        circuit_breaker_cooldown_s=cooldown,
    )


def _request(
    user: str = "alice",
    roles: list[str] | None = None,
    tables: list[str] | None = None,
    operation: str = "SELECT",
) -> ApprovalRequest:
    return ApprovalRequest(
        user=user,
        roles=roles or ["analyst"],
        tables=tables or ["orders"],
        columns=["id", "amount"],
        operation=operation,
    )


def _mock_http_response(status_code: int, body: dict) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = body
    if status_code >= 400:
        resp.raise_for_status.side_effect = httpx.HTTPStatusError(
            message=f"HTTP {status_code}",
            request=MagicMock(),
            response=resp,
        )
    else:
        resp.raise_for_status.return_value = None
    return resp


def _mock_proto_response(approved: bool, reason: str = "") -> MagicMock:
    """Minimal object that looks like a protobuf ApprovalResponse."""
    resp = MagicMock()
    resp.approved = approved
    resp.reason = reason
    return resp


# ---------------------------------------------------------------------------
# gRPC transport tests
# ---------------------------------------------------------------------------


class TestGrpcApprovalHook:
    async def test_grpc_approved_response(self):
        """gRPC stub returning approved=True must yield approved=True."""
        config = _grpc_config(fallback=FallbackPolicy.DENY)
        hook = GrpcApprovalHook(config)

        proto_resp = _mock_proto_response(approved=True, reason="policy ok")

        stub_mock = MagicMock()
        stub_mock.Evaluate = AsyncMock(return_value=proto_resp)

        with (
            patch("grpc.aio.insecure_channel", return_value=MagicMock()),
            patch("provisa.auth.approval_pb2_grpc.ApprovalServiceStub", return_value=stub_mock),
        ):
            response = await hook.evaluate(_request())

        assert response.approved is True
        assert response.reason == "policy ok"

    async def test_grpc_denied_response(self):
        """gRPC stub returning approved=False must yield approved=False."""
        config = _grpc_config(fallback=FallbackPolicy.ALLOW)
        hook = GrpcApprovalHook(config)

        proto_resp = _mock_proto_response(approved=False, reason="denied by opa")

        stub_mock = MagicMock()
        stub_mock.Evaluate = AsyncMock(return_value=proto_resp)

        with (
            patch("grpc.aio.insecure_channel", return_value=MagicMock()),
            patch("provisa.auth.approval_pb2_grpc.ApprovalServiceStub", return_value=stub_mock),
        ):
            response = await hook.evaluate(_request())

        assert response.approved is False
        assert "denied by opa" in response.reason

    async def test_grpc_connection_error_deny_fallback(self):
        """gRPC connection error with fallback=deny must return approved=False."""
        config = _grpc_config(fallback=FallbackPolicy.DENY)
        hook = GrpcApprovalHook(config)

        # Simulate stub raising a generic RPC error
        stub_mock = MagicMock()
        stub_mock.Evaluate = AsyncMock(side_effect=RuntimeError("grpc unavailable"))

        with (
            patch("grpc.aio.insecure_channel", return_value=MagicMock()),
            patch("provisa.auth.approval_pb2_grpc.ApprovalServiceStub", return_value=stub_mock),
        ):
            response = await hook.evaluate(_request())

        assert response.approved is False
        assert "fallback deny" in response.reason

    async def test_grpc_connection_error_allow_fallback(self):
        """gRPC connection error with fallback=allow must return approved=True."""
        config = _grpc_config(fallback=FallbackPolicy.ALLOW)
        hook = GrpcApprovalHook(config)

        stub_mock = MagicMock()
        stub_mock.Evaluate = AsyncMock(side_effect=RuntimeError("grpc unavailable"))

        with (
            patch("grpc.aio.insecure_channel", return_value=MagicMock()),
            patch("provisa.auth.approval_pb2_grpc.ApprovalServiceStub", return_value=stub_mock),
        ):
            response = await hook.evaluate(_request())

        assert response.approved is True
        assert "fallback allow" in response.reason

    async def test_grpc_sends_correct_payload_fields(self):
        """The gRPC stub must receive a proto request with the correct field values."""
        config = _grpc_config()
        hook = GrpcApprovalHook(config)

        captured_req = {}
        proto_resp = _mock_proto_response(approved=True, reason="")

        async def _capture_evaluate(proto_req, timeout=None):
            captured_req["user"] = proto_req.user
            captured_req["roles"] = list(proto_req.roles)
            captured_req["tables"] = list(proto_req.tables)
            captured_req["operation"] = proto_req.operation
            return proto_resp

        stub_mock = MagicMock()
        stub_mock.Evaluate = _capture_evaluate

        with (
            patch("grpc.aio.insecure_channel", return_value=MagicMock()),
            patch("provisa.auth.approval_pb2_grpc.ApprovalServiceStub", return_value=stub_mock),
        ):
            await hook.evaluate(_request(user="bob", roles=["manager"], tables=["customers"]))

        assert captured_req["user"] == "bob"
        assert "manager" in captured_req["roles"]
        assert "customers" in captured_req["tables"]
        assert captured_req["operation"] == "SELECT"

    async def test_grpc_circuit_breaker_opens_after_threshold(self):
        """After exceeding the failure threshold the circuit opens for gRPC."""
        config = _grpc_config(fallback=FallbackPolicy.ALLOW, threshold=2, cooldown=9999.0)
        hook = GrpcApprovalHook(config)

        stub_mock = MagicMock()
        stub_mock.Evaluate = AsyncMock(side_effect=RuntimeError("down"))

        with (
            patch("grpc.aio.insecure_channel", return_value=MagicMock()),
            patch("provisa.auth.approval_pb2_grpc.ApprovalServiceStub", return_value=stub_mock),
        ):
            for _ in range(2):
                await hook.evaluate(_request())

        # Circuit is now open; evaluate must short-circuit to fallback
        response = await hook.evaluate(_request())
        assert response.approved is True
        assert "circuit breaker open" in response.reason


# ---------------------------------------------------------------------------
# Unix socket transport tests
# ---------------------------------------------------------------------------


class TestUnixSocketApprovalHook:
    async def test_unix_socket_approved(self):
        """Unix-socket hook returning approved=True must yield approved=True."""
        config = _unix_config(fallback=FallbackPolicy.DENY)
        hook = UnixSocketApprovalHook(config)

        mock_resp = _mock_http_response(200, {"approved": True, "reason": "opa ok"})

        with patch("httpx.AsyncClient.post", new_callable=AsyncMock, return_value=mock_resp):
            response = await hook.evaluate(_request())

        assert response.approved is True
        assert response.reason == "opa ok"

    async def test_unix_socket_denied(self):
        """Unix-socket hook returning approved=False must yield approved=False."""
        config = _unix_config(fallback=FallbackPolicy.ALLOW)
        hook = UnixSocketApprovalHook(config)

        mock_resp = _mock_http_response(200, {"approved": False, "reason": "rule violation"})

        with patch("httpx.AsyncClient.post", new_callable=AsyncMock, return_value=mock_resp):
            response = await hook.evaluate(_request())

        assert response.approved is False
        assert "rule violation" in response.reason

    async def test_unix_socket_connection_error_deny_fallback(self):
        """Connection error over unix socket with fallback=deny must deny."""
        config = _unix_config(fallback=FallbackPolicy.DENY)
        hook = UnixSocketApprovalHook(config)

        with patch(
            "httpx.AsyncClient.post",
            new_callable=AsyncMock,
            side_effect=httpx.ConnectError("socket not found"),
        ):
            response = await hook.evaluate(_request())

        assert response.approved is False
        assert "fallback deny" in response.reason

    async def test_unix_socket_connection_error_allow_fallback(self):
        """Connection error over unix socket with fallback=allow must allow."""
        config = _unix_config(fallback=FallbackPolicy.ALLOW)
        hook = UnixSocketApprovalHook(config)

        with patch(
            "httpx.AsyncClient.post",
            new_callable=AsyncMock,
            side_effect=httpx.ConnectError("socket not found"),
        ):
            response = await hook.evaluate(_request())

        assert response.approved is True
        assert "fallback allow" in response.reason

    async def test_unix_socket_uses_configured_path(self):
        """The transport is created with the configured socket path."""
        config = _unix_config(socket_path="/run/my-opa.sock")
        hook = UnixSocketApprovalHook(config)

        assert hook._socket_path == "/run/my-opa.sock"

    async def test_unix_socket_uses_default_url_when_not_set(self):
        """When url is not set, the default localhost evaluate URL is used."""
        cfg = ApprovalHookConfig(
            type=HookType.UNIX_SOCKET,
            socket_path="/run/opa.sock",
            url="",
            timeout_ms=500,
            fallback=FallbackPolicy.DENY,
        )
        hook = UnixSocketApprovalHook(cfg)
        assert hook._url == "http://localhost/evaluate"

    async def test_unix_socket_circuit_breaker_opens(self):
        """Unix socket circuit breaker opens after consecutive failures."""
        config = _unix_config(fallback=FallbackPolicy.DENY, threshold=2, cooldown=9999.0)
        hook = UnixSocketApprovalHook(config)

        with patch(
            "httpx.AsyncClient.post",
            new_callable=AsyncMock,
            side_effect=httpx.ConnectError("down"),
        ):
            for _ in range(2):
                await hook.evaluate(_request())

        response = await hook.evaluate(_request())
        assert response.approved is False
        assert "circuit breaker open" in response.reason


# ---------------------------------------------------------------------------
# Timeout as a failure (fallback applied)
# ---------------------------------------------------------------------------


class TestApprovalHookTimeout:
    async def test_webhook_timeout_triggers_deny_fallback(self):
        """ReadTimeout from httpx must be treated as a failure and use fallback=deny."""
        config = ApprovalHookConfig(
            type=HookType.WEBHOOK,
            url="http://localhost:9999/approve",
            timeout_ms=100,
            fallback=FallbackPolicy.DENY,
        )
        hook = WebhookApprovalHook(config)

        with patch(
            "httpx.AsyncClient.post",
            new_callable=AsyncMock,
            side_effect=httpx.ReadTimeout("timed out"),
        ):
            response = await hook.evaluate(_request())

        assert response.approved is False
        assert "fallback deny" in response.reason

    async def test_webhook_timeout_triggers_allow_fallback(self):
        """ReadTimeout from httpx must be treated as a failure and use fallback=allow."""
        config = ApprovalHookConfig(
            type=HookType.WEBHOOK,
            url="http://localhost:9999/approve",
            timeout_ms=100,
            fallback=FallbackPolicy.ALLOW,
        )
        hook = WebhookApprovalHook(config)

        with patch(
            "httpx.AsyncClient.post",
            new_callable=AsyncMock,
            side_effect=httpx.ReadTimeout("timed out"),
        ):
            response = await hook.evaluate(_request())

        assert response.approved is True
        assert "fallback allow" in response.reason


# ---------------------------------------------------------------------------
# CircuitBreaker half-open recovery (synchronous — no event loop needed)
# ---------------------------------------------------------------------------


class TestCircuitBreakerHalfOpen:
    async def test_half_open_after_cooldown(self):
        """After the cooldown period the circuit enters half-open state."""
        import time

        breaker = CircuitBreaker(threshold=2, cooldown_s=0.01)
        breaker.record_failure()
        breaker.record_failure()
        # Circuit is now open
        assert breaker.is_open

        # Wait for cooldown to elapse
        time.sleep(0.05)
        # Should now be half-open (is_open returns False, is_half_open returns True)
        assert not breaker.is_open
        assert breaker.is_half_open

    async def test_success_after_half_open_resets_breaker(self):
        """A successful probe after half-open must reset the circuit to closed."""
        import time

        breaker = CircuitBreaker(threshold=2, cooldown_s=0.01)
        breaker.record_failure()
        breaker.record_failure()
        time.sleep(0.05)

        # Probe succeeds
        breaker.record_success()
        assert not breaker.is_open
        assert not breaker.is_half_open

    async def test_failure_while_closed_does_not_open_early(self):
        """Failures below threshold must not open the circuit."""
        breaker = CircuitBreaker(threshold=5, cooldown_s=30.0)
        for _ in range(4):
            breaker.record_failure()
        assert not breaker.is_open

    async def test_success_resets_consecutive_failure_count(self):
        """A success before threshold is reached resets the failure counter."""
        breaker = CircuitBreaker(threshold=3, cooldown_s=30.0)
        breaker.record_failure()
        breaker.record_failure()
        breaker.record_success()
        # Counter must be reset; one more failure should not open the circuit
        breaker.record_failure()
        assert not breaker.is_open


# ---------------------------------------------------------------------------
# create_hook factory — GRPC and UNIX_SOCKET types (synchronous)
# ---------------------------------------------------------------------------


class TestCreateHookFactory:
    async def test_create_grpc_hook(self):
        """create_hook factory must return GrpcApprovalHook for GRPC type."""
        config = _grpc_config()
        hook = create_hook(config)
        assert isinstance(hook, GrpcApprovalHook)

    async def test_create_unix_socket_hook(self):
        """create_hook factory must return UnixSocketApprovalHook for UNIX_SOCKET type."""
        config = _unix_config()
        hook = create_hook(config)
        assert isinstance(hook, UnixSocketApprovalHook)

    async def test_create_webhook_hook(self):
        """create_hook factory must return WebhookApprovalHook for WEBHOOK type."""
        config = ApprovalHookConfig(
            type=HookType.WEBHOOK,
            url="http://localhost:9999/approve",
            timeout_ms=500,
            fallback=FallbackPolicy.DENY,
        )
        hook = create_hook(config)
        assert isinstance(hook, WebhookApprovalHook)

    async def test_create_hook_unknown_type_raises(self):
        """An unrecognised hook type must raise ValueError."""
        config = ApprovalHookConfig(
            type=HookType.WEBHOOK,
            url="http://x",
            timeout_ms=500,
            fallback=FallbackPolicy.DENY,
        )
        # Patch the type attribute directly to simulate an unknown enum value
        config.type = "unknown_type"  # type: ignore[assignment]
        with pytest.raises((ValueError, AttributeError)):
            create_hook(config)
