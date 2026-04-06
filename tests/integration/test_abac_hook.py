# Copyright (c) 2026 Kenneth Stott
# Canary: d4e5f6a7-b8c9-0123-defa-234567890123
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests: ABAC approval hook — allow/deny/fallback behaviour."""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from provisa.auth.approval_hook import (
    ApprovalHookConfig,
    ApprovalRequest,
    FallbackPolicy,
    HookType,
    WebhookApprovalHook,
    create_hook,
    should_check,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

# ---------------------------------------------------------------------------
# These tests do NOT require a database connection.
# They mock httpx to avoid needing a live webhook server.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_config(
    url: str = "http://localhost:9999/approve",
    fallback: FallbackPolicy = FallbackPolicy.DENY,
    scope: str = "",
    threshold: int = 5,
    cooldown: float = 30.0,
) -> ApprovalHookConfig:
    return ApprovalHookConfig(
        type=HookType.WEBHOOK,
        url=url,
        timeout_ms=1000,
        fallback=fallback,
        scope=scope,
        circuit_breaker_threshold=threshold,
        circuit_breaker_cooldown_s=cooldown,
    )


def _make_request(
    user: str = "alice",
    roles: list[str] | None = None,
    tables: list[str] | None = None,
    columns: list[str] | None = None,
    operation: str = "SELECT",
) -> ApprovalRequest:
    return ApprovalRequest(
        user=user,
        roles=roles or ["analyst"],
        tables=tables or ["orders"],
        columns=columns or ["id", "amount"],
        operation=operation,
    )


def _mock_httpx_response(status_code: int, body: dict) -> MagicMock:
    """Build a minimal mock httpx.Response."""
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


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestABACHook:
    async def test_hook_allow_response_permits_query(self):
        """Mock webhook returning 200 approved=True must yield approved=True."""
        config = _make_config(fallback=FallbackPolicy.DENY)
        hook = WebhookApprovalHook(config)
        request = _make_request()

        mock_resp = _mock_httpx_response(200, {"approved": True, "reason": "ok"})

        with patch("httpx.AsyncClient.post", new_callable=AsyncMock, return_value=mock_resp):
            response = await hook.evaluate(request)

        assert response.approved is True
        assert response.reason == "ok"

    async def test_hook_deny_response_blocks_query(self):
        """Mock webhook returning 200 approved=False must yield approved=False."""
        config = _make_config(fallback=FallbackPolicy.ALLOW)
        hook = WebhookApprovalHook(config)
        request = _make_request()

        mock_resp = _mock_httpx_response(200, {"approved": False, "reason": "policy violation"})

        with patch("httpx.AsyncClient.post", new_callable=AsyncMock, return_value=mock_resp):
            response = await hook.evaluate(request)

        assert response.approved is False
        assert "policy violation" in response.reason

    async def test_hook_unavailable_with_allow_fallback(self):
        """When webhook raises a connection error, fallback=allow must approve."""
        config = _make_config(fallback=FallbackPolicy.ALLOW)
        hook = WebhookApprovalHook(config)
        request = _make_request()

        with patch(
            "httpx.AsyncClient.post",
            new_callable=AsyncMock,
            side_effect=httpx.ConnectError("connection refused"),
        ):
            response = await hook.evaluate(request)

        assert response.approved is True
        assert "fallback allow" in response.reason

    async def test_hook_unavailable_with_deny_fallback(self):
        """When webhook raises a connection error, fallback=deny must deny."""
        config = _make_config(fallback=FallbackPolicy.DENY)
        hook = WebhookApprovalHook(config)
        request = _make_request()

        with patch(
            "httpx.AsyncClient.post",
            new_callable=AsyncMock,
            side_effect=httpx.ConnectError("connection refused"),
        ):
            response = await hook.evaluate(request)

        assert response.approved is False
        assert "fallback deny" in response.reason

    async def test_hook_receives_correct_payload(self):
        """The payload sent to the webhook must contain role, table, operation, user."""
        config = _make_config()
        hook = WebhookApprovalHook(config)
        request = _make_request(
            user="bob",
            roles=["manager"],
            tables=["customers"],
            columns=["name", "email"],
            operation="SELECT",
        )

        captured_payload: dict = {}
        mock_resp = _mock_httpx_response(200, {"approved": True, "reason": ""})

        async def _capture_post(url, *, json=None, timeout=None):  # noqa: A002
            captured_payload.update(json or {})
            return mock_resp

        with patch("httpx.AsyncClient.post", new_callable=AsyncMock, side_effect=_capture_post):
            await hook.evaluate(request)

        assert captured_payload.get("user") == "bob"
        assert "manager" in captured_payload.get("roles", [])
        assert "customers" in captured_payload.get("tables", [])
        assert "name" in captured_payload.get("columns", [])
        assert captured_payload.get("operation") == "SELECT"

    async def test_hook_scoped_to_table(self):
        """Table-scoped hook (via should_check) only fires for that specific table."""
        config = _make_config(scope="")  # not scope="all"; relies on table_hooks

        # Hook should fire for 'orders' table
        fire_for_orders = should_check(
            table_ids=["orders"],
            source_ids=["test-pg"],
            config=config,
            table_hooks={"orders": True},
            source_hooks={},
        )
        assert fire_for_orders is True

        # Hook must NOT fire for 'customers' table
        no_fire_for_customers = should_check(
            table_ids=["customers"],
            source_ids=["test-pg"],
            config=config,
            table_hooks={"orders": True},
            source_hooks={},
        )
        assert no_fire_for_customers is False

    async def test_hook_scope_all_always_fires(self):
        """scope='all' config must trigger the hook for any table."""
        config = _make_config(scope="all")

        result = should_check(
            table_ids=["any_table"],
            source_ids=["any_source"],
            config=config,
            table_hooks={},
            source_hooks={},
        )
        assert result is True

    async def test_hook_403_response_falls_back(self):
        """A 403 HTTP error from the webhook must trigger the fallback policy."""
        config = _make_config(fallback=FallbackPolicy.ALLOW)
        hook = WebhookApprovalHook(config)
        request = _make_request()

        mock_resp = _mock_httpx_response(403, {"approved": False})

        with patch("httpx.AsyncClient.post", new_callable=AsyncMock, return_value=mock_resp):
            response = await hook.evaluate(request)

        # HTTP error triggers exception path → fallback=allow
        assert response.approved is True
        assert "fallback allow" in response.reason

    async def test_circuit_breaker_opens_after_threshold(self):
        """After exceeding failure threshold the circuit opens and uses fallback."""
        config = _make_config(
            fallback=FallbackPolicy.DENY,
            threshold=3,
            cooldown=9999.0,  # prevent half-open during this test
        )
        hook = WebhookApprovalHook(config)
        request = _make_request()

        # Trigger three consecutive failures to open the breaker
        with patch(
            "httpx.AsyncClient.post",
            new_callable=AsyncMock,
            side_effect=httpx.ConnectError("refused"),
        ):
            for _ in range(3):
                await hook.evaluate(request)

        # Now the circuit should be open; next call must return fallback without HTTP
        response = await hook.evaluate(request)
        assert response.approved is False
        assert "circuit breaker open" in response.reason

    async def test_create_hook_returns_webhook_hook(self):
        """create_hook factory must return a WebhookApprovalHook for WEBHOOK type."""
        config = _make_config()
        hook = create_hook(config)
        assert isinstance(hook, WebhookApprovalHook)
