# Copyright (c) 2026 Kenneth Stott
# Canary: f8a2c5d1-b9e4-4a73-c7f2-1d3e6b8a9c47
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Tests for fail-fast error handling (REQ-064).

Provisa must never swallow errors silently in the query pipeline.
All governance, rights, ceiling, and execution errors must propagate.
Scheduled and event-delivery contexts have documented log-and-continue
behaviour, tested separately to confirm the contract is intentional.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from provisa.security.rights import (
    Capability,
    InsufficientRightsError,
    check_capability,
)
from provisa.webhooks.executor import execute_webhook


# ── Insufficient rights errors propagate ─────────────────────────────────────


class TestInsufficientRightsErrorPropagates:
    def test_missing_capability_raises(self):
        """Role without required capability raises InsufficientRightsError."""
        role = {"id": "analyst", "capabilities": ["query_development"]}
        with pytest.raises(InsufficientRightsError) as exc_info:
            check_capability(role, Capability.APPROVE_VIEW)
        assert exc_info.value.role_id == "analyst"
        assert exc_info.value.required == Capability.APPROVE_VIEW

    def test_present_capability_does_not_raise(self):
        """Role with the required capability does not raise."""
        role = {"id": "steward", "capabilities": ["approve_view"]}
        result = check_capability(role, Capability.APPROVE_VIEW)
        assert result is None

    def test_admin_capability_satisfies_any_requirement(self):
        """Admin capability acts as a wildcard — satisfies any check."""
        role = {"id": "superuser", "capabilities": ["admin"]}
        for cap in Capability:
            result = check_capability(role, cap)
            assert result is None

    def test_empty_capabilities_raises_for_any_requirement(self):
        """Role with no capabilities raises for every capability check."""
        role = {"id": "noop", "capabilities": []}
        for cap in (Capability.QUERY_DEVELOPMENT, Capability.APPROVE_VIEW, Capability.ADMIN):
            with pytest.raises(InsufficientRightsError):
                check_capability(role, cap)

    def test_error_message_includes_role_and_capability(self):
        """InsufficientRightsError message is self-describing."""
        role = {"id": "guest", "capabilities": []}
        with pytest.raises(InsufficientRightsError, match="guest"):
            check_capability(role, Capability.SOURCE_REGISTRATION)


# ── Webhook executor raises on HTTP errors ────────────────────────────────────


class TestWebhookExecutorFailFast:
    @pytest.mark.asyncio
    async def test_4xx_response_raises_http_status_error(self):
        """execute_webhook raises httpx.HTTPStatusError on 4xx response (REQ-064)."""
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Not Found", request=MagicMock(), response=mock_response
        )

        with patch("httpx.AsyncClient") as mock_cls:
            mock_client = AsyncMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client.request = AsyncMock(return_value=mock_response)
            mock_cls.return_value = mock_client

            from provisa.core.models import Webhook

            webhook = Webhook(
                name="test-hook",
                url="https://example.com/hook",
                method="POST",
                timeout_ms=5000,
            )

            with pytest.raises(httpx.HTTPStatusError):
                await execute_webhook(webhook, {"arg": "value"})

    @pytest.mark.asyncio
    async def test_5xx_response_raises_http_status_error(self):
        """execute_webhook raises httpx.HTTPStatusError on 5xx response (REQ-064)."""
        mock_response = MagicMock()
        mock_response.status_code = 503
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Service Unavailable", request=MagicMock(), response=mock_response
        )

        with patch("httpx.AsyncClient") as mock_cls:
            mock_client = AsyncMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client.request = AsyncMock(return_value=mock_response)
            mock_cls.return_value = mock_client

            from provisa.core.models import Webhook

            webhook = Webhook(
                name="test-hook",
                url="https://example.com/hook",
                method="POST",
                timeout_ms=5000,
            )

            with pytest.raises(httpx.HTTPStatusError):
                await execute_webhook(webhook, {"arg": "value"})

    @pytest.mark.asyncio
    async def test_timeout_raises_timeout_exception(self):
        """execute_webhook raises httpx.TimeoutException on request timeout (REQ-064)."""
        with patch("httpx.AsyncClient") as mock_cls:
            mock_client = AsyncMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client.request = AsyncMock(side_effect=httpx.TimeoutException("timed out"))
            mock_cls.return_value = mock_client

            from provisa.core.models import Webhook

            webhook = Webhook(
                name="test-hook",
                url="https://example.com/hook",
                method="POST",
                timeout_ms=100,
            )

            with pytest.raises(httpx.TimeoutException):
                await execute_webhook(webhook, {"arg": "value"})


# ── Scheduled trigger intentionally swallows exceptions ──────────────────────


class TestScheduledTriggerSwallowsExceptions:
    """Documented by-design exception swallowing in scheduled trigger context.

    The scheduler must not crash when a webhook fails — REQ-064 applies to
    the query pipeline (execute_webhook), not background scheduler jobs.
    """

    @pytest.mark.asyncio
    async def test_scheduled_webhook_does_not_propagate_http_error(self):
        """_execute_webhook (scheduler) catches all exceptions — scheduler stays alive."""
        from provisa.scheduler.jobs import _execute_webhook

        with patch("httpx.AsyncClient") as mock_cls:
            mock_client = AsyncMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client.post = AsyncMock(side_effect=httpx.ConnectError("connection refused"))
            mock_cls.return_value = mock_client

            # Must not raise — scheduler-level exception swallowing is intentional
            result = await _execute_webhook("https://example.com/hook", "trigger-1")
        assert result is None

    @pytest.mark.asyncio
    async def test_scheduled_webhook_does_not_propagate_500(self):
        """_execute_webhook (scheduler) catches HTTPStatusError from 500 responses."""
        from provisa.scheduler.jobs import _execute_webhook

        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Server Error", request=MagicMock(), response=mock_response
        )

        with patch("httpx.AsyncClient") as mock_cls:
            mock_client = AsyncMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client.post = AsyncMock(return_value=mock_response)
            mock_cls.return_value = mock_client

            result = await _execute_webhook("https://example.com/hook", "trigger-1")
        assert result is None
