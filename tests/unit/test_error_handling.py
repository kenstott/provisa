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

from provisa.registry.ceiling import CeilingViolationError, check_ceiling
from provisa.registry.governance import (
    GovernanceError,
    GovernanceMode,
    check_deprecated,
    check_governance,
    check_output_type,
)
from provisa.security.rights import (
    Capability,
    InsufficientRightsError,
    check_capability,
)
from provisa.webhooks.executor import execute_webhook


# ── Governance errors propagate ──────────────────────────────────────────────


class TestGovernanceErrorPropagates:
    def test_registry_required_table_without_stable_id_raises(self):
        """Raw query against registry-required table raises GovernanceError (REQ-001)."""
        with pytest.raises(GovernanceError, match="requires an approved query"):
            check_governance(
                mode=GovernanceMode.PRODUCTION,
                target_table_ids=[1],
                table_governance={1: "registry-required"},
                stable_id=None,
            )

    def test_approved_query_passes_governance(self):
        """Approved query (stable_id set) does not raise in production mode."""
        check_governance(
            mode=GovernanceMode.PRODUCTION,
            target_table_ids=[1],
            table_governance={1: "registry-required"},
            stable_id="approved-query-id",
        )

    def test_pre_approved_table_passes_without_stable_id(self):
        """pre-approved table never requires a registry entry (REQ-003)."""
        check_governance(
            mode=GovernanceMode.PRODUCTION,
            target_table_ids=[1],
            table_governance={1: "pre-approved"},
            stable_id=None,
        )

    def test_test_mode_allows_all_queries(self):
        """Test mode bypasses governance gate — no error raised."""
        check_governance(
            mode=GovernanceMode.TEST,
            target_table_ids=[1],
            table_governance={1: "registry-required"},
            stable_id=None,
        )

    def test_deprecated_query_raises_governance_error(self):
        """Deprecated query raises GovernanceError with replacement pointer (REQ-026)."""
        with pytest.raises(GovernanceError, match="deprecated"):
            check_deprecated(
                {"status": "deprecated", "stable_id": "old-query", "deprecated_by": "new-query"}
            )

    def test_deprecated_query_message_includes_replacement(self):
        """Deprecation error includes the replacement query ID."""
        with pytest.raises(GovernanceError, match="new-query"):
            check_deprecated(
                {"status": "deprecated", "stable_id": "old-query", "deprecated_by": "new-query"}
            )

    def test_disallowed_output_type_raises(self):
        """Requesting an output type not in permitted_outputs raises GovernanceError."""
        with pytest.raises(GovernanceError, match="not permitted"):
            check_output_type(
                {"permitted_outputs": ["json"]},
                requested_output="parquet",
            )

    def test_permitted_output_type_passes(self):
        """Requesting an allowed output type does not raise."""
        check_output_type(
            {"permitted_outputs": ["json", "parquet"]},
            requested_output="parquet",
        )


# ── Insufficient rights errors propagate ─────────────────────────────────────


class TestInsufficientRightsErrorPropagates:
    def test_missing_capability_raises(self):
        """Role without required capability raises InsufficientRightsError."""
        role = {"id": "analyst", "capabilities": ["query_development"]}
        with pytest.raises(InsufficientRightsError) as exc_info:
            check_capability(role, Capability.QUERY_APPROVAL)
        assert exc_info.value.role_id == "analyst"
        assert exc_info.value.required == Capability.QUERY_APPROVAL

    def test_present_capability_does_not_raise(self):
        """Role with the required capability does not raise."""
        role = {"id": "steward", "capabilities": ["query_approval"]}
        check_capability(role, Capability.QUERY_APPROVAL)

    def test_admin_capability_satisfies_any_requirement(self):
        """Admin capability acts as a wildcard — satisfies any check."""
        role = {"id": "superuser", "capabilities": ["admin"]}
        for cap in Capability:
            check_capability(role, cap)

    def test_empty_capabilities_raises_for_any_requirement(self):
        """Role with no capabilities raises for every capability check."""
        role = {"id": "noop", "capabilities": []}
        for cap in (Capability.QUERY_DEVELOPMENT, Capability.QUERY_APPROVAL, Capability.ADMIN):
            with pytest.raises(InsufficientRightsError):
                check_capability(role, cap)

    def test_error_message_includes_role_and_capability(self):
        """InsufficientRightsError message is self-describing."""
        role = {"id": "guest", "capabilities": []}
        with pytest.raises(InsufficientRightsError, match="guest"):
            check_capability(role, Capability.SOURCE_REGISTRATION)


# ── Ceiling violation errors propagate ───────────────────────────────────────


class TestCeilingViolationErrorPropagates:
    def test_extra_columns_raise_ceiling_error(self):
        """Requesting columns beyond the approved ceiling raises CeilingViolationError."""
        approved = "query Approved { orders { id amount } }"
        client = "query Req { orders { id amount secret_column } }"
        with pytest.raises(CeilingViolationError):
            check_ceiling(approved, client)

    def test_subset_columns_passes(self):
        """Requesting fewer columns than approved is allowed."""
        approved = "query Approved { orders { id amount region } }"
        client = "query Req { orders { id } }"
        check_ceiling(approved, client)

    def test_exact_approved_columns_passes(self):
        """Requesting exactly the approved columns is allowed."""
        approved = "query Approved { orders { id amount } }"
        client = "query Req { orders { id amount } }"
        check_ceiling(approved, client)

    def test_ceiling_error_message_is_descriptive(self):
        """CeilingViolationError message identifies the violation."""
        approved = "query Approved { orders { id } }"
        client = "query Req { orders { id secret } }"
        with pytest.raises(CeilingViolationError, match="ceiling"):
            check_ceiling(approved, client)


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
            mock_client.request = AsyncMock(
                side_effect=httpx.TimeoutException("timed out")
            )
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
            mock_client.post = AsyncMock(
                side_effect=httpx.ConnectError("connection refused")
            )
            mock_cls.return_value = mock_client

            # Must not raise — scheduler-level exception swallowing is intentional
            await _execute_webhook("https://example.com/hook", "trigger-1")

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

            await _execute_webhook("https://example.com/hook", "trigger-1")


# ── Test mode gatekeeping (REQ-004) ──────────────────────────────────────────


class TestGovernanceModeGating:
    def test_test_mode_from_env(self, monkeypatch):
        """PROVISA_MODE unset → test mode (REQ-004)."""
        monkeypatch.delenv("PROVISA_MODE", raising=False)
        from provisa.registry.governance import get_mode

        assert get_mode() == GovernanceMode.TEST

    def test_production_mode_from_env(self, monkeypatch):
        """PROVISA_MODE=production → production mode (REQ-004)."""
        monkeypatch.setenv("PROVISA_MODE", "production")
        from provisa.registry.governance import get_mode

        assert get_mode() == GovernanceMode.PRODUCTION

    def test_prod_alias_from_env(self, monkeypatch):
        """PROVISA_MODE=prod is accepted as production (REQ-004)."""
        monkeypatch.setenv("PROVISA_MODE", "prod")
        from provisa.registry.governance import get_mode

        assert get_mode() == GovernanceMode.PRODUCTION
