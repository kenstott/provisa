# Copyright (c) 2026 Kenneth Stott
# Canary: 3278ffc9-272f-40be-8b9e-1372e85a51f3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Unit tests for provisa/scheduler/jobs.py.

Covers:
- _execute_webhook: HTTP POST behaviour, body payload, error handling
- build_scheduler: scheduler construction, enabled/disabled filtering,
  webhook jobs, internal-function warnings, replace_existing dedup
"""

from __future__ import annotations

import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from provisa.core.models import ScheduledTrigger
from provisa.scheduler.jobs import _execute_webhook, build_scheduler

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _trigger(
    trigger_id: str = "t-1",
    cron: str = "0 * * * *",
    url: str | None = "https://example.com/hook",
    function: str | None = None,
    enabled: bool = True,
) -> ScheduledTrigger:
    return ScheduledTrigger(
        id=trigger_id,
        cron=cron,
        url=url,
        function=function,
        enabled=enabled,
    )


def _mock_client_context() -> tuple[MagicMock, AsyncMock]:
    """Return (mock_cls, mock_client) wired up as an async context manager."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.raise_for_status = MagicMock()

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_client.post = AsyncMock(return_value=mock_response)

    mock_cls = MagicMock(return_value=mock_client)
    return mock_cls, mock_client


# ---------------------------------------------------------------------------
# _execute_webhook
# ---------------------------------------------------------------------------

class TestExecuteWebhook:
    pytestmark = pytest.mark.asyncio(loop_scope="session")

    async def test_posts_to_correct_url(self):
        """_execute_webhook sends a POST to the supplied URL."""
        mock_cls, mock_client = _mock_client_context()

        with patch("provisa.scheduler.jobs.httpx.AsyncClient", mock_cls):
            await _execute_webhook("https://example.com/hook", "t-1")

        mock_client.post.assert_called_once()
        call_args = mock_client.post.call_args
        # First positional arg is the URL
        actual_url = call_args.args[0] if call_args.args else call_args.kwargs.get("url")
        assert actual_url == "https://example.com/hook"

    async def test_payload_contains_trigger_id(self):
        """_execute_webhook sends {trigger_id: <id>} as the JSON body."""
        mock_cls, mock_client = _mock_client_context()

        with patch("provisa.scheduler.jobs.httpx.AsyncClient", mock_cls):
            await _execute_webhook("https://example.com/hook", "trigger-abc")

        call_args = mock_client.post.call_args
        json_body = call_args.kwargs.get("json") or (call_args.args[1] if len(call_args.args) > 1 else None)
        assert json_body is not None, "Expected a json= keyword argument"
        assert json_body["trigger_id"] == "trigger-abc"

    async def test_uses_httpx_async_client(self):
        """_execute_webhook uses httpx.AsyncClient as an async context manager."""
        mock_cls, mock_client = _mock_client_context()

        with patch("provisa.scheduler.jobs.httpx.AsyncClient", mock_cls) as patched:
            await _execute_webhook("https://example.com/hook", "t-1")

        patched.assert_called_once()
        mock_client.__aenter__.assert_called_once()
        mock_client.__aexit__.assert_called_once()

    async def test_returns_none_on_http_200(self):
        """_execute_webhook returns None (implicitly) on a successful 200 response."""
        mock_cls, mock_client = _mock_client_context()

        with patch("provisa.scheduler.jobs.httpx.AsyncClient", mock_cls):
            result = await _execute_webhook("https://example.com/hook", "t-1")

        assert result is None

    async def test_exception_is_caught_and_logged(self):
        """_execute_webhook catches all exceptions and logs them rather than raising."""
        import httpx as _httpx

        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = _httpx.HTTPStatusError(
            "server error", request=MagicMock(), response=mock_response
        )

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock(return_value=mock_response)
        mock_cls = MagicMock(return_value=mock_client)

        with patch("provisa.scheduler.jobs.httpx.AsyncClient", mock_cls):
            # Must not raise — the function swallows all exceptions
            result = await _execute_webhook("https://example.com/hook", "t-fail")

        assert result is None

    async def test_network_error_is_caught(self):
        """_execute_webhook also swallows low-level network errors (ConnectError etc.)."""
        import httpx as _httpx

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock(side_effect=_httpx.ConnectError("refused"))
        mock_cls = MagicMock(return_value=mock_client)

        with patch("provisa.scheduler.jobs.httpx.AsyncClient", mock_cls):
            result = await _execute_webhook("https://example.com/hook", "t-net")

        assert result is None

    async def test_different_trigger_ids_produce_different_payloads(self):
        """Each call uses the trigger_id passed to it, not a cached value."""
        mock_cls_a, mock_client_a = _mock_client_context()
        mock_cls_b, mock_client_b = _mock_client_context()

        with patch("provisa.scheduler.jobs.httpx.AsyncClient", mock_cls_a):
            await _execute_webhook("https://example.com/hook", "first-id")

        with patch("provisa.scheduler.jobs.httpx.AsyncClient", mock_cls_b):
            await _execute_webhook("https://example.com/hook", "second-id")

        payload_a = mock_client_a.post.call_args.kwargs.get("json")
        payload_b = mock_client_b.post.call_args.kwargs.get("json")
        assert payload_a["trigger_id"] == "first-id"
        assert payload_b["trigger_id"] == "second-id"


# ---------------------------------------------------------------------------
# build_scheduler
# ---------------------------------------------------------------------------

class TestBuildScheduler:
    def test_empty_list_returns_none(self):
        """build_scheduler returns None when no triggers are provided."""
        result = build_scheduler([])
        assert result is None

    def test_all_disabled_returns_none(self):
        """build_scheduler returns None when all triggers are disabled."""
        triggers = [
            _trigger("t-1", enabled=False),
            _trigger("t-2", enabled=False),
        ]
        result = build_scheduler(triggers)
        assert result is None

    def test_single_enabled_trigger_returns_scheduler(self):
        """build_scheduler returns an AsyncIOScheduler for at least one enabled trigger."""
        triggers = [_trigger("t-1", cron="0 * * * *")]
        result = build_scheduler(triggers)
        assert isinstance(result, AsyncIOScheduler)

    def test_enabled_webhook_trigger_adds_job(self):
        """An enabled trigger with a URL results in exactly one job on the scheduler."""
        triggers = [_trigger("t-webhook", cron="0 0 * * *", url="https://example.com/wh")]
        scheduler = build_scheduler(triggers)

        assert scheduler is not None
        jobs = scheduler.get_jobs()
        assert len(jobs) == 1

    def test_job_id_matches_trigger_id(self):
        """The APScheduler job ID equals the ScheduledTrigger id field."""
        triggers = [_trigger("my-trigger", cron="30 6 * * 1")]
        scheduler = build_scheduler(triggers)

        job = scheduler.get_job("my-trigger")
        assert job is not None

    def test_disabled_trigger_job_not_added(self):
        """A disabled trigger does not produce a job in the scheduler."""
        triggers = [
            _trigger("enabled-t", cron="0 * * * *", url="https://example.com/a"),
            _trigger("disabled-t", cron="0 * * * *", url="https://example.com/b", enabled=False),
        ]
        scheduler = build_scheduler(triggers)

        assert scheduler is not None
        jobs = scheduler.get_jobs()
        job_ids = {j.id for j in jobs}
        assert "enabled-t" in job_ids
        assert "disabled-t" not in job_ids

    def test_multiple_enabled_triggers_all_added(self):
        """All enabled triggers produce individual jobs."""
        triggers = [
            _trigger("t-1", cron="0 * * * *", url="https://example.com/1"),
            _trigger("t-2", cron="0 0 * * *", url="https://example.com/2"),
            _trigger("t-3", cron="*/15 * * * *", url="https://example.com/3"),
        ]
        scheduler = build_scheduler(triggers)

        assert scheduler is not None
        jobs = scheduler.get_jobs()
        assert len(jobs) == 3
        job_ids = {j.id for j in jobs}
        assert job_ids == {"t-1", "t-2", "t-3"}

    def test_internal_function_trigger_logs_warning_and_no_job(self, caplog):
        """A trigger with function= (no url) logs a warning and adds no job."""
        triggers = [_trigger("fn-trigger", url=None, function="refresh_cache")]
        with caplog.at_level(logging.WARNING, logger="provisa.scheduler.jobs"):
            scheduler = build_scheduler(triggers)

        # Scheduler is returned (enabled trigger exists) but has no jobs
        assert scheduler is not None
        assert len(scheduler.get_jobs()) == 0
        # Warning must mention the trigger id or function name
        assert any("fn-trigger" in record.message for record in caplog.records)

    def test_duplicate_trigger_id_passes_replace_existing(self):
        """build_scheduler passes replace_existing=True to add_job for idempotent re-registration."""
        t1 = _trigger("dup-id", cron="0 * * * *", url="https://example.com/first")

        with patch("provisa.scheduler.jobs.AsyncIOScheduler") as mock_sched_cls:
            mock_sched = MagicMock()
            mock_sched_cls.return_value = mock_sched
            build_scheduler([t1])

        call_kwargs = mock_sched.add_job.call_args.kwargs
        assert call_kwargs.get("replace_existing") is True

    def test_mixed_enabled_and_disabled_only_counts_enabled(self):
        """A mix of enabled and disabled triggers only creates jobs for enabled ones."""
        triggers = [
            _trigger("active-1", cron="0 * * * *"),
            _trigger("inactive-1", cron="0 * * * *", enabled=False),
            _trigger("active-2", cron="0 12 * * *"),
            _trigger("inactive-2", cron="0 12 * * *", enabled=False),
        ]
        scheduler = build_scheduler(triggers)

        assert scheduler is not None
        job_ids = {j.id for j in scheduler.get_jobs()}
        assert job_ids == {"active-1", "active-2"}

    def test_job_function_is_execute_webhook(self):
        """The job added for a webhook trigger calls _execute_webhook."""
        triggers = [_trigger("t-fn-check", cron="0 * * * *", url="https://example.com/wh")]
        scheduler = build_scheduler(triggers)

        job = scheduler.get_job("t-fn-check")
        assert job is not None
        assert job.func is _execute_webhook

    def test_job_args_contain_url_and_trigger_id(self):
        """The job is created with [url, trigger_id] as positional args."""
        url = "https://example.com/test-hook"
        triggers = [_trigger("t-args", cron="0 * * * *", url=url)]
        scheduler = build_scheduler(triggers)

        job = scheduler.get_job("t-args")
        assert job is not None
        assert job.args[0] == url
        assert job.args[1] == "t-args"
