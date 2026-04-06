# Copyright (c) 2026 Kenneth Stott
# Canary: 6b2c3d4e-5f6a-7890-bcde-f01234567890
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for APScheduler-based scheduled trigger execution."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from provisa.core.models import ScheduledTrigger
from provisa.scheduler.jobs import _execute_webhook, build_scheduler

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_trigger(**kwargs) -> ScheduledTrigger:
    defaults = dict(
        id="trigger-1",
        cron="* * * * *",
        url="https://example.com/hook",
        enabled=True,
    )
    defaults.update(kwargs)
    return ScheduledTrigger(**defaults)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestScheduledTriggers:
    async def test_trigger_fires_webhook_on_schedule(self):
        """_execute_webhook POSTs to the configured URL with trigger_id."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()

        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("provisa.scheduler.jobs.httpx.AsyncClient", return_value=mock_client):
            await _execute_webhook("https://example.com/hook", "trigger-1")

        mock_client.post.assert_called_once_with(
            "https://example.com/hook",
            json={"trigger_id": "trigger-1"},
        )
        mock_response.raise_for_status.assert_called_once()

    async def test_trigger_interval_creates_job(self):
        """build_scheduler creates an APScheduler job for an enabled trigger."""
        trigger = _make_trigger(cron="*/5 * * * *")
        scheduler = build_scheduler([trigger])

        assert scheduler is not None
        jobs = scheduler.get_jobs()
        assert len(jobs) == 1
        job = jobs[0]
        assert job.id == "trigger-1"
        assert job.name == "trigger:trigger-1"

    async def test_cron_trigger_creates_job(self):
        """Cron expression is applied correctly to the APScheduler job."""
        trigger = _make_trigger(
            id="daily-export",
            cron="0 2 * * *",
            url="https://example.com/daily",
        )
        scheduler = build_scheduler([trigger])

        assert scheduler is not None
        jobs = scheduler.get_jobs()
        assert any(j.id == "daily-export" for j in jobs)

    async def test_trigger_disabled_does_not_fire(self):
        """Disabled triggers are excluded; build_scheduler returns None."""
        trigger = _make_trigger(enabled=False)
        scheduler = build_scheduler([trigger])
        assert scheduler is None

    async def test_trigger_error_does_not_crash_scheduler(self):
        """A failed webhook call is caught; the coroutine returns normally."""
        import httpx

        mock_client = AsyncMock()
        mock_client.post = AsyncMock(
            side_effect=httpx.RequestError("connection refused", request=MagicMock())
        )
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("provisa.scheduler.jobs.httpx.AsyncClient", return_value=mock_client):
            # Should not raise
            await _execute_webhook("https://bad-host.example.com/hook", "trigger-err")

    async def test_build_scheduler_multiple_triggers(self):
        """Multiple enabled triggers each become a distinct job."""
        triggers = [
            _make_trigger(id="t1", cron="0 * * * *", url="https://example.com/t1"),
            _make_trigger(id="t2", cron="30 * * * *", url="https://example.com/t2"),
            _make_trigger(id="t3", cron="0 0 * * *", enabled=False, url="https://example.com/t3"),
        ]
        scheduler = build_scheduler(triggers)
        assert scheduler is not None
        job_ids = {j.id for j in scheduler.get_jobs()}
        assert "t1" in job_ids
        assert "t2" in job_ids
        assert "t3" not in job_ids

    async def test_trigger_without_url_does_not_create_job(self):
        """A trigger with only a function name (no URL) is warned but not scheduled."""
        trigger = ScheduledTrigger(
            id="fn-trigger",
            cron="0 * * * *",
            url=None,
            function="my_internal_fn",
            enabled=True,
        )
        scheduler = build_scheduler([trigger])
        # No webhook URL — no job added; scheduler returned but empty
        if scheduler is not None:
            job_ids = {j.id for j in scheduler.get_jobs()}
            assert "fn-trigger" not in job_ids
