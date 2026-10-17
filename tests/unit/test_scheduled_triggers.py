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

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from provisa.core.models import ScheduledTrigger
from provisa.scheduler.jobs import build_scheduler
from provisa.scheduler.trigger_runs import run_trigger

pytestmark = [pytest.mark.asyncio(loop_scope="session")]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _webhook_row(trigger_id: str, url: str = "https://example.com/hook") -> dict:
    """A webhook trigger's stored row, as a run is given it."""
    return {"id": trigger_id, "kind": "webhook", "url": url, "webhook_name": None, "args": {}}


def _make_trigger(**kwargs: object) -> ScheduledTrigger:
    defaults: dict[str, object] = dict(
        id="trigger-1",
        cron="* * * * *",
        url="https://example.com/hook",
        enabled=True,
    )
    defaults.update(kwargs)
    return ScheduledTrigger(**defaults)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestScheduledTriggers:
    async def test_trigger_fires_webhook_on_schedule(self):
        """A webhook trigger's run POSTs to the configured URL with trigger_id, and is recorded."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()

        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        recorded = AsyncMock()
        with (
            patch("provisa.scheduler.trigger_runs.httpx.AsyncClient", return_value=mock_client),
            patch("provisa.scheduler.trigger_runs.record", recorded),
        ):
            outcome = await run_trigger(
                _webhook_row("trigger-1"), "default", given={}, fired_by=None
            )
        assert outcome == "succeeded"
        assert recorded.await_args.args[1:] == ("trigger-1", "succeeded", None, None)

        mock_client.post.assert_called_once_with(
            "https://example.com/hook",
            json={"trigger_id": "trigger-1"},
        )
        mock_response.raise_for_status.assert_called_once()
        assert mock_client.post.call_count == 1

    async def test_a_url_naming_an_environment_variable_is_resolved_at_the_run(self, monkeypatch):
        """A trigger's URL is kept as written -- ``${env:VAR}/path``, as an import writes a
        Hasura cron trigger's ``{{VAR}}/path`` -- and resolved where the run posts it."""
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        monkeypatch.setenv("REPORTS_BASE_URL", "https://reports.internal")
        with (
            patch("provisa.scheduler.trigger_runs.httpx.AsyncClient", return_value=mock_client),
            patch("provisa.scheduler.trigger_runs.record", AsyncMock()),
        ):
            outcome = await run_trigger(
                _webhook_row("digest", "${env:REPORTS_BASE_URL}/digest"),
                "default",
                given={},
                fired_by=None,
            )
        assert outcome == "succeeded"
        mock_client.post.assert_called_once_with(
            "https://reports.internal/digest", json={"trigger_id": "digest"}
        )

    async def test_trigger_interval_creates_job(self):
        """build_scheduler creates an APScheduler job for an enabled trigger."""
        trigger = _make_trigger(cron="*/5 * * * *")
        scheduler = build_scheduler([trigger], "default")

        assert scheduler is not None
        jobs = scheduler.get_jobs()
        assert len(jobs) == 1
        job = jobs[0]
        assert job.id == "trigger-1:org_default"
        assert job.name == "trigger:trigger-1"

    async def test_cron_trigger_creates_job(self):
        """Cron expression is applied correctly to the APScheduler job."""
        trigger = _make_trigger(
            id="daily-export",
            cron="0 2 * * *",
            url="https://example.com/daily",
        )
        scheduler = build_scheduler([trigger], "default")

        assert scheduler is not None
        jobs = scheduler.get_jobs()
        assert any(j.id == "daily-export:org_default" for j in jobs)

    async def test_trigger_disabled_does_not_fire(self):
        """Disabled triggers are excluded; build_scheduler returns None."""
        trigger = _make_trigger(enabled=False)
        scheduler = build_scheduler([trigger], "default")
        assert scheduler is None

    async def test_trigger_error_does_not_crash_scheduler(self):
        """A failed webhook call is recorded as failed; the run returns normally."""
        import httpx

        mock_client = AsyncMock()
        mock_client.post = AsyncMock(
            side_effect=httpx.RequestError("connection refused", request=MagicMock())
        )
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        recorded = AsyncMock()
        with (
            patch("provisa.scheduler.trigger_runs.httpx.AsyncClient", return_value=mock_client),
            patch("provisa.scheduler.trigger_runs.record", recorded),
        ):
            # Should not raise
            url = "https://bad-host.example.com/hook"
            outcome = await run_trigger(
                _webhook_row("trigger-err", url), "default", given={}, fired_by=None
            )

        assert outcome == "failed"
        assert recorded.await_args.args[1:3] == ("trigger-err", "failed")
        mock_client.post.assert_called_once_with(
            "https://bad-host.example.com/hook",
            json={"trigger_id": "trigger-err"},
        )

    async def test_build_scheduler_multiple_triggers(self):
        """Multiple enabled triggers each become a distinct job."""
        triggers = [
            _make_trigger(id="t1", cron="0 * * * *", url="https://example.com/t1"),
            _make_trigger(id="t2", cron="30 * * * *", url="https://example.com/t2"),
            _make_trigger(id="t3", cron="0 0 * * *", enabled=False, url="https://example.com/t3"),
        ]
        scheduler = build_scheduler(triggers, "default")
        assert scheduler is not None
        job_ids = {j.id for j in scheduler.get_jobs()}
        assert "t1:org_default" in job_ids
        assert "t2:org_default" in job_ids
        assert "t3:org_default" not in job_ids

    async def test_a_command_trigger_on_a_schedule_creates_a_job(self):  # REQ-2054
        """A trigger running a registered command on a schedule is scheduled like any other."""
        trigger = ScheduledTrigger(
            id="fn-trigger",
            cron="0 * * * *",
            command="my_command",
            role="analyst",
            enabled=True,
        )
        scheduler = build_scheduler([trigger], "default")
        assert scheduler is not None
        assert {j.id for j in scheduler.get_jobs()} == {"fn-trigger:org_default"}

    async def test_a_trigger_on_a_table_has_no_job(self):  # REQ-2054
        """A trigger on a table is fired by the table's change, never by the clock."""
        trigger = ScheduledTrigger(
            id="on-change",
            on={"table": "pg.public.orders", "event": "change"},
            command="my_command",
            role="analyst",
        )
        scheduler = build_scheduler([trigger, _make_trigger(id="t1")], "default")
        assert scheduler is not None
        assert {j.id for j in scheduler.get_jobs()} == {"t1:org_default"}
