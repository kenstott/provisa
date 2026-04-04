# Copyright (c) 2025 Kenneth Stott
# Canary: 0e6e660b-d8f3-44d4-99ed-c101398243a6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Unit tests for scheduled triggers (REQ-216)."""

from provisa.core.models import ScheduledTrigger
from provisa.scheduler.jobs import build_scheduler


class TestBuildScheduler:
    def test_no_triggers_returns_none(self):
        assert build_scheduler([]) is None

    def test_all_disabled_returns_none(self):
        triggers = [
            ScheduledTrigger(id="t1", cron="0 * * * *", url="http://example.com", enabled=False),
        ]
        assert build_scheduler(triggers) is None

    def test_webhook_trigger_registered(self):
        triggers = [
            ScheduledTrigger(id="hourly", cron="0 * * * *", url="http://example.com/hook"),
        ]
        scheduler = build_scheduler(triggers)
        assert scheduler is not None
        jobs = scheduler.get_jobs()
        assert len(jobs) == 1
        assert jobs[0].id == "hourly"
        assert jobs[0].name == "trigger:hourly"

    def test_multiple_triggers(self):
        triggers = [
            ScheduledTrigger(id="t1", cron="0 * * * *", url="http://a.com"),
            ScheduledTrigger(id="t2", cron="*/5 * * * *", url="http://b.com"),
            ScheduledTrigger(id="t3", cron="0 0 * * *", url="http://c.com", enabled=False),
        ]
        scheduler = build_scheduler(triggers)
        assert scheduler is not None
        jobs = scheduler.get_jobs()
        assert len(jobs) == 2  # t3 is disabled
        job_ids = {j.id for j in jobs}
        assert job_ids == {"t1", "t2"}

    def test_model_validation(self):
        t = ScheduledTrigger(id="test", cron="0 * * * *", url="http://example.com")
        assert t.enabled is True
        assert t.function is None
