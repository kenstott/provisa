# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Scheduled SQL execution + date-token substitution (REQ-1003, REQ-1004)."""

from datetime import datetime, timezone

import pytest

from provisa.core.models import ScheduledTrigger
from provisa.scheduler import jobs
from provisa.scheduler.templating import substitute_date_tokens

RUN_AT = datetime(2026, 7, 13, 14, 30, 0, tzinfo=timezone.utc)


@pytest.mark.parametrize(
    "template,expected",
    [
        ("{{yyyymmdd}}", "20260713"),
        ("{{YYYY-MM-DD}}", "2026-07-13"),
        ("{{iso8601}}", "2026-07-13T14:30:00+00:00"),
        ("{{timestamp}}", str(int(RUN_AT.timestamp()))),
        (
            "SELECT * FROM t WHERE d = '{{YYYY-MM-DD}}' AND p = {{yyyymmdd}}",
            "SELECT * FROM t WHERE d = '2026-07-13' AND p = 20260713",
        ),
        ("{{ yyyymmdd }}", "20260713"),  # whitespace tolerant
    ],
)
def test_substitute_tokens(template, expected):
    assert substitute_date_tokens(template, RUN_AT) == expected


def test_no_tokens_unchanged():
    sql = "SELECT 1 FROM dual"
    assert substitute_date_tokens(sql, RUN_AT) == sql


def test_unrecognized_token_raises():
    with pytest.raises(ValueError, match="Unrecognized scheduled-SQL date token"):
        substitute_date_tokens("SELECT '{{bogus}}'", RUN_AT)


def test_build_scheduler_creates_sql_job():
    trig = ScheduledTrigger(id="nightly", cron="0 0 * * *", sql="SELECT count(*) FROM meta.tables")
    scheduler = jobs.build_scheduler([trig])
    assert scheduler is not None
    job = scheduler.get_job("nightly")
    assert job is not None
    assert job.func is jobs._execute_sql
    assert list(job.args) == ["SELECT count(*) FROM meta.tables", "nightly"]


def test_build_scheduler_mutual_exclusivity_raises():
    trig = ScheduledTrigger(id="bad", cron="0 0 * * *", sql="SELECT 1", url="http://x/hook")
    with pytest.raises(ValueError, match="mutually exclusive"):
        jobs.build_scheduler([trig])


@pytest.mark.asyncio
async def test_execute_sql_substitutes_and_routes(monkeypatch):
    captured = {}

    class _Result:
        rows = [(1,)]

    async def _fake_govern(sql, role_id):
        captured["sql"] = sql
        captured["role"] = role_id
        return object()

    async def _fake_execute(plan):
        captured["executed"] = True
        return _Result()

    monkeypatch.setattr("provisa.pgwire._pipeline._govern_and_route", _fake_govern)
    monkeypatch.setattr("provisa.pgwire._pipeline._execute_plan", _fake_execute)

    await jobs._execute_sql("SELECT '{{YYYY-MM-DD}}'", "t1")

    # Token substituted before routing (REQ-1004) and routed as governed (REQ-1003).
    assert "{{" not in captured["sql"]
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    assert today in captured["sql"]
    assert captured["role"] == "admin"
    assert captured["executed"] is True
