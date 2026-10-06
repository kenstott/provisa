# Copyright (c) 2026 Kenneth Stott
# Canary: 63163177-db9c-40d4-95b5-0d25a47ea498
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Scheduled SQL execution + date-token substitution (REQ-1003, REQ-1004)."""

from datetime import datetime, timezone

import pytest

from pydantic import ValidationError

from provisa.core.models import ScheduledTrigger
from provisa.scheduler import jobs
from provisa.scheduler.templating import DateTokenNotAValue, substitute_date_tokens
from provisa.scheduler.trigger_sql import TriggerSqlRefused, checked_trigger_sql

RUN_AT = datetime(2026, 7, 13, 14, 30, 0, tzinfo=timezone.utc)


@pytest.mark.parametrize(
    "template,expected",
    [
        # A token standing alone is a literal of its own: a number, or a quoted string.
        ("{{yyyymmdd}}", "20260713"),
        ("{{YYYY-MM-DD}}", "'2026-07-13'"),
        ("{{iso8601}}", "'2026-07-13T14:30:00+00:00'"),
        ("{{timestamp}}", str(int(RUN_AT.timestamp()))),
        # Inside a string literal it is written into the literal.
        ("SELECT 'run {{yyyymmdd}}'", "SELECT 'run 20260713'"),
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


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM orders_{{yyyymmdd}}",
        'SELECT * FROM "orders_{{yyyymmdd}}"',
        "DELETE FROM sales.{{yyyymmdd}}x",
    ],
)
def test_a_token_never_supplies_part_of_a_name(sql):
    with pytest.raises(DateTokenNotAValue):
        substitute_date_tokens(sql, RUN_AT)


_PURGE = "DELETE FROM sales.events WHERE day < '{{YYYY-MM-DD}}'"


def test_build_scheduler_creates_sql_job():
    trig = ScheduledTrigger(id="nightly", cron="0 0 * * *", sql=_PURGE, role="ops")
    scheduler = jobs.build_scheduler([trig], "default")
    assert scheduler is not None
    job = scheduler.get_job("nightly:org_default")
    assert job is not None
    assert job.func is jobs._execute_sql
    assert list(job.args) == [_PURGE, "nightly", "ops", "default"]


def test_a_sql_trigger_names_the_role_it_runs_as():
    with pytest.raises(ValidationError, match="'nightly': a SQL trigger names the role it runs as"):
        ScheduledTrigger(id="nightly", cron="0 0 * * *", sql=_PURGE)


@pytest.mark.parametrize(
    "sql, said",
    [
        ("CREATE TABLE sales.snap AS SELECT * FROM sales.orders", "creates an object"),
        ("CREATE TABLE sales.snap (id int)", "creates an object"),
        ("DROP TABLE sales.orders", "this one is DROP"),
        ("SELECT count(*) FROM sales.orders", "this one is SELECT"),
        ("DELETE FROM sales.a; DELETE FROM sales.b", "holds 2"),
    ],
)
def test_a_trigger_only_writes_rows_refused_when_saved(sql, said):
    with pytest.raises(ValidationError) as refused:
        ScheduledTrigger(id="nightly", cron="0 0 * * *", sql=sql, role="ops")
    assert "trigger 'nightly'" in str(refused.value) and said in str(refused.value)


@pytest.mark.asyncio
async def test_a_create_is_refused_when_the_trigger_runs(monkeypatch):
    # A trigger saved before this rule, or written into the store by hand, is refused at run too,
    # before anything is governed or sent.
    async def _never(*_a, **_k):
        raise AssertionError("must not be governed")

    monkeypatch.setattr("provisa.pgwire._pipeline._govern_and_route", _never)
    with pytest.raises(TriggerSqlRefused, match="trigger 'snap': creates an object"):
        await jobs._execute_sql("CREATE TABLE s.x AS SELECT 1", "snap", "ops", "default")


def test_row_writes_are_admitted_with_their_tokens_as_values():
    for sql in (
        "INSERT INTO sales.daily (day, n) SELECT {{yyyymmdd}}, count(*) FROM sales.orders",
        "UPDATE sales.orders SET seen = '{{iso8601}}' WHERE seen IS NULL",
        _PURGE,
    ):
        rendered = checked_trigger_sql(sql, "nightly", RUN_AT)
        assert "{{" not in rendered


def test_build_scheduler_mutual_exclusivity_raises():
    trig = ScheduledTrigger(id="bad", cron="0 0 * * *", sql=_PURGE, role="ops", url="http://x/hook")
    with pytest.raises(ValueError, match="mutually exclusive"):
        jobs.build_scheduler([trig], "default")


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

    await jobs._execute_sql(_PURGE, "t1", "ops", "default")

    # Token substituted before routing (REQ-1004) and routed as governed (REQ-1003), as the
    # trigger's own role — through the one write admission.
    assert "{{" not in captured["sql"]
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    assert today in captured["sql"]
    assert captured["role"] == "ops"
    assert captured["executed"] is True
