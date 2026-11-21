# Copyright (c) 2026 Kenneth Stott
# Canary: 158a6dc7-80de-4934-9181-202716a18ac1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Cron expressions follow standard crontab weekdays (GitHub issue 343)."""

from datetime import UTC, datetime, timedelta

import pytest

from provisa.core.cron import cron_trigger

SINCE = datetime(2026, 11, 21, 12, 0, tzinfo=UTC)  # a Saturday


def _fires(expr: str, count: int) -> set[int]:
    """Python weekdays (Monday = 0) of the next ``count`` firings."""
    trigger = cron_trigger(expr, timezone=UTC)
    out: set[int] = set()
    at = SINCE
    for _ in range(count):
        at = trigger.get_next_fire_time(None, at)
        out.add(at.weekday())
        at = at + timedelta(seconds=1)
    return out


@pytest.mark.parametrize(
    ("expr", "days"),
    [
        ("0 6 * * 1", {0}),
        ("0 6 * * 0", {6}),
        ("0 6 * * 7", {6}),
        ("0 6 * * 6", {5}),
        ("0 6 * * MON", {0}),
        ("0 6 * * sun", {6}),
        ("0 6 * * 1-5", {0, 1, 2, 3, 4}),
        ("0 6 * * MON-FRI", {0, 1, 2, 3, 4}),
        ("0 6 * * 5-7", {4, 5, 6}),
        ("0 6 * * FRI-SUN", {4, 5, 6}),
        ("0 6 * * 0-6", {0, 1, 2, 3, 4, 5, 6}),
        ("0 6 * * 1,3,5", {0, 2, 4}),
        ("0 6 * * 0,6", {5, 6}),
        ("0 6 * * */2", {6, 1, 3, 5}),
        ("0 6 * * 1-5/2", {0, 2, 4}),
        ("0 6 * * *", {0, 1, 2, 3, 4, 5, 6}),
    ],
)
def test_weekdays(expr, days):
    assert _fires(expr, 14) == days


def test_monday_is_a_monday():
    fire = cron_trigger("0 6 * * 1", timezone=UTC).get_next_fire_time(None, SINCE)
    assert fire.strftime("%A") == "Monday"


@pytest.mark.parametrize(
    "expr", ["0 6 * *", "0 6 * * 8", "0 6 * * 5-1", "0 6 * * foo", "0 6 * * 1/0"]
)
def test_refused(expr):
    with pytest.raises(ValueError):
        cron_trigger(expr)
