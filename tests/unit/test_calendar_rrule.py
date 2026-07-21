# Copyright (c) 2026 Kenneth Stott
# Canary: 90b00336-db81-419f-8dfa-4cc71366b554
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""RFC 5545 RRULE recurrence grains (REQ-1169): the general Outlook/iCalendar recurrence form.

An RRULE grain TILES the timeline exactly like the ``NthWeekday`` shorthand — consecutive occurrences
yield a half-open ``[start, end)`` tiling — so it drops into ``window_for``/``next_boundary``
unchanged. These prove the tiling boundaries, the phase anchor, the shorthand equivalence, and that a
bounded rule fails LOUD.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from provisa.events.calendars import (
    Calendar,
    RRuleRecurrence,
    next_boundary,
    parse_grain_spec,
    window_for,
)

UTC = timezone.utc


def _cal() -> Calendar:
    return Calendar(name="g", version="v1")


def test_rrule_prefix_and_bare_freq_both_parse():
    assert isinstance(parse_grain_spec("RRULE:FREQ=MONTHLY;BYDAY=3WE"), RRuleRecurrence)
    assert isinstance(parse_grain_spec("FREQ=WEEKLY;BYDAY=MO"), RRuleRecurrence)


def test_monthly_nth_weekday_tiles_like_shorthand():
    """RRULE '3rd Wednesday' and the '3WE' shorthand must yield the identical window (REQ-1168/1169)."""
    cal = _cal()
    instant = datetime(2026, 2, 20, tzinfo=UTC)
    w_rrule = window_for(cal, "RRULE:FREQ=MONTHLY;BYDAY=3WE", instant)
    w_short = window_for(cal, "3WE", instant)
    assert w_rrule is not None and w_short is not None
    # 3rd Wed of Feb 2026 = Feb 18 → tile closes at 3rd Wed of Mar = Mar 18.
    assert w_rrule.start == w_short.start == datetime(2026, 2, 18, tzinfo=UTC)
    assert w_rrule.end == w_short.end == datetime(2026, 3, 18, tzinfo=UTC)
    assert w_rrule.window_id == "2026-02-18-M3WE"


def test_last_day_of_month_tiles():
    cal = _cal()
    w = window_for(cal, "FREQ=MONTHLY;BYMONTHDAY=-1", datetime(2026, 7, 21, tzinfo=UTC))
    assert w is not None
    assert w.start == datetime(2026, 6, 30, tzinfo=UTC)  # last day on/before Jul 21
    assert w.end == datetime(2026, 7, 31, tzinfo=UTC)


def test_biweekly_interval_anchored_to_fixed_epoch():
    """INTERVAL phase is anchored to a fixed civil epoch, so the tiling is query-independent."""
    cal = _cal()
    w1 = window_for(cal, "FREQ=WEEKLY;INTERVAL=2;BYDAY=MO", datetime(2026, 7, 21, tzinfo=UTC))
    w2 = window_for(cal, "FREQ=WEEKLY;INTERVAL=2;BYDAY=MO", datetime(2026, 7, 28, tzinfo=UTC))
    assert w1 is not None and w2 is not None
    assert w1.start == datetime(2026, 7, 20, tzinfo=UTC)
    assert (w1.end - w1.start).days == 14
    assert w1 == w2  # both instants fall in the same 2-week tile → identical window


def test_next_boundary_advances_one_tile():
    cal = _cal()
    b = next_boundary(cal, "RRULE:FREQ=MONTHLY;BYDAY=3WE", datetime(2026, 2, 20, tzinfo=UTC))
    assert b == datetime(2026, 3, 18, tzinfo=UTC)


def test_bounded_rrule_fails_loud():
    for spec in ("FREQ=DAILY;COUNT=5", "FREQ=WEEKLY;UNTIL=20261231T000000Z"):
        with pytest.raises(ValueError, match="unbounded"):
            parse_grain_spec(spec)


def test_invalid_rrule_fails_loud():
    with pytest.raises(ValueError, match="invalid RRULE grain"):
        RRuleRecurrence("FREQ=NONSENSE")


def test_yearly_bymonth_tiles_annually():
    cal = _cal()
    w = window_for(cal, "FREQ=YEARLY;BYMONTH=12;BYMONTHDAY=-1", datetime(2026, 7, 21, tzinfo=UTC))
    assert w is not None
    assert w.start == datetime(2025, 12, 31, tzinfo=UTC)
    assert w.end == datetime(2026, 12, 31, tzinfo=UTC)
