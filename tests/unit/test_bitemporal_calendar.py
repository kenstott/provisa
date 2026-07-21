# Copyright (c) 2026 Kenneth Stott
# Canary: 7b4e1c92-3a6d-45f8-9e21-0c5b8d3f7a46
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Calendar-addressed bitemporal snapshots (REQ-1166/1167/1168) executed on real DuckDB.

A repeating-calendar snapshot MV = a calendar TRIGGER (when a version is cut + its window_id) over an
append-only bitemporal store (how history is kept). This proves the binding: stamping each append
with the calendar boundary (``window.end`` via ``system_ts_literal``) instead of wall-clock makes the
snapshot DETERMINISTIC and time-travel-addressable — a read as-of a boundary returns exactly the
period sealed at that boundary. Time travel is intrinsic to the append log, not a separate feature.
"""

from __future__ import annotations

from datetime import datetime, timezone

import duckdb
import pytest

from provisa.events.calendars import Calendar, NthWeekday, window_for
from provisa.mv.bitemporal import (
    MODE_DELTA,
    MODE_SNAPSHOT,
    BitemporalSpec,
    append_sql,
    create_sql,
    reconstruct_as_of_sql,
    system_ts_literal,
)

UTC = timezone.utc
_COLS = ["id", "region", "amount"]
_SELECT = "SELECT id, region, amount FROM base"
_TARGET = "mv_bt"


class _CalendarDriver:
    """Seal one bitemporal batch per calendar boundary, stamped by ``window.end`` (REQ-1166/1167)."""

    def __init__(self, cal: Calendar, grain, mode: str):
        self.cal, self.grain, self.spec = cal, grain, BitemporalSpec(key=("id",), mode=mode)
        self.con = duckdb.connect(":memory:")
        self.con.execute("CREATE TABLE base (id INTEGER, region VARCHAR, amount INTEGER)")
        self._created = False

    def seal(self, rows: list[tuple], instant: datetime) -> datetime:
        """Seal ``rows`` for the window covering ``instant``, stamped at that window's end. Returns
        the boundary the batch is addressed by (for as-of assertions)."""
        win = window_for(self.cal, self.grain, instant)
        assert win is not None
        stamp = system_ts_literal(win.end)
        self.con.execute("DELETE FROM base")
        for r in rows:
            self.con.execute("INSERT INTO base VALUES (?, ?, ?)", r)
        if not self._created:
            self.con.execute(create_sql(_TARGET, _SELECT, self.spec, stamp))
            self._created = True
        else:
            for stmt in append_sql(_TARGET, _SELECT, self.spec, _COLS, stamp, "duckdb"):
                self.con.execute(stmt)
        return win.end

    def as_of(self, ts: datetime | None) -> set[tuple]:
        ts_sql = system_ts_literal(ts) if ts is not None else None
        return set(self.con.execute(reconstruct_as_of_sql(_TARGET, self.spec, _COLS, ts_sql)).fetchall())


@pytest.mark.parametrize("mode", [MODE_SNAPSHOT, MODE_DELTA])
def test_monthly_snapshot_addressable_as_of_each_boundary(mode):
    """Seal a dataset per month, stamped at each month-end boundary; a read as-of a boundary returns
    exactly the month sealed there — calendar-addressed time travel (REQ-1166/1167)."""
    cal = Calendar(name="g", version="v1")
    d = _CalendarDriver(cal, "monthly", mode)
    jan_end = d.seal([(1, "west", 10), (2, "east", 20)], datetime(2026, 1, 15, tzinfo=UTC))
    feb_end = d.seal([(1, "west", 15), (2, "east", 20)], datetime(2026, 2, 15, tzinfo=UTC))
    mar_end = d.seal([(1, "west", 15), (3, "north", 30)], datetime(2026, 3, 15, tzinfo=UTC))

    assert jan_end == datetime(2026, 2, 1, tzinfo=UTC)  # Jan closes at Feb 1
    assert feb_end == datetime(2026, 3, 1, tzinfo=UTC)
    assert mar_end == datetime(2026, 4, 1, tzinfo=UTC)

    # as-of each boundary → exactly that period's sealed dataset
    assert d.as_of(jan_end) == {(1, "west", 10), (2, "east", 20)}
    assert d.as_of(feb_end) == {(1, "west", 15), (2, "east", 20)}
    assert d.as_of(mar_end) == {(1, "west", 15), (3, "north", 30)}  # id=2 gone in March
    # current state = the latest sealed period
    assert d.as_of(None) == {(1, "west", 15), (3, "north", 30)}


def test_as_of_between_boundaries_returns_prior_seal():
    """A read as-of an instant BETWEEN boundaries returns the most recently sealed period, not a
    partial — the append log has no version stamped inside the open window (REQ-1167)."""
    cal = Calendar(name="g", version="v1")
    d = _CalendarDriver(cal, "monthly", MODE_SNAPSHOT)
    d.seal([(1, "west", 10)], datetime(2026, 1, 15, tzinfo=UTC))  # stamped Feb 1
    d.seal([(1, "west", 99)], datetime(2026, 2, 15, tzinfo=UTC))  # stamped Mar 1
    # mid-February: only January's seal (Feb 1) is <= now; March's seal (Mar 1) is in the future.
    assert d.as_of(datetime(2026, 2, 14, tzinfo=UTC)) == {(1, "west", 10)}


def test_nth_weekday_snapshot_addressable_by_boundary():
    """The recurrence grain (REQ-1168) composes with the bitemporal binding: seals addressed by the
    3rd-Wednesday tile boundary reconstruct as-of that boundary."""
    cal = Calendar(name="g", version="v1")
    rule = NthWeekday(weekday=2, n=3)  # 3rd Wednesday
    d = _CalendarDriver(cal, rule, MODE_SNAPSHOT)
    # 3rd Wed of Feb 2026 = Feb 18 → tile ends at 3rd Wed of Mar = Mar 18.
    b1 = d.seal([(1, "west", 10)], datetime(2026, 2, 20, tzinfo=UTC))
    b2 = d.seal([(1, "west", 20)], datetime(2026, 3, 20, tzinfo=UTC))
    assert b1 == datetime(2026, 3, 18, tzinfo=UTC)
    assert b2 == datetime(2026, 4, 15, tzinfo=UTC)
    assert d.as_of(b1) == {(1, "west", 10)}
    assert d.as_of(b2) == {(1, "west", 20)}
