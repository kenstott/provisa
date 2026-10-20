# Copyright (c) 2026 Kenneth Stott
# Canary: 5e1a9c47-2b6d-4f83-9a0e-7c4d2b8f1e65
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""``refreshMv(window:)`` builds a view sealed at a business day's close (REQ-2120).

A view naming a calendar is built by the event loop at each window's close, its append stamped
by the window's end. The admin refresh's ``window`` builds it for a given business day the same
way: the append stamp is the end of the calendar's window for that date -- the day's close in its
zone (REQ-2111) -- through the one refresh path. Run against real rows in DuckDB through
refresh_mv, the inherited view of test_bitemporal_inherit (one versioned table grouped), its
calendar read from the model store."""

# Requirements: REQ-2120, REQ-2111, REQ-2109

from __future__ import annotations

import datetime as dt

import pytest

from provisa.core.repositories import calendar as calendar_repo
from provisa.mv.models import MVStatus
from provisa.mv.refresh import SealedBuildRefused, refresh_mv
from tests.helpers import build_through_a_stand_in_pipeline, hold_registered_tables
from tests.unit.test_bitemporal_inherit import World

pytestmark = pytest.mark.unit

# A bank's day closes at 18:00 New York time: 22:00 UTC in October (EDT).
FRIDAY, MONDAY = dt.date(2026, 10, 2), dt.date(2026, 10, 5)
FRIDAY_CLOSE, MONDAY_CLOSE = "2026-10-02 22:00:00", "2026-10-05 22:00:00"


@pytest.fixture(autouse=True)
def _stand_in(monkeypatch):
    build_through_a_stand_in_pipeline(monkeypatch)
    hold_registered_tables(monkeypatch, "payments", schema="fin")


@pytest.fixture
async def store(control_plane):
    plane = control_plane.org()
    async with plane.acquire() as conn:
        await calendar_repo.upsert(
            conn,
            {
                "name": "bank",
                "version": "v1",
                "tz": "America/New_York",
                "day_close": "18:00",
                "holidays": ["2026-12-25"],
            },
        )
    return plane


@pytest.fixture
def bank(monkeypatch) -> World:
    w = World(monkeypatch)
    w.mv.calendar, w.mv.grain, w.mv.business_day_grain = "bank", "daily", True
    return w


async def _seal(w: World, store, day: dt.date) -> None:
    await refresh_mv(w.engine, w.mv, w.registry, store=store, window=day)
    assert w.mv.status == MVStatus.FRESH, w.mv.last_error


async def test_each_sealed_build_is_stamped_at_its_days_close(bank, store):
    await _seal(bank, store, FRIDAY)
    bank.add((4, "p4", "B", "2026-10-05", "2026-10-05 12:00:00", 30))
    await _seal(bank, store, MONDAY)

    stamps = bank.con.execute(
        f"SELECT DISTINCT CAST(sys_recorded_at AS VARCHAR) FROM {bank.target} ORDER BY 1"
    ).fetchall()
    assert stamps == [(FRIDAY_CLOSE,), (MONDAY_CLOSE,)]
    assert bank.log_rows(FRIDAY_CLOSE) == [
        ("A", "2026-10-01", 100, "upsert"),
        ("A", "2026-10-02", 70, "upsert"),
        ("B", "2026-10-01", 50, "upsert"),
    ]
    assert bank.log_rows(MONDAY_CLOSE) == [("B", "2026-10-05", 30, "upsert")]


async def test_a_restatement_before_the_next_build_is_picked_up_for_its_past_date(bank, store):
    await _seal(bank, store, FRIDAY)
    # Over the weekend p1, booked for 1 Oct, is restated from 100 to 120.
    bank.add((5, "p1", "A", "2026-10-01", "2026-10-03 09:00:00", 120))
    await _seal(bank, store, MONDAY)

    assert bank.log_rows(MONDAY_CLOSE) == [("A", "2026-10-01", 120, "upsert")]
    assert ("A", "2026-10-01", 120) in bank.read()
    # As recorded at Friday's close, the first version answers.
    assert bank.read(as_recorded="2026-10-02T22:00:00") == {
        ("A", "2026-10-01", 100),
        ("B", "2026-10-01", 50),
        ("A", "2026-10-02", 70),
    }


async def test_a_day_its_calendar_has_no_window_on_is_refused(bank, store):
    with pytest.raises(ValueError, match=r"calendar 'bank' has no daily window on 2026-10-03"):
        await refresh_mv(
            bank.engine, bank.mv, bank.registry, store=store, window=dt.date(2026, 10, 3)
        )


async def test_a_view_naming_no_calendar_is_refused_by_name(monkeypatch, store):
    w = World(monkeypatch)  # no calendar
    with pytest.raises(SealedBuildRefused, match=r"MV 'by_store' names no calendar"):
        await refresh_mv(w.engine, w.mv, w.registry, store=store, window=FRIDAY)
    # Refused before anything was built or recorded.
    assert w.mv.status != MVStatus.FRESH and w.mv.last_error is None
    tables = w.con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'mvs'"
    ).fetchall()
    assert tables == []
