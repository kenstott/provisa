# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Live-data temporal processing: calendars (REQ-962), the periodic freshness contract (REQ-961),
live debounce (REQ-963), and completeness-gated windows (REQ-958)."""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from provisa.core.database import Database
from provisa.core.schema_org import event_status, events, node_freshness_state
from provisa.events import queue
from provisa.events.calendars import (
    BaseSystem,
    Calendar,
    CalendarRegistry,
    Grain,
    next_boundary,
    parse_grain,
    window_for,
)
from provisa.events.deadlines import (
    LiveDebounce,
    PeriodicCalendar,
    build_deadline_source,
)
from provisa.events.freshness_contract import evaluate_contract
from provisa.events.processor import MVTableProcessor, TableProcessor
from provisa.freshness.adapters import StateSubject

UTC = timezone.utc


# ============================ REQ-962: calendars ============================


def _greg(**kw) -> Calendar:
    return Calendar(name="g", version="v1", tz=kw.pop("tz", "UTC"), **kw)


def test_daily_window_gregorian_utc():
    cal = _greg()
    w = window_for(cal, "daily", datetime(2026, 7, 10, 14, tzinfo=UTC))
    assert w is not None
    assert w.start == datetime(2026, 7, 10, tzinfo=UTC)
    assert w.end == datetime(2026, 7, 11, tzinfo=UTC)
    assert w.window_id == "2026-07-10"
    assert (w.end - w.start) == timedelta(hours=24)


def test_daily_dst_spring_forward_is_23h():
    cal = _greg(tz="America/New_York")
    w = window_for(cal, "daily", datetime(2026, 3, 8, 12, tzinfo=UTC))
    assert w is not None and (w.end - w.start) == timedelta(hours=23)  # never a fixed 24h


def test_daily_dst_fall_back_is_25h():
    cal = _greg(tz="America/New_York")
    w = window_for(cal, "daily", datetime(2026, 11, 1, 12, tzinfo=UTC))
    assert w is not None and (w.end - w.start) == timedelta(hours=25)


def test_weekly_monthly_quarterly_annual_gregorian():
    cal = _greg()
    inst = datetime(2026, 2, 18, tzinfo=UTC)  # a Wednesday
    wk = window_for(cal, Grain.WEEKLY, inst)
    assert wk is not None and wk.start == datetime(2026, 2, 16, tzinfo=UTC)  # Monday
    assert (wk.end - wk.start) == timedelta(days=7)
    mo = window_for(cal, "monthly", inst)
    assert mo is not None and mo.window_id == "2026-02"
    assert mo.start == datetime(2026, 2, 1, tzinfo=UTC)
    assert mo.end == datetime(2026, 3, 1, tzinfo=UTC)
    q = window_for(cal, "quarterly", inst)
    assert q is not None and q.window_id == "2026-Q1"
    assert q.start == datetime(2026, 1, 1, tzinfo=UTC)
    assert q.end == datetime(2026, 4, 1, tzinfo=UTC)
    yr = window_for(cal, "annual", inst)
    assert yr is not None and yr.window_id == "2026"
    assert yr.end == datetime(2027, 1, 1, tzinfo=UTC)


def test_grains_nest():
    """day ⊂ month ⊂ quarter ⊂ year — sub-windows fall inside their parent (REQ-962)."""
    cal = _greg()
    inst = datetime(2026, 8, 20, tzinfo=UTC)
    d = window_for(cal, "daily", inst)
    m = window_for(cal, "monthly", inst)
    q = window_for(cal, "quarterly", inst)
    y = window_for(cal, "annual", inst)
    assert d and m and q and y
    assert q.start <= m.start and m.end <= q.end
    assert y.start <= q.start and q.end <= y.end
    assert m.start <= d.start and d.end <= m.end


def test_fiscal_quarter_and_year_anchor_october():
    # US-federal-style fiscal year: starts Oct 1, labeled by the calendar year it ENDS.
    cal = Calendar(name="fy", version="v1", base_system=BaseSystem.FISCAL, fiscal_anchor=(10, 1))
    inst = datetime(2025, 11, 15, tzinfo=UTC)  # first fiscal quarter of FY2026
    q = window_for(cal, "quarterly", inst)
    assert q is not None and q.window_id == "2026-Q1"
    assert q.start == datetime(2025, 10, 1, tzinfo=UTC)
    assert q.end == datetime(2026, 1, 1, tzinfo=UTC)
    yr = window_for(cal, "annual", inst)
    assert yr is not None and yr.window_id == "2026"
    assert yr.start == datetime(2025, 10, 1, tzinfo=UTC)
    assert yr.end == datetime(2026, 10, 1, tzinfo=UTC)


def test_retail_445_quarter_and_periods():
    anchor = date(2026, 2, 1)  # reference retail-year start
    cal = Calendar(name="r", version="v1", base_system=BaseSystem.RETAIL_445, retail_anchor=anchor)
    inst = datetime(2026, 2, 15, tzinfo=UTC)  # within the first 4-4-5 quarter
    q = window_for(cal, "quarterly", inst)
    assert q is not None and q.window_id == "2026-Q1"
    assert (q.end - q.start) == timedelta(weeks=13)  # 4+4+5 = 13 weeks
    p1 = window_for(cal, "monthly", inst)  # retail PERIOD 1 = 4 weeks
    assert p1 is not None and p1.window_id == "2026-P1"
    assert (p1.end - p1.start) == timedelta(weeks=4)
    # a date in the 3rd period of the quarter → a 5-week period
    p3 = window_for(cal, "monthly", datetime(2026, 4, 1, tzinfo=UTC))
    assert p3 is not None and p3.window_id == "2026-P3"
    assert (p3.end - p3.start) == timedelta(weeks=5)


def test_business_day_grain_gates_holiday_and_weekend():
    holiday = date(2026, 7, 3)
    cal = _greg(holidays=frozenset({holiday}))
    # holiday on a business-day grain → NO window
    assert window_for(cal, "daily", datetime(2026, 7, 3, 12, tzinfo=UTC), business_day=True) is None
    # weekend (2026-07-04 is a Saturday) → NO window
    assert window_for(cal, "daily", datetime(2026, 7, 4, 12, tzinfo=UTC), business_day=True) is None
    # a business day → window exists
    assert window_for(cal, "daily", datetime(2026, 7, 2, 12, tzinfo=UTC), business_day=True)
    # calendar-day grain still opens a window on the holiday
    assert window_for(cal, "daily", datetime(2026, 7, 3, 12, tzinfo=UTC), business_day=False)


def test_versioned_holidays_are_immutable_per_version():
    day = datetime(2026, 12, 24, 12, tzinfo=UTC)
    v1 = Calendar(name="c", version="v1", holidays=frozenset({date(2026, 12, 24)}))
    v2 = Calendar(name="c", version="v2", holidays=frozenset())  # a later edit removed it
    assert window_for(v1, "daily", day, business_day=True) is None  # v1: a holiday, no window
    assert window_for(v2, "daily", day, business_day=True) is not None  # v2: a business day


def test_next_boundary_skips_holiday_run():
    cal = _greg(holidays=frozenset({date(2026, 7, 3)}))
    # Thu 2026-07-02 business day → next boundary skips the Fri holiday + weekend to Mon 07-06.
    nb = next_boundary(cal, "daily", datetime(2026, 7, 3, 10, tzinfo=UTC), business_day=True)
    assert nb == datetime(2026, 7, 7, tzinfo=UTC)  # end of Mon 2026-07-06


def test_unknown_grain_fails_loud():
    with pytest.raises(ValueError, match="unknown grain"):
        parse_grain("hourly")
    with pytest.raises(ValueError, match="unknown grain"):
        window_for(_greg(), "hourly", datetime(2026, 1, 1, tzinfo=UTC))


def test_unknown_calendar_fails_loud():
    reg = CalendarRegistry()
    reg.register(_greg())
    with pytest.raises(ValueError, match="unknown calendar"):
        reg.get("nope")


def test_retail_without_anchor_fails_loud():
    cal = Calendar(name="r", version="v1", base_system=BaseSystem.RETAIL_445)
    with pytest.raises(ValueError, match="retail_anchor"):
        window_for(cal, "weekly", datetime(2026, 2, 1, tzinfo=UTC))


# ==================== REQ-963: live debounce deadline ====================


def test_live_debounce_min_quiet_or_cap():
    now = datetime(2026, 7, 8, tzinfo=UTC)
    t0 = now
    peeked = [{"created_at": t0}, {"created_at": t0 + timedelta(seconds=2)}]
    # quiet wins when the cap is far out
    assert LiveDebounce(quiet=10, max_delay=300).deadline(now, peeked) == t0 + timedelta(seconds=12)
    # cap wins under a long tail
    assert LiveDebounce(quiet=10, max_delay=5).deadline(now, peeked) == t0 + timedelta(seconds=5)
    # quiet<=0 → real-time (no deadline)
    assert LiveDebounce(quiet=0, max_delay=5).deadline(now, peeked) is None


def test_live_debounce_mandatory_cap():
    with pytest.raises(ValueError, match="max_delay"):
        LiveDebounce(quiet=2, max_delay=0)
    with pytest.raises(ValueError, match="max_delay"):
        build_deadline_source(debounce_quiet=2, debounce_max_delay=None)


def test_continuous_churn_still_fires_at_cap():
    """Under continuous churn the quiet window never elapses, so the cap guarantees a fire — the
    deadline never drifts past first_change + max_delay (REQ-963)."""
    src = LiveDebounce(quiet=2, max_delay=30)
    t0 = datetime(2026, 7, 8, tzinfo=UTC)
    # a new change every 1s (< quiet) for 60s: last is always moving, but the cap pins the deadline.
    peeked = [{"created_at": t0 + timedelta(seconds=i)} for i in range(60)]
    assert src.deadline(t0, peeked) == t0 + timedelta(seconds=30)  # first + max_delay, capped


def test_build_deadline_source_rejects_both_live_and_periodic():
    cal = _greg()
    with pytest.raises(ValueError, match="not both"):
        build_deadline_source(debounce_quiet=2, debounce_max_delay=5, calendar=cal, grain="daily")


# ==================== processor-level: DB helpers ====================


@asynccontextmanager
async def _db(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'q.db'}")
    async with engine.begin() as c:
        await c.run_sync(
            lambda s: events.metadata.create_all(
                s, tables=[events, event_status, node_freshness_state]
            )
        )
    try:
        yield Database(engine, name="q")
    finally:
        await engine.dispose()


class _RecProc(TableProcessor):
    """Records the ctx it was handed so a window peg / claimed-set can be asserted."""

    kind = "mv"

    def __init__(self, *a, result, **k):
        super().__init__(*a, **k)
        self._result = result
        self.last_ctx = None
        self.handle_calls = 0

    async def handle(self, pending, *, prior_hash, ctx=None):
        self.handle_calls += 1
        self.last_ctx = ctx
        return self._result


async def _fan_in(db, node, source, n=1):
    ids = []
    async with db.acquire() as conn:
        for _ in range(n):
            e = await queue.post_event(conn, source_table=source, event_type="append")
            await queue.fan_out(conn, e, [node])
            ids.append(e)
    return ids


def _periodic_proc(db, *, source, expected, freshness_of, result, lateness=3600.0, deps=None):
    cal = _greg()
    return _RecProc(
        "mat.daily_sales",
        change_signal="ttl",
        watermark_column=None,
        dependents_of=lambda n: deps or ["down.x"],
        db=db,
        name="box-1",
        result=result,
        deadline_source=PeriodicCalendar(cal, "daily", allowed_lateness=lateness),
        expected_events=expected,
        freshness_of=freshness_of,
    )


# ==================== REQ-961: periodic freshness contract ====================


def test_evaluate_contract_pull_pass_and_fail():
    win_end = datetime(2026, 7, 10, tzinfo=UTC).timestamp()
    fresh = StateSubject(refreshed_at=win_end, ok=True)  # refreshed to cover the boundary
    stale = StateSubject(refreshed_at=win_end - 100, ok=True)  # did not cover the window
    failed = StateSubject(refreshed_at=win_end + 100, ok=False)  # last refresh failed

    def of(inp):
        return {"a": fresh, "b": stale, "c": failed}[inp]

    assert evaluate_contract(["a"], of, win_end).trusted  # all fresh-through
    assert evaluate_contract([], of, win_end).trusted  # calendar-only: verify nothing
    r = evaluate_contract(["a", "b", "c"], of, win_end)
    assert r.is_outage and set(r.outages) == {"b", "c"}


def test_trustworthy_zero_is_fresh():
    """A fresh input with zero rows is a trustworthy zero — the contract still passes (REQ-961)."""
    win_end = datetime(2026, 7, 10, tzinfo=UTC).timestamp()
    zero = StateSubject(refreshed_at=win_end, ok=True)  # fresh; row count is irrelevant here
    assert evaluate_contract(["z"], lambda _i: zero, win_end).trusted


@pytest.mark.asyncio
async def test_periodic_defers_until_boundary_plus_lateness(tmp_path, monkeypatch):
    import provisa.events.processor as pm

    win_end = datetime(2026, 7, 10, tzinfo=UTC).timestamp()
    fresh = StateSubject(refreshed_at=win_end, ok=True)
    async with _db(tmp_path) as db:
        await _fan_in(db, "mat.daily_sales", "s.transactions")
        proc = _periodic_proc(
            db,
            source="s.transactions",
            expected=["s.transactions"],
            freshness_of=lambda _i: fresh,
            result=("replace", {"rows": 5}, "h1"),
            lateness=3600.0,
        )
        # 30 min after the boundary but before boundary+lateness (1h) → defer.
        monkeypatch.setattr(pm, "_now", lambda: datetime(2026, 7, 10, 0, 30, tzinfo=UTC))
        async with db.acquire() as conn:
            assert await proc.process_pending(conn) is None
            assert proc.handle_calls == 0
        # past boundary+lateness → fire, sealing the just-closed window 2026-07-09, pegged as-of
        # its end (2026-07-10 00:00).
        monkeypatch.setattr(pm, "_now", lambda: datetime(2026, 7, 10, 1, 30, tzinfo=UTC))
        async with db.acquire() as conn:
            my_event = await proc.process_pending(conn)
            assert my_event is not None
            assert proc.last_ctx.window_id == "2026-07-09"
            assert proc.last_ctx.window == (
                datetime(2026, 7, 9, tzinfo=UTC),
                datetime(2026, 7, 10, tzinfo=UTC),
            )
            posted = [
                r
                for r in await queue.read_since(conn, cursor=0)
                if r["source_table"] == "mat.daily_sales"
            ]
            assert [p["event_type"] for p in posted] == ["replace"]  # no NO_CHANGE type exists


@pytest.mark.asyncio
async def test_periodic_stale_input_is_outage_warn_hold(tmp_path, monkeypatch):
    import provisa.events.processor as pm

    win_end = datetime(2026, 7, 10, tzinfo=UTC).timestamp()
    stale = StateSubject(refreshed_at=win_end - 500, ok=True)  # not fresh-through window.end
    async with _db(tmp_path) as db:
        await _fan_in(db, "mat.daily_sales", "s.transactions")
        proc = _periodic_proc(
            db,
            source="s.transactions",
            expected=["s.transactions"],
            freshness_of=lambda _i: stale,
            result=("replace", {"rows": 5}, "h1"),
            lateness=0.0,
        )
        monkeypatch.setattr(pm, "_now", lambda: datetime(2026, 7, 10, 1, 30, tzinfo=UTC))
        async with db.acquire() as conn:
            assert await proc.process_pending(conn) is None  # outage → hold, no seal
            assert proc.handle_calls == 0  # never produced
            posted = [r for r in await queue.read_since(conn, cursor=0)]
            kinds = {r["source_table"]: r["event_type"] for r in posted}
            assert kinds.get("mat.daily_sales") == "warn"  # warn/hold, not a silent skip
            # the claim was completed (not orphaned) so the loop does not spin on it
            assert (
                await queue.resume_claims(
                    conn, dependent_table="mat.daily_sales", processor_name="box-1"
                )
                == []
            )


@pytest.mark.asyncio
async def test_periodic_holiday_no_window_no_alarm(tmp_path, monkeypatch):
    import provisa.events.processor as pm

    cal = _greg(holidays=frozenset({date(2026, 7, 9)}))
    fresh = StateSubject(refreshed_at=datetime(2026, 7, 10, tzinfo=UTC).timestamp(), ok=True)
    async with _db(tmp_path) as db:
        await _fan_in(db, "mat.daily_sales", "s.transactions")
        proc = _RecProc(
            "mat.daily_sales",
            change_signal="ttl",
            watermark_column=None,
            dependents_of=lambda n: ["down.x"],
            db=db,
            name="box-1",
            result=("replace", {"rows": 5}, "h1"),
            deadline_source=PeriodicCalendar(cal, "daily", business_day=True),
            expected_events=["s.transactions"],
            freshness_of=lambda _i: fresh,
        )
        # now sits in 2026-07-10; the just-closed window is 2026-07-09 — a holiday → gated out.
        monkeypatch.setattr(pm, "_now", lambda: datetime(2026, 7, 10, 1, 30, tzinfo=UTC))
        async with db.acquire() as conn:
            assert await proc.process_pending(conn) is None  # no fire
            assert proc.handle_calls == 0
            # the MV emits NOTHING — no seal, no warn, no deadline alarm (only the upstream event).
            emitted = [
                r
                for r in await queue.read_since(conn, cursor=0)
                if r["source_table"] == "mat.daily_sales"
            ]
            assert emitted == []


# ==================== REQ-958: completeness-gated windows ====================


@pytest.mark.asyncio
async def test_window_opens_coalesces_fires_once_with_peg(tmp_path, monkeypatch):
    """A window opens on the first fanned-in event, gathers subsequent events across inputs, and
    fires ONCE at the deadline coalescing them all (fan-in collapse) — pegged to the window (REQ-958)."""
    import provisa.events.processor as pm

    win_end = datetime(2026, 7, 10, tzinfo=UTC).timestamp()
    fresh = StateSubject(refreshed_at=win_end, ok=True)
    async with _db(tmp_path) as db:
        # cross-input burst: two events from A, one from B — all fan into the temporal MV.
        await _fan_in(db, "mat.daily_sales", "s.a", n=2)
        await _fan_in(db, "mat.daily_sales", "s.b", n=1)
        proc = _periodic_proc(
            db,
            source="s.a",
            expected=[],  # calendar-only contract: verify nothing, isolate the window lifecycle
            freshness_of=lambda _i: fresh,
            result=("replace", {"rows": 9}, "h1"),
            lateness=3600.0,
        )
        # before the deadline: the window stays open, events accumulate unclaimed (not fired).
        monkeypatch.setattr(pm, "_now", lambda: datetime(2026, 7, 10, 0, 30, tzinfo=UTC))
        async with db.acquire() as conn:
            assert await proc.process_pending(conn) is None
            assert len(await queue.peek_pending(conn, dependent_table="mat.daily_sales")) == 3
        # at the deadline: ONE fire coalescing all three, a single downstream ripple.
        monkeypatch.setattr(pm, "_now", lambda: datetime(2026, 7, 10, 1, 30, tzinfo=UTC))
        async with db.acquire() as conn:
            my_event = await proc.process_pending(conn)
            assert my_event is not None
            assert proc.handle_calls == 1  # collapsed to one recompute
            assert len(proc.last_ctx.claimed) == 3  # all fan-ins coalesced
            assert proc.last_ctx.window_id == "2026-07-09"  # pegged to the closed window
            posted = [
                r
                for r in await queue.read_since(conn, cursor=0)
                if r["source_table"] == "mat.daily_sales"
            ]
            assert len(posted) == 1  # a single ripple, not three


@pytest.mark.asyncio
async def test_live_debounce_window_is_none(tmp_path, monkeypatch):
    """A live (debounce) MV has no calendar peg — ctx.window stays None; it computes as-of now."""
    import provisa.events.processor as pm

    async with _db(tmp_path) as db:
        await _fan_in(db, "mv.live", "s.o", n=2)
        proc = _RecProc(
            "mv.live",
            change_signal="ttl",
            watermark_column=None,
            dependents_of=lambda n: ["down.x"],
            db=db,
            name="box-1",
            result=("replace", {"n": 2}, "h1"),
            debounce_quiet=2.0,
            debounce_max_delay=30.0,
        )
        monkeypatch.setattr(pm, "_now", lambda: pm.datetime.now(UTC) + timedelta(seconds=120))
        async with db.acquire() as conn:
            assert await proc.process_pending(conn) is not None
            assert proc.last_ctx.window is None and proc.last_ctx.window_id is None


def test_mv_processor_variant_still_constructs():
    """The MV variant accepts the new periodic knobs without disturbing its generate contract."""

    async def generate(pending, *, prior_hash):
        return ("replace", {"g": 1}, "h")

    cal = _greg()
    mv = MVTableProcessor(
        "m",
        change_signal="ttl",
        watermark_column=None,
        dependents_of=lambda n: [],
        db=None,
        name="b",
        generate=generate,
        deadline_source=PeriodicCalendar(cal, "monthly"),
        expected_events=["s.x"],
        freshness_of=lambda _i: StateSubject(refreshed_at=0.0, ok=True),
    )
    assert mv._deadline is not None
