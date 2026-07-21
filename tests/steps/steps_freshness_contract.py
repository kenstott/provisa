# Copyright (c) 2026 Kenneth Stott
# Canary: 9e2f4b71-6a3c-4d58-8e19-2c7b0f5a3d64
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""pytest-bdd steps for REQ-961 — the periodic MV freshness contract on its inputs.

Drives the REAL ``MVTableProcessor`` + REAL DB-backed ``make_db_freshness_of`` reader + REAL
``node_freshness_state`` persistence over an isolated sqlite control plane. The one scenario bundles
four independent cases (trusted / trustworthy-zero / outage / holiday); each ``When`` runs a fresh
case so state never leaks between them.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import date, datetime, timedelta, timezone

import pytest
from pytest_bdd import given, when, then, scenarios
from sqlalchemy.ext.asyncio import create_async_engine

from provisa.core.database import Database
from provisa.core.schema_org import event_status, events, node_freshness_state
from provisa.events import queue
from provisa.events.calendars import Calendar
from provisa.events.deadlines import PeriodicCalendar
from provisa.events.freshness_reader import make_db_freshness_of
from provisa.events.processor import MVTableProcessor

scenarios("../features/REQ-961.feature")

UTC = timezone.utc
MV_NODE = "mat.daily_sales"
INPUT = "transactions"
WIN_END = datetime(2026, 7, 10, tzinfo=UTC)  # end of the just-closed daily window 2026-07-09
AFTER_BOUNDARY = datetime(2026, 7, 10, 1, 30, tzinfo=UTC)
HOLIDAY = frozenset({date(2026, 7, 9)})  # the just-closed window's day, marked a holiday


@pytest.fixture
def loop():
    lp = asyncio.new_event_loop()
    yield lp
    lp.close()


@pytest.fixture
def ctx(loop, tmp_path) -> dict:
    return {"loop": loop, "tmp_path": tmp_path}


async def _build_db(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / (uuid.uuid4().hex + '.db')}")
    async with engine.begin() as c:
        await c.run_sync(
            lambda s: events.metadata.create_all(
                s, tables=[events, event_status, node_freshness_state]
            )
        )
    return engine, Database(engine, name="q")


async def _run_case(tmp_path, monkeypatch, *, input_state, generate, holidays=frozenset()):
    """Build a fresh control plane, seed the input's freshness state, fan a change into the MV, and
    run the REAL processor with the REAL reader. Returns (fired, posted_for_mv)."""
    import provisa.events.processor as pm

    engine, db = await _build_db(tmp_path)
    try:
        async with db.acquire() as conn:
            e = await queue.post_event(conn, source_table=INPUT, event_type="append")
            await queue.fan_out(conn, e, [MV_NODE])
            if input_state is not None:  # None = never refreshed
                at, ok = input_state
                await queue.record_refresh(conn, INPUT, at=at, ok=ok)

        async def _generate(pending, *, prior_hash, forced):
            return generate

        proc = MVTableProcessor(
            MV_NODE,
            change_signal="ttl",
            watermark_column=None,
            dependents_of=lambda n: ["down.x"],
            db=db,
            name="box-1",
            generate=_generate,
            deadline_source=PeriodicCalendar(
                Calendar(name="g", version="v1", holidays=holidays),
                "daily",
                allowed_lateness=0.0,
                business_day=True,
            ),
            expected_events=[INPUT],
            freshness_of=make_db_freshness_of(db),
        )
        monkeypatch.setattr(pm, "_now", lambda: AFTER_BOUNDARY)
        async with db.acquire() as conn:
            fired = await proc.process_pending(conn)
            posted = [
                r
                for r in await queue.read_since(conn, cursor=0)
                if r["source_table"] == MV_NODE
            ]
        return fired, posted
    finally:
        await engine.dispose()


def _run(ctx, monkeypatch, **kw):
    fired, posted = ctx["loop"].run_until_complete(
        _run_case(ctx["tmp_path"], monkeypatch, **kw)
    )
    ctx["fired"] = fired
    ctx["posted"] = posted


# -- Given ------------------------------------------------------------------


@given("a daily-sales MV with expected-events list [transactions] and a business-day calendar")
def given_mv(ctx: dict) -> None:
    ctx["expected"] = [INPUT]  # the case wiring is applied per-When below


# -- When / Then: trusted ----------------------------------------------------


@when("the day is a business day and transactions are fresh-through-end-of-day")
def when_fresh(ctx: dict, monkeypatch) -> None:
    _run(ctx, monkeypatch, input_state=(WIN_END, True), generate=("replace", {"rows": 5}, "h5"))


@then("at the deadline the window generates the day's partition, trusted")
def then_trusted(ctx: dict) -> None:
    assert ctx["fired"] is not None
    assert [p["event_type"] for p in ctx["posted"]] == ["replace"]


# -- When / Then: trustworthy zero -------------------------------------------


@when("the day is a business day with genuinely no sales but transactions refreshed cleanly")
def when_zero(ctx: dict, monkeypatch) -> None:
    _run(ctx, monkeypatch, input_state=(WIN_END, True), generate=("replace", {"rows": 0}, "h0"))


@then("transactions is fresh-through-end-of-day with zero rows and a trustworthy zero is sealed")
def then_zero_sealed(ctx: dict) -> None:
    assert ctx["fired"] is not None
    assert [p["event_type"] for p in ctx["posted"]] == ["replace"]


# -- When / Then: outage -----------------------------------------------------


@when("the day is a business day but transactions is not fresh-through-end-of-day at the deadline")
def when_stale(ctx: dict, monkeypatch) -> None:
    _run(
        ctx,
        monkeypatch,
        input_state=(WIN_END - timedelta(hours=2), True),
        generate=("replace", {"rows": 5}, "h5"),
    )


@then("an outage is raised as warn/hold (not a silent skip)")
def then_outage(ctx: dict) -> None:
    assert ctx["fired"] is None  # held, not sealed
    assert [p["event_type"] for p in ctx["posted"]] == ["warn"]  # not a silent skip


# -- When / Then: holiday ----------------------------------------------------


@when("the calendar marks the day a holiday")
def when_holiday(ctx: dict, monkeypatch) -> None:
    _run(
        ctx,
        monkeypatch,
        input_state=(WIN_END, True),
        generate=("replace", {"rows": 5}, "h5"),
        holidays=HOLIDAY,
    )


@then("no window exists, the MV does not generate, and no alarm is raised")
def then_holiday_silent(ctx: dict) -> None:
    assert ctx["fired"] is None  # gated out of existence
    assert ctx["posted"] == []  # no seal, no warn, no alarm
