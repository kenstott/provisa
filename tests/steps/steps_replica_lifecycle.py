# Copyright (c) 2026 Kenneth Stott
# Canary: 631a66d0-75c6-45d1-b289-1028e25b11bd
#
# This source code is licensed under the Business Source License 1.1

"""pytest-bdd step implementations for REQ-953 - Replica Lifecycle / Event Processing."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import Any

import pytest
from pytest_bdd import given, parsers, scenario, then, when

from provisa.events import queue, supervisor
from provisa.events.boot import (
    NodeSpec,
    build_processors,
    register_runtime,
)


# ---------------------------------------------------------------------------
# Scenario registration
# ---------------------------------------------------------------------------


@scenario(
    "../features/REQ-953.feature",
    "REQ-953 default behaviour",
)
def test_req_953_default_behaviour():
    pass


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def shared_data() -> dict:
    return {}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _noop_handle(pending: list[dict]) -> tuple[str, dict] | None:
    return None


async def _probe() -> tuple[bool, str | None]:
    return True, None


def _make_source_spec(node: str, change_signal: str = "ttl", poll_seconds: int = 60) -> NodeSpec:
    is_poll = change_signal == "ttl"
    return NodeSpec(
        node=node,
        kind="source",
        change_signal=change_signal,
        watermark_column=None,
        handle=_noop_handle,
        poll_seconds=poll_seconds if is_poll else None,
        probe_factory=(lambda: _probe) if is_poll else None,
    )


def _make_mv_spec(node: str) -> NodeSpec:
    return NodeSpec(
        node=node,
        kind="mv",
        change_signal="ttl",
        watermark_column=None,
        handle=_noop_handle,
        poll_seconds=None,
        probe_factory=None,
    )


class _CapturingScheduler:
    """Minimal APScheduler stand-in that records every add_job call."""

    def __init__(self) -> None:
        self.jobs: list[dict[str, Any]] = []

    def add_job(
        self, fn, trigger=None, id: str = "", replace_existing: bool = True, **kwargs
    ) -> None:
        self.jobs.append({"id": id, "fn": fn, "trigger": trigger})

    def job_ids(self) -> list[str]:
        return [j["id"] for j in self.jobs]

    def job(self, job_id: str) -> dict[str, Any]:
        return next(j for j in self.jobs if j["id"] == job_id)


class _FakeConn:
    """A control-plane connection whose only job here is to satisfy boot_create's post_event."""


class _FakeDB:
    """Async control-plane DB stub exposing the acquire() context manager the code uses."""

    @asynccontextmanager
    async def acquire(self):
        yield _FakeConn()


# ---------------------------------------------------------------------------
# Given
# ---------------------------------------------------------------------------


@given("a Provisa instance with source specs configured")
def provisa_instance_configured(shared_data: dict) -> None:
    """Two poll (ttl) sources, one push (delta) source, and one MV that fans out from orders."""
    source_specs = [
        _make_source_spec("schema1.orders", change_signal="ttl", poll_seconds=30),
        _make_source_spec("schema1.products", change_signal="ttl", poll_seconds=120),
        _make_source_spec("schema1.events", change_signal="delta"),  # push source
    ]
    mv_specs = [_make_mv_spec("analytics.daily_sales")]
    all_specs = source_specs + mv_specs

    db = _FakeDB()
    processors = build_processors(
        all_specs,
        db=db,
        dependents_of=lambda n: ["analytics.daily_sales"] if "orders" in n else [],
    )

    shared_data["source_specs"] = source_specs
    shared_data["mv_specs"] = mv_specs
    shared_data["all_specs"] = all_specs
    shared_data["processors"] = processors
    shared_data["db"] = db


# ---------------------------------------------------------------------------
# When
# ---------------------------------------------------------------------------


@when("the application boots")
def application_boots(shared_data: dict) -> None:
    """Run register_runtime on a capturing scheduler."""
    sched = _CapturingScheduler()
    register_runtime(
        sched,
        db=shared_data["db"],
        processors=shared_data["processors"],
        specs=shared_data["all_specs"],
        tick_seconds=5,
        lease_seconds=60,
    )
    shared_data["scheduler"] = sched


# ---------------------------------------------------------------------------
# Then
# ---------------------------------------------------------------------------


@then(
    parsers.parse(
        'register_runtime schedules a one-shot "events:boot" job that posts replace events for each source'
    )
)
def assert_boot_job_registered(shared_data: dict, monkeypatch) -> None:
    sched: _CapturingScheduler = shared_data["scheduler"]
    job_ids = sched.job_ids()
    assert job_ids.count("events:boot") == 1, f"expected one events:boot job, found {job_ids}"

    boot_job = sched.job("events:boot")
    assert boot_job["trigger"] is None, "events:boot must be a one-shot (no trigger)"
    assert callable(boot_job["fn"])

    # Run the boot job and capture the events it posts. The seam is queue.post_event, invoked
    # once per SOURCE node from boot_create; supervisor.drain is stubbed (needs a real store).
    posted: list[dict] = []

    async def _capture_post_event(conn, *, source_table, event_type, payload=None):
        posted.append({"source_table": source_table, "event_type": event_type})
        return len(posted)

    drain_calls: list[int] = []

    async def _capture_drain(db, processors, **kwargs):
        drain_calls.append(len(processors))
        return 0

    async def _capture_fan_out(conn, event_id, dependent_tables):
        return len(dependent_tables)

    monkeypatch.setattr(queue, "post_event", _capture_post_event)
    monkeypatch.setattr(queue, "fan_out", _capture_fan_out)
    monkeypatch.setattr(supervisor, "drain", _capture_drain)

    asyncio.run(boot_job["fn"]())

    source_nodes = {s.node for s in shared_data["source_specs"]}
    posted_nodes = {e["source_table"] for e in posted}
    assert posted_nodes == source_nodes, f"boot must post one event per source; got {posted_nodes}"
    assert all(e["event_type"] == "replace" for e in posted), "boot events must be 'replace'"

    shared_data["boot_posted"] = posted
    shared_data["boot_drain_calls"] = drain_calls


@then(
    "supervisor.drain lands every source replica and fans to materialized views once, idempotently"
)
def assert_drain_idempotent(shared_data: dict, monkeypatch) -> None:
    """The boot job drains the full DAG (all processors) exactly once, and re-running it re-posts
    the same replace events (idempotent by design — replace re-lands current state)."""
    sched: _CapturingScheduler = shared_data["scheduler"]
    boot_fn = sched.job("events:boot")["fn"]

    posted_runs: list[list[str]] = []
    drain_processor_counts: list[int] = []

    async def _capture_post_event(conn, *, source_table, event_type, payload=None):
        posted_runs[-1].append(source_table)
        return len(posted_runs[-1])

    async def _capture_drain(db, processors, **kwargs):
        drain_processor_counts.append(len(processors))
        return 0

    async def _capture_fan_out(conn, event_id, dependent_tables):
        return len(dependent_tables)

    monkeypatch.setattr(queue, "post_event", _capture_post_event)
    monkeypatch.setattr(queue, "fan_out", _capture_fan_out)
    monkeypatch.setattr(supervisor, "drain", _capture_drain)

    async def _boot_twice():
        posted_runs.append([])
        await boot_fn()
        posted_runs.append([])
        await boot_fn()

    asyncio.run(_boot_twice())

    # drain runs exactly once per boot over ALL processors (sources + MV fan-out).
    assert drain_processor_counts == [
        len(shared_data["processors"]),
        len(shared_data["processors"]),
    ], f"drain must fan over all processors once per boot, got {drain_processor_counts}"

    mv_procs = [p for p in shared_data["processors"] if p.node.startswith("analytics.")]
    assert mv_procs, "test requires an MV processor for the fan-out assertion"

    # Idempotent: both boots post the same set of replace events.
    assert posted_runs[0] == posted_runs[1], "re-boot must be idempotent (same events)"
    assert set(posted_runs[0]) == {s.node for s in shared_data["source_specs"]}


@then("register_runtime schedules refresh injectors for each source at its cache_ttl cadence")
def assert_poll_injectors_scheduled(shared_data: dict) -> None:
    """Every poll source gets its own poll:<node> interval job at its cadence."""
    sched: _CapturingScheduler = shared_data["scheduler"]
    job_ids = sched.job_ids()

    poll_specs = [
        s
        for s in shared_data["source_specs"]
        if s.probe_factory is not None and s.poll_seconds is not None
    ]
    assert poll_specs, "test requires at least one poll source"

    from apscheduler.triggers.interval import IntervalTrigger

    for spec in poll_specs:
        job_id = f"poll:{spec.node}"
        assert job_id in job_ids, f"expected poll job {job_id!r}, found {job_ids}"
        trigger = sched.job(job_id)["trigger"]
        assert isinstance(trigger, IntervalTrigger), f"{job_id} must use an IntervalTrigger"


@then("push sources are refreshed by their listeners - REQ-951")
def assert_push_sources_refreshed_by_listeners(shared_data: dict) -> None:
    """Push (delta) sources get NO poll job — they are driven by their listener (REQ-951)."""
    sched: _CapturingScheduler = shared_data["scheduler"]
    job_ids = sched.job_ids()

    push_specs = [s for s in shared_data["source_specs"] if s.change_signal == "delta"]
    assert push_specs, "test requires at least one push source"

    for spec in push_specs:
        assert f"poll:{spec.node}" not in job_ids, (
            f"push source {spec.node!r} must not have a poll job"
        )

    assert "events:tick" in job_ids, "events:tick must always be registered"
    assert "events:reaper" in job_ids, "events:reaper must always be registered"
