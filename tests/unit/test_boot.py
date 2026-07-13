# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-941: event-loop boot — build processors from specs, register tick/reaper/poll jobs."""

from __future__ import annotations

import pytest

from types import SimpleNamespace

from provisa.events.boot import (
    NodeSpec,
    build_processors,
    register_runtime,
    specs_from_config,
)
from provisa.events.processor import MVTableProcessor, SourceTableProcessor
from provisa.federation.engine import build_duckdb_engine


async def _noop_handle(pending):
    return None


async def _probe():
    return False, None


def _spec(node, kind, **kw):
    return NodeSpec(
        node=node,
        kind=kind,
        change_signal=kw.get("cs", "ttl"),
        watermark_column=None,
        handle=_noop_handle,
        poll_seconds=kw.get("poll"),
        probe_factory=kw.get("pf"),
    )


class _Sched:
    def __init__(self):
        self.jobs: list[str | None] = []

    def add_job(self, fn, trigger=None, id=None, replace_existing=None):
        self.jobs.append(id)


def test_build_processors_picks_variant():
    specs = [_spec("s.orders", "source"), _spec("mv.daily", "mv")]
    procs = build_processors(specs, db=object(), dependents_of=lambda n: [])
    assert isinstance(procs[0], SourceTableProcessor) and procs[0].node == "s.orders"
    assert isinstance(procs[1], MVTableProcessor) and procs[1].node == "mv.daily"
    assert procs[0].name == "source:s.orders"  # unique lease name


def test_build_processors_rejects_unknown_kind():
    with pytest.raises(ValueError, match="unknown node kind"):
        build_processors([_spec("x", "widget")], db=object(), dependents_of=lambda n: [])


def _src(sid, stype):
    return SimpleNamespace(id=sid, type=SimpleNamespace(value=stype), change_signal="ttl")


def _col(name, data_type: str | None = "bigint", pk=False, nf=None):
    return SimpleNamespace(name=name, data_type=data_type, is_primary_key=pk, native_filter_type=nf)


def _tbl(sid, tname, cols, *, cache_ttl=300):
    return SimpleNamespace(
        source_id=sid,
        schema_name="default",
        table_name=tname,
        change_signal=None,
        watermark_column=None,
        live=None,
        columns=cols,
        cache_ttl=cache_ttl,
    )


def _mv(name):
    return SimpleNamespace(
        target_schema="analytics",
        target_table=name,
        freshness_mode="ttl",
        refresh_interval=600,
        debounce_quiet=0.0,
        debounce_max_delay=None,
    )


async def _fetch(_pending):
    return []


async def _run_query():
    return []


def test_specs_from_config_binds_materialized_sources_and_mvs():
    engine = build_duckdb_engine()
    sources = [_src("api", "openapi"), _src("pg", "postgresql")]
    tables = [
        _tbl(
            "api", "events", [_col("id", "bigint", pk=True), _col("status", "text")]
        ),  # MATERIALIZED
        _tbl("pg", "users", [_col("id")]),  # postgresql on duckdb → VIRTUAL (attach) → skip
        _tbl("api", "bad", [_col("id", None)]),  # untyped → skip
        # parameterized (native-filter arg) → a function, no snapshot → not a source node
        _tbl("api", "one", [_col("_nf_key", "text", nf="query_param"), _col("val", "text")]),
    ]
    specs = specs_from_config(
        sources=sources,
        tables=tables,
        mvs=[_mv("daily")],
        engine=engine,
        store_dsn="sqlite://",
        source_fetch=lambda s, t: _fetch,
        mv_columns=lambda m: [("d", "date")],
        mv_run_query=lambda m: _run_query,
    )
    kinds = {s.node: s.kind for s in specs}
    assert kinds == {
        "default.events": "source",
        "analytics.daily": "mv",
    }  # virtual + untyped skipped
    src = next(s for s in specs if s.kind == "source")
    assert src.change_signal == "ttl" and src.poll_seconds == 300
    # A poll source carries a probe_factory so register_runtime schedules its refresh (REQ-941).
    assert src.probe_factory is not None
    mv_spec = next(s for s in specs if s.kind == "mv")
    assert mv_spec.poll_seconds == 600
    # A poll-mode (ttl) MV must ALSO carry a probe_factory so its poll job registers — otherwise a
    # source-less view MV never recomputes and sits STALE forever despite its TTL.
    assert mv_spec.probe_factory is not None
    assert mv_spec.probe_type == "none"


def test_ttl_mv_poll_job_is_registered():
    """Regression: a poll-mode MV must get its own poll job in register_runtime. It previously had no
    probe_factory, so a source-less view MV (no upstream ripple) never fired → STALE forever."""
    specs = [
        _spec(
            "mv.ttl", "mv", cs="ttl", poll=300, pf=lambda: _probe
        ),  # poll MV WITH probe → poll job
        _spec("mv.driven", "mv"),  # no cadence/probe → upstream-driven only, no poll job
    ]
    procs = build_processors(specs, db=object(), dependents_of=lambda n: [])
    sched = _Sched()
    register_runtime(sched, db=object(), processors=procs, specs=specs)
    assert "poll:mv.ttl" in sched.jobs
    assert "poll:mv.driven" not in sched.jobs


def test_register_runtime_adds_tick_reaper_and_poll_jobs():
    specs = [
        _spec("s.poll", "source", poll=30, pf=lambda: _probe),  # poll node with cadence → poll job
        _spec("s.kafka", "source", cs="kafka"),  # push → no poll job (listener owned by processor)
        _spec("mv.driven", "mv"),  # no cadence → driven only by upstream events, no poll job
    ]
    procs = build_processors(specs, db=object(), dependents_of=lambda n: [])
    sched = _Sched()
    register_runtime(sched, db=object(), processors=procs, specs=specs)
    assert "events:tick" in sched.jobs and "events:reaper" in sched.jobs
    assert "events:boot" in sched.jobs  # one-shot boot-create job (build replicas at boot)
    assert "poll:s.poll" in sched.jobs  # only the poll node with a cadence gets its own job
    assert "poll:s.kafka" not in sched.jobs and "poll:mv.driven" not in sched.jobs


def test_register_runtime_seed_false_skips_boot_create():
    # A RE-wire after a runtime MV create must register poll jobs WITHOUT re-seeding/re-landing every
    # source — so no one-shot boot-create job, but poll jobs (and tick/reaper) still (re)register.
    specs = [_spec("mv.ttl", "mv", poll=300, pf=lambda: _probe)]
    procs = build_processors(specs, db=object(), dependents_of=lambda n: [])
    sched = _Sched()
    register_runtime(sched, db=object(), processors=procs, specs=specs, seed=False)
    assert "events:boot" not in sched.jobs  # no source re-seed on a re-wire
    assert "events:tick" in sched.jobs and "events:reaper" in sched.jobs
    assert "poll:mv.ttl" in sched.jobs  # the new MV's refresh cadence is registered


@pytest.mark.asyncio
async def test_boot_create_seeds_one_replace_event_per_source(monkeypatch):
    # Design: replicas are BUILT at boot. boot_create posts one 'replace' event per SOURCE node
    # (never MV nodes — those are driven downstream); the caller drains to land them.
    posted: list[tuple[str, str]] = []
    fanned: list[tuple[int, list]] = []

    async def _fake_post(conn, *, source_table, event_type, payload=None):
        posted.append((source_table, event_type))
        return len(posted)

    async def _fake_fan_out(conn, event_id, dependent_tables):
        fanned.append((event_id, list(dependent_tables)))
        return len(dependent_tables)

    monkeypatch.setattr("provisa.events.boot.queue.post_event", _fake_post)
    monkeypatch.setattr("provisa.events.boot.queue.fan_out", _fake_fan_out)

    class _Ctx:
        async def __aenter__(self):
            return object()

        async def __aexit__(self, *a):
            return False

    db = SimpleNamespace(acquire=lambda: _Ctx())
    from provisa.events.boot import boot_create

    specs = [_spec("s.a", "source"), _spec("mv.x", "mv"), _spec("s.b", "source")]
    n = await boot_create(db, specs)
    assert n == 2
    assert posted == [("s.a", "replace"), ("s.b", "replace")]
    # each source's boot event is enqueued as work for the source node itself (self-dependent)
    assert fanned == [(1, ["s.a"]), (2, ["s.b"])]
