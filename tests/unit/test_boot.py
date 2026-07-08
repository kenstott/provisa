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
        self.jobs: list[str] = []

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


def _col(name, data_type: str | None = "bigint", pk=False):
    return SimpleNamespace(name=name, data_type=data_type, is_primary_key=pk)


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
        target_schema="analytics", target_table=name, freshness_mode="ttl", refresh_interval=600
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
    assert next(s for s in specs if s.kind == "mv").poll_seconds == 600


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
    assert "poll:s.poll" in sched.jobs  # only the poll node with a cadence gets its own job
    assert "poll:s.kafka" not in sched.jobs and "poll:mv.driven" not in sched.jobs
