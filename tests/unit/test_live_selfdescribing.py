# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-964 (data-model-driven estate: declare SQL + rules → the system DERIVES the process),
REQ-966 (zero-config happy path: an MV needs only its SQL), and REQ-967 (self-describing estate:
operational metadata is an MV over the streams the machinery already emits — no bespoke subsystem)."""

from __future__ import annotations

from contextlib import asynccontextmanager

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from provisa.core.database import Database
from provisa.core.schema_org import event_status, events, node_freshness_state
from provisa.events import lineage, queue, supervisor
from provisa.events.handlers import make_mv_generate, make_source_land
from provisa.events.processor import MVTableProcessor, SourceTableProcessor
from provisa.federation import store_writer
from provisa.mv.models import MVDefinition

_COLS = [("id", "bigint"), ("status", "text")]


def _store(tmp_path) -> str:
    return f"sqlite+aiosqlite:///{tmp_path / 'store.db'}"


async def _rows(dsn, table):
    async with store_writer.store_connection(dsn) as conn:
        return await conn.fetch(f"SELECT id, status FROM {table} ORDER BY id")


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


# ---------------------------------------------------------------------------
# REQ-964: declare SQL + rules → the system DERIVES the process (lineage, schema, pk, determinism)
# ---------------------------------------------------------------------------


def test_process_is_derived_from_sql_declaration():
    sql = "SELECT id, status, count(*) AS n FROM shop.orders GROUP BY id, status"
    # lineage (fan-out edges) is DERIVED from the SQL — never hand-declared
    assert lineage.extract_inputs(sql) == {"shop.orders"}
    assert lineage.dependents({"mart.by_status": sql}) == {"shop.orders": ["mart.by_status"]}
    # output schema + PK are DERIVED from the SELECT
    inputs = {"shop.orders": {"id": "bigint", "status": "varchar"}}
    assert lineage.derive_output_schema(sql, inputs) == [
        ("id", "bigint"),
        ("status", "text"),
        ("n", "bigint"),
    ]
    assert lineage.infer_pk(sql) == ["id", "status"]


def test_determinism_proof_obligation_rejects_wall_clock_and_random():
    # REQ-964 obligation #1: a pure transform is required for addressable/replayable materialization
    for bad in (
        "SELECT id, now() AS t FROM orders",
        "SELECT id, current_timestamp AS t FROM orders",
        "SELECT id, random() AS r FROM orders",
    ):
        with pytest.raises(ValueError, match="non-deterministic"):
            lineage.reject_nondeterministic(bad)
    lineage.reject_nondeterministic("SELECT id, status FROM orders")  # pure → ok


def test_acyclic_invariant_derived_from_sql():
    cyclic = {"a": "SELECT * FROM b", "b": "SELECT * FROM a"}
    assert lineage.find_cycle(cyclic) is not None
    assert lineage.find_cycle({"a": "SELECT * FROM src", "b": "SELECT * FROM a"}) is None


# ---------------------------------------------------------------------------
# REQ-966: zero-config — an MV declared with ONLY its SQL auto-processes end-to-end
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sql_only_mv_auto_processes_zero_config(tmp_path):
    dsn = _store(tmp_path)
    # an MV declared with ONLY its SQL — every other knob is a default (persist=replace, emit=None →
    # single-shape fan-out, no calendar, no debounce → real-time). Nothing hand-orchestrated.
    mv = MVDefinition(
        id="hot",
        source_tables=["src"],
        target_catalog="mem",
        target_schema="",
        target_table="hot",
        sql="SELECT id, status FROM src",
    )
    assert mv.persist == "replace" and mv.emit is None and mv.debounce_quiet == 0.0
    assert mv.incremental is False

    # lineage auto-derived from the SQL; fan-out auto-wired
    deps = supervisor.dependents_of({"hot": mv.sql})
    assert deps("src") == ["hot"]

    async def src_fetch(_pending):
        return [{"id": 1, "status": "new"}]

    async def mv_run():
        return [{"id": 1, "status": "new"}]

    land = make_source_land(
        dsn,
        schema="",
        table="src",
        columns=_COLS,
        change_signal="ttl",
        watermark_column=None,
        pk_columns=["id"],
        fetch=src_fetch,
        probe_type="none",
    )
    gen = make_mv_generate(
        dsn,
        schema="",
        table="hot",
        columns=_COLS,
        run_query=mv_run,
        pk_columns=lineage.infer_pk(mv.sql) or ["id"],
    )
    src = SourceTableProcessor(
        "src",
        change_signal="ttl",
        watermark_column=None,
        dependents_of=deps,
        db=None,
        name="s",
        land=land,
    )
    mv_proc = MVTableProcessor(
        "hot",
        change_signal="ttl",
        watermark_column=None,
        dependents_of=deps,
        db=None,
        name="m",
        generate=gen,
    )
    async with _db(tmp_path) as db:
        src._db = db
        mv_proc._db = db
        # seed the source once; the DAG self-organizes and reprocesses in near-real-time
        async with db.acquire() as conn:
            e = await queue.post_event(conn, source_table="src", event_type="replace")
            await queue.fan_out(conn, e, ["src"])
        await supervisor.drain(db, [src, mv_proc])
    # the MV materialized with no configuration beyond its SQL
    assert [(r[0], r[1]) for r in await _rows(dsn, "hot")] == [(1, "new")]


# ---------------------------------------------------------------------------
# REQ-967: self-describing — DQ scorecard over warn/error emissions; SCD-2 history over append emit
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dq_scorecard_is_an_mv_over_warn_error_emissions(tmp_path):
    dsn = _store(tmp_path)

    async def run_ok():
        return [{"id": 1, "status": "a"}]

    def warn_hook(rows, ctx):
        ctx.warn("late arrival on order 1")  # REQ-957 advisory → a warn event about the node
        return rows

    def error_hook(rows, ctx):
        raise ValueError("schema drift")  # REQ-957 fatal → an error event about the node

    warn_gen = make_mv_generate(dsn, schema="", table="ok1", columns=_COLS, run_query=run_ok)
    err_gen = make_mv_generate(dsn, schema="", table="ok2", columns=_COLS, run_query=run_ok)
    async with _db(tmp_path) as db:
        warn_mv = MVTableProcessor(
            "dq.warn",
            change_signal="ttl",
            watermark_column=None,
            dependents_of=lambda n: [],
            db=db,
            name="w",
            generate=warn_gen,
            preprocess=warn_hook,
        )
        err_mv = MVTableProcessor(
            "dq.err",
            change_signal="ttl",
            watermark_column=None,
            dependents_of=lambda n: [],
            db=db,
            name="e",
            generate=err_gen,
            preprocess=error_hook,
        )
        async with db.acquire() as conn:
            for node in ("dq.warn", "dq.err"):
                e = await queue.post_event(conn, source_table="s.o", event_type="append")
                await queue.fan_out(conn, e, [node])
            await warn_mv.process_pending(conn)
            await err_mv.process_pending(conn)

            # the DQ scorecard is literally a query over the warn/error stream — NO bespoke subsystem
            scorecard = await conn.fetch(
                "SELECT source_table, event_type, count(*) AS n FROM events "
                "WHERE event_type IN ('warn','error') GROUP BY source_table, event_type "
                "ORDER BY source_table"
            )
        rows = {(r[0], r[1]): r[2] for r in scorecard}
        assert rows == {("dq.err", "error"): 1, ("dq.warn", "warn"): 1}


@pytest.mark.asyncio
async def test_scd2_history_is_an_mv_over_an_append_emit(tmp_path):
    dsn = _store(tmp_path)
    # a replace-persist "current" upstream whose snapshots feed a history accumulator: the history MV
    # is persist=append, so each fire APPENDS the upstream's current state → an SCD-2 time series.
    current = {"status": "v1"}

    async def history_run():
        return [{"id": 1, "status": current["status"]}]  # the upstream's current snapshot

    history_gen = make_mv_generate(
        dsn,
        schema="",
        table="hist",
        columns=_COLS,
        run_query=history_run,
        persist="append",
    )
    # fire 1: append the v1 snapshot
    await history_gen([{"event_type": "append", "payload": {}}], prior_hash=None)
    # upstream overwrites itself to v2 and emits another append
    current["status"] = "v2"
    await history_gen([{"event_type": "append", "payload": {}}], prior_hash="different")
    # the history MV accumulated BOTH points-in-time (SCD-2) — a self-describing dataset, no subsystem
    assert [(r[0], r[1]) for r in await _rows(dsn, "hist")] == [(1, "v1"), (1, "v2")]
