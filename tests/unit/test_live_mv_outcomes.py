# Copyright (c) 2026 Kenneth Stott
# Canary: 21bc1dce-11a0-42f3-800b-4a44d76d91a1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-965 (two independent MV outcomes: persistence + demand-driven emit set) and
REQ-970 (derived store schema from the SELECT) — end to end through the write face and event loop."""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from provisa.core.database import Database
from provisa.core.schema_org import event_status, events, node_freshness_state
from provisa.events import outcomes, queue
from provisa.events.handlers import make_mv_generate
from provisa.events.lineage import derive_output_schema, infer_pk
from provisa.events.processor import MVTableProcessor
from provisa.federation import store_writer

_COLS = [("id", "bigint"), ("status", "text")]


def _dsn(tmp_path) -> str:
    return f"sqlite+aiosqlite:///{tmp_path / 'store.db'}"


async def _rows(dsn, table):
    async with store_writer.store_connection(dsn) as conn:
        return await conn.fetch(f"SELECT id, status FROM {table} ORDER BY id")


# ---------------------------------------------------------------------------
# REQ-965 axis 1: PERSISTENCE (replace / append / upsert) into the MV's own store
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_persist_replace_overwrites_store(tmp_path):
    dsn = _dsn(tmp_path)
    await store_writer.persist_land(
        dsn,
        schema="",
        table="mv",
        columns=_COLS,
        rows=[{"id": 1, "status": "a"}],
        persist="replace",
    )
    await store_writer.persist_land(
        dsn,
        schema="",
        table="mv",
        columns=_COLS,
        rows=[{"id": 2, "status": "b"}],
        persist="replace",
    )
    assert [(r[0], r[1]) for r in await _rows(dsn, "mv")] == [(2, "b")]  # replaced, not accumulated


@pytest.mark.asyncio
async def test_persist_append_accumulates(tmp_path):
    dsn = _dsn(tmp_path)
    await store_writer.persist_land(
        dsn, schema="", table="mv", columns=_COLS, rows=[{"id": 1, "status": "a"}], persist="append"
    )
    await store_writer.persist_land(
        dsn, schema="", table="mv", columns=_COLS, rows=[{"id": 2, "status": "b"}], persist="append"
    )
    assert [(r[0], r[1]) for r in await _rows(dsn, "mv")] == [(1, "a"), (2, "b")]  # accumulated


@pytest.mark.asyncio
async def test_persist_upsert_maintains_by_pk(tmp_path):
    dsn = _dsn(tmp_path)
    await store_writer.persist_land(
        dsn,
        schema="",
        table="mv",
        columns=_COLS,
        rows=[{"id": 1, "status": "a"}],
        persist="upsert",
        pk_columns=["id"],
    )
    # same PK, changed value → UPDATE in place (not a second row)
    await store_writer.persist_land(
        dsn,
        schema="",
        table="mv",
        columns=_COLS,
        rows=[{"id": 1, "status": "b"}],
        persist="upsert",
        pk_columns=["id"],
    )
    assert [(r[0], r[1]) for r in await _rows(dsn, "mv")] == [(1, "b")]


@pytest.mark.asyncio
async def test_persist_upsert_without_pk_fails_loud(tmp_path):
    dsn = _dsn(tmp_path)
    with pytest.raises(ValueError, match="primary key"):
        await store_writer.persist_land(
            dsn,
            schema="",
            table="mv",
            columns=_COLS,
            rows=[{"id": 1, "status": "a"}],
            persist="upsert",
        )


@pytest.mark.asyncio
async def test_persist_invalid_outcome_fails_loud(tmp_path):
    dsn = _dsn(tmp_path)
    with pytest.raises(ValueError, match="invalid persistence outcome"):
        await store_writer.persist_land(
            dsn,
            schema="",
            table="mv",
            columns=_COLS,
            rows=[{"id": 1, "status": "a"}],
            persist="merge",
        )


@pytest.mark.asyncio
async def test_make_mv_generate_persist_upsert_lands_upsert(tmp_path):
    dsn = _dsn(tmp_path)

    async def run_query():
        return [{"id": 1, "status": "a"}]

    gen = make_mv_generate(
        dsn,
        schema="",
        table="mv",
        columns=_COLS,
        run_query=run_query,
        persist="upsert",
        pk_columns=["id"],
    )
    et, payload, digest = await gen([], prior_hash=None)
    assert et == "replace" and payload["rows"] == 1 and digest  # primary change + gate hash
    assert [(r[0], r[1]) for r in await _rows(dsn, "mv")] == [(1, "a")]


def test_make_mv_generate_invalid_persist_fails_loud():
    with pytest.raises(ValueError, match="invalid persistence outcome"):
        make_mv_generate(
            "sqlite://",
            schema="",
            table="mv",
            columns=_COLS,
            run_query=None,
            persist="nope",  # type: ignore[arg-type]
        )


# ---------------------------------------------------------------------------
# REQ-965 axis 2: EMIT set — demand-driven, per-shape routing (independent of persistence)
# ---------------------------------------------------------------------------


def test_resolve_emitted_is_demand_driven():
    # declared {replace, append, delta}; only append + delta have subscribers → only those produced
    assert outcomes.resolve_emitted({"replace", "append", "delta"}, {"append", "delta"}) == [
        "append",
        "delta",
    ]
    # declared delta but nobody subscribes → NOT produced (pay-per-consumer)
    assert outcomes.resolve_emitted({"delta"}, {"replace"}) == []
    # emit NONE
    assert outcomes.resolve_emitted(set(), {"replace"}) == []


def test_validate_emit_rejects_unknown_shape():
    with pytest.raises(ValueError, match="invalid emit outcome"):
        outcomes.validate_emit({"replace", "snapshot"})


def test_require_pk_delta_without_pk_fails_loud():
    with pytest.raises(ValueError, match="require a primary key"):
        outcomes.require_pk("replace", {"delta"}, None)
    outcomes.require_pk("replace", {"delta"}, ["id"])  # with PK → ok
    outcomes.require_pk("replace", {"append", "replace"}, None)  # no delta/upsert → ok


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


def _mv_proc(db, *, emit, subscribers, node="mv.a", deps=("d.replace", "d.append", "d.delta")):
    async def generate(pending, *, prior_hash=None, ctx=None, preprocess=None, forced=False):
        return "replace", {"rows": 1}, "h1"

    return MVTableProcessor(
        node,
        change_signal="ttl",
        watermark_column=None,
        dependents_of=lambda n: list(deps),
        db=db,
        name="box-1",
        generate=generate,
        emit_outcomes=outcomes.validate_emit(emit),
        subscribers_of=subscribers,
    )


async def _events_from(conn, node):
    return [r for r in await queue.read_since(conn, cursor=0) if r["source_table"] == node]


@pytest.mark.asyncio
async def test_emit_set_routes_each_shape_to_shape_matched_dependents(tmp_path):
    # demand map: d.replace consumes replace, d.append consumes append, d.delta consumes delta
    _consumes = {"d.replace": {"replace"}, "d.append": {"append"}, "d.delta": {"delta"}}

    def subscribers(node, shape):
        return [d for d, c in _consumes.items() if shape in c]

    async with _db(tmp_path) as db, db.acquire() as conn:
        up = await queue.post_event(conn, source_table="s.o", event_type="append")
        await queue.fan_out(conn, up, ["mv.a"])
        proc = _mv_proc(db, emit={"replace", "append", "delta"}, subscribers=subscribers)
        await proc.process_pending(conn)

        posted = await _events_from(conn, "mv.a")
        assert sorted(e["event_type"] for e in posted) == ["append", "delta", "replace"]
        by_shape = {e["event_type"]: e["id"] for e in posted}
        now = datetime.now(timezone.utc)
        # each shape reached ONLY its shape-matched dependent
        assert await queue.claim(conn, dependent_table="d.append", processor_name="p", now=now) == [
            by_shape["append"]
        ]
        assert await queue.claim(conn, dependent_table="d.delta", processor_name="p", now=now) == [
            by_shape["delta"]
        ]
        assert await queue.claim(
            conn, dependent_table="d.replace", processor_name="p", now=now
        ) == [by_shape["replace"]]


@pytest.mark.asyncio
async def test_emit_none_when_no_subscriber(tmp_path):
    # declared delta, but the only dependent consumes replace → delta NOT produced (emit NONE)
    def subscribers(node, shape):
        return ["d.replace"] if shape == "replace" else []

    async with _db(tmp_path) as db, db.acquire() as conn:
        up = await queue.post_event(conn, source_table="s.o", event_type="append")
        await queue.fan_out(conn, up, ["mv.a"])
        proc = _mv_proc(db, emit={"delta"}, subscribers=subscribers)
        assert await proc.process_pending(conn) is None  # nothing emitted
        assert await _events_from(conn, "mv.a") == []
        # but the recompute's content hash WAS persisted (persistence happened, emit did not)
        st = await queue.get_node_state(conn, "mv.a")
        assert st is not None and st["content_hash"] == "h1"


@pytest.mark.asyncio
async def test_persist_and_emit_are_independent(tmp_path):
    # persist=upsert into the store, emit=delta downstream — the two axes decoupled (REQ-965)
    dsn = _dsn(tmp_path)

    async def run_query():
        return [{"id": 1, "status": "a"}]

    gen = make_mv_generate(
        dsn,
        schema="",
        table="mv",
        columns=_COLS,
        run_query=run_query,
        persist="upsert",
        pk_columns=["id"],
    )

    def subscribers(node, shape):
        return ["d.delta"] if shape == "delta" else []

    async with _db(tmp_path) as db, db.acquire() as conn:
        up = await queue.post_event(conn, source_table="s.o", event_type="append")
        await queue.fan_out(conn, up, ["mv.a"])
        proc = MVTableProcessor(
            "mv.a",
            change_signal="ttl",
            watermark_column=None,
            dependents_of=lambda n: ["d.delta"],
            db=db,
            name="b",
            generate=gen,
            emit_outcomes=outcomes.validate_emit({"delta"}),
            subscribers_of=subscribers,
        )
        await proc.process_pending(conn)
        # emit side: a single delta event
        posted = await _events_from(conn, "mv.a")
        assert [e["event_type"] for e in posted] == ["delta"]
    # persist side: the row was upserted into the store
    assert [(r[0], r[1]) for r in await _rows(dsn, "mv")] == [(1, "a")]


def test_construct_emit_processor_without_router_fails_loud(tmp_path):
    with pytest.raises(ValueError, match="subscribers_of"):
        MVTableProcessor(
            "mv.a",
            change_signal="ttl",
            watermark_column=None,
            dependents_of=lambda n: [],
            db=None,
            name="b",
            generate=lambda *a, **k: None,
            emit_outcomes=frozenset({"replace"}),
        )


# ---------------------------------------------------------------------------
# REQ-970: derived store schema from the SELECT (SQLGlot type inference) + reconcile
# ---------------------------------------------------------------------------

_INPUTS = {"orders": {"id": "bigint", "status": "varchar", "amt": "double"}}


def test_derive_output_schema_from_select_computed_columns():
    cols = derive_output_schema(
        "SELECT id, status, count(*) AS n, sum(amt) AS total FROM orders GROUP BY id, status",
        _INPUTS,
    )
    assert cols == [("id", "bigint"), ("status", "text"), ("n", "bigint"), ("total", "double")]


def test_infer_pk_from_group_by():
    assert infer_pk("SELECT id, count(*) AS n FROM orders GROUP BY id") == ["id"]
    assert infer_pk("SELECT count(*) AS n FROM orders") == []  # no group-by → no inferable PK


def test_derive_output_schema_undeterminable_fails_loud():
    with pytest.raises(ValueError, match="cannot determine the output type"):
        derive_output_schema("SELECT id, NULL AS mystery FROM orders", _INPUTS)


@pytest.mark.asyncio
async def test_reconcile_mv_schema_creates_then_recreates_on_drift(tmp_path):
    dsn = _dsn(tmp_path)
    sql1 = "SELECT id, status FROM orders"
    status, cols = await store_writer.reconcile_mv_schema(
        dsn, schema="", table="mv", sql=sql1, input_schemas=_INPUTS, pk_columns=["id"]
    )
    assert status == "created" and cols == [("id", "bigint"), ("status", "text")]
    # same shape → kept
    status, _ = await store_writer.reconcile_mv_schema(
        dsn, schema="", table="mv", sql=sql1, input_schemas=_INPUTS, pk_columns=["id"]
    )
    assert status == "kept"
    # SELECT changes shape → recreate (drop + reland next fire)
    sql2 = "SELECT id, status, amt FROM orders"
    status, cols = await store_writer.reconcile_mv_schema(
        dsn, schema="", table="mv", sql=sql2, input_schemas=_INPUTS, pk_columns=["id"]
    )
    assert status == "recreated" and cols == [
        ("id", "bigint"),
        ("status", "text"),
        ("amt", "double"),
    ]


@pytest.mark.asyncio
async def test_reconcile_mv_schema_infers_pk_from_group_by(tmp_path):
    dsn = _dsn(tmp_path)
    status, cols = await store_writer.reconcile_mv_schema(
        dsn,
        schema="",
        table="mv",
        sql="SELECT id, count(*) AS n FROM orders GROUP BY id",
        input_schemas=_INPUTS,
    )
    assert status == "created" and cols == [("id", "bigint"), ("n", "bigint")]
