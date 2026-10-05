# Copyright (c) 2026 Kenneth Stott
# Canary: 091d826e-6bb9-40ca-80e1-62bea91061a9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-961/REQ-965: a push listener's landed batch is the table's change. It is posted to the
views that read the table and stamps the table's freshness, as the node's own processor does for
its landings. Before this, a relay landing told no one: dependent views never refreshed from a
push table and its freshness never moved. Real event queue on sqlite."""

from __future__ import annotations

import threading
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from provisa.core.database import Database, create_engine_from_url
from provisa.core.schema_org import event_status, events, node_freshness_state
from provisa.events import push_wiring, queue
from provisa.events.processor import SourceTableProcessor

_NODE = "kafka_src/s.orders"
_VIEW = "view-orders_by_day"


@asynccontextmanager
async def _db(tmp_path):
    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'q.db'}")
    with engine.begin() as c:
        events.metadata.create_all(c, tables=[events, event_status, node_freshness_state])
    try:
        yield Database(engine, name="q")
    finally:
        engine.dispose()


class _Engine:
    def __init__(self, counts: dict[str, int]) -> None:
        self.counts = counts
        self.applied: list = []

    async def apply_cdc_events(self, **kwargs) -> dict[str, int]:
        self.applied.append(kwargs["events"])
        return self.counts


async def _listen_once(state, engine, batch, *, row_materialize: bool = False) -> None:
    """Run the listener with a provider that delivers one batch and then ends."""

    async def _consume(provider, land_fn, **kwargs):
        return await land_fn(batch)

    with patch("provisa.subscriptions.cdc_landing.consume_cdc_into_store", _consume):
        await push_wiring._run_listener(
            state=state,
            engine=engine,
            provider=object(),
            watch_target="orders-topic",
            land_schema="org_a_replicas",
            land_table="kafka_src__s__orders",
            columns=[("id", "bigint")],
            pk_columns=["id"],
            disconnect=threading.Event(),
            debounce_quiet=0.0,
            debounce_max_delay=5.0,
            node=_NODE,
            log=SimpleNamespace(exception=lambda *a, **k: None),
            row_materialize=row_materialize,
        )


def _state(db) -> SimpleNamespace:
    processor = SourceTableProcessor(
        _NODE,
        change_signal="kafka",
        watermark_column=None,
        dependents_of=lambda node: [_VIEW] if node == _NODE else [],
        db=db,
        name="box-1",
        land=lambda *a, **k: None,
    )
    return SimpleNamespace(tenant_db=db, event_loop_processors=[processor])


@pytest.mark.asyncio
async def test_a_landed_relay_batch_fans_out_to_a_dependent_view_and_moves_freshness(tmp_path):
    async with _db(tmp_path) as db:
        state = _state(db)
        engine = _Engine({"upserted": 2, "deleted": 0})
        await _listen_once(state, engine, [{"op": "insert", "row": {"id": 1}}])

        async with db.acquire() as conn:
            pending = await queue.peek_pending(conn, dependent_table=_VIEW)
            posted = await queue.get_events(conn, [item["event_id"] for item in pending])
            freshness = await queue.get_node_state(conn, _NODE)
        assert engine.applied, "the batch was landed"
        assert [(e["source_table"], e["event_type"]) for e in posted] == [(_NODE, "delta")]
        assert posted[0]["payload"] == {"landed": {"upserted": 2, "deleted": 0}}
        assert freshness is not None and freshness["last_refresh_ok"]
        assert freshness["last_refresh_at"] is not None


@pytest.mark.asyncio
async def test_a_batch_that_changed_nothing_ripples_nothing(tmp_path):
    async with _db(tmp_path) as db:
        state = _state(db)
        await _listen_once(state, _Engine({"upserted": 0, "deleted": 0}), [])
        async with db.acquire() as conn:
            assert await queue.peek_pending(conn, dependent_table=_VIEW) == []
            assert await queue.get_node_state(conn, _NODE) is None


@pytest.mark.asyncio
async def test_a_row_level_table_is_left_to_its_row_cache(tmp_path):
    async with _db(tmp_path) as db:
        state = _state(db)
        await _listen_once(
            state, _Engine({"upserted": 1}), [{"row": {"id": 1}}], row_materialize=True
        )
        async with db.acquire() as conn:
            assert await queue.peek_pending(conn, dependent_table=_VIEW) == []
