# Copyright (c) 2026 Kenneth Stott
# Canary: 091d826e-6bb9-40ca-80e1-62bea91061a9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-961/REQ-965: a push listener's landed batch is the table's change. It is posted to the
views that read the table and stamps the table's freshness, as the node's own processor does for
its landings. Before this, a relay landing told no one: dependent views never refreshed from a
push table and its freshness never moved. Real event queue."""

from __future__ import annotations

import threading
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from provisa.events import push_wiring, queue
from provisa.events.processor import SourceTableProcessor

_NODE = "kafka_src/s.orders"
_VIEW = "view-orders_by_day"


@asynccontextmanager
async def _db(control_plane):
    yield control_plane.org()


class _Engine:
    def __init__(self, counts: dict[str, int]) -> None:
        self.counts = counts
        self.applied: list = []

    async def apply_cdc_events(self, **kwargs) -> dict[str, int]:
        self.applied.append(kwargs["events"])
        # REQ-1733: an apply lists the upserts that changed nothing; none here.
        return {**self.counts, "unchanged": []}


async def _listen_once(state, engine, batch, *, row_materialize: bool = False) -> None:
    """Run the listener with a provider that delivers one batch and then ends."""

    async def _consume(provider, land_fn, **kwargs):
        # As a provider yields them: each event with its operation and row.
        return await land_fn([SimpleNamespace(operation=e["op"], row=e["row"]) for e in batch])

    with (
        patch("provisa.subscriptions.cdc_landing.consume_cdc_into_store", _consume),
        # REQ-2054: what a landed batch fires is test_table_triggers.py's.
        patch("provisa.scheduler.table_triggers.table_changed", lambda *a, **k: None),
    ):
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
            key=("kafka_src", "s", "orders"),
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
async def test_a_landed_relay_batch_fans_out_to_a_dependent_view_and_moves_freshness(control_plane):
    async with _db(control_plane) as db:
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
async def test_a_batch_that_changed_nothing_ripples_nothing(control_plane):
    async with _db(control_plane) as db:
        state = _state(db)
        await _listen_once(state, _Engine({"upserted": 0, "deleted": 0}), [])
        async with db.acquire() as conn:
            assert await queue.peek_pending(conn, dependent_table=_VIEW) == []
            assert await queue.get_node_state(conn, _NODE) is None


@pytest.mark.asyncio
async def test_a_row_level_table_is_left_to_its_row_cache(control_plane):
    async with _db(control_plane) as db:
        state = _state(db)
        await _listen_once(
            state, _Engine({"upserted": 1}), [{"row": {"id": 1}}], row_materialize=True
        )
        async with db.acquire() as conn:
            assert await queue.peek_pending(conn, dependent_table=_VIEW) == []
