# Copyright (c) 2026 Kenneth Stott
# Canary: 377c293f-3c33-46e3-a089-30f51b36f1c1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1915: a source table whose replica is a whole copy is not fetched or written by its
event-loop node. The node asks the data replicator for the build, and re-posts the table's
change to its dependents when the build runner posts a completed, changed build to it."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from provisa.core.database import Database, create_engine_from_url
from provisa.core.schema_org import metadata, replica_state as replica_state_table
from provisa.events.boot import build_source_node_spec
from provisa.events.handlers import make_source_build
from provisa.federation import replica_state

pytestmark = pytest.mark.unit

KEY = ("src", "public", "orders")
STORE = "store-a"


@pytest.fixture
def db(tmp_path):
    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'cp.db'}")
    with engine.begin() as conn:
        metadata.create_all(conn, tables=[replica_state_table])
    yield Database(engine, "test")
    engine.dispose()


def _handle(db, kicks):
    return make_source_build(db=db, key=KEY, store_of=lambda: STORE, kick=lambda: kicks.append(1))


async def _complete(db, *, content_hash: str, rows: int = 7) -> None:
    now = datetime.now(UTC)
    async with db.acquire() as conn:
        await replica_state.claim(conn, KEY, holder="t:1", now=now, retry_interval=60)
        await replica_state.record_completed(
            conn,
            KEY,
            rows_copied=rows,
            method="stream_batches",
            content_hash=content_hash,
            store=STORE,
            next_refresh_at=None,
            now=now,
        )


async def _record(db):
    async with db.acquire() as conn:
        return await replica_state.read(conn, KEY)


async def test_a_change_signal_asks_for_a_build_and_ripples_nothing(db):
    kicks: list = []
    land = _handle(db, kicks)
    poll = [{"id": 1, "event_type": "replace", "source_table": "public.orders", "payload": {}}]
    assert await land(poll, prior_hash=None) is None
    record = await _record(db)
    assert (record.build_state, record.requested_reason) == ("requested", "refresh")
    assert kicks == [1]
    assert land.replica_key == KEY


async def test_the_boot_seed_asks_for_a_build_like_any_other_signal(db):
    kicks: list = []
    seed = [{"id": 1, "event_type": "replace", "payload": {"bootstrap": True}}]
    assert await _handle(db, kicks)(seed, prior_hash=None) is None
    assert (await _record(db)).build_state == "requested"


async def test_a_completed_changed_build_is_the_nodes_own_change(db):
    kicks: list = []
    land = _handle(db, kicks)
    await land([{"id": 1, "event_type": "replace", "payload": {}}], prior_hash=None)
    await _complete(db, content_hash="5:abc")
    built = [{"id": 2, "event_type": "replace", "payload": {"built": True, "rows": 7}}]
    assert await land(built, prior_hash=None) == (
        "replace",
        {"rows": 7, "built": "src.public.orders"},
        "5:abc",
    )
    # REQ-981: the same content as the node last posted ripples nothing
    assert await land(built, prior_hash="5:abc") is None
    # REQ-968: a forced regen ripples whatever the hash
    assert (await land(built, prior_hash="5:abc", forced=True))[2] == "5:abc"
    assert kicks == [1]  # observing a build asks for none


async def test_a_forced_regen_asks_for_a_build_as_the_operator(db):
    land = _handle(db, [])
    forced = [{"id": 1, "event_type": "replace", "payload": {"forced": True}}]
    assert await land(forced, prior_hash=None, forced=True) is None
    assert (await _record(db)).requested_reason == "operator"


async def test_a_built_event_for_a_replica_in_another_store_asks_for_a_build_here(db):
    land = make_source_build(db=db, key=KEY, store_of=lambda: "other-store", kick=lambda: None)
    await land([{"id": 1, "event_type": "replace", "payload": {}}], prior_hash=None)
    await _complete(db, content_hash="5:abc")  # completed in STORE, not in this engine's store
    built = [{"id": 2, "event_type": "replace", "payload": {"built": True}}]
    assert await land(built, prior_hash=None) is None
    assert (await _record(db)).build_state == "requested"


def _spec(monkeypatch, *, probe_type="none", watermark=None, preprocess=None, replica_build=True):
    from provisa.federation.strategy import Strategy

    monkeypatch.setattr(
        "provisa.federation.strategy.federate", lambda src, engine, **kw: Strategy.MATERIALIZED
    )
    monkeypatch.setattr(
        "provisa.federation.residency.resolve_landing_args",
        lambda src, tbl, platform=None: SimpleNamespace(
            columns=[("id", "bigint")],
            change_signal="ttl",
            watermark_column=watermark,
            pk_columns=["id"],
            probe_type=probe_type,
        ),
    )
    table = SimpleNamespace(
        source_id="src",
        schema_name="public",
        table_name="orders",
        columns=[SimpleNamespace(name="id", native_filter_type=None)],
        row_materialize=False,
        cache_ttl=60,
        mv_preprocess=preprocess,
    )
    runtime = SimpleNamespace(
        replica_address=lambda *, source_id, schema_name, table_name: SimpleNamespace(
            schema="org_a_replicas", table="src__public__orders"
        )
    )
    built: list = []

    def factory(key):
        built.append(key)

        async def land(pending, **kw):
            return None

        land.replica_key = key
        return land

    spec = build_source_node_spec(
        table,
        SimpleNamespace(id="src", type=SimpleNamespace(value="openapi")),
        engine=SimpleNamespace(dialect="postgres"),
        engine_runtime=runtime,
        source_fetch=lambda src, tbl: None,
        replica_build=factory if replica_build else None,
    )
    return spec, built


def test_a_whole_copy_source_node_is_built_by_the_replicator(monkeypatch):
    spec, built = _spec(monkeypatch)
    assert built == [KEY] and spec.handle.replica_key == KEY


def test_an_append_source_node_keeps_its_own_land(monkeypatch):
    """A watermark probe appends what is past the cursor: not a whole copy, not the
    replicator's."""
    spec, built = _spec(monkeypatch, probe_type="watermark", watermark="updated_at")
    assert built == [] and not hasattr(spec.handle, "replica_key")


def test_a_table_with_a_preflight_check_over_its_rows_keeps_its_own_land(monkeypatch):
    spec, built = _spec(monkeypatch, preprocess="def preflight(streams, ctx): return None")
    assert built == [] and not hasattr(spec.handle, "replica_key")


def _seed(posted: datetime) -> list[dict]:
    return [
        {"id": 9, "event_type": "replace", "payload": {"bootstrap": True}, "created_at": posted}
    ]


async def _start_model_build(db, at: datetime) -> None:
    async with db.acquire() as conn:
        await replica_state.request_build(conn, KEY, replica_state.REASON_MODEL, now=at)
        await replica_state.claim(conn, KEY, holder="t:1", now=at, retry_interval=60)


async def test_the_boot_seed_is_answered_by_the_model_build_it_raced(db):
    """The model's build of this launch started after the seed was posted: it reads the source's
    current rows, so the seed asks for nothing more -- not a second, 'refresh' build."""
    seeded = datetime(2026, 10, 4, 12, 0, 0, tzinfo=UTC)
    await _start_model_build(db, seeded.replace(second=1))
    kicks: list = []
    assert await _handle(db, kicks)(_seed(seeded.replace(tzinfo=None)), prior_hash=None) is None
    record = await _record(db)
    assert (record.build_state, record.requested_reason) == ("building", "model")
    assert kicks == []
    await _complete(db, content_hash="5:abc")
    assert (await _record(db)).build_state == "idle"  # built once


async def test_a_build_already_running_when_the_seed_was_posted_is_built_again(db):
    """A build that started before the seed may have read past what the seed stands for."""
    started = datetime(2026, 10, 4, 12, 0, 0, tzinfo=UTC)
    await _start_model_build(db, started)
    kicks: list = []
    assert await _handle(db, kicks)(_seed(started.replace(second=5)), prior_hash=None) is None
    assert (await _record(db)).requested_reason == "refresh"
    assert kicks == [1]
