# Copyright (c) 2026 Kenneth Stott
# Canary: f7365f47-bba4-469a-9f66-7a4641856256
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-981: canonical content hash + the event-loop output gate.

Covers the hash's determinism guarantees (order-independence, value canonicalization) and the gate's
behavior in the source-land and MV-generate handlers + the processor's persist/re-post path.
"""

from __future__ import annotations

import datetime as dt
from contextlib import asynccontextmanager

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from provisa.core.database import Database
from provisa.core.schema_org import event_status, events, node_freshness_state
from provisa.events import queue
from provisa.events.content_hash import content_hash
from provisa.events.handlers import make_source_land
from provisa.events.processor import SourceTableProcessor

_COLS = [("id", "bigint"), ("status", "text")]


# -- the hash itself -------------------------------------------------------------------------------


def test_hash_is_stable_for_same_rows():
    rows = [{"id": 1, "status": "a"}, {"id": 2, "status": "b"}]
    assert content_hash(rows, ["id"]) == content_hash(rows, ["id"])


def test_hash_is_order_independent_by_pk():
    a = [{"id": 1, "status": "a"}, {"id": 2, "status": "b"}]
    b = [{"id": 2, "status": "b"}, {"id": 1, "status": "a"}]
    assert content_hash(a, ["id"]) == content_hash(b, ["id"])


def test_hash_order_independent_without_pk():
    a = [{"k": 1}, {"k": 2}]
    b = [{"k": 2}, {"k": 1}]
    assert content_hash(a) == content_hash(b)


def test_hash_changes_on_content_change():
    a = [{"id": 1, "status": "a"}]
    b = [{"id": 1, "status": "b"}]
    assert content_hash(a, ["id"]) != content_hash(b, ["id"])


def test_hash_distinguishes_row_boundaries():
    # length-prefixing prevents "ab"+"c" colliding with "a"+"bc"
    a = [{"v": "ab"}, {"v": "c"}]
    b = [{"v": "a"}, {"v": "bc"}]
    assert content_hash(a) != content_hash(b)


def test_hash_canonicalizes_datetime_tz():
    aware = dt.datetime(2026, 7, 10, 12, 0, tzinfo=dt.timezone.utc)
    same_instant_other_tz = aware.astimezone(dt.timezone(dt.timedelta(hours=5)))
    assert content_hash([{"t": aware}]) == content_hash([{"t": same_instant_other_tz}])


def test_hash_canonicalizes_decimal_scale():
    import decimal

    assert content_hash([{"n": decimal.Decimal("1.0")}]) == content_hash(
        [{"n": decimal.Decimal("1.00")}]
    )


# -- the gate in the handlers ----------------------------------------------------------------------


def _dsn(tmp_path) -> str:
    return f"sqlite+aiosqlite:///{tmp_path / 'store.db'}"


@pytest.mark.asyncio
async def test_replace_land_gated_when_content_unchanged(tmp_path):
    dsn = _dsn(tmp_path)
    rows = [{"id": 1, "status": "new"}]

    async def fetch(_pending):
        return list(rows)

    land = make_source_land(
        dsn,
        schema="",
        table="orders",
        columns=_COLS,
        change_signal="ttl",  # no watermark → replace shape → gated
        watermark_column=None,
        pk_columns=["id"],
        fetch=fetch,
    )
    first = await land([{"e": 1}], prior_hash=None)
    assert first is not None
    event_type, _payload, digest = first
    assert event_type == "replace" and digest
    # same content re-fetched under the prior hash → gated (no re-post)
    assert await land([{"e": 2}], prior_hash=digest) is None


@pytest.mark.asyncio
async def test_replace_land_not_gated_when_content_changes(tmp_path):
    dsn = _dsn(tmp_path)
    state = {"rows": [{"id": 1, "status": "new"}]}

    async def fetch(_pending):
        return list(state["rows"])

    land = make_source_land(
        dsn,
        schema="",
        table="orders",
        columns=_COLS,
        change_signal="ttl",
        watermark_column=None,
        pk_columns=["id"],
        fetch=fetch,
    )
    _, _, h1 = await land([{"e": 1}], prior_hash=None)
    state["rows"] = [{"id": 1, "status": "sold"}]  # content changed
    result = await land([{"e": 2}], prior_hash=h1)
    assert result is not None and result[2] != h1


@pytest.mark.asyncio
async def test_append_shape_is_never_gated(tmp_path):
    # watermark → append; append deltas are new rows by definition → no content hash, always re-posts
    dsn = _dsn(tmp_path)

    async def fetch(_pending):
        return [{"id": 1, "status": "new"}]

    land = make_source_land(
        dsn,
        schema="",
        table="orders",
        columns=_COLS,
        change_signal="ttl_probe",
        watermark_column="id",
        pk_columns=["id"],
        fetch=fetch,
        probe_type="watermark",  # REQ-982: authoritative → append
    )
    result = await land([{"e": 1}], prior_hash="anything")
    assert result is not None and result[0] == "append" and result[2] is None


# -- the gate end-to-end through the processor -----------------------------------------------------


@asynccontextmanager
async def _db(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'cp.db'}")
    async with engine.begin() as c:
        await c.run_sync(
            lambda s: events.metadata.create_all(
                s, tables=[events, event_status, node_freshness_state]
            )
        )
    try:
        yield Database(engine, name="cp")
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_processor_gates_second_identical_land(tmp_path):
    store = f"sqlite+aiosqlite:///{tmp_path / 'store.db'}"

    async def fetch(_pending):
        return [{"id": 1, "status": "new"}]

    proc = SourceTableProcessor(
        "s.orders",
        change_signal="ttl",
        watermark_column=None,
        dependents_of=lambda n: ["mv.a"],
        db=None,
        name="src",
        land=make_source_land(
            store,
            schema="",
            table="orders",
            columns=_COLS,
            change_signal="ttl",
            watermark_column=None,
            pk_columns=["id"],
            fetch=fetch,
        ),
    )
    async with _db(tmp_path) as db:
        proc._db = db
        # first cycle: an upstream trigger lands + re-posts
        async with db.acquire() as conn:
            e1 = await queue.post_event(conn, source_table="trigger", event_type="replace")
            await queue.fan_out(conn, e1, ["s.orders"])
            first = await proc.process_pending(conn)
            assert first is not None  # landed + re-posted
            baseline = (await queue.get_node_state(conn, "s.orders"))["content_hash"]
            assert baseline

        # second cycle: identical fetch → gated (no re-post to mv.a)
        async with db.acquire() as conn:
            e2 = await queue.post_event(conn, source_table="trigger", event_type="replace")
            await queue.fan_out(conn, e2, ["s.orders"])
            second = await proc.process_pending(conn)
            assert second is None  # content unchanged → gate → no downstream ripple
            reposts = [
                r for r in await queue.read_since(conn, cursor=0) if r["source_table"] == "s.orders"
            ]
            assert len(reposts) == 1  # only the first cycle re-posted
