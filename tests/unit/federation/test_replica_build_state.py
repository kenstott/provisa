# Copyright (c) 2026 Kenneth Stott
# Canary: a1044d5c-0a76-4261-b80e-6c305e21f26b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1915: the central record of a replica's build — a request joins a build already asked
for, and a read does not ask again too soon after a failure."""

from datetime import UTC, datetime, timedelta

import pytest

from provisa.core.database import Database, create_engine_from_url
from provisa.core import config_stamp
from provisa.core.schema_org import config_stamp as stamps
from provisa.core.schema_org import metadata, replica_state
from provisa.federation import replica_state as build_state
from provisa.federation.replica_state import promoted_keys, promotion, serving_keys, set_promoted

KEY = ("src", "public", "orders")


@pytest.fixture
async def conn(tmp_path):
    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'cp.db'}")
    with engine.begin() as raw:
        metadata.create_all(raw, tables=[replica_state, stamps])
        # REQ-826: the replica-state stamp has a row and no trigger; the state store advances it.
        config_stamp.install(raw, {}, advanced=config_stamp.TENANT_ADVANCED)
    async with Database(engine, "test").acquire() as connection:
        yield connection
    engine.dispose()


async def test_the_first_request_creates_the_record_and_later_ones_join_it(conn):
    assert await build_state.read(conn, KEY) is None
    assert await build_state.request_build(conn, KEY, build_state.REASON_MODEL) is True
    assert await build_state.request_build(conn, KEY, build_state.REASON_READ) is False
    record = await build_state.read(conn, KEY)
    assert (record.build_state, record.requested_reason) == ("requested", "model")
    assert not record.exists

    now = datetime.now(UTC)
    assert await build_state.claim(conn, KEY, holder="h:1", retry_interval=60, now=now)
    assert await build_state.request_build(conn, KEY, build_state.REASON_OPERATOR) is False
    assert (await build_state.read(conn, KEY)).build_state == "building"


async def test_a_completed_replica_stays_readable_while_its_refresh_is_requested_or_fails(conn):
    now = datetime.now(UTC)
    await build_state.request_build(conn, KEY, build_state.REASON_MODEL)
    await build_state.claim(conn, KEY, holder="h:1", retry_interval=60, now=now)
    await build_state.record_progress(conn, KEY, rows_copied=40)
    assert (await build_state.read(conn, KEY)).rows_copied == 40
    await build_state.record_completed(
        conn,
        KEY,
        rows_copied=100,
        method="stream_batches",
        content_hash="abc",
        store="store-a",
        next_refresh_at=now + timedelta(hours=1),
        now=now,
    )
    record = await build_state.read(conn, KEY)
    assert record.exists and record.build_state == "idle" and record.rows_copied == 100

    assert await build_state.request_build(conn, KEY, build_state.REASON_REFRESH) is True
    assert (await build_state.read(conn, KEY)).exists
    await build_state.claim(conn, KEY, holder="h:1", retry_interval=60, now=now)
    await build_state.record_failed(conn, KEY, error="boom", now=now)
    record = await build_state.read(conn, KEY)
    assert record.exists and record.build_state == "failed" and record.last_error == "boom"


async def test_a_read_does_not_ask_again_inside_the_retry_interval(conn):
    failed_at = datetime.now(UTC)
    await build_state.request_build(conn, KEY, build_state.REASON_MODEL)
    await build_state.claim(conn, KEY, holder="h:1", retry_interval=60, now=failed_at)
    await build_state.record_failed(conn, KEY, error="boom", now=failed_at)

    soon = failed_at + timedelta(seconds=30)
    later = failed_at + timedelta(seconds=61)
    ask = build_state.request_build
    assert await ask(conn, KEY, build_state.REASON_READ, retry_interval=60, now=soon) is False
    assert (await build_state.read(conn, KEY)).build_state == "failed"
    # An operator's request is not held back.
    assert await ask(conn, KEY, build_state.REASON_OPERATOR, now=soon) is True
    await build_state.claim(conn, KEY, holder="h:1", retry_interval=60, now=soon)
    await build_state.record_failed(conn, KEY, error="boom", now=failed_at)
    assert await ask(conn, KEY, build_state.REASON_READ, retry_interval=60, now=later) is True


async def test_candidates_are_requested_due_and_building_rows_oldest_request_first(conn):
    now = datetime.now(UTC)
    a, b, c, d = (("s", "p", name) for name in "abcd")
    await build_state.request_build(conn, b, "model", now=now - timedelta(minutes=2))
    await build_state.request_build(conn, a, "model", now=now - timedelta(minutes=1))
    for key, due in ((c, now - timedelta(seconds=1)), (d, now + timedelta(hours=1))):
        await build_state.request_build(conn, key, "model", now=now - timedelta(hours=1))
        await build_state.claim(conn, key, holder="h:1", retry_interval=60, now=now)
        await build_state.record_completed(
            conn,
            key,
            rows_copied=1,
            method="m",
            content_hash=None,
            store="store-a",
            next_refresh_at=due,
            now=now,
        )
    assert await build_state.candidates(conn, retry_interval=60, now=now, limit=10) == [c, b, a]
    assert await build_state.candidates(conn, retry_interval=60, now=now, limit=1) == [c]


async def test_the_promoted_flag_and_the_build_state_share_the_row(conn):
    await set_promoted(conn, KEY, True)
    assert (await build_state.read(conn, KEY)).build_state == "idle"
    assert await build_state.request_build(conn, KEY, build_state.REASON_HOT) is True
    assert await promoted_keys(conn) == frozenset({KEY})


# -- the replica-state stamp (REQ-826): two transitions advance it, nothing else does ------------


async def _stamp(conn) -> int:
    rows = await conn.fetch("SELECT stamp FROM config_stamp WHERE kind = 'replica'")
    return int(rows[0]["stamp"])


async def _complete(conn, key, *, store: str = "store-a") -> None:
    now = datetime.now(UTC)
    await build_state.record_completed(
        conn,
        key,
        rows_copied=3,
        method="stream_batches",
        content_hash="h",
        store=store,
        next_refresh_at=None,
        now=now,
    )


async def test_promoting_and_demoting_each_advance_the_stamp_once(conn):
    before = await _stamp(conn)
    assert await set_promoted(conn, KEY, True) is True
    assert await _stamp(conn) == before + 1
    # saying the same again changes nothing and tells no one
    assert await set_promoted(conn, KEY, True) is False
    assert await _stamp(conn) == before + 1
    assert await set_promoted(conn, KEY, False) is True
    assert await _stamp(conn) == before + 2
    assert await promoted_keys(conn) == frozenset()


async def test_demoting_a_table_that_was_never_promoted_changes_nothing(conn):
    before = await _stamp(conn)
    assert await set_promoted(conn, KEY, False) is False
    assert await _stamp(conn) == before
    assert await build_state.read(conn, KEY) is None


async def test_a_promoted_table_serves_once_its_first_build_completes_in_this_store(conn):
    await set_promoted(conn, KEY, True)
    await build_state.request_build(conn, KEY, build_state.REASON_HOT)
    assert await promotion(conn, lambda: "store-a") == (frozenset({KEY}), frozenset())
    promoted_at = await _stamp(conn)
    await _complete(conn, KEY)
    # the completion that makes the replica readable is announced with it
    assert await _stamp(conn) == promoted_at + 1
    assert await serving_keys(conn, "store-a") == frozenset({KEY})


async def test_a_refresh_after_the_first_build_does_not_advance_the_stamp(conn):
    await set_promoted(conn, KEY, True)
    await _complete(conn, KEY)
    served_at = await _stamp(conn)
    for _ in range(3):
        await _complete(conn, KEY)
    assert await _stamp(conn) == served_at


async def test_a_build_of_a_table_that_is_not_promoted_does_not_advance_the_stamp(conn):
    """Always and load-protected tables are addressed at their replica from the moment the
    setting is saved: nothing about their route changes when a build completes."""
    await build_state.request_build(conn, KEY, build_state.REASON_MODEL)
    before = await _stamp(conn)
    await _complete(conn, KEY)
    await _complete(conn, KEY)
    assert await _stamp(conn) == before
    assert await serving_keys(conn, "store-a") == frozenset()


async def test_a_replica_built_in_another_store_is_not_served_until_it_is_built_in_this_one(conn):
    """The record is one per table, not per engine: after a move to another engine or store the
    promoted table is read live until its replica is built there, and that build is announced."""
    await set_promoted(conn, KEY, True)
    await _complete(conn, KEY, store="store-a")
    assert await serving_keys(conn, "store-b") == frozenset()
    before = await _stamp(conn)
    await _complete(conn, KEY, store="store-b")
    assert await _stamp(conn) == before + 1
    assert await serving_keys(conn, "store-b") == frozenset({KEY})
    assert await serving_keys(conn, "store-a") == frozenset()


async def test_demotion_takes_the_table_out_of_the_serving_set_at_once(conn):
    await set_promoted(conn, KEY, True)
    await _complete(conn, KEY)
    await set_promoted(conn, KEY, False)
    assert await promotion(conn, lambda: "store-a") == (frozenset(), frozenset())
    # its replica is left standing for the replicator to retire
    assert (await build_state.read(conn, KEY)).exists_in("store-a")


async def test_promoting_again_a_table_whose_replica_still_stands_serves_without_a_rebuild(conn):
    await set_promoted(conn, KEY, True)
    await _complete(conn, KEY)
    await set_promoted(conn, KEY, False)
    await set_promoted(conn, KEY, True)
    assert await serving_keys(conn, "store-a") == frozenset({KEY})


async def test_advancing_a_stamp_that_has_no_row_is_refused(tmp_path):
    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'bare.db'}")
    with engine.begin() as raw:
        metadata.create_all(raw, tables=[replica_state, stamps])
    try:
        async with Database(engine, "test").acquire() as connection:
            with pytest.raises(RuntimeError, match="config stamp 'replica' has no row"):
                await set_promoted(connection, KEY, True)
            # the promotion was not recorded without its stamp
            assert await promoted_keys(connection) == frozenset()
    finally:
        engine.dispose()


async def test_an_unknown_reason_is_refused(conn):
    with pytest.raises(ValueError, match="unknown build reason"):
        await build_state.request_build(conn, KEY, "because")


async def test_with_nothing_promoted_and_built_no_store_is_asked_for(conn):
    """A deployment with nothing replicated for being busy may have no store at all (an engine
    that is not its own store, with none configured). Reading the promoted and serving sets must
    not ask which store it is; only a completed build of a promoted table has a store to match."""

    def _no_store() -> str:
        raise AssertionError("the store was asked for with nothing built to place in it")

    assert await promotion(conn, _no_store) == (frozenset(), frozenset())
    await set_promoted(conn, KEY, True)  # promoted, no build yet
    assert await promotion(conn, _no_store) == (frozenset({KEY}), frozenset())
    await _complete(conn, KEY)
    assert await promotion(conn, lambda: "store-a") == (frozenset({KEY}), frozenset({KEY}))
