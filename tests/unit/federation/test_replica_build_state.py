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
from provisa.core.schema_org import metadata, replica_state
from provisa.federation import replica_state as build_state
from provisa.federation.replica_state import promoted_keys, set_promoted

KEY = ("src", "public", "orders")


@pytest.fixture
async def conn(tmp_path):
    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'cp.db'}")
    with engine.begin() as raw:
        metadata.create_all(raw, tables=[replica_state])
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


async def test_an_unknown_reason_is_refused(conn):
    with pytest.raises(ValueError, match="unknown build reason"):
        await build_state.request_build(conn, KEY, "because")
