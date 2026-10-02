# Copyright (c) 2026 Kenneth Stott
# Canary: 40b3b34c-6fdb-4c26-99c9-062fdbaecc8b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1915: replica builds on a PostgreSQL control plane — the replica lock and the engine's
job slots are session advisory locks, so a builder that dies frees them at once.

Two runners on two control-plane connections stand for two building nodes.
"""

# Requirements: REQ-1915, REQ-1916

from __future__ import annotations

import asyncio
import os
import uuid

import pytest
import sqlalchemy as sa

from provisa.core.database import Database, create_engine_from_url
from provisa.core.schema_org import metadata, replica_state
from provisa.federation import replica_state as build_state
from provisa.federation.replica_locks import BuildLocks
from provisa.federation.replica_errors import WAITING_ENGINE
from provisa.federation.replica_runner import BuildOutcome, ReplicaRunner

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = int(os.environ.get("PG_PORT", "5432"))
_PG_USER = os.environ.get("PG_USER", "provisa")
_PG_PASSWORD = os.environ.get("PG_PASSWORD", "provisa")
_BASE = f"postgresql+psycopg://{_PG_USER}:{_PG_PASSWORD}@{_PG_HOST}:{_PG_PORT}"
_ADMIN_URL = f"{_BASE}/{os.environ.get('PG_DATABASE', 'provisa')}"
ORG = "org1"
ENGINE = "trino@engine:8080"


def _async(fn):
    """``fn`` as the coroutine function the runner awaits."""

    async def call(*args):
        return fn(*args)

    return call


def _key(n: int):
    return ("src", "public", f"t{n}")


@pytest.fixture
def plane():
    """A fresh control-plane database holding ``replica_state``, and a connection factory."""
    name = f"replica_locks_{uuid.uuid4().hex[:10]}"
    admin = sa.create_engine(_ADMIN_URL, isolation_level="AUTOCOMMIT")
    with admin.connect() as conn:
        conn.execute(sa.text(f'CREATE DATABASE "{name}"'))
    url = f"{_BASE}/{name}"
    engines = []

    def connect() -> Database:
        engine = create_engine_from_url(url)
        engines.append(engine)
        return Database(engine, "test")

    first = create_engine_from_url(url)
    with first.begin() as conn:
        metadata.create_all(conn, tables=[replica_state])
    first.dispose()
    try:
        yield url, connect
    finally:
        for engine in engines:
            engine.dispose()
        with admin.connect() as conn:
            conn.execute(sa.text(f'DROP DATABASE "{name}" WITH (FORCE)'))
        admin.dispose()


class _NoCap:
    """No source in these tests has a live-read cap."""

    @staticmethod
    def key(org_id, source_id):
        return f"{org_id}:{source_id}"


class _Node:
    def __init__(self, name, url, db, tmp_path, *, build, engine_jobs=2):
        self.tasks: list[asyncio.Task] = []
        self.locks = BuildLocks(url)
        self.locks._slots = tmp_path / f"slots-{name}"  # a host of its own
        self.runner = ReplicaRunner(
            db=db,
            org_id=ORG,
            locks=self.locks,
            engine_key=lambda: ENGINE,
            build=build,
            source_cap=_async(lambda _key: None),
            permits=_NoCap(),
            next_refresh_at=_async(lambda _key, _now: None),
            store=lambda: "store-a",
            retry_interval=lambda: 60.0,
            builds_per_node=lambda: 1,
            engine_jobs=lambda: engine_jobs,
            spawn=lambda coro, name: self.tasks.append(asyncio.ensure_future(coro)),
        )

    async def drain(self):
        while self.tasks:
            await asyncio.gather(*self.tasks[:])
            self.tasks = [t for t in self.tasks if not t.done()]


async def _request(db, *keys):
    async with db.acquire() as conn:
        for key in keys:
            assert await build_state.request_build(conn, key, build_state.REASON_MODEL)


async def _records(db):
    async with db.acquire() as conn:
        return {r.key: r for r in await build_state.read_all(conn)}


def _terminate(url: str, claim) -> None:
    """End the claim's control-plane session from outside, as a dead process's would end."""
    pid = claim._conn.execute(sa.text("SELECT pg_backend_pid()")).scalar()
    admin = sa.create_engine(url, isolation_level="AUTOCOMMIT")
    with admin.connect() as conn:
        conn.execute(sa.text("SELECT pg_terminate_backend(:pid)"), {"pid": pid})
    admin.dispose()


async def test_two_runners_build_each_requested_replica_once_and_both_build(plane, tmp_path):
    url, connect = plane
    db_a, db_b = connect(), connect()
    keys = [_key(n) for n in range(8)]
    await _request(db_a, *keys)
    built: list[tuple[str, tuple]] = []
    both_building = asyncio.Event()
    in_flight: set[str] = set()

    def build_as(name):
        async def build(key, progress):
            in_flight.add(name)
            if len(in_flight) == 2:
                both_building.set()
            await asyncio.wait_for(both_building.wait(), 10)
            built.append((name, key))
            in_flight.discard(name)
            return BuildOutcome(rows_copied=1, method="stream_batches")

        return build

    a = _Node("a", url, db_a, tmp_path, build=build_as("a"))
    b = _Node("b", url, db_b, tmp_path, build=build_as("b"))
    assert await a.runner.run_pass() == 1
    assert await b.runner.run_pass() == 1
    await asyncio.gather(a.drain(), b.drain())

    assert sorted(key for _name, key in built) == sorted(keys)
    assert {name for name, _key in built} == {"a", "b"}
    assert all(r.build_state == "idle" and r.exists for r in (await _records(db_a)).values())


async def test_with_the_engine_cap_at_one_two_rows_never_build_at_once_and_both_get_built(
    plane, tmp_path
):
    url, connect = plane
    db_a, db_b = connect(), connect()
    await _request(db_a, _key(0), _key(1))
    running = most = 0
    release = asyncio.Event()

    async def build(key, progress):
        nonlocal running, most
        running += 1
        most = max(most, running)
        await release.wait()
        running -= 1
        return BuildOutcome(rows_copied=0, method="engine_statement")

    a = _Node("a", url, db_a, tmp_path, build=build, engine_jobs=1)
    b = _Node("b", url, db_b, tmp_path, build=build, engine_jobs=1)
    assert await a.runner.run_pass() == 1
    await asyncio.sleep(0)
    assert await b.runner.run_pass() == 0
    waiting = [r for r in (await _records(db_b)).values() if r.build_state == "requested"]
    assert [r.waiting_on for r in waiting] == [WAITING_ENGINE]
    release.set()
    await a.drain()
    assert await b.runner.run_pass() == 0  # nothing left to build
    assert most == 1
    assert all(r.build_state == "idle" and r.exists for r in (await _records(db_a)).values())


async def test_a_killed_builder_frees_its_engine_slot_and_replica_and_the_row_is_rebuilt(
    plane, tmp_path
):
    url, connect = plane
    db = connect()
    await _request(db, _key(0))

    # A builder claims the row, then its control-plane session ends without a word.
    dead = BuildLocks(url).claim()
    assert dead.try_engine_slot(ENGINE, 1)
    assert dead.try_replica(ORG, _key(0))
    async with db.acquire() as conn:
        from datetime import UTC, datetime

        assert await build_state.claim(
            conn, _key(0), holder="gone:1", retry_interval=60, now=datetime.now(UTC)
        )

    other = BuildLocks(url).claim()
    try:
        assert not other.try_engine_slot(ENGINE, 1)
        assert not other.try_replica(ORG, _key(0))
        _terminate(url, dead)
        for _ in range(50):  # the server frees the session's locks as the backend exits
            if other.try_engine_slot(ENGINE, 1):
                break
            await asyncio.sleep(0.1)
        else:
            raise AssertionError("the dead builder's engine slot was not freed")
        assert other.try_replica(ORG, _key(0))
    finally:
        other.close()
        dead._conn = None  # its session is gone; nothing to close on it
        dead.close()

    async def build(key, progress):
        return BuildOutcome(rows_copied=3, method="stream_batches")

    node = _Node("a", url, db, tmp_path, build=build, engine_jobs=1)
    assert await node.runner.run_pass() == 1  # the row still said building; its lock was free
    await node.drain()
    record = (await _records(db))[_key(0)]
    assert (record.build_state, record.rows_copied, record.exists) == ("idle", 3, True)


async def test_two_orgs_on_one_engine_share_its_job_slots_but_not_their_replica_locks(plane):
    url, _connect = plane
    one, two = BuildLocks(url).claim(), BuildLocks(url).claim()
    try:
        assert one.try_engine_slot(ENGINE, 1)
        assert not two.try_engine_slot(ENGINE, 1)  # the engine counts once, whatever the org
        assert two.try_engine_slot("trino@other-engine:8080", 1)
        assert one.try_replica("org1", _key(0))
        assert two.try_replica("org2", _key(0))  # the same table name in another org
        assert not two.try_replica("org1", _key(0))
    finally:
        one.close()
        two.close()
