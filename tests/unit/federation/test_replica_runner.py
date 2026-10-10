# Copyright (c) 2026 Kenneth Stott
# Canary: 3a75d1d5-518f-4ee2-b253-69057e35ad3a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1915 / REQ-1916: every node that does background work builds replicas, within three
limits, and each requested replica is built once.

The control plane here is a SQLite file, where the locks are ``flock`` files; the same races on
PostgreSQL's advisory locks are in ``tests/integration/test_replica_build_locks_pg.py``.
"""

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from provisa.core import process_mode
from provisa.core.database import Database, create_engine_from_url
from provisa.core.schema_org import metadata, replica_state
from provisa.federation import replica_state as build_state
from provisa.federation.replica_locks import BuildLocks
from provisa.federation.replica_errors import WAITING_ENGINE, WAITING_SOURCE
from provisa.federation.replica_runner import (
    BuildOutcome,
    ReplicaRunner,
    engine_job_key,
)
from provisa.federation.replica_state import RetryPolicy

ORG = "org1"


def _async(fn):
    """``fn`` as the coroutine function the runner awaits."""

    async def call(*args):
        return fn(*args)

    return call


def _key(n: int):
    return ("src", "public", f"t{n}")


@pytest.fixture
def plane(tmp_path):
    """A control plane file, and a factory of independent connections to it."""
    url = f"sqlite+pysqlite:///{tmp_path / 'cp.db'}"
    engines = []

    def connect() -> Database:
        engine = create_engine_from_url(url)
        engines.append(engine)
        return Database(engine, "test")

    first = create_engine_from_url(url)
    with first.begin() as conn:
        metadata.create_all(conn, tables=[replica_state])
    first.dispose()
    yield url, connect
    for engine in engines:
        engine.dispose()


class _Permits:
    """The live-read permit store's two calls, counting holders per key in memory."""

    def __init__(self) -> None:
        self.held: dict[str, set[str]] = {}
        self._n = 0

    @staticmethod
    def key(org_id, source_id):
        return f"{org_id}:{source_id}"

    def try_acquire(self, key, cap):
        holders = self.held.setdefault(key, set())
        if len(holders) >= cap:
            return None
        self._n += 1
        token = f"p{self._n}"
        holders.add(token)
        return token

    def release(self, key, token):
        self.held[key].discard(token)


class _Node:
    """One building node: its own host slots, its own control-plane connection, one runner."""

    def __init__(
        self,
        name,
        url,
        db,
        tmp_path,
        *,
        build,
        permits,
        per_node=1,
        engine_jobs=2,
        cap=None,
        engine="trino@engine:8080",
        retry=60.0,
    ):
        self.tasks: list[asyncio.Task] = []
        self.locks = BuildLocks(url)
        self.locks._slots = tmp_path / f"slots-{name}"  # a host of its own
        self.runner = ReplicaRunner(
            db=db,
            org_id=ORG,
            locks=self.locks,
            engine_key=lambda: engine,
            build=build,
            source_cap=_async(lambda _key: cap),
            permits=permits,
            next_refresh_at=_async(lambda _key, _now: None),
            store=lambda: "store-a",
            retry=lambda: RetryPolicy(retry, 3600),
            builds_per_node=lambda: per_node,
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


async def test_two_runners_build_each_requested_replica_once_and_both_build(plane, tmp_path):
    url, connect = plane
    db_a, db_b = connect(), connect()
    keys = [_key(n) for n in range(6)]
    await _request(db_a, *keys)

    built: list[tuple[str, tuple]] = []
    both_building = asyncio.Event()
    in_flight: set[str] = set()

    def build_as(name):
        async def build(key, progress):
            in_flight.add(name)
            if len(in_flight) == 2:
                both_building.set()
            # The first build of each node holds until the other node is mid-build too.
            await asyncio.wait_for(both_building.wait(), 5)
            await progress(1)
            built.append((name, key))
            in_flight.discard(name)
            return BuildOutcome(rows_copied=1, method="stream_batches")

        return build

    permits = _Permits()
    a = _Node("a", url, db_a, tmp_path, build=build_as("a"), permits=permits)
    b = _Node("b", url, db_b, tmp_path, build=build_as("b"), permits=permits)
    assert await a.runner.run_pass() == 1  # one slot on node a
    assert await b.runner.run_pass() == 1
    await asyncio.gather(a.drain(), b.drain())

    assert sorted(key for _name, key in built) == sorted(keys)  # each exactly once
    assert {name for name, _key in built} == {"a", "b"}  # both nodes built some
    records = await _records(db_a)
    assert all(r.build_state == build_state.IDLE and r.exists for r in records.values())
    assert all(r.rows_copied == 1 and r.build_method == "stream_batches" for r in records.values())


async def test_a_runner_at_its_node_limit_claims_nothing_more(plane, tmp_path):
    url, connect = plane
    db = connect()
    await _request(db, _key(0), _key(1), _key(2))
    release = asyncio.Event()

    async def build(key, progress):
        await release.wait()
        return BuildOutcome(rows_copied=0, method="stream_batches")

    node = _Node("a", url, db, tmp_path, build=build, permits=_Permits(), per_node=2, engine_jobs=5)
    assert await node.runner.run_pass() == 2
    await asyncio.sleep(0)
    assert await node.runner.run_pass() == 0  # both of the host's slots are held
    states = [r.build_state for r in (await _records(db)).values()]
    assert sorted(states) == ["building", "building", "requested"]
    release.set()
    await node.drain()
    assert all(r.build_state == build_state.IDLE for r in (await _records(db)).values())


async def test_a_source_at_its_cap_leaves_the_build_requested_and_the_replica_unlocked(
    plane, tmp_path
):
    url, connect = plane
    db = connect()
    await _request(db, _key(0))
    permits = _Permits()
    assert permits.try_acquire(permits.key(ORG, "src"), 1)  # a live read holds the one permit

    async def build(key, progress):
        raise AssertionError("a build must not start without its source permit")

    node = _Node("a", url, db, tmp_path, build=build, permits=permits, cap=1)
    assert await node.runner.run_pass() == 0
    record = (await _records(db))[_key(0)]
    assert (record.build_state, record.waiting_on) == (build_state.REQUESTED, WAITING_SOURCE)
    # Nothing is held while it waits: another runner takes the replica's lock, the engine's
    # slot and this host's slot at once.
    other = BuildLocks(url).claim()
    try:
        assert other.try_replica(ORG, _key(0))
        assert other.try_engine_slot("trino@engine:8080", 1)
    finally:
        other.close()
    slot = node.locks.try_node_slot(1)
    assert slot is not None
    slot.release()


async def test_the_engine_job_cap_holds_across_nodes_and_a_dead_builder_frees_its_slot(
    plane, tmp_path
):
    url, connect = plane
    db_a, db_b = connect(), connect()
    await _request(db_a, _key(0), _key(1))
    running = 0
    most = 0
    release = asyncio.Event()

    async def build(key, progress):
        nonlocal running, most
        running += 1
        most = max(most, running)
        await release.wait()
        running -= 1
        return BuildOutcome(rows_copied=0, method="engine_statement")

    permits = _Permits()
    a = _Node("a", url, db_a, tmp_path, build=build, permits=permits, engine_jobs=1)
    b = _Node("b", url, db_b, tmp_path, build=build, permits=permits, engine_jobs=1)
    assert await a.runner.run_pass() == 1
    await asyncio.sleep(0)
    assert await b.runner.run_pass() == 0  # the engine's one slot is taken by node a
    waiting = [r for r in (await _records(db_b)).values() if r.build_state == "requested"]
    assert [r.waiting_on for r in waiting] == [WAITING_ENGINE]

    release.set()
    await a.drain()  # node a finishes, frees the slot, and takes the second build itself
    assert most == 1
    assert all(
        r.build_state == build_state.IDLE and r.exists for r in (await _records(db_a)).values()
    )

    # A builder that dies: its claim's locks go with it, and the slot is free at once.
    dead = BuildLocks(url).claim()
    assert dead.try_engine_slot("trino@engine:8080", 1)
    survivor = BuildLocks(url).claim()
    try:
        assert not survivor.try_engine_slot("trino@engine:8080", 1)
        dead.close()  # what the operating system does to a dead process's open files
        assert survivor.try_engine_slot("trino@engine:8080", 1)
    finally:
        survivor.close()


async def test_a_row_left_building_by_a_dead_builder_is_rebuilt(plane, tmp_path):
    url, connect = plane
    db = connect()
    await _request(db, _key(0))
    async with db.acquire() as conn:
        assert await build_state.claim(
            conn, _key(0), holder="gone:1", retry=RetryPolicy(60, 3600), now=datetime.now(UTC)
        )

    async def build(key, progress):
        return BuildOutcome(rows_copied=7, method="stream_batches", content_hash="h")

    node = _Node("a", url, db, tmp_path, build=build, permits=_Permits())
    assert await node.runner.run_pass() == 1  # nobody holds the replica's lock
    await node.drain()
    record = (await _records(db))[_key(0)]
    assert (record.build_state, record.rows_copied, record.content_hash) == ("idle", 7, "h")


async def test_what_a_build_had_to_say_is_recorded_with_its_completion(plane, tmp_path):
    from provisa.federation.data_replicator import BuildNote

    url, connect = plane
    db = connect()
    await _request(db, _key(0))
    note = BuildNote("replication.unreadable_messages", {"count": 1, "ids": ["m9"], "more": 0})

    async def build(key, progress):
        return BuildOutcome(rows_copied=4, method="stream_batches", content_hash="h", notes=(note,))

    node = _Node("a", url, db, tmp_path, build=build, permits=_Permits())
    assert await node.runner.run_pass() == 1
    await node.drain()
    record = (await _records(db))[_key(0)]
    assert record.build_state == "idle"
    assert record.build_notes == [{"code": note.code, "params": note.params}]


async def test_a_failed_build_is_recorded_and_frees_what_it_held(plane, tmp_path):
    url, connect = plane
    db = connect()
    await _request(db, _key(0))
    permits = _Permits()

    async def build(key, progress):
        raise RuntimeError("source refused the connection")

    node = _Node("a", url, db, tmp_path, build=build, permits=permits, cap=1)
    assert await node.runner.run_pass() == 1
    await node.drain()
    record = (await _records(db))[_key(0)]
    assert record.build_state == build_state.FAILED
    assert record.last_error == "source refused the connection" and record.failed_at is not None
    assert not record.exists
    assert permits.held[permits.key(ORG, "src")] == set()
    assert node.locks.try_node_slot(1) is not None
    # A driver's own error has no code: the UI shows its text.
    assert (record.last_error_code, record.last_error_params, record.failed_attempts) == (
        None,
        None,
        1,
    )


async def test_a_build_that_keeps_failing_is_retried_and_says_how_often_and_why(plane, tmp_path):
    """REQ-1350: a failure whose cause Provisa names is recorded with its code and params.
    The runner tries a failed build again once the retry interval has passed; each failure in
    a row is counted, so a replica that never builds does not fail in silence, and a build
    that completes puts the count back to none."""
    from provisa.federation.replica_spool import ReplicaSpoolFull

    url, connect = plane
    db = connect()
    await _request(db, _key(0))
    outcomes: list = [ReplicaSpoolFull("src.t0", 3_000, 2_000), RuntimeError("boom"), None]

    async def build(key, progress):
        failure = outcomes.pop(0)
        if failure is not None:
            raise failure
        return BuildOutcome(rows_copied=1, method="stream_batches")

    node = _Node("a", url, db, tmp_path, build=build, permits=_Permits(), retry=0.0)
    assert await node.runner.run_pass() == 1
    await node.drain()  # each ended build looks for the next: the failed one is due at once
    record = (await _records(db))[_key(0)]
    assert outcomes == [] and record.build_state == build_state.IDLE and record.exists
    assert (record.failed_attempts, record.last_error, record.last_error_code) == (0, None, None)

    await _request(db, _key(1))
    outcomes[:] = [ReplicaSpoolFull("src.t1", 3_000, 2_000)]
    held = _Node("b", url, db, tmp_path, build=build, permits=_Permits())  # retry in 60 s
    assert await held.runner.run_pass() == 1
    await held.drain()
    record = (await _records(db))[_key(1)]
    assert (record.build_state, record.failed_attempts) == (build_state.FAILED, 1)
    assert record.last_error_code == "replication.spool_full"
    assert record.last_error_params == {"table": "src.t1", "needed": 3_000, "limit": 2_000}
    assert "over the limit of 2,000" in record.last_error


async def test_a_refresh_that_is_due_is_claimed_like_a_request(plane, tmp_path):
    url, connect = plane
    db = connect()
    await _request(db, _key(0))
    now = datetime.now(UTC)
    async with db.acquire() as conn:
        await build_state.record_completed(
            conn,
            _key(0),
            rows_copied=1,
            method="stream_batches",
            content_hash=None,
            store="store-a",
            next_refresh_at=now - timedelta(seconds=1),
            now=now - timedelta(minutes=5),
        )
    built = []

    async def build(key, progress):
        built.append(key)
        return BuildOutcome(rows_copied=2, method="stream_batches")

    node = _Node("a", url, db, tmp_path, build=build, permits=_Permits())
    assert await node.runner.run_pass() == 1
    await node.drain()
    assert built == [_key(0)]
    assert (await _records(db))[_key(0)].next_refresh_at is None


async def test_a_query_process_builds_nothing(plane, tmp_path, monkeypatch):
    url, connect = plane
    db = connect()
    await _request(db, _key(0))
    monkeypatch.setattr(process_mode, "_mode", process_mode.QUERY)

    async def build(key, progress):
        raise AssertionError("a query process runs no build")

    node = _Node("a", url, db, tmp_path, build=build, permits=_Permits())
    assert await node.runner.run_pass() == 0
    assert (await _records(db))[_key(0)].build_state == build_state.REQUESTED


def test_a_process_is_every_until_its_launch_says_otherwise(monkeypatch):
    assert process_mode.mode() == process_mode.EVERY and process_mode.runs_background_work()
    monkeypatch.setattr(process_mode, "_mode", process_mode.EVERY)
    process_mode.set_mode(process_mode.COORDINATOR)
    assert process_mode.runs_background_work()
    process_mode.set_mode(process_mode.QUERY)
    assert not process_mode.runs_background_work()
    with pytest.raises(ValueError, match="unknown process mode"):
        process_mode.set_mode("both")


def test_the_engine_key_has_no_org_and_names_the_host_of_an_embedded_engine():
    import socket

    assert engine_job_key("trino", "engine:8080") == "trino@engine:8080"
    assert engine_job_key("duckdb", None) == f"duckdb@{socket.gethostname()}"


@pytest.mark.unbound
async def test_a_pass_builds_in_its_own_org_whoever_started_it(plane, tmp_path):
    """REQ-1266: the scheduled pass and a read's kick may run with no org bound; the pass binds
    its runner's org, and the builds it spawns run in it."""
    from provisa.core.request_context import current_org

    url, connect = plane
    db = connect()
    await _request(db, _key(0))
    seen: list[str | None] = []

    async def build(key, progress):
        seen.append(current_org.get())
        return BuildOutcome(rows_copied=0, method="stream_batches")

    node = _Node("solo", url, db, tmp_path, build=build, permits=_Permits())
    assert current_org.get() is None
    assert await node.runner.run_pass() == 1
    await node.drain()
    assert seen == [ORG]
    assert current_org.get() is None
