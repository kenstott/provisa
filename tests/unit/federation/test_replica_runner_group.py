# Copyright (c) 2026 Kenneth Stott
# Canary: 896d4c6d-1151-466b-9ae5-90f21e98ff9e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

# ruff: noqa: F811  (the `plane` fixture is imported and named as a test's argument)
"""REQ-1915: a runner builds the replicas that come from one read of their source in one job.
On claiming one table it claims its siblings without waiting, runs one read, and records each
table's outcome on its own record. The fixtures are the single-build runner's."""

import asyncio
from datetime import UTC, datetime, timedelta

from provisa.federation import replica_state as build_state
from provisa.federation.data_replicator import BuildOutcome
from provisa.federation.replica_errors import BuildFailure
from tests.unit.federation.test_replica_runner import (  # noqa: F401  (plane is a fixture)
    ORG,
    _key,
    _Node,
    _Permits,
    _records,
    _request,
    plane,
)

GROUP = [_key(1), _key(2), _key(3)]


class _Reads:
    """What a node was asked to build: single builds, and group reads with their keys."""

    def __init__(self, *, fail: BaseException | None = None, per_key=None, hold=None):
        self.single: list[tuple] = []
        self.groups: list[list[tuple]] = []
        self._fail = fail
        self._per_key = per_key or {}
        self._hold = hold

    async def build(self, key, progress):
        self.single.append(key)
        return BuildOutcome(rows_copied=1, method="stream_batches")

    async def build_group(self, keys, progress):
        self.groups.append(list(keys))
        if self._hold is not None:
            await self._hold()
        if self._fail is not None:
            raise self._fail
        for key in keys:
            await progress(key, 7)
        return {
            key: self._per_key.get(key, BuildOutcome(rows_copied=7, method="stream_batches"))
            for key in keys
        }


def _node(name, url, db, tmp_path, reads, *, group=GROUP, permits=None, due=None, **kw):
    node = _Node(name, url, db, tmp_path, build=reads.build, permits=permits or _Permits(), **kw)

    async def group_of(key):
        return [k for k in group if k != key] if key in group else []

    node.runner._group_of = group_of
    node.runner._build_group = reads.build_group
    if due is not None:

        async def next_refresh_at(_key, now):
            return now + due

        node.runner._next_refresh_at = next_refresh_at
    return node


async def _complete(db, key, *, next_refresh_at=None):
    """A replica built before, idle now."""
    now = datetime.now(UTC)
    async with db.acquire() as conn:
        assert await build_state.claim(
            conn, key, holder="earlier", now=now, retry=build_state.RetryPolicy(60, 3600)
        )
        await build_state.record_completed(
            conn,
            key,
            rows_copied=1,
            method="stream_batches",
            content_hash="h",
            store="store-a",
            next_refresh_at=next_refresh_at,
            now=now,
        )


async def _fail(db, key):
    async with db.acquire() as conn:
        assert await build_state.claim(
            conn,
            key,
            holder="earlier",
            now=datetime.now(UTC),
            retry=build_state.RetryPolicy(60, 3600),
        )
        await build_state.record_failed(conn, key, error="earlier failure", now=datetime.now(UTC))


async def test_one_requested_table_brings_its_group_into_one_read(plane, tmp_path):
    url, connect = plane
    db = connect()
    await _request(db, *GROUP, _key(9))  # t9 is no part of the group
    reads = _Reads()
    node = _node("a", url, db, tmp_path, reads, per_node=4, engine_jobs=4)
    await asyncio.wait_for(node.runner.run_pass(), 10)
    await asyncio.wait_for(node.drain(), 10)
    assert [sorted(g) for g in reads.groups] == [GROUP]  # one read for the three
    assert reads.single == [_key(9)]  # a table of no group is built as it always was
    records = await _records(db)
    for key in GROUP:
        assert records[key].build_state == build_state.IDLE and records[key].rows_copied == 7


async def test_a_sibling_that_is_idle_and_not_due_is_rebuilt_and_its_clock_restarts(
    plane, tmp_path
):
    url, connect = plane
    db = connect()
    await _request(db, *GROUP)
    far = datetime.now(UTC) + timedelta(days=30)
    await _complete(db, _key(2), next_refresh_at=far)
    await _complete(db, _key(3), next_refresh_at=far)
    reads = _Reads()
    node = _node("a", url, db, tmp_path, reads, due=timedelta(hours=1))
    await asyncio.wait_for(node.runner.run_pass(), 10)
    await asyncio.wait_for(node.drain(), 10)
    assert [sorted(g) for g in reads.groups] == [GROUP] and reads.single == []
    records = await _records(db)
    for key in GROUP:
        assert records[key].rows_copied == 7
        # Its next refresh counts from this build, so it is not read again alone tomorrow.
        assert records[key].next_refresh_at < datetime.now(UTC) + timedelta(hours=2)


async def test_a_failed_sibling_still_waiting_is_rebuilt_and_its_failure_cleared(plane, tmp_path):
    url, connect = plane
    db = connect()
    await _request(db, *GROUP)
    await _fail(db, _key(2))
    before = (await _records(db))[_key(2)]
    assert before.build_state == build_state.FAILED and before.failed_attempts == 1
    reads = _Reads()
    node = _node("a", url, db, tmp_path, reads)
    await asyncio.wait_for(node.runner.run_pass(), 10)
    await asyncio.wait_for(node.drain(), 10)
    after = (await _records(db))[_key(2)]
    assert after.build_state == build_state.IDLE
    assert after.failed_attempts == 0 and after.last_error is None


async def test_a_failed_read_is_recorded_once_on_every_table_with_the_same_reason(plane, tmp_path):
    class Throttled(BuildFailure, RuntimeError):
        code = "replication.source_throttled"

        def __init__(self):
            super().__init__("the source is throttling")
            self.params = {"wait": 30}

    url, connect = plane
    db = connect()
    await _request(db, *GROUP)
    await _fail(db, _key(2))  # one attempt already
    reads = _Reads(fail=Throttled())
    node = _node("a", url, db, tmp_path, reads)
    await asyncio.wait_for(node.runner.run_pass(), 10)
    await asyncio.wait_for(node.drain(), 10)
    records = await _records(db)
    assert [records[k].build_state for k in GROUP] == [build_state.FAILED] * 3
    assert {records[k].last_error for k in GROUP} == {"the source is throttling"}
    assert {records[k].last_error_code for k in GROUP} == {"replication.source_throttled"}
    # One more attempt each: the read failed once, whatever the number of tables it built.
    assert [records[k].failed_attempts for k in GROUP] == [1, 2, 1]
    assert len(reads.groups) == 1


async def test_one_tables_own_failure_is_its_own_and_the_others_complete(plane, tmp_path):
    url, connect = plane
    db = connect()
    await _request(db, *GROUP)
    reads = _Reads(per_key={_key(2): LookupError("table t2 is no longer declared")})
    node = _node("a", url, db, tmp_path, reads)
    await asyncio.wait_for(node.runner.run_pass(), 10)
    await asyncio.wait_for(node.drain(), 10)
    records = await _records(db)
    assert records[_key(2)].build_state == build_state.FAILED
    assert records[_key(1)].build_state == records[_key(3)].build_state == build_state.IDLE


async def test_what_a_group_job_holds_is_released_whether_it_succeeds_or_fails(plane, tmp_path):
    url, connect = plane
    db = connect()
    permits = _Permits()
    for reads in (_Reads(), _Reads(fail=RuntimeError("boom"))):
        await _request(db, *GROUP)
        node = _node("a", url, db, tmp_path, reads, permits=permits, cap=1)
        await asyncio.wait_for(node.runner.run_pass(), 10)
        await asyncio.wait_for(node.drain(), 10)
        assert all(not holders for holders in permits.held.values())
        # Every lock is free: another holder takes each of them at once.
        claim = node.locks.claim()
        try:
            assert all(claim.try_replica(ORG, key) for key in GROUP)
        finally:
            claim.close()


async def test_a_sibling_another_runner_holds_is_left_out_and_built_on_its_own(plane, tmp_path):
    url, connect = plane
    db_a, db_b = connect(), connect()
    await _request(db_a, *GROUP)
    release = asyncio.Event()

    async def hold():
        await asyncio.wait_for(release.wait(), 5)

    held_reads = _Reads(hold=hold)
    b = _node("b", url, db_b, tmp_path, held_reads, group=[_key(3)], per_node=1)
    # b takes t3 alone first and holds it mid-build.
    b.runner._group_of = None
    await _complete(db_a, _key(1))
    await _complete(db_a, _key(2))
    assert await b.runner.run_pass() == 1
    await _request(db_a, _key(1), _key(2))
    reads = _Reads()
    a = _node("a", url, db_a, tmp_path, reads)
    await asyncio.wait_for(a.runner.run_pass(), 10)
    await asyncio.wait_for(a.drain(), 10)
    release.set()
    await asyncio.wait_for(b.drain(), 10)
    (group,) = reads.groups
    assert _key(3) not in group and sorted(group) == [_key(1), _key(2)]
    records = await _records(db_a)
    assert all(records[k].build_state == build_state.IDLE for k in GROUP)


async def test_two_runners_starting_from_different_tables_of_a_group_never_wait_on_each_other(
    plane, tmp_path
):
    """Each holds the table it claimed first and TRIES the others: whatever each gets, both
    finish, no table is built twice at once, and every table ends built."""
    url, connect = plane
    db_a, db_b = connect(), connect()
    await _request(db_a, *GROUP)
    both = asyncio.Event()
    started: list[str] = []
    building: list[tuple] = []
    overlap: list[tuple] = []

    def reads_as(name):
        async def hold():
            started.append(name)
            if len(started) == 2:
                both.set()
            # Held until the other runner is mid-read too, or it turns out it has nothing.
            try:
                await asyncio.wait_for(both.wait(), 0.5)
            except TimeoutError:
                pass

        reads = _Reads(hold=hold)
        original = reads.build_group

        async def build_group(keys, progress):
            overlap.extend(k for k in keys if k in building)
            building.extend(keys)
            try:
                return await original(keys, progress)
            finally:
                for k in keys:
                    building.remove(k)

        reads.build_group = build_group
        return reads

    ra, rb = reads_as("a"), reads_as("b")
    a = _node("a", url, db_a, tmp_path, ra)
    b = _node("b", url, db_b, tmp_path, rb)
    await asyncio.wait_for(asyncio.gather(a.runner.run_pass(), b.runner.run_pass()), 10)
    await asyncio.wait_for(asyncio.gather(a.drain(), b.drain()), 10)
    assert overlap == []  # no table was in two reads at once
    records = await _records(db_a)
    assert all(records[k].build_state == build_state.IDLE for k in GROUP)
    built = [k for g in ra.groups + rb.groups for k in g] + ra.single + rb.single
    assert sorted(set(built)) == GROUP


async def test_a_runner_given_no_group_builds_each_table_alone(plane, tmp_path):
    url, connect = plane
    db = connect()
    await _request(db, *GROUP)
    reads = _Reads()
    node = _Node(
        "a", url, db, tmp_path, build=reads.build, permits=_Permits(), per_node=3, engine_jobs=3
    )
    await asyncio.wait_for(node.runner.run_pass(), 10)
    await asyncio.wait_for(node.drain(), 10)
    assert sorted(reads.single) == GROUP and reads.groups == []


async def test_a_read_that_is_stopped_frees_every_table_and_records_it_on_each(plane, tmp_path):
    url, connect = plane
    db = connect()
    await _request(db, *GROUP)
    reading = asyncio.Event()

    async def hold():
        reading.set()
        await asyncio.Event().wait()

    node = _node("a", url, db, tmp_path, _Reads(hold=hold))
    await asyncio.wait_for(node.runner.run_pass(), 10)
    await asyncio.wait_for(reading.wait(), 10)
    probe = node.locks.claim()
    try:
        assert not any(probe.try_replica(ORG, key) for key in GROUP)  # all three are held
    finally:
        probe.close()
    for task in node.tasks:
        task.cancel()
    await asyncio.gather(*node.tasks, return_exceptions=True)
    records = await _records(db)
    assert [records[k].build_state for k in GROUP] == [build_state.FAILED] * 3
    assert [records[k].failed_attempts for k in GROUP] == [1, 1, 1]
    after = node.locks.claim()
    try:
        assert all(after.try_replica(ORG, key) for key in GROUP)
    finally:
        after.close()
