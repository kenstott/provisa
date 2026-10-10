# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
# ruff: noqa: F811  (the `plane` fixture is imported and named as a test's argument)

"""REQ-1915: replicas that come from one read of their source, on a PostgreSQL control plane.
Each sibling's lock is a session advisory lock taken without waiting, so two runners that
start from different tables of one group never wait on each other, no table is in two reads at
once, and a read that is stopped gives back every table it held.

Two runners on two control-plane connections stand for two building nodes.
"""

# Requirements: REQ-1915

from __future__ import annotations

import asyncio

import pytest

from provisa.federation import replica_state as build_state
from provisa.federation.data_replicator import BuildOutcome
from tests.integration.test_replica_build_locks_pg import (  # noqa: F401  (plane is a fixture)
    ORG,
    _key,
    _Node,
    _records,
    _request,
    plane,
)

pytestmark = [pytest.mark.integration]

GROUP = [_key(1), _key(2), _key(3)]


def _grouped(node, reads):
    async def group_of(key):
        return [k for k in GROUP if k != key] if key in GROUP else []

    node.runner._group_of = group_of
    node.runner._build_group = reads
    return node


async def _single(key, progress):
    return BuildOutcome(rows_copied=1, method="stream_batches")


async def test_one_requested_table_brings_its_group_into_one_read(plane, tmp_path):
    url, connect = plane
    db = connect()
    await _request(db, *GROUP)
    groups: list[list] = []

    async def reads(keys, progress):
        groups.append(sorted(keys))
        return {key: BuildOutcome(rows_copied=5, method="stream_batches") for key in keys}

    node = _grouped(_Node("a", url, db, tmp_path, build=_single), reads)
    await asyncio.wait_for(node.runner.run_pass(), 30)
    await asyncio.wait_for(node.drain(), 30)
    assert groups == [GROUP]
    records = await _records(db)
    assert all(records[k].build_state == "idle" and records[k].rows_copied == 5 for k in GROUP)


async def test_two_runners_starting_from_different_tables_never_wait_and_never_overlap(
    plane, tmp_path
):
    url, connect = plane
    db_a, db_b = connect(), connect()
    await _request(db_a, *GROUP)
    building: list = []
    overlap: list = []
    read: list = []

    async def reads(keys, progress):
        overlap.extend(k for k in keys if k in building)
        building.extend(keys)
        await asyncio.sleep(0.2)  # both runners are mid-read together
        for k in keys:
            building.remove(k)
        read.extend(keys)
        return {key: BuildOutcome(rows_copied=1, method="stream_batches") for key in keys}

    a = _grouped(_Node("a", url, db_a, tmp_path, build=_single), reads)
    b = _grouped(_Node("b", url, db_b, tmp_path, build=_single), reads)
    await asyncio.wait_for(asyncio.gather(a.runner.run_pass(), b.runner.run_pass()), 30)
    await asyncio.wait_for(asyncio.gather(a.drain(), b.drain()), 30)
    assert overlap == []
    records = await _records(db_a)
    assert all(records[k].build_state == "idle" for k in GROUP)
    assert sorted(set(read)) == GROUP


async def test_a_sibling_held_by_another_session_is_left_out(plane, tmp_path):
    url, connect = plane
    db = connect()
    await _request(db, *GROUP)
    holder = _Node("h", url, connect(), tmp_path, build=_single)
    claim = holder.locks.claim()
    assert claim.try_replica(ORG, _key(3))  # another runner's session holds t3
    groups: list[list] = []

    async def reads(keys, progress):
        groups.append(sorted(keys))
        return {key: BuildOutcome(rows_copied=1, method="stream_batches") for key in keys}

    node = _grouped(_Node("a", url, db, tmp_path, build=_single), reads)
    try:
        await asyncio.wait_for(node.runner.run_pass(), 30)
        await asyncio.wait_for(node.drain(), 30)
    finally:
        claim.close()
    assert groups == [[_key(1), _key(2)]]
    records = await _records(db)
    assert records[_key(3)].build_state == build_state.REQUESTED  # still to be built, alone


async def test_a_read_that_is_stopped_frees_every_table_and_records_it_on_each(plane, tmp_path):
    url, connect = plane
    db = connect()
    await _request(db, *GROUP)
    reading = asyncio.Event()
    never = asyncio.Event()

    async def stuck(keys, progress):
        reading.set()
        await never.wait()
        return {}

    node = _grouped(_Node("a", url, db, tmp_path, build=_single), stuck)
    await asyncio.wait_for(node.runner.run_pass(), 30)
    await asyncio.wait_for(reading.wait(), 30)
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
        assert all(after.try_replica(ORG, key) for key in GROUP)  # every lock was given back
    finally:
        after.close()
