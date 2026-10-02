# Copyright (c) 2026 Kenneth Stott
# Canary: d5ef5b6b-257b-414a-a443-ad8dd92bc8e9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1915: a write of deltas into a replica (an append past the cursor, a batch of change
events) holds the replica's own lock, the one a build holds: a delta is never written into a
table a build is about to replace, and a build never starts under a delta."""

from __future__ import annotations

import asyncio

import pytest

from provisa.events.handlers import make_source_land
from provisa.federation import replica_locks
from provisa.federation.replica_locks import BuildLocks, replica_write_lock

pytestmark = pytest.mark.unit

ORG = "org1"
KEY = ("src", "public", "orders")


@pytest.fixture
def url(tmp_path, monkeypatch):
    monkeypatch.setattr(replica_locks, "_delta_claims", {})
    monkeypatch.setattr(replica_locks, "_DELTA_POLL_S", 0.01)
    yield f"sqlite+pysqlite:///{tmp_path / 'cp.db'}"
    for claim in replica_locks._delta_claims.values():
        claim.close()


async def test_a_delta_waits_for_a_running_build_and_lands_after_it(url):
    build = BuildLocks(url).claim()
    assert build.try_replica(ORG, KEY)  # a build of the replica is running
    order: list[str] = []

    async def delta() -> None:
        async with replica_write_lock(url, ORG, KEY):
            order.append("delta")

    task = asyncio.ensure_future(delta())
    await asyncio.sleep(0.05)
    assert order == [] and not task.done()  # it waits; it does not write under the build
    order.append("swap")
    build.close()  # the build swaps and releases the lock
    await asyncio.wait_for(task, 2)
    assert order == ["swap", "delta"]


async def test_a_build_does_not_start_under_a_delta(url):
    other = BuildLocks(url).claim()
    try:
        async with replica_write_lock(url, ORG, KEY):
            assert not other.try_replica(ORG, KEY)
            assert other.try_replica(ORG, ("src", "public", "another"))  # per replica
        assert other.try_replica(ORG, KEY)  # released with the delta
    finally:
        other.close()


async def test_an_append_land_takes_the_replicas_lock(url):
    """The event loop's append land writes with the lock held, and gives it back when the write
    fails."""
    held_during: list[bool] = []
    probe = BuildLocks(url).claim()

    class _Engine:
        def __init__(self, fail: bool = False) -> None:
            self.fail = fail

        async def land_source_table(self, **kw):
            held_during.append(not probe.try_replica(ORG, KEY))
            if self.fail:
                raise RuntimeError("store down")
            return "org_org1_replicas.src__public__orders"

    async def fetch(pending):
        return [{"id": 1, "updated_at": "2026-01-01"}]

    def land_with(engine):
        return make_source_land(
            engine,
            schema="org_org1_replicas",
            table="src__public__orders",
            columns=[("id", "bigint"), ("updated_at", "text")],
            change_signal="ttl_probe",
            watermark_column="updated_at",
            pk_columns=["id"],
            fetch=fetch,
            probe_type="watermark",
            write_lock=lambda: replica_write_lock(url, ORG, KEY),
        )

    try:
        landed = await land_with(_Engine())([], prior_hash=None)
        assert landed is not None and landed[0] == "append"
        with pytest.raises(RuntimeError, match="store down"):
            await land_with(_Engine(fail=True))([], prior_hash=None)
        assert held_during == [True, True]
        assert probe.try_replica(ORG, KEY)  # given back both times
    finally:
        probe.close()
