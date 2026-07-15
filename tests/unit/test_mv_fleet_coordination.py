# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Cross-instance MV refresh coordination (REQ-879).

Two fleet instances share one control-plane catalog (a single SQLite ``materialized_views``
table). The tests drive the real CAS on that shared row — no per-instance in-memory state — to
prove: exactly one instance claims a given MV, a crashed instance's lease expires and is
reclaimed, a released claim frees the next refresh, a fenced commit rejects a superseded writer,
and ``refresh_mv`` consults the shared row (not the registry) so a second instance skips.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import insert, select, update
from sqlalchemy.ext.asyncio import create_async_engine

from provisa.core.database import Database
from provisa.core.schema_org import materialized_views as MVT
from provisa.core.schema_org import metadata
from provisa.executor.result import QueryResult
from provisa.mv.coordination import (
    claim_refresh,
    commit_refresh,
    release_refresh,
    renew_lease,
)
from provisa.mv.models import MVDefinition, MVStatus
from provisa.mv.refresh import refresh_mv
from provisa.mv.registry import MVRegistry

MV_ID = "mv-orders"
INST_A = "instance-a"
INST_B = "instance-b"


@pytest.fixture
async def store(tmp_path):
    """A single shared control-plane catalog (one file DB both 'instances' talk to)."""
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'cp.db'}")
    async with engine.begin() as c:
        await c.run_sync(lambda s: metadata.create_all(s, tables=[MVT]))
    db = Database(engine, name="cp")
    async with db.acquire() as conn:
        await conn.execute_core(
            insert(MVT).values(
                id=MV_ID,
                source_tables=["orders"],
                target_catalog="postgresql",
                target_schema="mv_cache",
                target_table="mv_orders",
                status="stale",
            )
        )
    yield db
    await engine.dispose()


async def _row(store):
    async with store.acquire() as conn:
        res = await conn.execute_core(select(MVT).where(MVT.c.id == MV_ID))
        return res.fetchone()._mapping


async def _set_lease(store, *, writer, lease_until, status="refreshing"):
    async with store.acquire() as conn:
        await conn.execute_core(
            update(MVT)
            .where(MVT.c.id == MV_ID)
            .values(writer=writer, lease_until=lease_until, status=status)
        )


# -- CAS claim: one wins, the concurrent other loses -------------------------------------------


@pytest.mark.asyncio
async def test_ensure_mv_row_seeds_catalog_so_claim_can_win(store):
    """Regression: the in-memory registry never writes the control-plane catalog row, so a
    shared-tier MV had no row for claim_refresh to elect on — the claim matched 0 rows and the
    MV stayed STALE forever. ensure_mv_row seeds it (idempotently) so the claim can win."""
    from provisa.mv.coordination import ensure_mv_row

    mv = MVDefinition(
        id="mv-unseeded",
        source_tables=["orders"],
        target_catalog="mat_store",
        target_schema="main",
        target_table="mv_unseeded",
        sql="SELECT 1",
    )
    # No catalog row yet → claim cannot win (the bug).
    assert await claim_refresh(store, mv.id, INST_A, target_input_version=None) is False
    # Seed the row → claim now wins; a second ensure is idempotent (no duplicate/raise).
    await ensure_mv_row(store, mv)
    await ensure_mv_row(store, mv)
    assert await claim_refresh(store, mv.id, INST_A, target_input_version=None) is True


@pytest.mark.asyncio
async def test_claim_is_exclusive_across_two_instances(store):
    won_a = await claim_refresh(store, MV_ID, INST_A, target_input_version="v1")
    won_b = await claim_refresh(store, MV_ID, INST_B, target_input_version="v1")
    assert won_a is True
    assert won_b is False
    row = await _row(store)
    assert row["writer"] == INST_A
    assert row["status"] == "refreshing"


@pytest.mark.asyncio
async def test_claim_dedups_on_already_materialized_version(store):
    async with store.acquire() as conn:
        await conn.execute_core(
            update(MVT).where(MVT.c.id == MV_ID).values(materialized_input_version="v9")
        )
    # Same version already in the store → nothing to do → claim denied.
    assert await claim_refresh(store, MV_ID, INST_A, target_input_version="v9") is False
    # A newer version → claim granted.
    assert await claim_refresh(store, MV_ID, INST_A, target_input_version="v10") is True


# -- lease expiry: a crashed refresher's claim times out ---------------------------------------


@pytest.mark.asyncio
async def test_expired_lease_is_reclaimable(store):
    # A claimed then crashed: its lease is in the past.
    stale = datetime.now(UTC) - timedelta(seconds=1)
    await _set_lease(store, writer=INST_A, lease_until=stale)
    # A live lease from B would block; the expired one from A does not.
    assert await claim_refresh(store, MV_ID, INST_B, target_input_version="v2") is True
    row = await _row(store)
    assert row["writer"] == INST_B


@pytest.mark.asyncio
async def test_live_lease_blocks_reclaim(store):
    future = datetime.now(UTC) + timedelta(seconds=60)
    await _set_lease(store, writer=INST_A, lease_until=future)
    assert await claim_refresh(store, MV_ID, INST_B, target_input_version="v2") is False


# -- released claim frees the next refresh -----------------------------------------------------


@pytest.mark.asyncio
async def test_released_claim_allows_next_refresh(store):
    assert await claim_refresh(store, MV_ID, INST_A, target_input_version="v1") is True
    # B cannot claim while A holds it.
    assert await claim_refresh(store, MV_ID, INST_B, target_input_version="v1") is False
    # A releases (refresh failed) — lease cleared, row stale.
    assert await release_refresh(store, MV_ID, INST_A, "boom") is True
    row = await _row(store)
    assert row["writer"] is None
    assert row["status"] == "stale"
    # Now B can claim.
    assert await claim_refresh(store, MV_ID, INST_B, target_input_version="v1") is True


# -- fenced commit rejects a superseded writer -------------------------------------------------


@pytest.mark.asyncio
async def test_fenced_commit_only_for_lease_owner(store):
    assert await claim_refresh(store, MV_ID, INST_A, target_input_version="v1") is True
    # B never owned the lease → its fenced commit is a no-op (0 rows) → discard.
    committed_b = await commit_refresh(
        store,
        MV_ID,
        INST_B,
        row_count=10,
        input_version="v1",
        definition_version="d1",
        snapshot_id="s1",
    )
    assert committed_b is False
    # A owns the live lease → its commit finalizes and clears the lease.
    committed_a = await commit_refresh(
        store,
        MV_ID,
        INST_A,
        row_count=10,
        input_version="v1",
        definition_version="d1",
        snapshot_id="s1",
    )
    assert committed_a is True
    row = await _row(store)
    assert row["status"] == "fresh"
    assert row["writer"] is None
    assert row["materialized_input_version"] == "v1"
    assert row["row_count"] == 10


@pytest.mark.asyncio
async def test_commit_fails_after_lease_expiry(store):
    assert await claim_refresh(store, MV_ID, INST_A, target_input_version="v1") is True
    # Simulate a slow refresher: its own lease expired (reclaimed by someone else).
    await _set_lease(store, writer=INST_A, lease_until=datetime.now(UTC) - timedelta(seconds=1))
    committed = await commit_refresh(
        store,
        MV_ID,
        INST_A,
        row_count=10,
        input_version="v1",
        definition_version="d1",
        snapshot_id="s1",
    )
    assert committed is False


@pytest.mark.asyncio
async def test_renew_lease_extends_only_for_owner(store):
    assert await claim_refresh(store, MV_ID, INST_A, target_input_version="v1") is True
    before = (await _row(store))["lease_until"]
    assert await renew_lease(store, MV_ID, INST_A) is True
    after = (await _row(store))["lease_until"]
    assert after >= before
    # A non-owner cannot renew.
    assert await renew_lease(store, MV_ID, INST_B) is False


# -- the refresh loop consults the shared row, not the per-instance registry -------------------


class _FakeEngine:
    """Records SQL; answers the count/introspection probes refresh_mv issues."""

    def __init__(self, count=5):
        self.count = count
        self.sqls: list[str] = []

    async def execute_engine(self, sql, *a, **k):
        self.sqls.append(sql)
        if "COUNT(*)" in sql:
            return QueryResult(rows=[(self.count,)], column_names=[])
        return QueryResult(rows=[], column_names=[])


def _mv():
    return MVDefinition(
        id=MV_ID,
        source_tables=["orders"],
        target_catalog="postgresql",
        target_schema="mv_cache",
        target_table="mv_orders",
        sql="SELECT * FROM orders",
        consistency="shared",
    )


@pytest.mark.asyncio
async def test_refresh_mv_skips_when_shared_row_already_claimed(store):
    # A concurrent instance holds a live lease on the shared row.
    await _set_lease(store, writer=INST_B, lease_until=datetime.now(UTC) + timedelta(seconds=60))
    reg = MVRegistry()
    mv = _mv()
    reg.register(mv)
    engine = _FakeEngine()

    await refresh_mv(engine, mv, reg, store=store, writer=INST_A)

    # No materialization SQL was issued — the shared claim gated it, not the local registry.
    assert not any("CREATE TABLE" in s or "INSERT INTO" in s for s in engine.sqls)
    assert reg.get(MV_ID).status != MVStatus.FRESH
    # The other instance's ownership is untouched.
    assert (await _row(store))["writer"] == INST_B


@pytest.mark.asyncio
async def test_refresh_mv_claims_commits_and_marks_fresh(store):
    reg = MVRegistry()
    mv = _mv()
    reg.register(mv)
    engine = _FakeEngine(count=7)

    await refresh_mv(engine, mv, reg, store=store, writer=INST_A)

    assert any("INSERT INTO" in s or "CREATE TABLE" in s for s in engine.sqls)
    row = await _row(store)
    assert row["status"] == "fresh"
    assert row["writer"] is None
    assert row["row_count"] == 7
    assert reg.get(MV_ID).status == MVStatus.FRESH
