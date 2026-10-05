# Copyright (c) 2026 Kenneth Stott
# Canary: 21177a51-5b44-4f8d-85a4-147af35d2030
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Cross-instance MV refresh coordination (REQ-879).

Two fleet instances of one region share its state store (a single SQLite ``mv_build_state``
table, REQ-1922: the region's build of the view). The tests drive the real CAS on that row — no
per-instance in-memory state — to prove: exactly one instance claims a given MV, a crashed instance's lease expires and is
reclaimed, a released claim frees the next refresh, a fenced commit rejects a superseded writer,
and ``refresh_mv`` consults the shared row (not the registry) so a second instance skips.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from tests.helpers import hold_registered_tables
from sqlalchemy import insert, select, update
from provisa.core.database import create_engine_from_url

from provisa.core.database import Database
from provisa.core.schema_org import materialized_views as DEFINITIONS
from provisa.core.schema_org import mv_build_state as MVT
from provisa.core.schema_org import metadata
from provisa.executor.result import QueryResult
from provisa.mv.coordination import (
    claim_refresh,
    commit_refresh,
    ensure_mv_row,
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
    """The region's state store, shared by both 'instances' (one file DB), holding the view's
    build row; the model's definition table sits beside it (one DB plays both stores here)."""
    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'cp.db'}")
    with engine.begin() as c:
        metadata.create_all(c, tables=[DEFINITIONS, MVT])
    db = Database(engine, name="cp")
    async with db.acquire() as conn:
        await conn.execute_core(insert(MVT).values(mv_id=MV_ID, region="default", status="stale"))
    yield db
    engine.dispose()


async def _row(store):
    async with store.acquire() as conn:
        res = await conn.execute_core(select(MVT).where(MVT.c.mv_id == MV_ID))
        return res.fetchone()._mapping


async def _set_lease(store, *, writer, lease_until, status="refreshing"):
    async with store.acquire() as conn:
        await conn.execute_core(
            update(MVT)
            .where(MVT.c.mv_id == MV_ID)
            .values(writer=writer, lease_until=lease_until, status=status)
        )


# -- CAS claim: one wins, the concurrent other loses -------------------------------------------


@pytest.mark.asyncio
async def test_ensure_mv_row_seeds_catalog_so_claim_can_win(store):
    """Regression: the in-memory registry never writes the control-plane catalog row, so a
    shared-tier MV had no row for claim_refresh to elect on — the claim matched 0 rows and the
    MV stayed STALE forever. ensure_mv_row seeds it (idempotently) so the claim can win."""
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
    await ensure_mv_row(store, store, mv)
    await ensure_mv_row(store, store, mv)
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
            update(MVT).where(MVT.c.mv_id == MV_ID).values(materialized_input_version="v9")
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

    # The engine seam names its SQL dialect; refresh resolves a view's settings in it.
    dialect = "duckdb"

    def address_replicas(self, sql):
        return sql  # this stand-in's tables are all read where the statement names them

    async def execute_engine(self, sql, *a, **k):
        self.sqls.append(sql)
        if "COUNT(*)" in sql:
            return QueryResult(rows=[(self.count,)], column_names=[])
        return QueryResult(rows=[], column_names=[])


@pytest.fixture(autouse=True)
def _model_holds_orders(monkeypatch):
    """The registered table these views read (a view's inputs resolve against the model)."""
    hold_registered_tables(monkeypatch, "orders")


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

    await refresh_mv(engine, mv, reg, store=store, writer=INST_A, ledger=store)

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

    await refresh_mv(engine, mv, reg, store=store, writer=INST_A, ledger=store)

    assert any("INSERT INTO" in s or "CREATE TABLE" in s for s in engine.sqls)
    row = await _row(store)
    assert row["status"] == "fresh"
    assert row["writer"] is None
    assert row["row_count"] == 7
    assert reg.get(MV_ID).status == MVStatus.FRESH


def _plain_db(path, *tables):
    engine = create_engine_from_url(f"sqlite+pysqlite:///{path}")
    with engine.begin() as c:
        metadata.create_all(c, tables=list(tables))
    return Database(engine, name=path.stem)


@pytest.mark.asyncio
async def test_each_region_builds_its_own_copy_of_a_view_against_its_own_state(
    tmp_path, monkeypatch
):
    """REQ-1922: one view definition in the model; each region's fleet claims, builds and records
    its own copy in its own state store, naming the region — eu building it neither blocks nor
    reports for us."""
    from provisa.core import process_region

    from provisa.mv import governed_build

    model = _plain_db(tmp_path / "model.db", DEFINITIONS)
    eu, us = (_plain_db(tmp_path / f"{r}.db", MVT) for r in ("eu", "us"))
    mv = _mv()

    # What the build reads is test_mv_governed_build's; here, who claims and records it.
    async def _select(view, _engine):
        return view.sql

    monkeypatch.setattr(governed_build, "view_build_sql", _select)

    async def _build(region, state, writer):
        monkeypatch.setattr(process_region, "_region", region)
        reg = MVRegistry()
        reg.register(mv)
        await refresh_mv(_FakeEngine(count=3), mv, reg, store=model, writer=writer, ledger=state)

    # eu holds a live claim on its copy: us still claims and builds its own.
    monkeypatch.setattr(process_region, "_region", "eu")
    await ensure_mv_row(model, eu, mv)
    assert await claim_refresh(eu, MV_ID, INST_A, target_input_version=None) is True
    await _build("us", us, INST_B)
    built_us, held_eu = await _row(us), await _row(eu)
    assert (built_us["region"], built_us["status"], built_us["row_count"]) == ("us", "fresh", 3)
    assert (held_eu["region"], held_eu["status"], held_eu["writer"]) == ("eu", "refreshing", INST_A)
    async with model.acquire() as conn:
        definitions = (await conn.execute_core(select(DEFINITIONS.c.id))).fetchall()
    assert [d.id for d in definitions] == [MV_ID]
