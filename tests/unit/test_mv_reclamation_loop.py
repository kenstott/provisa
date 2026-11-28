# Copyright (c) 2026 Kenneth Stott
# Canary: ec634bdc-8223-4242-94e7-0ce5b7e1a4c9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

# REQ-984: the legacy periodic CTAS refresh_loop is retired; reclamation_loop is the sole
# storage-GC path (drop removed MVs + reap orphans) and never recomputes MVs.
import inspect

from types import SimpleNamespace

import pytest

import provisa.mv.refresh as refresh
from provisa.mv.models import MVDefinition
from provisa.mv.registry import MVRegistry


class _RecordingEngine:
    # The engine it stands for names a catalog in its SQL (REQ-1730).
    engine = SimpleNamespace(catalog_qualified=True)

    def __init__(self, tables):
        self._tables = list(tables)
        self.executed = []

    def engine_physical(self, sql):
        return self.address_replicas(sql)

    def address_replicas(self, sql):
        return sql  # this stand-in's tables are all read where the statement names them

    async def execute_engine(self, sql, *, authorization=None):
        del authorization  # REQ-1760: execute_engine's caller-accountability param; this fake
        # only records the SQL, verify_execution_authorization has its own unit tests.
        self.executed.append(sql)
        if sql.startswith("SHOW TABLES"):
            return type("R", (), {"rows": [(t,) for t in self._tables]})()
        return type("R", (), {"rows": []})()


def test_refresh_loop_is_retired_reclamation_loop_is_the_path():
    # The periodic CTAS refresh loop no longer exists; reclamation_loop replaces it.
    assert not hasattr(refresh, "refresh_loop")
    assert inspect.iscoroutinefunction(refresh.reclamation_loop)


@pytest.mark.asyncio
async def test_reclaim_removed_mv_drops_table_and_unregisters_without_recompute():
    reg = MVRegistry()
    kept = MVDefinition(id="keep", source_tables=["t"], target_catalog="c", target_schema="s")
    gone = MVDefinition(id="gone", source_tables=["t"], target_catalog="c", target_schema="s")
    reg.register(kept)
    reg.register(gone)
    engine = _RecordingEngine(tables=[])

    reclaimed = await refresh.reclaim_removed_mvs(engine, reg, config_mv_ids={"keep"})

    assert reclaimed == ["gone"]
    assert reg.get("gone") is None
    assert reg.get("keep") is kept
    # Only a DROP is issued — no CREATE TABLE AS / INSERT recompute.
    assert engine.executed == ['DROP TABLE IF EXISTS "c"."s"."mv_gone"']
    assert not any("CREATE TABLE" in s or "INSERT" in s for s in engine.executed)


@pytest.mark.asyncio
async def test_detect_orphans_returns_untracked_tables():
    reg = MVRegistry()
    reg.register(MVDefinition(id="a", source_tables=["t"], target_catalog="c", target_schema="s"))
    engine = _RecordingEngine(tables=["mv_a", "mv_stray"])

    orphans = await refresh.detect_orphans(engine, reg, schema_name="s", catalog="c")

    assert orphans == ["mv_stray"]


@pytest.mark.asyncio
async def test_a_copy_of_a_view_another_region_keeps_is_an_orphan_here():
    """REQ-1921: a view naming eu is kept only in eu — on a us node its table (left when its
    region changed) is reaped like a removed view's."""
    from provisa.core import process_region

    platform = {
        "regions": [
            {"id": "eu", "description": "The eu region", "purposes": ["residency"]},
            {"id": "us", "description": "The us region", "purposes": ["residency"]},
        ]
    }
    reg = MVRegistry()
    for mv_id, region in (("here", None), ("ours", "us"), ("theirs", "eu")):
        reg.register(
            MVDefinition(
                id=mv_id, source_tables=["t"], target_catalog="c", target_schema="s", region=region
            )
        )
    engine = _RecordingEngine(tables=["mv_here", "mv_ours", "mv_theirs"])
    was = process_region._region
    try:
        process_region.bind_launch(platform, requested="us")
        orphans = await refresh.detect_orphans(engine, reg, schema_name="s", catalog="c")
    finally:
        process_region._region = was
    assert orphans == ["mv_theirs"]


class _BrokerBackedEngine:
    """An engine whose views live in an embedded DuckDB-file store, reached through its broker."""

    # The engine it stands for names a catalog in its SQL (REQ-1730).
    engine = SimpleNamespace(catalog_qualified=True)

    dialect = "duckdb"

    def __init__(self, db_path: str) -> None:
        from provisa.federation.materialize_broker import get_broker

        self._broker = get_broker(db_path)

    def mv_store_broker(self):
        return self._broker

    async def execute_engine(self, sql, *, authorization=None):
        raise AssertionError(f"the store is the broker's to read, not the engine's: {sql}")


async def test_a_view_schema_the_embedded_store_does_not_hold_yet_has_no_orphans(tmp_path):
    """An embedded store creates a view schema with the first view built into it. A sweep before
    that listed a schema that was not there and logged "Error in MV reclamation loop" (a catalog
    error from SHOW TABLES) on every pass; a schema the store does not hold has no tables."""
    engine = _BrokerBackedEngine(str(tmp_path / "materialize.duckdb"))
    reg = MVRegistry()
    reg.register(
        MVDefinition(
            id="orders",
            source_tables=["t"],
            target_catalog="mat_store",
            target_schema="org_a_mv_cache",
        )
    )
    assert await refresh.detect_orphans(engine, reg, "org_a_mv_cache", "mat_store") == []


async def test_orphans_in_an_embedded_store_are_found_through_its_broker(tmp_path):
    engine = _BrokerBackedEngine(str(tmp_path / "materialize.duckdb"))
    broker = engine.mv_store_broker()
    broker.execute('CREATE SCHEMA mat_store."org_a_mv_cache"')
    for table in ("mv_orders", "mv_removed"):
        broker.execute(f'CREATE TABLE mat_store."org_a_mv_cache"."{table}" (id INTEGER)')
    reg = MVRegistry()
    reg.register(
        MVDefinition(
            id="orders",
            source_tables=["t"],
            target_catalog="mat_store",
            target_schema="org_a_mv_cache",
        )
    )
    assert reg.get("orders").target_table == "mv_orders"
    assert await refresh.detect_orphans(engine, reg, "org_a_mv_cache", "mat_store") == [
        "mv_removed"
    ]


# --- the sweep never dials a shard that is not serving (REQ-1448) -------------------------------
#
# On cloud-dev (2026-11-28) the idle reaper scaled the shared shard to zero after its idle window,
# and from then on every 30s sweep dialed the released pod IP through the shared terminal
# (EngineUnreachable: dial tcp 10.20.0.7:8480: i/o timeout) -- the blind dial
# engine_wake.attach_if_serving exists to stop. The sweep asks it first, as the scheduled engine
# jobs do, and leaves the work for a sweep with a live shard.


def _one_sweep(engine_serving) -> list[str]:
    import asyncio
    from unittest.mock import patch

    ran: list[str] = []

    class _Registry:
        def all(self):
            return [SimpleNamespace(target_catalog="c", target_schema="s", orphan_grace_period=1)]

    async def _detect(*_a, **_k):
        ran.append("detect_orphans")
        return []

    async def _drop(*_a, **_k):
        ran.append("drop_expired_orphans")

    async def _stop(_seconds):
        raise asyncio.CancelledError

    with (
        patch.object(refresh, "detect_orphans", _detect),
        patch.object(refresh, "drop_expired_orphans", _drop),
        patch.object(refresh.asyncio, "sleep", _stop),
        pytest.raises(asyncio.CancelledError),
    ):
        asyncio.run(
            refresh.reclamation_loop(
                lambda: [(object(), _Registry())], engine_serving=engine_serving
            )
        )
    return ran


def test_the_sweep_waits_for_a_serving_engine():
    async def _down() -> bool:
        return False

    async def _up() -> bool:
        return True

    assert _one_sweep(_down) == []
    assert _one_sweep(_up) == ["detect_orphans", "drop_expired_orphans"]


def test_the_server_gates_the_sweep_on_attach_if_serving():
    from provisa.api import app_startup

    src = inspect.getsource(app_startup._start_background_tasks)
    loop = src[src.index("reclamation_loop(") : src.index('name="mv-reclamation"')]
    assert "engine_serving=" in loop
    assert "attach_if_serving" in src[: src.index('name="mv-reclamation"')]
