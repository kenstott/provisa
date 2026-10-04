# Copyright (c) 2026 Kenneth Stott
# Canary: 5ab38546-93b2-4622-90bd-f250b2b63545
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Hot promotion's evaluation (REQ-826): a table under a threshold is promoted when it is busy,
its build is requested with the promotion, and it is demoted when its traffic falls away.

The control plane's replica state is real (SQLite); the registry read and the engine are stand
ins. What the evaluation writes is read back through the state store, and the replica-state stamp
is asserted, because that stamp is how every other process learns of the change."""

# Requirements: REQ-826, REQ-238, REQ-239, REQ-241, REQ-1916, REQ-1920

from __future__ import annotations

import asyncio
import uuid
from types import SimpleNamespace

import pytest

from provisa.core import config_stamp, settings_registry
from provisa.core.database import Database, create_engine_from_url
from provisa.core.models import Source, SourceType
from provisa.core.schema_org import config_stamp as stamps
from provisa.core.schema_org import metadata, replica_state as replica_state_table
from provisa.federation import replica_hot, replica_state
from provisa.federation.engine import build_engine
from provisa.federation.replica_hot import (
    HOT_TIER,
    TOO_LARGE,
    HotCounts,
    count_scope,
    evaluate,
    evaluation_loop,
)

pytestmark = pytest.mark.unit

_INTERVAL = 60
_SETTINGS = {
    "replication.hot_interval": _INTERVAL,
    "replication.hot_threshold": 100,
    "replication.hot_max_rows": 10_000_000,
}
ORDERS = ("pg", "public", "orders")
ITEMS = ("pg", "public", "items")


def _pg(**settings) -> Source:
    base = dict(host="h", port=5432, database="d", username="u", cache_ttl=60)
    base.update(settings)
    return Source(id="pg", type=SourceType.postgresql, **base)


def _reg(table_id: int, name: str, **settings) -> SimpleNamespace:
    row = {
        "id": table_id,
        "source_id": "pg",
        "schema_name": "public",
        "table_name": name,
        "replicate": None,
        "load_protected": None,
        "change_signal": None,
        "cache_ttl": None,
        "columns": [SimpleNamespace(name="id", native_filter_type=None)],
        "row_materialize": False,
        "region": None,
    }
    row.update(settings)
    return SimpleNamespace(**row)


class _Engine:
    """The engine runtime's two calls the size check makes. ``rows`` per table; a table mapped
    to an exception fails its count."""

    def __init__(self, rows: dict[str, object]) -> None:
        self.engine = build_engine("trino")
        self.rows = rows
        self.sent: list[str] = []

    async def read_ref(self, table) -> str:
        """The registered table (its identity) at its address on this engine."""
        return f'"{table.source_id}"."{table.schema_name}"."{table.table_name}"'

    async def execute_engine(self, sql: str, *_a, **_k):
        self.sent.append(sql)
        answer = self.rows[sql.rsplit('"', 2)[1]]
        if isinstance(answer, BaseException):
            raise answer
        return SimpleNamespace(rows=[(answer,)])


@pytest.fixture
async def world(tmp_path, monkeypatch):
    """One org environment: a real replica-state control plane, a registry of two tables on a
    source the engine reads in place, an embedded count store with its own clock."""
    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'cp.db'}")
    with engine.begin() as raw:
        metadata.create_all(raw, tables=[replica_state_table, stamps])
        config_stamp.install(raw, {}, advanced=config_stamp.TENANT_ADVANCED)
    db = Database(engine, "test")
    registry = SimpleNamespace(tables=[_reg(1, "orders"), _reg(2, "items")], source=_pg())

    async def _tables(_state, _conn=None):
        return registry.tables

    async def _sources(_state, _conn=None):
        return [registry.source]

    monkeypatch.setattr("provisa.federation.registry_view.registered_tables", _tables)
    monkeypatch.setattr("provisa.federation.registry_view.registered_sources", _sources)
    monkeypatch.setattr(settings_registry, "value", lambda key: _SETTINGS[key])
    monkeypatch.setattr(replica_hot, "_size_checks", replica_hot._SizeChecks())
    # the store this engine's replicas are built in (the stand-in engine has no address)
    monkeypatch.setattr(
        "provisa.federation.replica_builds.store_identity", lambda _state: "store-a"
    )
    org = f"org-{uuid.uuid4().hex}"
    state = SimpleNamespace(
        model_db=db,
        tenant_db=db,
        org_id=org,
        hot_counts=HotCounts(None, clock=lambda: 9_000_000.0),
        hot_manager=None,
        federation_engine=_Engine({"orders": 5000, "items": 5000}),
    )
    scope = count_scope(org, "prod")

    def seen(table_id: int, statements: int) -> None:
        """The statements that read ``table_id`` in the current interval, as the audit writer
        counts them."""
        state.hot_counts.add({(scope, table_id): statements}, _INTERVAL)

    async def stored() -> dict:
        async with db.acquire() as conn:
            rows = await conn.fetch("SELECT stamp FROM config_stamp WHERE kind = 'replica'")
            return {
                "promoted": await replica_state.promoted_keys(conn),
                "orders": await replica_state.read(conn, ORDERS),
                "items": await replica_state.read(conn, ITEMS),
                "stamp": int(rows[0]["stamp"]),
            }

    yield SimpleNamespace(
        state=state, registry=registry, seen=seen, stored=stored, db=db, scope=scope
    )
    engine.dispose()


async def _promote(world, key=ORDERS, *, built: bool = False) -> None:
    async with world.db.acquire() as conn:
        await replica_state.set_promoted(conn, key, True)
        if built:
            from datetime import UTC, datetime

            await replica_state.record_completed(
                conn,
                key,
                rows_copied=1,
                method="stream_batches",
                content_hash=None,
                store="store-a",
                next_refresh_at=None,
                now=datetime.now(UTC),
            )


# -- promotion -------------------------------------------------------------------------------------


async def test_a_table_at_its_threshold_is_promoted_and_its_build_requested(world):
    world.seen(1, 100)
    before = (await world.stored())["stamp"]
    outcome = await evaluate(world.state, workers=1)
    assert outcome.promoted == (ORDERS,) and outcome.demoted == ()
    after = await world.stored()
    assert after["promoted"] == frozenset({ORDERS})
    # the build is asked of the replicator, with the reason — nothing is copied here
    assert (after["orders"].build_state, after["orders"].requested_reason) == ("requested", "hot")
    assert after["stamp"] == before + 1  # every process is told
    # the table was sized where the engine reads it, once
    assert world.state.federation_engine.sent == ['SELECT COUNT(*) FROM "pg"."public"."orders"']


async def test_a_table_below_its_threshold_is_left_live_and_is_not_even_sized(world):
    world.seen(1, 99)
    outcome = await evaluate(world.state, workers=1)
    assert outcome.promoted == ()
    assert (await world.stored())["promoted"] == frozenset()
    assert world.state.federation_engine.sent == []


async def test_a_hot_n_table_is_judged_against_its_own_n_not_the_default(world):
    world.registry.tables = [_reg(1, "orders", replicate=500), _reg(2, "items", replicate=50)]
    world.seen(1, 100)  # past the default, below its own 500
    world.seen(2, 50)
    outcome = await evaluate(world.state, workers=1)
    assert outcome.promoted == (ITEMS,)


async def test_an_already_promoted_table_is_not_promoted_or_requested_again(world):
    await _promote(world, built=True)
    world.seen(1, 500)
    before = await world.stored()
    outcome = await evaluate(world.state, workers=1)
    assert outcome.promoted == () and outcome.demoted == ()
    after = await world.stored()
    assert after["stamp"] == before["stamp"]
    assert after["orders"].build_state == "idle"
    assert world.state.federation_engine.sent == []


async def test_a_table_over_the_size_ceiling_is_not_promoted_and_the_reason_is_kept(world):
    world.state.federation_engine.rows["orders"] = 10_000_001
    world.seen(1, 1000)
    outcome = await evaluate(world.state, workers=1)
    assert outcome.promoted == () and outcome.skipped == {ORDERS: TOO_LARGE}
    assert (await world.stored())["promoted"] == frozenset()
    # ...for the admin summary to state
    assert world.state.hot_counts.too_large(world.scope, ORDERS) is True
    assert world.state.hot_counts.too_large(world.scope, ITEMS) is False


async def test_a_table_at_exactly_the_size_ceiling_is_promoted(world):
    world.state.federation_engine.rows["orders"] = 10_000_000
    world.seen(1, 100)
    assert (await evaluate(world.state, workers=1)).promoted == (ORDERS,)


async def test_a_table_promoted_again_while_its_replica_stands_serves_without_a_rebuild(world):
    """Demoted, and the replicator has not dropped its replica yet: busy again, it is promoted
    and served from the standing replica at once — no build is requested."""
    await _promote(world, built=True)
    async with world.db.acquire() as conn:
        await replica_state.set_promoted(conn, ORDERS, False)
    world.seen(1, 100)
    assert (await evaluate(world.state, workers=1)).promoted == (ORDERS,)
    after = await world.stored()
    assert after["orders"].build_state == "idle"  # nothing requested
    async with world.db.acquire() as conn:
        assert await replica_state.serving_keys(conn, "store-a") == frozenset({ORDERS})


async def test_a_replica_standing_in_another_store_does_not_count_and_a_build_is_requested(
    world, monkeypatch
):
    await _promote(world, built=True)
    async with world.db.acquire() as conn:
        await replica_state.set_promoted(conn, ORDERS, False)
    world.seen(1, 100)
    # the deployment now runs on another engine or store
    monkeypatch.setattr(
        "provisa.federation.replica_builds.store_identity", lambda _state: "store-b"
    )
    assert (await evaluate(world.state, workers=1)).promoted == (ORDERS,)
    after = await world.stored()
    assert (after["orders"].build_state, after["orders"].requested_reason) == ("requested", "hot")


# -- demotion --------------------------------------------------------------------------------------


async def test_a_promoted_table_below_half_its_threshold_is_demoted(world):
    await _promote(world, built=True)
    world.seen(1, 49)
    before = (await world.stored())["stamp"]
    outcome = await evaluate(world.state, workers=1)
    assert outcome.demoted == (ORDERS,)
    after = await world.stored()
    assert after["promoted"] == frozenset()
    assert after["stamp"] == before + 1
    # only the flag: the replica is left for the replicator to retire
    assert after["orders"].exists_in("store-a")


async def test_a_promoted_table_at_half_its_threshold_stays_promoted(world):
    await _promote(world, built=True)
    world.seen(1, 50)
    outcome = await evaluate(world.state, workers=1)
    assert outcome.demoted == ()
    assert (await world.stored())["promoted"] == frozenset({ORDERS})


async def test_a_promoted_table_whose_setting_became_never_is_demoted(world):
    """It is no longer under a threshold at all, however busy."""
    await _promote(world, built=True)
    world.registry.tables = [_reg(1, "orders", replicate=-1), _reg(2, "items")]
    world.seen(1, 1000)
    outcome = await evaluate(world.state, workers=1)
    assert outcome.demoted == (ORDERS,)
    assert (await world.stored())["promoted"] == frozenset()


async def test_a_promoted_table_that_is_no_longer_registered_is_demoted(world):
    await _promote(world, built=True)
    world.registry.tables = [_reg(2, "items")]
    assert (await evaluate(world.state, workers=1)).demoted == (ORDERS,)


# -- one tier (REQ-241) ----------------------------------------------------------------------------


async def test_a_table_the_redis_hot_tier_manages_is_not_promoted(world):
    world.state.hot_manager = SimpleNamespace(managed_tables=lambda: {1})  # orders
    world.seen(1, 1000)
    world.seen(2, 1000)
    outcome = await evaluate(world.state, workers=1)
    assert outcome.promoted == (ITEMS,)
    assert outcome.skipped == {ORDERS: HOT_TIER}


async def test_a_promoted_table_the_hot_tier_takes_is_demoted(world):
    await _promote(world, built=True)
    world.state.hot_manager = SimpleNamespace(managed_tables=lambda: {1})  # orders
    world.seen(1, 1000)
    assert (await evaluate(world.state, workers=1)).demoted == (ORDERS,)


# -- whether promotion is decided here at all ------------------------------------------------------


async def test_with_the_embedded_redis_and_several_workers_nothing_is_promoted(world):
    world.seen(1, 1000)
    outcome = await evaluate(world.state, workers=4)
    assert outcome.ran is False and outcome.promoted == ()
    assert (await world.stored())["promoted"] == frozenset()
    assert world.state.federation_engine.sent == []


async def test_with_the_embedded_redis_and_one_worker_promotion_runs(world):
    world.seen(1, 1000)
    outcome = await evaluate(world.state, workers=1)
    assert outcome.ran is True and outcome.promoted == (ORDERS,)


async def test_with_a_shared_redis_promotion_runs_with_several_workers(world, monkeypatch):
    """The store is shared: every worker's statements are in the one count."""
    monkeypatch.setattr(HotCounts, "shared", property(lambda self: True))
    world.seen(1, 1000)
    outcome = await evaluate(world.state, workers=4)
    assert outcome.ran is True and outcome.promoted == (ORDERS,)


# -- a size check that fails -----------------------------------------------------------------------


def _errors(caplog) -> list[str]:
    return [r.getMessage() for r in caplog.records if r.levelname == "ERROR"]


async def test_a_failing_size_check_leaves_the_table_live_and_is_logged_once(world, caplog):
    world.state.federation_engine.rows["orders"] = ConnectionError("source unreachable")
    world.seen(1, 1000)
    with caplog.at_level("INFO", logger="provisa.federation.replica_hot"):
        for _ in range(3):
            assert (await evaluate(world.state, workers=1)).promoted == ()
    said = [m for m in _errors(caplog) if "could not size the table" in m]
    assert len(said) == 1 and "pg.public.orders" in said[0] and "source unreachable" in said[0]
    assert (await world.stored())["promoted"] == frozenset()


async def test_a_changed_failure_is_logged_again_and_recovery_is_logged_once(world, caplog):
    engine = world.state.federation_engine
    world.seen(1, 1000)
    with caplog.at_level("INFO", logger="provisa.federation.replica_hot"):
        engine.rows["orders"] = ConnectionError("source unreachable")
        await evaluate(world.state, workers=1)
        engine.rows["orders"] = TimeoutError("count timed out")
        await evaluate(world.state, workers=1)
        engine.rows["orders"] = 5000
        assert (await evaluate(world.state, workers=1)).promoted == (ORDERS,)
    assert len([m for m in _errors(caplog) if "could not size the table" in m]) == 2
    assert any("can size pg.public.orders again" in r.getMessage() for r in caplog.records)


async def test_one_tables_failing_size_check_does_not_stop_the_others(world):
    world.state.federation_engine.rows["orders"] = ConnectionError("source unreachable")
    world.seen(1, 1000)
    world.seen(2, 1000)
    assert (await evaluate(world.state, workers=1)).promoted == (ITEMS,)


# -- the loop: one process of the deployment decides (REQ-1916) ------------------------------------


async def _loop_for(seconds: float, world, *, should_run, monkeypatch, evaluate_fn=None) -> list:
    """Run the evaluation loop over the one org environment for ``seconds``; the calls made."""
    calls: list[str] = []

    async def _recorded(state, *, workers):
        calls.append("evaluated")
        if evaluate_fn is not None:
            await evaluate_fn()
        return replica_hot.Evaluation()

    monkeypatch.setattr(replica_hot, "evaluate", _recorded)
    monkeypatch.setattr(
        settings_registry,
        "value",
        lambda key: 0.01 if key == "replication.hot_interval" else _SETTINGS[key],
    )
    world.state.org_registry = SimpleNamespace(
        all_org_ids=lambda: ["default"],
        get=lambda key: SimpleNamespace(
            org_id="default", env="prod", model_db=world.db, tenant_db=world.db
        ),
    )
    task = asyncio.ensure_future(evaluation_loop(world.state, should_run=should_run, workers=1))
    await asyncio.sleep(seconds)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    return calls


async def test_a_worker_that_does_not_hold_the_scheduler_lock_evaluates_nothing(world, monkeypatch):
    assert await _loop_for(0.1, world, should_run=lambda: False, monkeypatch=monkeypatch) == []


async def test_the_holder_evaluates_every_interval(world, monkeypatch):
    calls = await _loop_for(0.15, world, should_run=lambda: True, monkeypatch=monkeypatch)
    assert len(calls) >= 2


async def test_holding_is_asked_again_at_every_pass(world, monkeypatch):
    """The lock can move between workers; a worker that gains it starts evaluating."""
    answers = iter([False, False, True])
    calls = await _loop_for(
        0.2, world, should_run=lambda: next(answers, True), monkeypatch=monkeypatch
    )
    assert calls, "the worker never evaluated after gaining the lock"


async def test_an_evaluation_that_fails_is_logged_and_the_loop_goes_on(world, monkeypatch, caplog):
    attempts = {"n": 0}

    async def _fails_once():
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise RuntimeError("control plane unreachable")

    with caplog.at_level("ERROR", logger="provisa.federation.replica_hot"):
        calls = await _loop_for(
            0.15, world, should_run=lambda: True, monkeypatch=monkeypatch, evaluate_fn=_fails_once
        )
    assert any("Hot promotion evaluation failed for default" in m for m in _errors(caplog))
    assert len(calls) >= 2, "the loop stopped after one failed evaluation"


# -- regions (REQ-1922) ----------------------------------------------------------------------------


@pytest.fixture
def _region():
    from provisa.core import process_region

    was = process_region._region
    yield process_region
    process_region._region = was


_PLATFORM = {
    "regions": [
        {"id": "eu", "address": "https://eu.example.com"},
        {"id": "us", "address": "https://us.example.com"},
    ]
}


async def test_a_table_naming_another_region_is_not_promoted_here(world, _region):
    """Only the region a table names promotes, builds and counts it; the org's other regions read
    it from there."""
    _region.bind_launch(_PLATFORM, requested="us")
    world.registry.tables[0].region = "eu"
    world.seen(1, 100)
    outcome = await evaluate(world.state, workers=1)
    assert outcome.promoted == ()
    stored = await world.stored()
    assert stored["promoted"] == frozenset() and stored["orders"] is None  # nothing requested


async def test_the_region_a_table_names_promotes_it(world, _region):
    _region.bind_launch(_PLATFORM, requested="eu")
    world.registry.tables[0].region = "eu"
    world.seen(1, 100)
    outcome = await evaluate(world.state, workers=1)
    assert outcome.promoted == (ORDERS,)
