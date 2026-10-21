# Copyright (c) 2026 Kenneth Stott
# Canary: 670fb69a-e84d-4308-b8d7-7c96f9f6d473
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2015: a replica carries its key and the columns its declared relationships join on,
built after it lands. The replica is read from its landing; its indexes follow, each recorded by
name; a failed one is kept with its cause and never fails the landing; a landing that replaces
the table while the last one's indexes are being built leaves them behind; a changed
relationship is indexed by the next landing."""

# Requirements: REQ-2015

from __future__ import annotations

import uuid

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import insert, select

from provisa.core.schema_org import (
    domains,
    registered_tables,
    relationships,
    replica_state,
    sources,
)
from provisa.federation import replica_indexes as ri
from provisa.federation import replica_state as replica_state_reads
from provisa.federation import replica_state as build_state
from provisa.federation.replica_locks import BuildLocks
from provisa.federation.replica_runner import BuildOutcome, ReplicaRunner
from provisa.federation.replica_state import RetryPolicy

pytestmark = pytest.mark.asyncio

ORG = "org1"

# PostgreSQL advisory locks are held server-wide, not per database: every xdist worker's control
# plane shares one server, so each worker process names an engine of its own, or a slot another
# worker's test holds reads here as taken.
ENGINE = f"trino@engine-{uuid.uuid4().hex[:8]}:8080"

ORDERS = ("shop", "public", "orders")


# -- the plan --------------------------------------------------------------------------------------


def test_the_key_comes_first_and_each_join_column_set_once():
    specs = ri.plan(
        "replicas.shop__orders",
        ["id", "customer_id", "product_id", "note"],
        ["id"],
        [("customer_id",), ("product_id",), ("customer_id",), ("id",)],
    )
    assert [(s.columns, s.primary) for s in specs] == [
        (("id",), True),
        (("customer_id",), False),
        (("product_id",), False),
    ]
    # Fixed by the replica and the columns: a rebuild names the same indexes.
    assert specs == ri.plan(
        "replicas.shop__orders",
        ["id", "customer_id", "product_id", "note"],
        ["id"],
        [("customer_id",), ("product_id",)],
    )
    assert specs[0].name.startswith("pk__") and specs[1].name.startswith("idx__")
    assert len({s.name for s in specs}) == 3


def test_a_join_on_a_column_the_replica_does_not_hold_has_nothing_to_index():
    assert ri.plan("r.t", ["id"], [], [("computed",)]) == []


def test_a_table_with_no_key_and_no_relationship_asks_for_nothing():
    assert ri.plan("r.t", ["a", "b"], [], []) == []


# -- the columns the model's relationships join on -------------------------------------------------


async def _model(db, *, junction: bool = False) -> dict[str, int]:
    """Orders, customers and products of source ``shop``; orders.customer_id -> customers.id,
    and (``junction``) products <-> customers through orders."""
    ids: dict[str, int] = {}
    async with db.acquire() as conn:
        await conn.execute_core(insert(domains).values(id="sales", description=""))
        await conn.execute_core(insert(sources).values(id="shop", type="postgresql"))
        for n, name in enumerate(("orders", "customers", "products"), start=1):
            await conn.execute_core(
                insert(registered_tables).values(
                    id=n, source_id="shop", domain_id="sales", schema_name="public", table_name=name
                )
            )
            ids[name] = n
        await conn.execute_core(
            insert(relationships).values(
                id="orders_customer",
                source_table_id=ids["orders"],
                target_table_id=ids["customers"],
                source_column="customer_id",
                target_column="id",
                cardinality="many-to-one",
            )
        )
        if junction:
            await conn.execute_core(
                insert(relationships).values(
                    id="bought",
                    source_table_id=ids["customers"],
                    target_table_id=ids["products"],
                    source_column="id",
                    target_column="id",
                    cardinality="one-to-many",
                    via_table_id=ids["orders"],
                    via_source_column="customer_id",
                    via_target_column="product_id",
                    via_label_source="table",
                )
            )
    return ids


async def test_the_foreign_key_of_a_relationship_from_the_table_is_a_join_column(control_plane):
    db = control_plane.org()
    await _model(db)
    async with db.acquire() as conn:
        assert await ri.join_columns(conn, ORDERS, ["id"]) == [("customer_id",)]
        # The target side joins on customers.id, its key: nothing more to index there.
        assert await ri.join_columns(conn, ("shop", "public", "customers"), ["id"]) == []
        # Without a declared key, the target column is indexed for the join.
        assert await ri.join_columns(conn, ("shop", "public", "customers"), []) == [("id",)]


async def test_a_junction_table_is_indexed_on_both_its_foreign_keys(control_plane):
    db = control_plane.org()
    await _model(db, junction=True)
    async with db.acquire() as conn:
        joins = await ri.join_columns(conn, ORDERS, ["id"])
        customers = await ri.join_columns(conn, ("shop", "public", "customers"), ["id"])
    assert set(joins) == {("customer_id",), ("product_id",)}
    # The junction's ends join through it, on their own key: no foreign key of theirs here.
    assert customers == []


# -- the record and the builds ---------------------------------------------------------------------


async def _landed(db, key, at: datetime) -> None:
    async with db.acquire() as conn:
        await build_state.request_build(conn, key, build_state.REASON_MODEL)
        await build_state.record_completed(
            conn,
            key,
            rows_copied=1,
            method="stream_batches",
            content_hash=None,
            store="store-a",
            next_refresh_at=None,
            now=at,
        )


def _specs() -> list[ri.IndexSpec]:
    return ri.plan("replicas.shop__orders", ["id", "customer_id"], ["id"], [("customer_id",)])


async def _index_state(db, key) -> dict | None:
    async with db.acquire() as conn:
        return (
            await conn.execute_core(
                select(replica_state.c.index_state).where(
                    replica_state.c.source_id == key[0],
                    replica_state.c.schema_name == key[1],
                    replica_state.c.table_name == key[2],
                )
            )
        ).scalar_one()


class _Indexer:
    """A store that builds what it is asked, failing the names it is told to."""

    def __init__(self, failing: dict[str, Exception] | None = None) -> None:
        self.built: list[str] = []
        self.failing = failing or {}
        self.before_each: object = None

    async def build(self, spec: ri.IndexSpec) -> None:
        if callable(self.before_each):
            await self.before_each(spec)
        if spec.name in self.failing:
            raise self.failing[spec.name]
        self.built.append(spec.name)


async def test_each_index_is_recorded_built_by_name(control_plane):
    db = control_plane.org()
    landed = datetime.now(UTC)
    await _landed(db, ORDERS, landed)
    async with db.acquire() as conn:
        await ri.record_plan(conn, ORDERS, landed, _specs())
        (item,) = await ri.due(conn)
    indexer = _Indexer()
    await ri.build_due(db, item, indexer)
    state = await _index_state(db, ORDERS)
    assert state["built"] == [s.name for s in _specs()] == indexer.built
    assert state["failed"] == {}
    async with db.acquire() as conn:
        assert await ri.due(conn) == []


async def test_a_failed_index_is_kept_with_its_cause_and_the_rest_are_built(control_plane):
    db = control_plane.org()
    landed = datetime.now(UTC)
    await _landed(db, ORDERS, landed)
    pk, join = _specs()
    async with db.acquire() as conn:
        await ri.record_plan(conn, ORDERS, landed, [pk, join])
        (item,) = await ri.due(conn)
    indexer = _Indexer({pk.name: ri.DuplicateKey(["id"], "key (id)=(7) is duplicated")})
    await ri.build_due(db, item, indexer)
    state = await _index_state(db, ORDERS)
    assert state["built"] == [join.name]
    failed = state["failed"][pk.name]
    # A duplicate key is a fact of the source's data, named with its columns.
    assert failed["code"] == ri.DUPLICATE_KEY and failed["columns"] == ["id"]
    assert "key (id)=(7) is duplicated" in failed["error"]
    # Not retried by every pass: the next landing asks again.
    async with db.acquire() as conn:
        assert await ri.due(conn) == []
        record = await build_state.read(conn, ORDERS)
    assert record is not None and record.exists  # the landing stands
    # The record an operator's view is built from carries it (REQ-2015).
    async with db.acquire() as conn:
        (shown,) = [r for r in await replica_state_reads.read_all(conn) if r.key == ORDERS]
    assert shown.index_state["failed"][pk.name]["code"] == ri.DUPLICATE_KEY


async def test_any_other_failure_is_kept_under_its_own_code(control_plane):
    db = control_plane.org()
    landed = datetime.now(UTC)
    await _landed(db, ORDERS, landed)
    pk, join = _specs()
    async with db.acquire() as conn:
        await ri.record_plan(conn, ORDERS, landed, [pk, join])
        (item,) = await ri.due(conn)
    await ri.build_due(db, item, _Indexer({join.name: RuntimeError("store is away")}))
    failed = (await _index_state(db, ORDERS))["failed"][join.name]
    assert failed == {"error": "store is away", "code": ri.INDEX_FAILED, "columns": ["customer_id"]}


async def test_a_landing_mid_build_leaves_the_earlier_indexes_to_the_new_one(control_plane):
    db = control_plane.org()
    first = datetime.now(UTC)
    await _landed(db, ORDERS, first)
    pk, join = _specs()
    async with db.acquire() as conn:
        await ri.record_plan(conn, ORDERS, first, [pk, join])
        (item,) = await ri.due(conn)
    second = first + timedelta(seconds=5)
    indexer = _Indexer()

    async def land_again_after_the_key(spec):
        if spec.name == join.name:
            await _landed(db, ORDERS, second)
            async with db.acquire() as conn:
                await ri.record_plan(conn, ORDERS, second, [pk, join])

    indexer.before_each = land_again_after_the_key
    await ri.build_due(db, item, indexer)
    # The earlier landing's key was recorded on its own state; the join index it was about to
    # build went to a table that is gone, and the new landing asks for both again.
    state = await _index_state(db, ORDERS)
    assert state["for"] == second.isoformat() and state["built"] == [] and state["failed"] == {}
    async with db.acquire() as conn:
        (again,) = await ri.due(conn)
    assert [s.name for s in again.specs] == [pk.name, join.name]


async def test_an_abandoned_build_records_nothing(control_plane):
    db = control_plane.org()
    landed = datetime.now(UTC)
    await _landed(db, ORDERS, landed)
    pk, join = _specs()
    async with db.acquire() as conn:
        await ri.record_plan(conn, ORDERS, landed, [pk, join])
        (item,) = await ri.due(conn)
    await ri.build_due(db, item, _Indexer({pk.name: ri.IndexBuildAbandoned(pk.name)}))
    state = await _index_state(db, ORDERS)
    assert state["built"] == [] and state["failed"] == {}


# -- the runner: read at once, indexed after -------------------------------------------------------


def _runner(control_plane, tmp_path, *, build, plan_indexes, build_indexes, tasks):
    locks = BuildLocks(control_plane.url)
    locks._slots = tmp_path / "slots"

    async def none(*_args):
        return None

    return ReplicaRunner(
        db=control_plane.org(),
        org_id=ORG,
        locks=locks,
        engine_key=lambda: ENGINE,
        build=build,
        source_cap=none,
        permits=None,
        next_refresh_at=none,
        store=lambda: "store-a",
        retry=lambda: RetryPolicy(60.0, 3600),
        builds_per_node=lambda: 1,
        engine_jobs=lambda: 1,
        spawn=lambda coro, name: tasks.append(asyncio.ensure_future(coro)),
        plan_indexes=plan_indexes,
        build_indexes=build_indexes,
    )


async def _drain(tasks):
    while tasks:
        await asyncio.gather(*tasks[:])
        tasks[:] = [t for t in tasks if not t.done()]


async def test_a_replica_is_read_at_its_landing_and_indexed_after_it(control_plane, tmp_path):
    db = control_plane.org()
    async with db.acquire() as conn:
        await build_state.request_build(conn, ORDERS, build_state.REASON_MODEL)
    tasks: list = []
    release = asyncio.Event()
    seen_at_index_time: list = []
    indexer = _Indexer()

    async def build(key, progress):
        return BuildOutcome(rows_copied=3, method="stream_batches")

    async def plan_indexes(key, landed_at):
        async with db.acquire() as conn:
            await ri.record_plan(conn, key, landed_at, _specs())

    async def build_indexes(item):
        async with db.acquire() as conn:
            seen_at_index_time.append(await build_state.read(conn, item.key))
        await release.wait()
        await ri.build_due(db, item, indexer)

    runner = _runner(
        control_plane,
        tmp_path,
        build=build,
        plan_indexes=plan_indexes,
        build_indexes=build_indexes,
        tasks=tasks,
    )
    assert await runner.run_pass() == 1
    # Let the build land and the index job start; it waits on ``release``.
    for _ in range(50):
        await asyncio.sleep(0.02)
        if seen_at_index_time:
            break
    (record,) = seen_at_index_time
    assert record.exists and record.build_state == build_state.IDLE  # readable already
    assert (await _index_state(db, ORDERS))["built"] == []  # and not yet indexed
    release.set()
    await _drain(tasks)
    assert (await _index_state(db, ORDERS))["built"] == [s.name for s in _specs()]


async def test_a_failed_plan_is_noted_and_the_landing_stands(control_plane, tmp_path):
    db = control_plane.org()
    async with db.acquire() as conn:
        await build_state.request_build(conn, ORDERS, build_state.REASON_MODEL)
    tasks: list = []

    async def build(key, progress):
        return BuildOutcome(rows_copied=3, method="stream_batches")

    async def plan_indexes(key, landed_at):
        raise RuntimeError("model read failed")

    async def build_indexes(item):
        raise AssertionError("nothing was planned")

    runner = _runner(
        control_plane,
        tmp_path,
        build=build,
        plan_indexes=plan_indexes,
        build_indexes=build_indexes,
        tasks=tasks,
    )
    await runner.run_pass()
    await _drain(tasks)
    async with db.acquire() as conn:
        record = await build_state.read(conn, ORDERS)
    assert record is not None and record.exists
    assert record.build_notes == [
        {"code": "replication.indexes_not_planned", "params": {"reason": "model read failed"}}
    ]


async def test_a_relationship_added_after_a_landing_is_indexed_by_the_next(control_plane):
    """The plan is the model's at each landing: a join column declared since the last one is in
    the next one's plan, and nothing is rebuilt for it in between."""
    db = control_plane.org()
    ids = await _model(db)
    async with db.acquire() as conn:
        before = await ri.join_columns(conn, ORDERS, ["id"])
        await conn.execute_core(
            insert(relationships).values(
                id="orders_product",
                source_table_id=ids["orders"],
                target_table_id=ids["products"],
                source_column="product_id",
                target_column="id",
                cardinality="many-to-one",
            )
        )
        after = await ri.join_columns(conn, ORDERS, ["id"])
    columns = ["id", "customer_id", "product_id"]
    old = ri.plan("replicas.shop__orders", columns, ["id"], before)
    new = ri.plan("replicas.shop__orders", columns, ["id"], after)
    assert [s.columns for s in new] == [("id",), ("customer_id",), ("product_id",)]
    assert new[: len(old)] == old  # the indexes it had keep their names


async def test_a_landing_is_planned_from_the_model_store_into_the_state_store(
    control_plane, monkeypatch
):
    """An org keeps its model and its replica state in separate stores, each behind a handle
    that refuses the other's tables: the plan reads the relationships through the model's
    handle and records the indexes through the state's."""
    from types import SimpleNamespace

    from provisa.federation import replica_builds, residency

    model_db, state_db = control_plane.org(holds="model"), control_plane.org(holds="state")
    await _model(model_db)
    landed = datetime.now(UTC)
    await _landed(state_db, ORDERS, landed)
    address = SimpleNamespace(schema="replicas", table="shop__orders")

    async def _indexed(state, key):
        return SimpleNamespace(), SimpleNamespace(), address, _Indexer()

    monkeypatch.setattr(replica_builds, "_indexed_replica", _indexed)
    monkeypatch.setattr(
        residency,
        "resolve_landing_args",
        lambda source, table, platform: SimpleNamespace(
            columns=[("id", "bigint"), ("customer_id", "bigint")], pk_columns=["id"]
        ),
    )
    state = SimpleNamespace(
        model_db=model_db,
        tenant_db=state_db,
        federation_engine=SimpleNamespace(
            engine=SimpleNamespace(backend=SimpleNamespace(dialect="postgres"))
        ),
    )
    await replica_builds.plan_indexes(state, ORDERS, landed)
    async with state_db.acquire() as conn:
        (due,) = await ri.due(conn)
    assert due.key == ORDERS
    assert [s.columns for s in due.specs] == [("id",), ("customer_id",)]


def test_every_engine_that_keeps_its_own_replicas_says_how_they_are_indexed():
    """An engine whose replicas live somewhere of its own (it overrides ``replica_target``) says
    how they are indexed there, or that they are not: the base indexer looks at the engine's
    materialization store, which is not where such replicas are -- BigQuery and Databricks were
    otherwise handed a PostgreSQL indexer aimed at the tenant database."""
    import importlib
    import pkgutil

    import provisa.federation as federation
    from provisa.federation.backend import EngineBackend

    for module in pkgutil.iter_modules(federation.__path__):
        if module.name.endswith("_backend"):
            importlib.import_module(f"provisa.federation.{module.name}")

    def subclasses(cls):
        for sub in cls.__subclasses__():
            yield sub
            yield from subclasses(sub)

    missing = sorted(
        cls.__qualname__
        for cls in subclasses(EngineBackend)
        if "replica_target" in vars(cls) and "replica_indexer" not in vars(cls)
    )
    assert missing == []
