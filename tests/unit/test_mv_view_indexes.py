# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2015, REQ-2163: a materialized view is indexed as a replica is -- its key and the columns
the relationships declared on it join on, built after it lands or refreshes, through the replica's
own plan, record, lock and indexers, on a store that indexes. A source's own table is never
indexed. A view of a Class's union is indexed on ``_source_table`` with the member's key."""

# Requirements: REQ-2015, REQ-2163

from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import psycopg
import pyarrow as pa
import pytest
from sqlalchemy import insert, select

from provisa.core.schema_org import (
    domains,
    mv_build_state,
    registered_tables,
    relationships,
    sources,
)
from provisa.executor.result import QueryResult
from provisa.federation import replica_builds
from provisa.federation import replica_indexes as ri
from provisa.federation import replica_state as build_state
from provisa.federation.replica_locks import BuildLocks
from provisa.federation.replica_runner import ReplicaRunner
from provisa.federation.replica_state import RetryPolicy
from provisa.federation.replica_target import DuckDBStoreIndexer, PostgresIndexer
from provisa.mv.bitemporal import BitemporalSpec
from provisa.mv.models import MVDefinition
from provisa.mv.refresh import _replace_plan, refresh_mv
from provisa.mv.registry import MVRegistry
from provisa.mv.view_indexes import plan_view_indexes
from tests.helpers import build_through_a_stand_in_pipeline, hold_registered_tables

pytestmark = pytest.mark.asyncio

# PostgreSQL advisory locks are held server-wide, not per database: each worker process names an
# org and an engine of its own (tests/unit/federation/test_lock_names_per_worker.py).
ORG = f"org1{uuid.uuid4().hex[:8]}"
ENGINE = f"trino@engine-{uuid.uuid4().hex[:8]}:8080"

COLUMNS = ["id", "customer_id", "note"]
STORE_NAME = "mv_cache.mv_gold"


class _Indexer:
    """A store that builds what it is asked."""

    def __init__(self) -> None:
        self.built: list[str] = []

    async def build(self, spec: ri.IndexSpec) -> None:
        self.built.append(spec.name)


class _Engine:
    """The engine terminal a view is stored through: the columns its store table holds, and how
    the store indexes (None: it keeps no secondary index)."""

    # The engine it stands for names a catalog in its SQL (REQ-1730).
    engine = SimpleNamespace(catalog_qualified=True)
    dialect = "duckdb"

    def __init__(self, indexer=None, *, columns=None, catalog="store") -> None:
        self.indexer = indexer
        self.columns = COLUMNS if columns is None else columns
        self.catalog = catalog
        self.asked_for: list[tuple[str, str]] = []
        self.sqls: list[str] = []

    def view_indexer(self, schema: str, table: str):
        self.asked_for.append((schema, table))
        return self.indexer

    def materialize_store_target(self, org_id: str) -> tuple[str, str]:
        return self.catalog, "mv_cache"

    def engine_physical(self, sql: str) -> str:
        return sql

    async def execute_engine(self, sql, *a, **k):
        self.sqls.append(sql)
        if "COUNT(*)" in sql:
            return QueryResult(rows=[(5,)], column_names=[])
        return QueryResult(rows=[], column_names=self.columns)


def _served(**held) -> SimpleNamespace:
    return SimpleNamespace(active_org_id=ORG, tables=[], class_tables={}, **held)


def _view(**fields) -> MVDefinition:
    values = {
        "id": "view-gold",
        "source_tables": [],
        "target_catalog": "store",
        "target_schema": "mv_cache",
        "target_table": "mv_gold",
        "view_table": "gold",
        "sql": "SELECT * FROM orders",
        "semantic_sql": "SELECT * FROM orders",
        "primary_key": ["id"],
    }
    return MVDefinition(**{**values, **fields})


async def _model(db) -> None:
    """The registered view ``gold``, with gold.customer_id -> customers.id, and the source
    ``shop``'s own tables, ``orders`` among them in the schema views are stored in."""
    async with db.acquire() as conn:
        await conn.execute_core(insert(domains).values(id="sales", description=""))
        await conn.execute_core(insert(sources).values(id="shop", type="postgresql"))
        await conn.execute_core(insert(sources).values(id="__derived__", type="postgresql"))
        for n, (source, schema, name) in enumerate(
            (
                ("__derived__", "derived", "gold"),
                ("shop", "public", "customers"),
                ("shop", "mv_cache", "orders"),
            ),
            start=1,
        ):
            await conn.execute_core(
                insert(registered_tables).values(
                    id=n, source_id=source, domain_id="sales", schema_name=schema, table_name=name
                )
            )
        await conn.execute_core(
            insert(relationships).values(
                id="gold_customer",
                source_table_id=1,
                target_table_id=2,
                source_column="customer_id",
                target_column="id",
                cardinality="many-to-one",
            )
        )


def _wanted(key: str = "id", join: str = "customer_id") -> list[ri.IndexSpec]:
    return ri.plan(STORE_NAME, COLUMNS, [key], [(join,)])


async def _due(db) -> list:
    async with db.acquire() as conn:
        return await ri.due(conn)


async def _index_state(db, mv_id: str = "view-gold") -> dict | None:
    async with db.acquire() as conn:
        return (
            await conn.execute_core(
                select(mv_build_state.c.index_state).where(mv_build_state.c.mv_id == mv_id)
            )
        ).scalar_one_or_none()


# -- the plan --------------------------------------------------------------------------------------


async def test_a_view_asks_for_its_key_and_the_join_columns_of_its_relationships(control_plane):
    model, state = control_plane.org(), control_plane.org(holds="state")
    await _model(model)
    await plan_view_indexes(
        _Engine(_Indexer()), _view(), model_db=model, state_db=state, served=_served()
    )
    (item,) = await _due(state)
    assert list(item.specs) == _wanted()
    assert item.specs[0].primary and not item.specs[1].primary
    assert item.address == ("mv_cache", "mv_gold")


async def test_a_join_on_a_column_the_view_does_not_hold_has_nothing_to_index(control_plane):
    model, state = control_plane.org(), control_plane.org(holds="state")
    await _model(model)
    await plan_view_indexes(
        _Engine(_Indexer(), columns=["id", "note"]),
        _view(),
        model_db=model,
        state_db=state,
        served=_served(),
    )
    (item,) = await _due(state)
    assert [s.columns for s in item.specs] == [("id",)]


async def test_a_view_with_no_key_and_no_relationship_asks_for_nothing(control_plane):
    model, state = control_plane.org(), control_plane.org(holds="state")
    await plan_view_indexes(
        _Engine(_Indexer()),
        _view(primary_key=[], view_table=None),
        model_db=model,
        state_db=state,
        served=_served(),
    )
    assert await _due(state) == []


async def test_a_versioned_view_keeps_its_key_as_an_index_never_as_a_primary_key(control_plane):
    """Its log holds a row per version of a key, so the key cannot be enforced on it."""
    model, state = control_plane.org(), control_plane.org(holds="state")
    await plan_view_indexes(
        _Engine(_Indexer()),
        _view(primary_key=[], view_table=None, bitemporal=BitemporalSpec(key=("id",))),
        model_db=model,
        state_db=state,
        served=_served(),
    )
    (item,) = await _due(state)
    assert [(s.columns, s.primary) for s in item.specs] == [(("id",), False)]


async def test_a_plan_that_cannot_be_made_never_fails_the_landing(control_plane, caplog):
    class _Broken(_Engine):
        def view_indexer(self, schema, table):
            raise RuntimeError("the store is away")

    model, state = control_plane.org(), control_plane.org(holds="state")
    await plan_view_indexes(_Broken(), _view(), model_db=model, state_db=state, served=_served())
    assert "the store is away" in caplog.text


# -- a store that does not index, and a table that is not ours --------------------------------------


async def test_a_store_that_keeps_no_index_gets_none(control_plane):
    model, state = control_plane.org(), control_plane.org(holds="state")
    await _model(model)
    await plan_view_indexes(
        _Engine(None), _view(), model_db=model, state_db=state, served=_served()
    )
    assert await _due(state) == []
    assert await _index_state(state) is None


async def test_a_view_kept_outside_the_materialization_store_is_not_indexed(control_plane):
    """A view configured into another catalog -- a source's -- is not in the store the indexers
    reach."""
    model, state = control_plane.org(), control_plane.org(holds="state")
    await _model(model)
    await plan_view_indexes(
        _Engine(_Indexer()),
        _view(target_catalog="shop"),
        model_db=model,
        state_db=state,
        served=_served(),
    )
    assert await _due(state) == []


async def test_a_source_table_is_never_indexed(control_plane):
    """A view whose target names a source's own table is refused an index, whatever store it
    stands in: Provisa never creates an index on a source's own tables."""
    model, state = control_plane.org(), control_plane.org(holds="state")
    await _model(model)
    await plan_view_indexes(
        _Engine(_Indexer()),
        _view(target_table="orders"),
        model_db=model,
        state_db=state,
        served=_served(),
    )
    assert await _due(state) == []
    assert await _index_state(state) is None


async def test_nothing_but_the_views_own_table_is_asked_of_the_store(control_plane):
    """The tables a view reads are never addressed: the indexer is the stored table's alone."""
    model, state = control_plane.org(), control_plane.org(holds="state")
    await _model(model)
    engine = _Engine(_Indexer())
    mv = _view(source_tables=["orders"], read_tables=frozenset({"orders"}))
    await plan_view_indexes(engine, mv, model_db=model, state_db=state, served=_served())
    (item,) = await _due(state)
    await replica_builds.build_indexes(
        SimpleNamespace(tenant_db=state, federation_engine=engine), item
    )
    assert set(engine.asked_for) == {("mv_cache", "mv_gold")}


# -- the record and the builds ---------------------------------------------------------------------


async def test_each_index_of_a_view_is_recorded_built_by_name(control_plane):
    model, state = control_plane.org(), control_plane.org(holds="state")
    await _model(model)
    indexer = _Indexer()
    engine = _Engine(indexer)
    await plan_view_indexes(engine, _view(), model_db=model, state_db=state, served=_served())
    (item,) = await _due(state)
    await replica_builds.build_indexes(
        SimpleNamespace(tenant_db=state, federation_engine=engine), item
    )
    recorded = await _index_state(state)
    assert recorded is not None
    assert recorded["built"] == [s.name for s in _wanted()] == indexer.built
    assert recorded["failed"] == {}
    assert await _due(state) == []


async def test_a_refresh_asks_for_its_indexes_again(control_plane):
    """A refresh's table may be a new one, and the model's relationships may have moved: each
    landing states its own indexes, and each is built, or found there, again."""
    model, state = control_plane.org(), control_plane.org(holds="state")
    await _model(model)
    engine = _Engine(_Indexer())
    served = _served()
    await plan_view_indexes(engine, _view(), model_db=model, state_db=state, served=served)
    (first,) = await _due(state)
    await ri.build_due(state, first, engine.indexer)
    assert await _due(state) == []
    await plan_view_indexes(engine, _view(), model_db=model, state_db=state, served=served)
    (again,) = await _due(state)
    assert list(again.specs) == _wanted() and again.landed_at != first.landed_at


async def test_a_relationship_added_after_a_landing_is_indexed_by_the_next(control_plane):
    model, state = control_plane.org(), control_plane.org(holds="state")
    await _model(model)
    engine = _Engine(_Indexer())
    served = _served()
    mv = _view(primary_key=["customer_id"])
    await plan_view_indexes(engine, mv, model_db=model, state_db=state, served=served)
    async with model.acquire() as conn:
        await conn.execute_core(
            insert(relationships).values(
                id="gold_note",
                source_table_id=1,
                target_table_id=2,
                source_column="note",
                target_column="id",
                cardinality="many-to-one",
            )
        )
    await plan_view_indexes(engine, mv, model_db=model, state_db=state, served=served)
    (item,) = await _due(state)
    assert [s.columns for s in item.specs] == [("customer_id",), ("note",)]


async def test_a_landing_mid_build_leaves_the_earlier_indexes_to_the_new_one(control_plane):
    model, state = control_plane.org(), control_plane.org(holds="state")
    await _model(model)
    engine = _Engine(_Indexer())
    served = _served()
    await plan_view_indexes(engine, _view(), model_db=model, state_db=state, served=served)
    (item,) = await _due(state)

    class _LandsAgain(_Indexer):
        async def build(self, spec):
            await plan_view_indexes(engine, _view(), model_db=model, state_db=state, served=served)
            raise RuntimeError("the table was replaced")

    await ri.build_due(state, item, _LandsAgain())
    recorded = await _index_state(state)
    assert recorded is not None and recorded["for"] != item.landed_at
    assert recorded["built"] == [] and recorded["failed"] == {}


async def test_a_duplicate_key_is_recorded_naming_its_columns(control_plane):
    model, state = control_plane.org(), control_plane.org(holds="state")
    await _model(model)

    class _Repeats(_Indexer):
        async def build(self, spec):
            if spec.primary:
                raise ri.DuplicateKey(spec.columns, "key (id)=(7) is duplicated")
            await super().build(spec)

    engine = _Engine(_Repeats())
    await plan_view_indexes(engine, _view(), model_db=model, state_db=state, served=_served())
    (item,) = await _due(state)
    await ri.build_due(state, item, engine.indexer)
    recorded = await _index_state(state)
    assert recorded is not None
    pk, join = _wanted()
    assert recorded["built"] == [join.name]
    assert recorded["failed"][pk.name]["code"] == ri.DUPLICATE_KEY
    assert recorded["failed"][pk.name]["columns"] == ["id"]


# -- the landing paths: a first build and a refresh ------------------------------------------------


@pytest.fixture
def _refreshable(monkeypatch):
    build_through_a_stand_in_pipeline(monkeypatch)
    hold_registered_tables(monkeypatch, "orders")


async def test_a_first_build_and_a_refresh_each_plan_their_indexes(control_plane, _refreshable):
    model, state = control_plane.org(), control_plane.org(holds="state")
    await _model(model)
    registry = MVRegistry()
    mv = _view(source_tables=["orders"])
    registry.register(mv)
    engine = _Engine(_Indexer())

    await refresh_mv(engine, mv, registry, store=model, ledger=state, writer="box-1")
    assert mv.last_error is None
    (first,) = await _due(state)
    assert list(first.specs) == _wanted()
    await ri.build_due(state, first, engine.indexer)
    assert await _due(state) == []

    await refresh_mv(engine, mv, registry, store=model, ledger=state, writer="box-1")
    assert mv.last_error is None
    (second,) = await _due(state)
    assert list(second.specs) == _wanted() and second.landed_at != first.landed_at


async def test_a_refresh_that_changed_nothing_asks_for_nothing(control_plane, _refreshable):
    model, state = control_plane.org(), control_plane.org(holds="state")
    await _model(model)
    registry = MVRegistry()
    mv = _view(source_tables=["orders"], freshness_mode="probe", last_input_token="same")
    registry.register(mv)

    async def same(*_args):
        return []

    import provisa.mv.input_signals as signals

    token, gather = signals.input_token, signals.gather_input_signals
    signals.input_token, signals.gather_input_signals = lambda *_a: "same", same
    try:
        await refresh_mv(
            _Engine(_Indexer()), mv, registry, store=model, ledger=state, writer="box-1"
        )
    finally:
        signals.input_token, signals.gather_input_signals = token, gather
    assert await _due(state) == []


async def test_the_event_loops_landing_plans_the_views_indexes(control_plane):
    from provisa.events.app_wiring import registry_marking

    model, state = control_plane.org(), control_plane.org(holds="state")
    registry = MVRegistry()
    mv = _view()
    registry.register(mv)
    planned: list[str] = []

    async def plan_indexes(view):
        planned.append(view.id)

    mark = registry_marking(registry, model, state, plan_indexes=plan_indexes)(mv)
    await mark(("replace", {"rows": 5, "landed": "x"}))
    await mark(("delta", {"rows": 2, "delta": [], "landed": "x"}))
    assert planned == ["view-gold", "view-gold"]
    await mark(None)
    await mark(("error", {"error": "boom"}))
    assert planned == ["view-gold", "view-gold"]


# -- one build at a time: the per-object lock --------------------------------------------------------


def _runner(control_plane, tmp_path, *, build_indexes, tasks) -> tuple[ReplicaRunner, BuildLocks]:
    locks = BuildLocks(control_plane.url)
    locks._slots = tmp_path / "slots"

    async def none(*_args):
        return None

    async def build(key, progress):
        raise AssertionError("no replica is due")

    runner = ReplicaRunner(
        db=control_plane.org(holds="state"),
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
        build_indexes=build_indexes,
    )
    return runner, locks


async def _drain(tasks) -> None:
    while tasks:
        await asyncio.gather(*tasks[:])
        tasks[:] = [t for t in tasks if not t.done()]


async def test_the_runner_builds_a_views_indexes_under_its_own_lock(control_plane, tmp_path):
    model, state = control_plane.org(), control_plane.org(holds="state")
    await _model(model)
    engine = _Engine(_Indexer())
    await plan_view_indexes(engine, _view(), model_db=model, state_db=state, served=_served())
    tasks: list = []
    held_during: list[bool] = []

    async def build_indexes(item):
        other = locks.claim()
        held_during.append(not other.try_indexes(ORG, item.key))
        other.close()
        await ri.build_due(state, item, engine.indexer)

    runner, locks = _runner(control_plane, tmp_path, build_indexes=build_indexes, tasks=tasks)
    await runner.run_pass()
    await _drain(tasks)
    assert held_during == [True]
    assert engine.indexer.built == [s.name for s in _wanted()]


async def test_a_build_of_the_same_view_waits_on_its_lock(control_plane, tmp_path):
    """Another process is building this view's indexes: this pass leaves them to it, and takes
    them up once the lock is free, never building alongside."""
    model, state = control_plane.org(), control_plane.org(holds="state")
    await _model(model)
    engine = _Engine(_Indexer())
    await plan_view_indexes(engine, _view(), model_db=model, state_db=state, served=_served())
    (item,) = await _due(state)
    tasks: list = []

    async def build_indexes(due):
        await ri.build_due(state, due, engine.indexer)

    runner, locks = _runner(control_plane, tmp_path, build_indexes=build_indexes, tasks=tasks)
    elsewhere = locks.claim()
    assert elsewhere.try_indexes(ORG, item.key)
    try:
        await runner.run_pass()
        await _drain(tasks)
        assert engine.indexer.built == []
        assert len(await _due(state)) == 1  # still due: it waits, it is not dropped
    finally:
        elsewhere.close()
    await runner.run_pass()
    await _drain(tasks)
    assert engine.indexer.built == [s.name for s in _wanted()]


async def test_two_views_are_two_locks(control_plane, tmp_path):
    model, state = control_plane.org(), control_plane.org(holds="state")
    await _model(model)
    engine = _Engine(_Indexer())
    served = _served()
    await plan_view_indexes(engine, _view(), model_db=model, state_db=state, served=served)
    await plan_view_indexes(
        engine,
        _view(id="view-silver", target_table="mv_silver", view_table=None),
        model_db=model,
        state_db=state,
        served=served,
    )
    tasks: list = []
    ran: list[str] = []

    async def build_indexes(item):
        ran.append(item.key[2])

    runner, locks = _runner(control_plane, tmp_path, build_indexes=build_indexes, tasks=tasks)
    (gold,) = [i for i in await _due(state) if i.key[2] == "view-gold"]
    elsewhere = locks.claim()
    assert elsewhere.try_indexes(ORG, gold.key)
    try:
        await runner.run_pass()
        await _drain(tasks)
    finally:
        elsewhere.close()
    assert ran == ["view-silver"]


async def test_a_view_and_a_replica_of_the_same_names_are_two_records(control_plane):
    """A view's key is no replica's: its state is on its build row, and a replica's untouched."""
    state = control_plane.org(holds="state")
    key = build_state.view_index_key("view-gold")
    assert key[0] != "__derived__"
    async with state.acquire() as conn:
        assert await build_state.read(conn, key) is None


# -- the Class union view --------------------------------------------------------------------------


def _class_row(*bindings) -> dict:
    return {
        "schema_name": "ontology",
        "table_name": "Customer",
        "class_bindings": list(bindings),
    }


def _binding(table_id: int, relation: str, **columns) -> dict:
    return {
        "table_id": table_id,
        "relation": relation,
        "columns": {p: [[c, "integer"] for c in cs] for p, cs in columns.items()},
    }


def _table(table_id: int, *key: str) -> dict:
    return {
        "id": table_id,
        "columns": [{"column_name": c, "is_primary_key": c in key} for c in (*key, "name")],
    }


async def _class_view_specs(model, state, *, classes, tables, columns) -> list[ri.IndexSpec]:
    await plan_view_indexes(
        _Engine(_Indexer(), columns=columns),
        _view(
            primary_key=[],
            view_table=None,
            sql="SELECT * FROM mat.physical",
            semantic_sql="SELECT * FROM ontology.Customer",
        ),
        model_db=model,
        state_db=state,
        served=SimpleNamespace(active_org_id=ORG, tables=tables, class_tables=classes),
    )
    due = await _due(state)
    return list(due[0].specs) if due else []


async def test_a_materialized_class_union_is_indexed_on_source_table_and_member_key(control_plane):
    model, state = control_plane.org(), control_plane.org(holds="state")
    specs = await _class_view_specs(
        model,
        state,
        classes={
            "ontology.Customer": _class_row(
                _binding(1, "sales.customers", customer_id=["id"], name=["name"]),
                _binding(2, "crm.people", customer_id=["person_id"], name=["name"]),
            )
        },
        tables=[_table(1, "id"), _table(2, "person_id")],
        columns=["_source_table", "customer_id", "name"],
    )
    assert [(s.columns, s.primary) for s in specs] == [(("_source_table", "customer_id"), False)]


async def test_each_members_key_is_indexed_when_their_properties_differ(control_plane):
    model, state = control_plane.org(), control_plane.org(holds="state")
    specs = await _class_view_specs(
        model,
        state,
        classes={
            "ontology.Customer": _class_row(
                _binding(1, "sales.customers", customer_id=["id"]),
                _binding(2, "crm.people", person_id=["person_id"]),
            )
        },
        tables=[_table(1, "id"), _table(2, "person_id")],
        columns=["_source_table", "customer_id", "person_id"],
    )
    assert [s.columns for s in specs] == [
        ("_source_table", "customer_id"),
        ("_source_table", "person_id"),
    ]


async def test_a_composite_member_key_keeps_its_columns_in_order(control_plane):
    model, state = control_plane.org(), control_plane.org(holds="state")
    specs = await _class_view_specs(
        model,
        state,
        classes={
            "ontology.Customer": _class_row(
                _binding(1, "sales.lines", order_no=["order_id"], line_no=["line"]),
            )
        },
        tables=[_table(1, "order_id", "line")],
        columns=["_source_table", "line_no", "order_no"],
    )
    assert [s.columns for s in specs] == [("_source_table", "order_no", "line_no")]


async def test_a_member_whose_key_is_bound_to_no_property_has_no_member_key_to_index(
    control_plane,
):
    """The union holds Property columns alone: a key bound to none is not in its rows."""
    model, state = control_plane.org(), control_plane.org(holds="state")
    specs = await _class_view_specs(
        model,
        state,
        classes={"ontology.Customer": _class_row(_binding(1, "sales.customers", name=["name"]))},
        tables=[_table(1, "id")],
        columns=["_source_table", "name"],
    )
    assert specs == []


async def test_a_view_not_keeping_source_table_is_not_indexed_on_it(control_plane):
    model, state = control_plane.org(), control_plane.org(holds="state")
    specs = await _class_view_specs(
        model,
        state,
        classes={
            "ontology.Customer": _class_row(_binding(1, "sales.customers", customer_id=["id"]))
        },
        tables=[_table(1, "id")],
        columns=["customer_id"],
    )
    assert specs == []


# -- the stores themselves ---------------------------------------------------------------------------

SCHEMA = "mv_cache_ix"
TABLE = "mv_gold"


def _store_specs() -> list[ri.IndexSpec]:
    return ri.plan(f"{SCHEMA}.{TABLE}", COLUMNS, ["id"], [("customer_id",)])


@pytest.fixture
def duck_view(tmp_path):
    from provisa.federation.materialize_broker import get_broker

    return get_broker(str(tmp_path / "views.duckdb"))


def _land(broker, ids: list[int]) -> int:
    fresh = pa.table(
        {
            "id": pa.array(ids, pa.int64()),
            "customer_id": pa.array([i % 2 for i in ids], pa.int64()),
            "note": pa.array([f"n{i}" for i in ids]),
        }
    )
    mv = _view(target_catalog="mat_store", target_schema=SCHEMA, target_table=TABLE)
    plan = _replace_plan(mv, mv.stored_ref())
    return broker.write_mv(
        SCHEMA, TABLE, fresh.to_reader(), lambda existing: plan(existing, COLUMNS)
    )


async def test_the_duckdb_store_keeps_a_views_indexes_across_its_refreshes(duck_view):
    """The view's key and join index, built after its first landing, stand through a refresh
    that rewrites every row under the same keys."""
    assert _land(duck_view, [1, 2, 3]) == 3
    indexer = DuckDBStoreIndexer(duck_view, schema=SCHEMA, table=TABLE)
    for spec in _store_specs():
        await indexer.build(spec)
    assert _land(duck_view, [1, 2, 3, 4]) == 4
    for spec in _store_specs():
        await indexer.build(spec)  # found there, nothing built twice

    def _read(con):
        keys = con.execute(
            "SELECT constraint_column_names FROM duckdb_constraints() WHERE table_name = ? "
            "AND constraint_type = 'PRIMARY KEY'",
            [TABLE],
        ).fetchall()
        indexes = con.execute(
            "SELECT index_name FROM duckdb_indexes() WHERE table_name = ?", [TABLE]
        ).fetchall()
        return [list(k[0]) for k in keys], [i[0] for i in indexes]

    keys, indexes = duck_view._run(_read, read_only=True)
    assert keys == [["id"]]
    assert indexes == [_store_specs()[1].name]


@pytest.fixture
def pg_view(embedded_postgres):
    dsn = embedded_postgres.dsn()
    with psycopg.connect(dsn, autocommit=True) as con:
        con.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
        con.execute(f"CREATE SCHEMA {SCHEMA}")
        # As reconcile_mv_table creates it: keyed, so the key is found there.
        con.execute(
            f"CREATE TABLE {SCHEMA}.{TABLE} (id integer PRIMARY KEY, customer_id integer, note text)"
        )
        con.execute(
            f"INSERT INTO {SCHEMA}.{TABLE} SELECT g, g % 10, 'n' FROM generate_series(1, 9) g"
        )
    yield dsn
    with psycopg.connect(dsn, autocommit=True) as con:
        con.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")


async def test_postgres_finds_the_views_key_and_builds_its_join_index(pg_view):
    indexer = PostgresIndexer(pg_view, schema=SCHEMA, table=TABLE)
    for spec in _store_specs():
        await indexer.build(spec)
    for spec in _store_specs():
        await indexer.build(spec)  # a refresh's plan asks again: both are found
    with psycopg.connect(pg_view) as con:
        rows = con.execute(
            "SELECT c.relname, i.indisprimary, i.indisvalid FROM pg_index i "
            "JOIN pg_class c ON c.oid = i.indexrelid WHERE i.indrelid = %s::regclass",
            (f"{SCHEMA}.{TABLE}",),
        ).fetchall()
    assert sum(1 for _, primary, _v in rows if primary) == 1
    assert (_store_specs()[1].name, False, True) in rows


async def test_a_later_landing_timestamp_is_a_later_record(control_plane):
    """The record is the landing's, as a replica's is: asked for by the instant it landed."""
    state = control_plane.org(holds="state")
    async with state.acquire() as conn:
        await conn.execute_core(insert(mv_build_state).values(mv_id="view-gold", region="default"))
    first, second = datetime.now(UTC), datetime.now(UTC) + timedelta(seconds=1)
    specs = _wanted()
    async with state.acquire() as conn:
        await build_state.record_view_index_plan(
            conn, "view-gold", first, specs, ("mv_cache", "mv_gold")
        )
        await build_state.record_view_index_plan(
            conn, "view-gold", second, specs, ("mv_cache", "mv_gold")
        )
        (item,) = await ri.due(conn)
    assert item.landed_at == second.isoformat()
