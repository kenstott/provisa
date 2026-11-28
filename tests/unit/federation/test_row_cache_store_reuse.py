# Copyright (c) 2026 Kenneth Stott
# Canary: ac01ab09-f42a-426e-b2f1-e058acf7df1d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""A cold keyed read through the row cache reuses one pooled store handle per DSN and the table
it already ensured: no engine built, no new connection, no reflection when the table matches
(REQ-1865, REQ-1316). A changed column set re-ensures; Provisa's own DDL on the table forgets it;
a read that finds the table or a column missing ensures it again and is retried once."""

from __future__ import annotations

import types

import pytest
import sqlalchemy as sa
from sqlalchemy import event

from provisa.compiler.pk_bounds import PkBound
from provisa.federation import store_writer
from tests.unit.federation.test_ensure_rows_resident import (
    _FakeEngine,
    _FakeLoader,
    _source,
    _table,
)


@pytest.fixture(autouse=True)
def _fresh_shared_stores():
    store_writer.dispose_shared_stores()
    yield
    store_writer.dispose_shared_stores()


@pytest.fixture
def env(monkeypatch, tmp_path):
    ns = types.SimpleNamespace(table=_table(), source=_source(), reflections=0)
    ns.loader = _FakeLoader({(i,): {"id": i, "status": "new"} for i in range(10)})

    async def _sources(state):
        return [ns.source]

    async def _tables(state):
        return [ns.table]

    monkeypatch.setattr("provisa.federation.registry_view.registered_sources", _sources)
    monkeypatch.setattr("provisa.federation.registry_view.registered_tables", _tables)
    monkeypatch.setattr(
        "provisa.events.source_loader.SourceRowLoader",
        lambda engine, adapter_loaders=None, keyed_adapter_loaders=None: ns.loader,
    )
    monkeypatch.setattr("provisa.events.app_wiring.build_adapter_loaders", lambda s, e: {})
    monkeypatch.setattr(
        "provisa.events.app_wiring.build_keyed_adapter_loaders", lambda s, e=None: {}
    )
    import provisa.core.db as core_db

    real = core_db.add_missing_columns

    def _counting(*a, **k):
        ns.reflections += 1
        return real(*a, **k)

    monkeypatch.setattr(core_db, "add_missing_columns", _counting)
    ns.dsn = f"sqlite:///{tmp_path}/row_cache.db"
    ns.state = types.SimpleNamespace(federation_engine=_FakeEngine(ns.dsn))
    return ns


def _bound(key: int) -> PkBound:
    return PkBound(
        source_id="pg1",
        schema_name="public",
        table_name="orders",
        pk_columns=("id",),
        values=((key,),),
    )


@pytest.mark.asyncio
async def test_a_second_cold_read_builds_no_engine_opens_no_connection_and_reflects_nothing(
    env, monkeypatch
):
    from provisa.federation.query_residency import ensure_rows_resident

    assert await ensure_rows_resident(env.state, [_bound(1)], reader_role=None) == [
        ("pg1", "orders", 1)
    ]
    assert env.reflections == 1
    engine = store_writer.shared_store(env.dsn).db.engine
    connects: list[object] = []
    event.listen(engine, "connect", lambda *a: connects.append(a))
    built: list[object] = []
    real_create = sa.create_engine
    monkeypatch.setattr(
        sa, "create_engine", lambda *a, **k: built.append(a) or real_create(*a, **k)
    )

    assert await ensure_rows_resident(env.state, [_bound(2)], reader_role=None) == [
        ("pg1", "orders", 1)
    ]
    assert (built, connects, env.reflections) == ([], [], 1)
    assert env.loader.calls == [[(1,)], [(2,)]]


@pytest.mark.asyncio
async def test_a_changed_column_set_ensures_the_table_again(env):
    from provisa.core.models import Column
    from provisa.federation.query_residency import ensure_rows_resident

    await ensure_rows_resident(env.state, [_bound(1)], reader_role=None)
    env.table.columns.append(Column(name="note", visible_to=["public"], data_type="text"))
    env.loader.rows_by_key[(2,)] = {"id": 2, "status": "new", "note": "n"}

    assert await ensure_rows_resident(env.state, [_bound(2)], reader_role=None) == [
        ("pg1", "orders", 1)
    ]
    assert env.reflections == 2
    with store_writer.shared_store(env.dsn).db.engine.connect() as conn:
        assert (
            conn.execute(sa.text('SELECT note FROM "pg1__public__orders" WHERE id = 2')).scalar()
            == "n"
        )


@pytest.mark.asyncio
async def test_an_external_drop_is_ensured_again_and_the_read_succeeds(env, monkeypatch):
    from provisa.federation import query_residency
    from provisa.federation.query_residency import ensure_rows_resident

    await ensure_rows_resident(env.state, [_bound(1)], reader_role=None)
    with store_writer.shared_store(env.dsn).db.engine.begin() as conn:
        conn.execute(sa.text('DROP TABLE "pg1__public__orders"'))
    reads: list[object] = []
    real_read = query_residency._read_cached

    async def _counting_read(*a, **k):
        reads.append(a)
        return await real_read(*a, **k)

    monkeypatch.setattr(query_residency, "_read_cached", _counting_read)

    # The table was dropped behind the record: the read fails once, the table is ensured
    # again, and the one read is retried.
    assert await ensure_rows_resident(env.state, [_bound(2)], reader_role=None) == [
        ("pg1", "orders", 1)
    ]
    assert env.reflections == 2
    # The failed read, its retry, and the re-check under the row lock.
    assert len(reads) == 3
    assert ("main", "pg1__public__orders") in store_writer.shared_store(env.dsn).ensured


@pytest.mark.asyncio
async def test_a_reconcile_that_recreates_the_table_is_read_with_no_failed_statement(
    env, monkeypatch
):
    from provisa.federation.query_residency import ensure_rows_resident

    await ensure_rows_resident(env.state, [_bound(1)], reader_role=None)
    # The boot reconcile converges the replica to its declared shape: the row cache's own
    # bookkeeping columns are not in it, so the table is recreated without them.
    status = await store_writer.reconcile_table(
        env.dsn,
        schema="main",
        table="pg1__public__orders",
        columns=[("id", "integer"), ("status", "text")],
        pk_columns=["id"],
    )
    assert status == "recreated"
    assert ("main", "pg1__public__orders") not in store_writer.shared_store(env.dsn).ensured

    def _no_failed_statement(exc, dialect):
        raise AssertionError(f"a statement failed against the row-cache table: {exc}")

    monkeypatch.setattr(store_writer, "missing_table_or_column", _no_failed_statement)

    assert await ensure_rows_resident(env.state, [_bound(2)], reader_role=None) == [
        ("pg1", "orders", 1)
    ]
    assert env.reflections == 2


@pytest.mark.asyncio
async def test_an_error_that_is_not_a_missing_table_or_column_is_raised_and_not_retried(
    env, monkeypatch
):
    import sqlite3

    from provisa.federation import query_residency
    from provisa.federation.query_residency import ensure_rows_resident

    await ensure_rows_resident(env.state, [_bound(1)], reader_role=None)
    reads: list[object] = []

    async def _locked(*a, **k):
        reads.append(a)
        raise sa.exc.OperationalError("SELECT", {}, sqlite3.OperationalError("database is locked"))

    monkeypatch.setattr(query_residency, "_read_cached", _locked)

    with pytest.raises(sa.exc.OperationalError, match="database is locked"):
        await ensure_rows_resident(env.state, [_bound(2)], reader_role=None)
    assert len(reads) == 1
    assert env.reflections == 1


@pytest.mark.asyncio
async def test_a_missing_table_again_after_the_ensure_is_raised(env, monkeypatch):
    import sqlite3

    from provisa.federation import query_residency
    from provisa.federation.query_residency import ensure_rows_resident

    reads: list[object] = []

    async def _gone(*a, **k):
        reads.append(a)
        raise sa.exc.OperationalError(
            "SELECT", {}, sqlite3.OperationalError("no such table: pg1__public__orders")
        )

    monkeypatch.setattr(query_residency, "_read_cached", _gone)

    with pytest.raises(sa.exc.OperationalError, match="no such table"):
        await ensure_rows_resident(env.state, [_bound(1)], reader_role=None)
    assert len(reads) == 2


@pytest.mark.parametrize(
    "orig, dialect, missing",
    [
        (types.SimpleNamespace(sqlstate="42P01", args=()), "postgresql", True),
        (types.SimpleNamespace(sqlstate="42703", args=()), "postgresql", True),
        (types.SimpleNamespace(pgcode="42P01", args=()), "postgresql", True),
        (types.SimpleNamespace(sqlstate="40P01", args=()), "postgresql", False),
        (types.SimpleNamespace(args=(1146, "Table 'x' doesn't exist")), "mysql", True),
        (types.SimpleNamespace(args=(1054, "Unknown column")), "mysql", True),
        (types.SimpleNamespace(args=(1213, "Deadlock")), "mysql", False),
        (types.SimpleNamespace(args=("42S02", "Invalid object name")), "mssql", True),
        (types.SimpleNamespace(args=("42S22", "Invalid column name")), "mssql", True),
        (Exception("no such table: t"), "sqlite", True),
        (Exception("no such column: c"), "sqlite", True),
        (Exception("table t has no column named c"), "sqlite", True),
        (Exception("database is locked"), "sqlite", False),
        # SQLite's message text carries no code: only a SQLite store's error is read by it.
        (Exception("no such table: t"), "postgresql", False),
    ],
)
def test_missing_table_or_column_reads_each_drivers_code(orig, dialect, missing):
    exc = sa.exc.DBAPIError("SELECT 1", {}, Exception("x"))
    exc.orig = orig  # pyright: ignore[reportAttributeAccessIssue]
    assert store_writer.missing_table_or_column(exc, dialect) is missing


def test_forget_ensured_forgets_the_address_in_every_store(tmp_path):
    first = store_writer.shared_store(f"sqlite:///{tmp_path}/a.db")
    second = store_writer.shared_store(f"sqlite:///{tmp_path}/b.db")
    first.ensured[("main", "t")] = (None, None)
    second.ensured[("main", "t")] = (None, None)
    second.ensured[("main", "kept")] = (None, None)

    store_writer.forget_ensured("main", "t")

    assert first.ensured == {}
    assert second.ensured == {("main", "kept"): (None, None)}


@pytest.mark.asyncio
async def test_dropping_a_replica_forgets_its_row_cache_table(tmp_path):
    from provisa.federation.replica_address import ReplicaAddress
    from provisa.federation.replica_converge import drop_replica_table

    dropped: list[bool] = []

    class _Target:
        async def drop(self):
            dropped.append(True)

    class _Backend:
        def replica_address(self, state, *, source_id, schema_name, table_name):
            return ReplicaAddress("replicas", f"{source_id}__{schema_name}__{table_name}")

        def replica_target(self, state, *, address, args, engine):
            return _Target()

    state = types.SimpleNamespace(
        federation_engine=types.SimpleNamespace(engine=types.SimpleNamespace(backend=_Backend()))
    )
    store = store_writer.shared_store(f"sqlite:///{tmp_path}/a.db")
    store.ensured[("replicas", "pg1__public__orders")] = (None, None)

    await drop_replica_table(state, ("pg1", "public", "orders"))

    assert dropped == [True]
    assert store.ensured == {}


def test_dispose_shared_stores_closes_each_pool_and_forgets_it(tmp_path):
    dsn = f"sqlite:///{tmp_path}/s.db"
    first = store_writer.shared_store(dsn)
    with first.db.engine.connect():
        pass
    assert first.db.engine.pool.checkedin() == 1

    store_writer.dispose_shared_stores()

    assert first.db.engine.pool.checkedin() == 0
    assert store_writer.shared_store(dsn) is not first


def test_a_duckdb_store_is_never_given_a_pooled_handle(tmp_path):
    with pytest.raises(ValueError, match="never through a pooled store handle"):
        store_writer.shared_store(f"duckdb:///{tmp_path}/s.duckdb")
