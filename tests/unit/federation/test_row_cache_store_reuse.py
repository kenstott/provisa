# Copyright (c) 2026 Kenneth Stott
# Canary: ac01ab09-f42a-426e-b2f1-e058acf7df1d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""A cold keyed read through the row cache reuses one pooled store handle per DSN and the table
it already ensured: no engine built, no new connection, no reflection when the table matches
(REQ-1865, REQ-1316). A changed column set re-ensures; a statement that fails forgets the table."""

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
async def test_a_failed_statement_forgets_the_table_and_the_next_read_ensures_it_again(env):
    from provisa.federation.query_residency import ensure_rows_resident

    await ensure_rows_resident(env.state, [_bound(1)], reader_role=None)
    with store_writer.shared_store(env.dsn).db.engine.begin() as conn:
        conn.execute(sa.text('DROP TABLE "pg1__public__orders"'))

    # The table was dropped behind the record: the read fails, raised as it is.
    with pytest.raises(sa.exc.OperationalError):
        await ensure_rows_resident(env.state, [_bound(2)], reader_role=None)
    assert ("main", "pg1__public__orders") not in store_writer.shared_store(env.dsn).ensured

    assert await ensure_rows_resident(env.state, [_bound(3)], reader_role=None) == [
        ("pg1", "orders", 1)
    ]
    assert env.reflections == 2


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
    with pytest.raises(ValueError, match="REQ-989"):
        store_writer.shared_store(f"duckdb:///{tmp_path}/s.duckdb")
