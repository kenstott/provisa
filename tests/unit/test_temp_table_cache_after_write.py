# Copyright (c) 2026 Kenneth Stott
# Canary: 3e8b1f62-7c4d-4a09-b5e2-9d1a6c0f7b43
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-615, REQ-2176: a cached answer to a read of a session's temporary table is never served
after the session writes to that table.

An opted-in governed SELECT of the session's temporary table is answered, then served from the
response cache; an INSERT, an UPDATE, a DELETE, an ``INSERT INTO t SELECT ... FROM t`` or a COPY
into the table follows; the same SELECT is then read again and answers the table's new rows."""

# Requirements: REQ-615, REQ-2176

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from provisa.compiler import temp_tables
from provisa.compiler.temp_tables import action_of
from provisa.executor.result import QueryResult
from provisa.pgwire import temp_exec
from provisa.transpiler.router import Route
from tests.unit.test_buffered_auto_threshold import _Source
from tests.unit.test_cache_before_route import pipe  # noqa: F401  (the shared pipeline fixture)
from tests.unit.test_temp_table_dml_governed import _ROLE, Store

pytestmark = [pytest.mark.asyncio(loop_scope="session")]

_SQL = "-- @provisa cache=true\nSELECT x.id, x.note FROM t x ORDER BY x.id"


class _Engine(_Source):
    """The serving engine over the store: a read of the session's temporary table reads ``t`` as
    it stands, and a landing of it replaces ``t``'s rows (the store the platform writes)."""

    def __init__(self, store: Store) -> None:
        super().__init__(lambda _sql, _params: [])
        self._store = store
        self.engine = SimpleNamespace(
            name="duckdb",
            catalog_qualified=True,
            backend=SimpleNamespace(replica_target=lambda state, **_kw: None),
        )

    async def execute_engine(self, sql, params=None, **_kwargs):
        self.engine_statements.append(sql)
        rows = self._store.duck.execute("SELECT id, note FROM t ORDER BY id").fetchall()
        return QueryResult(rows=rows, column_names=["id", "note"])


class _Replicator:
    """The store's landing of a session table: ``t`` holds the batch's rows, nothing else."""

    def __init__(self, store: Store, source: Any) -> None:
        self._store = store
        self._source = source

    async def run(self, progress: Any) -> None:
        del progress
        self._store.duck.execute("DELETE FROM t")
        async for part in self._source.batches(1000):
            for row in part.to_pylist():
                self._store.duck.execute("INSERT INTO t VALUES (?, ?)", list(row.values()))


_REAL_LAND = temp_exec._land


@pytest.fixture
def held(pipe, monkeypatch):  # noqa: F811
    """The pipeline and the store, the platform's landing of ``t`` run as written (only the copy
    into the store stood in for)."""
    store = Store(monkeypatch)
    # The landing the platform runs, not the DML test's stand-in for it.
    monkeypatch.setattr(temp_exec, "_land", _REAL_LAND)
    monkeypatch.setattr(
        "provisa.federation.data_replicator.data_replicator",
        lambda source, target, party, **_kw: _Replicator(store, source),
    )
    store.session.stored = True
    pipe.state.federation_engine = _Engine(store)
    return pipe, store


async def _read(p, store: Store):
    token = temp_tables.bind(store.session)
    try:
        plan = await p.mod._govern_and_route(_SQL, "analyst", params=[], serve_cached=True)
        return plan.route, (await p.mod._execute_plan(plan, p.state)).rows
    finally:
        temp_tables.unbind(token)


async def _write(p, store: Store, sql: str) -> QueryResult:
    token = temp_tables.bind(store.session)
    try:
        action = action_of(sql)
        assert action is not None
        return await temp_exec.apply(action, store.governed(action.select), p.state, _ROLE)
    finally:
        temp_tables.unbind(token)


async def _cached(p, store: Store) -> list[tuple]:
    """Read ``t`` twice: the second answer is the cache's, the same as the first."""
    first_route, rows = await _read(p, store)
    assert first_route != Route.CACHE
    assert await _read(p, store) == (Route.CACHE, rows)
    return rows


@pytest.mark.parametrize(
    ("sql", "after"),
    [
        (
            "INSERT INTO t (id, note) VALUES (4, 'd')",
            [(1, "a@x.io"), (2, "b@x.io"), (3, None), (4, "d")],
        ),
        ("UPDATE t SET note = 'z' WHERE id = 2", [(1, "a@x.io"), (2, "z"), (3, None)]),
        ("DELETE FROM t WHERE id = 1", [(2, "b@x.io"), (3, None)]),
        (
            "INSERT INTO t (id, note) SELECT id + 10, note FROM t WHERE id < 3",
            [(1, "a@x.io"), (2, "b@x.io"), (3, None), (11, "a@x.io"), (12, "b@x.io")],
        ),
    ],
)
async def test_a_cached_read_of_a_temp_table_is_read_again_after_a_write_to_it(held, sql, after):
    p, store = held
    assert await _cached(p, store) == [(1, "a@x.io"), (2, "b@x.io"), (3, None)]
    await _write(p, store, sql)
    assert store.held() == after
    route, rows = await _read(p, store)
    assert (route, rows) != (Route.CACHE, [(1, "a@x.io"), (2, "b@x.io"), (3, None)])
    assert route != Route.CACHE and rows == after


async def test_a_cached_read_of_a_temp_table_is_read_again_after_a_copy_into_it(held):
    p, store = held
    assert await _cached(p, store) == [(1, "a@x.io"), (2, "b@x.io"), (3, None)]
    landed = await temp_exec.copy_into(
        store.session, "t", None, [["5", "e"]], p.state, binary=False
    )
    assert landed == 1
    route, rows = await _read(p, store)
    assert route != Route.CACHE and rows == [(1, "a@x.io"), (2, "b@x.io"), (3, None), (5, "e")]


async def test_a_read_after_the_write_is_cached_again_until_the_next_write(held):
    p, store = held
    await _cached(p, store)
    await _write(p, store, "DELETE FROM t WHERE id = 3")
    assert await _cached(p, store) == [(1, "a@x.io"), (2, "b@x.io")]
    await _write(p, store, "UPDATE t SET note = NULL")
    assert await _cached(p, store) == [(1, None), (2, None)]
