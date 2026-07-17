# Copyright (c) 2026 Kenneth Stott
# Canary: a955eb5c-d5c4-4d00-b43a-e5dbdf4f427f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-877 row-level MV delta capture + REQ-878 point-in-time reconstruction.

Drives the real append-only ledger on a shared SQLite control-plane catalog: a refresh diffs the
prior and freshly landed row sets into insert/update/delete events; reconstruction folds the ledger
to rebuild the view as-of a refresh version, forward from base and in reverse from live, both
yielding the identical as-of-N set; an unknown version fails loud; an opt-out MV writes no ledger.
"""

from __future__ import annotations

import pytest
from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import create_async_engine

from provisa.core.database import Database
from provisa.core.schema_org import materialized_views as MVT
from provisa.core.schema_org import metadata
from provisa.core.schema_org import mv_delta_ledger as LEDGER
from provisa.executor.result import QueryResult
from provisa.mv.delta import (
    capture_row_deltas,
    compute_deltas,
    reconstruct_forward,
    reconstruct_reverse,
)
from provisa.mv.models import MVDefinition
from provisa.mv.refresh import refresh_mv
from provisa.mv.registry import MVRegistry

MV_ID = "mv-orders"


def _mv(mv_id=MV_ID, *, capture=True, key=("id",), exclude=()):
    return MVDefinition(
        id=mv_id,
        source_tables=["orders"],
        target_catalog="postgresql",
        target_schema="mv_cache",
        target_table="mv_orders",
        sql="SELECT id, v FROM orders",
        consistency="distributed",  # bypass REQ-879 coordination — exercise the delta path directly
        capture_row_deltas=capture,
        delta_key=list(key),
        delta_exclude_columns=list(exclude),
    )


@pytest.fixture
async def store(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'cp.db'}")
    async with engine.begin() as c:
        await c.run_sync(lambda s: metadata.create_all(s, tables=[MVT, LEDGER]))
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


async def _ledger_rows(store, version=None):
    stmt = select(LEDGER).where(LEDGER.c.mv_id == MV_ID)
    if version is not None:
        stmt = stmt.where(LEDGER.c.refresh_version == version)
    stmt = stmt.order_by(LEDGER.c.id)
    async with store.acquire() as conn:
        res = await conn.execute_core(stmt)
        return [r._mapping for r in res.fetchall()]


# Two-refresh scenario reused across reconstruction tests.
V1 = [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}, {"id": 3, "v": "c"}]
V2 = [
    {"id": 1, "v": "a"},
    {"id": 2, "v": "B"},
    {"id": 4, "v": "d"},
]  # 2 updated, 3 deleted, 4 inserted


def _by_id(rows):
    return {r["id"]: r["v"] for r in rows}


# ---- compute_deltas (pure) ----------------------------------------------------------------------


def test_compute_deltas_classifies_ins_upd_del():
    events = compute_deltas(V1, V2, ["id"], frozenset())
    kinds = {e.change_type for e in events}
    assert kinds == {"insert", "update", "delete"}
    by_type = {e.change_type: e for e in events}
    assert by_type["update"].old_values["v"] == "b"
    assert by_type["update"].new_values["v"] == "B"
    assert by_type["delete"].old_values["id"] == 3
    assert by_type["insert"].new_values["id"] == 4


def test_compute_deltas_excluded_column_not_a_change():
    prev = [{"id": 1, "v": "a", "updated_at": "t0"}]
    curr = [{"id": 1, "v": "a", "updated_at": "t1"}]
    assert compute_deltas(prev, curr, ["id"], frozenset({"updated_at"})) == []
    assert len(compute_deltas(prev, curr, ["id"], frozenset())) == 1


# ---- capture_row_deltas (ledger) ----------------------------------------------------------------


async def test_capture_records_per_refresh_version(store):
    mv = _mv()
    v1 = await capture_row_deltas(store, mv, [], V1)
    v2 = await capture_row_deltas(store, mv, V1, V2)
    assert (v1, v2) == (1, 2)
    r1 = await _ledger_rows(store, 1)
    assert [r["change_type"] for r in r1] == ["insert", "insert", "insert"]
    r2 = await _ledger_rows(store, 2)
    assert sorted(r["change_type"] for r in r2) == ["delete", "insert", "update"]


async def test_capture_no_change_records_no_events(store):
    mv = _mv()
    await capture_row_deltas(store, mv, [], V1)
    v = await capture_row_deltas(store, mv, V1, V1)  # identical snapshot
    assert v == 2
    assert await _ledger_rows(store, 2) == []


async def test_opt_out_mv_writes_no_ledger(store):
    mv = _mv(capture=False)
    assert await capture_row_deltas(store, mv, [], V1) is None
    assert await _ledger_rows(store) == []


async def test_capture_without_key_fails_loud(store):
    mv = _mv(key=())
    with pytest.raises(ValueError, match="requires a non-empty delta_key"):
        await capture_row_deltas(store, mv, [], V1)


# ---- reconstruction (REQ-878) -------------------------------------------------------------------


async def test_forward_fold_reconstructs_as_of_version(store):
    mv = _mv()
    await capture_row_deltas(store, mv, [], V1)
    await capture_row_deltas(store, mv, V1, V2)

    as_of_1 = await reconstruct_forward(store, mv, 1)
    assert _by_id(as_of_1) == _by_id(V1)

    as_of_2 = await reconstruct_forward(store, mv, 2)
    assert _by_id(as_of_2) == _by_id(V2)


async def test_reverse_from_live_reconstructs_as_of_version(store):
    mv = _mv()
    await capture_row_deltas(store, mv, [], V1)
    await capture_row_deltas(store, mv, V1, V2)

    # Live table == V2. Reverse back to version 1 must equal the forward fold (resurrecting the
    # deleted row 3, restoring the updated row 2, dropping the after-N insert row 4).
    as_of_1 = await reconstruct_reverse(store, mv, 1, live_rows=V2)
    assert _by_id(as_of_1) == _by_id(V1)

    # Reverse to the live version is a no-op — identical to live.
    as_of_2 = await reconstruct_reverse(store, mv, 2, live_rows=V2)
    assert _by_id(as_of_2) == _by_id(V2)


async def test_reconstruct_unknown_version_fails_loud(store):
    mv = _mv()
    await capture_row_deltas(store, mv, [], V1)
    with pytest.raises(ValueError, match="no such version"):
        await reconstruct_forward(store, mv, 99)
    with pytest.raises(ValueError, match="no such version"):
        await reconstruct_reverse(store, mv, 99, live_rows=V1)


# ---- refresh_mv wiring ---------------------------------------------------------------------------


class _FakeEngine:
    """Drives refresh_mv without a real engine: COUNT probes, a target that starts absent then holds
    ``curr`` rows, and SELECT * returning the staged snapshot for the delta diff."""

    def __init__(self, curr):
        self._curr = curr
        self._exists = False
        self.sqls: list[str] = []

    async def execute_engine(self, sql, *a, **k):
        self.sqls.append(sql)
        s = sql.strip()
        if s.startswith("CREATE SCHEMA"):
            return QueryResult(rows=[], column_names=[])
        if "COUNT(*)" in s:
            return QueryResult(rows=[(len(self._curr),)], column_names=[])
        if s.startswith("SELECT * FROM") and "LIMIT 0" in s:
            if not self._exists:
                raise RuntimeError("no such table")
            return QueryResult(rows=[], column_names=["id", "v"])
        if s.startswith("SELECT * FROM"):
            return QueryResult(
                rows=[(r["id"], r["v"]) for r in self._curr], column_names=["id", "v"]
            )
        if s.startswith("CREATE TABLE"):
            self._exists = True
        return QueryResult(rows=[], column_names=[])


async def test_refresh_mv_captures_deltas_when_opted_in(store):
    mv = _mv()
    reg = MVRegistry()
    reg.register(mv)
    await refresh_mv(_FakeEngine(V1), mv, reg, store=store)
    rows = await _ledger_rows(store, 1)
    assert [r["change_type"] for r in rows] == ["insert", "insert", "insert"]


async def test_refresh_mv_opt_out_writes_no_ledger(store):
    mv = _mv(capture=False)
    reg = MVRegistry()
    reg.register(mv)
    await refresh_mv(_FakeEngine(V1), mv, reg, store=store)
    assert await _ledger_rows(store) == []
