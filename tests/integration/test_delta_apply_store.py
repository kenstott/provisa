# Copyright (c) 2026 Kenneth Stott
# Canary: 1752a496-d179-48be-9d79-32bfd3b4b664
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-874: the delta apply path writes a SQL source's incremental rows into the replica store.

Exercises ``delta.apply_sql_delta`` end-to-end against a REAL DuckDB store at the store boundary:
the source read is stubbed (a fake ``execute_native`` returns the rows a generated SQL delta would),
and the keyed apply (``materialize_exec.apply_cdc`` over ``store_writer.store_connection``) runs for
real, so the test proves the upsert/tombstone application and the cursor advance, not a mock of them.
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from types import SimpleNamespace

import pytest

pytestmark = [pytest.mark.integration]

pytest.importorskip("duckdb")

from provisa.core.models import DeltaConfig  # noqa: E402
from provisa.executor.result import QueryResult  # noqa: E402
from provisa.federation import delta as _delta  # noqa: E402

_COLS = [
    ("id", "integer"),
    ("amount", "integer"),
    ("updated_at", "integer"),
    ("_deleted", "integer"),
]
_NAMES = [n for n, _ in _COLS]


class _FakeEngine:
    """Stands in for the EngineRuntime: its ``execute_native`` returns canned rows (the delta read)
    and answers the MAX(watermark) probe, capturing the generated SQL and bound params."""

    def __init__(self, delta_rows: list[tuple], max_wm: object | None = None) -> None:
        self._delta_rows = delta_rows
        self._max_wm = max_wm
        self.last_sql: str | None = None
        self.last_params: list | None = None

    async def execute_native(self, source_pools, source_id, sql, params):  # noqa: ANN001
        if sql.strip().upper().startswith("SELECT MAX"):
            return QueryResult(rows=[(self._max_wm,)], column_names=["m"])
        self.last_sql = sql
        self.last_params = params
        return QueryResult(rows=self._delta_rows, column_names=_NAMES)


def _ctx(store: Path):
    state = SimpleNamespace(source_pools=object())
    source = SimpleNamespace(id="pg")
    table = SimpleNamespace(
        table_name="orders",
        schema_name="public",
        watermark_column="updated_at",
        delta=DeltaConfig(apply="upsert", deletes="tombstone", tombstone_column="_deleted"),
    )
    args = SimpleNamespace(columns=_COLS, pk_columns=["id"])
    address = SimpleNamespace(schema="org_x_replicas", table="pg__public__orders")
    return state, source, table, args, address, f"duckdb:///{store}"


def _seed_replica(store: Path) -> None:
    """Create the replica schema + (empty) table, as a prior whole build would have (REQ-874: a
    delta only ever refreshes a replica that already exists)."""
    import duckdb

    con = duckdb.connect(str(store))
    try:
        con.execute('CREATE SCHEMA IF NOT EXISTS "org_x_replicas"')
        con.execute(
            'CREATE TABLE "org_x_replicas"."pg__public__orders" '
            '("id" INTEGER PRIMARY KEY, "amount" INTEGER, "updated_at" INTEGER, "_deleted" INTEGER)'
        )
    finally:
        con.close()


def _store_rows(store: Path) -> dict[int, int]:
    import duckdb

    con = duckdb.connect(str(store), read_only=True)
    try:
        rows = con.execute(
            'SELECT "id", "amount" FROM "org_x_replicas"."pg__public__orders" ORDER BY "id"'
        ).fetchall()
        return {int(r[0]): int(r[1]) for r in rows}
    finally:
        con.close()


async def _apply(engine, ctx, cursor):
    state, source, table, args, address, dsn = ctx
    return await _delta.apply_sql_delta(state, engine, source, table, args, address, cursor, dsn)


def test_delta_upserts_tombstones_and_advances_the_cursor():
    import asyncio

    with tempfile.TemporaryDirectory() as d:
        store = Path(d) / "mat.duckdb"
        _seed_replica(store)
        ctx = _ctx(store)

        # First delta from the empty cursor: two inserts. The generated query binds the cursor as a
        # parameter ($1), never spliced, and selects the registered fields ordered by the watermark.
        e1 = _FakeEngine([(1, 10, 1, 0), (2, 20, 2, 0)])
        applied, cursor = asyncio.run(_apply(e1, ctx, 0))
        assert applied == 2
        assert cursor == 2  # max(updated_at)
        assert e1.last_params == [0]
        assert e1.last_sql is not None
        assert '"updated_at" > $1' in e1.last_sql and 'ORDER BY "updated_at"' in e1.last_sql
        assert _store_rows(store) == {1: 10, 2: 20}

        # Second delta: id=2 updated (upsert), id=1 tombstoned (deleted). One transaction.
        e2 = _FakeEngine([(2, 25, 3, 0), (1, 0, 4, 1)])
        applied, cursor = asyncio.run(_apply(e2, ctx, cursor))
        assert applied == 2
        assert cursor == 4
        assert _store_rows(store) == {2: 25}  # id=1 gone, id=2 updated

        # Fresh read: empty result is a no-op that keeps the cursor and the store unchanged.
        e3 = _FakeEngine([])
        applied, cursor = asyncio.run(_apply(e3, ctx, cursor))
        assert (applied, cursor) == (0, 4)
        assert _store_rows(store) == {2: 25}


def test_source_max_watermark_reads_the_current_max():
    import asyncio

    state, source, table, _args, _addr, _dsn = _ctx(Path("/unused"))
    engine = _FakeEngine([], max_wm=99)
    assert asyncio.run(_delta.source_max_watermark(state, engine, source, table)) == 99
    engine_empty = _FakeEngine([], max_wm=None)
    assert asyncio.run(_delta.source_max_watermark(state, engine_empty, source, table)) is None
