# Copyright (c) 2026 Kenneth Stott
# Canary: f1e716cd-4c95-434b-bac0-67f27597a59b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1162: END-TO-END through refresh_mv against a real (DuckDB-executing) engine.

The other suites test the SQL generator directly; this drives the actual production code path —
refresh_mv → _refresh_bitemporal → create/append — proving the wiring (first-refresh create, later
appends, drift rebuild) works when a real engine executes the SQL, not a mock that records it.
"""

from __future__ import annotations

import duckdb
import pytest

from provisa.executor.result import QueryResult
from provisa.mv.bitemporal import BitemporalSpec, reconstruct_as_of_sql
from provisa.mv.models import MVDefinition
from provisa.mv.refresh import _target_ref, refresh_mv
from provisa.mv.registry import MVRegistry


class DuckEngine:
    """A minimal FederationEngine stand-in that actually EXECUTES SQL on DuckDB."""

    dialect = "duckdb"
    native_store = "duckdb"

    def __init__(self, con: duckdb.DuckDBPyConnection):
        self.con = con

    async def execute_engine(self, sql: str, *_a, **_k) -> QueryResult:
        cur = self.con.execute(sql)
        desc = cur.description
        if desc is None:
            return QueryResult(rows=[], column_names=[])
        return QueryResult(rows=cur.fetchall(), column_names=[d[0] for d in desc])


def _mv(mode: str) -> MVDefinition:
    return MVDefinition(
        id="orders",
        source_tables=[],  # custom-SQL MV: no source introspection needed
        target_catalog="memory",
        target_schema="mvs",
        sql="SELECT id, region, amount FROM base",
        bitemporal=BitemporalSpec(key=("id",), mode=mode),
    )


async def _refresh(engine: DuckEngine, mv: MVDefinition, registry: MVRegistry) -> None:
    await refresh_mv(engine, mv, registry)


def _current(engine: DuckEngine, mv: MVDefinition) -> set[tuple]:
    sql = reconstruct_as_of_sql(_target_ref(mv), mv.bitemporal, ["id", "region", "amount"], None)
    return set(engine.con.execute(sql).fetchall())


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["snapshot", "delta"])
async def test_refresh_mv_bitemporal_create_then_append(mode):
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE base (id INTEGER, region VARCHAR, amount INTEGER)")
    engine, registry, mv = DuckEngine(con), MVRegistry(), _mv(mode)
    registry.register(mv)

    # First refresh → CREATE the versioned table.
    con.execute("INSERT INTO base VALUES (1,'west',10),(2,'east',20)")
    await _refresh(engine, mv, registry)
    assert _current(engine, mv) == {(1, "west", 10), (2, "east", 20)}

    # Second refresh → APPEND (update id=1, add id=3). History preserved.
    con.execute("DELETE FROM base")
    con.execute("INSERT INTO base VALUES (1,'west',15),(2,'east',20),(3,'north',30)")
    await _refresh(engine, mv, registry)
    assert _current(engine, mv) == {(1, "west", 15), (2, "east", 20), (3, "north", 30)}

    # The store is append-only: the target holds more rows than the current state (history kept).
    target = _target_ref(mv)
    row = con.execute(f"SELECT COUNT(*) FROM {target}").fetchone()
    assert row is not None and row[0] > 3


@pytest.mark.asyncio
async def test_refresh_mv_bitemporal_delta_handles_deletion_as_tombstone():
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE base (id INTEGER, region VARCHAR, amount INTEGER)")
    engine, registry, mv = DuckEngine(con), MVRegistry(), _mv("delta")
    registry.register(mv)

    con.execute("INSERT INTO base VALUES (1,'west',10),(2,'east',20)")
    await _refresh(engine, mv, registry)
    con.execute("DELETE FROM base")
    con.execute("INSERT INTO base VALUES (1,'west',10)")  # id=2 gone
    await _refresh(engine, mv, registry)

    assert _current(engine, mv) == {(1, "west", 10)}
    # a tombstone was appended, not a mutation — the op column carries a delete marker
    target = _target_ref(mv)
    ops = con.execute(f'SELECT DISTINCT "sys_op" FROM {target}').fetchall()
    assert ("delete",) in ops
