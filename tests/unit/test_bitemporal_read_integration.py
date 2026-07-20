# Copyright (c) 2026 Kenneth Stott
# Canary: daed223d-216f-41d6-8756-c886f10026aa
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1162 + REQ-1163: read-path INTEGRATION for bitemporal MVs.

The other suites test each piece; this drives the whole read chain end-to-end through the REAL
components (no mocks of the compiler): refresh_mv materializes the append log on a DuckDB engine, the
view's inline-expansion entry is the view_read_sql reconstruction (as app_rebuild wires it), a query
referencing the view is put through the REAL expand_views, and the expanded SQL is executed on the
engine. It asserts current-by-default reads and X-Provisa-As-Of time-travel reads return the right
rows — the live create->materialize->query path, minus only the HTTP/control-plane layer.
"""

from __future__ import annotations

import duckdb
import pytest

import provisa.mv.refresh as refresh_mod
from provisa.compiler.sql_gen import CompiledQuery
from provisa.compiler.view_expand import expand_views
from provisa.executor.result import QueryResult
from provisa.mv.bitemporal import BitemporalSpec, as_of_view_map, parse_as_of, view_read_sql
from provisa.mv.models import MVDefinition
from provisa.mv.refresh import _target_ref, refresh_mv
from provisa.mv.registry import MVRegistry

VIEW = "orders_v"
VIEW_REF = f'"cat"."public"."{VIEW}"'  # how a query references the registered view
BUSINESS = ["id", "region", "amount"]

STEP1 = [(1, "west", 10), (2, "east", 20)]
STEP2 = [(1, "west", 15), (2, "east", 20), (3, "north", 30)]
STEP3 = [(1, "west", 15), (3, "north", 30)]  # id=2 deleted
TS = ["2026-01-01 00:00:00.000000", "2026-02-01 00:00:00.000000", "2026-03-01 00:00:00.000000"]


class DuckEngine:
    dialect = "duckdb"
    native_store = "duckdb"

    def __init__(self, con):
        self.con = con

    async def execute_engine(self, sql, *_a, **_k):
        cur = self.con.execute(sql)
        desc = cur.description
        if desc is None:
            return QueryResult(rows=[], column_names=[])
        return QueryResult(rows=cur.fetchall(), column_names=[d[0] for d in desc])


def _query(view_sql_map: dict[str, str]) -> str:
    """A compiled query that selects the business columns from the view, put through real expand."""
    cols = ", ".join(f'"t0"."{c}"' for c in BUSINESS)
    compiled = CompiledQuery(
        sql=f"SELECT {cols} FROM {VIEW_REF} \"t0\"",
        params=[],
        root_field=VIEW,
        columns=[],
        sources={"cat"},
    )
    return expand_views(compiled, view_sql_map).sql


async def _materialize(monkeypatch, mode: str):
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE base (id INTEGER, region VARCHAR, amount INTEGER)")
    engine, registry = DuckEngine(con), MVRegistry()
    mv = MVDefinition(
        id=VIEW,
        source_tables=[],
        target_catalog="memory",
        target_schema="mvs",
        sql="SELECT id, region, amount FROM base",
        bitemporal=BitemporalSpec(key=("id",), mode=mode),
    )
    registry.register(mv)

    # Deterministic system-time stamps so as-of boundaries are known.
    stamps = iter(f"TIMESTAMP '{t}'" for t in TS)
    monkeypatch.setattr(refresh_mod, "_now_ts_literal", lambda: next(stamps))

    for rows in (STEP1, STEP2, STEP3):
        con.execute("DELETE FROM base")
        for r in rows:
            con.execute("INSERT INTO base VALUES (?, ?, ?)", r)
        await refresh_mv(engine, mv, registry)

    # app_rebuild wires the view's inline-expansion entry to the reconstruction over the append log.
    spec = mv.bitemporal
    mv_ref = _target_ref(mv)
    view_sql_map = {VIEW: view_read_sql(mv_ref, spec)}
    bitemporal_reads = {VIEW: (mv_ref, spec)}
    return con, view_sql_map, bitemporal_reads, spec


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["snapshot", "delta"])
async def test_current_read_through_expand_and_engine(monkeypatch, mode):
    con, view_sql_map, _reads, _spec = await _materialize(monkeypatch, mode)
    sql = _query(view_sql_map)
    assert "(" in sql and VIEW_REF not in sql  # the view ref was expanded into a subquery
    assert set(con.execute(sql).fetchall()) == {(1, "west", 15), (3, "north", 30)}


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["snapshot", "delta"])
async def test_as_of_read_through_overlay_expand_and_engine(monkeypatch, mode):
    con, view_sql_map, reads, _spec = await _materialize(monkeypatch, mode)
    # X-Provisa-As-Of: 2026-02-01 → the state after STEP2 (id=2 still present, id=1 updated to 15)
    as_of_ts = parse_as_of("2026-02-01T00:00:00")
    overlaid = as_of_view_map(view_sql_map, reads, as_of_ts)
    sql = _query(overlaid)
    assert set(con.execute(sql).fetchall()) == {(1, "west", 15), (2, "east", 20), (3, "north", 30)}

    # And as-of the very first refresh → the original rows.
    sql_t1 = _query(as_of_view_map(view_sql_map, reads, parse_as_of("2026-01-01T00:00:00")))
    assert set(con.execute(sql_t1).fetchall()) == {(1, "west", 10), (2, "east", 20)}
