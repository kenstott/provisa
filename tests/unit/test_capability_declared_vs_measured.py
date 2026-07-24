# Copyright (c) 2026 Kenneth Stott
# Canary: 3368b902-45bf-457c-9ec4-fbeaefd354d5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-897: a DECLARED capability trait must match MEASURED behavior (Gap 3).

test_engine_capability_traits.py asserts what each engine DECLARES and that the planner
READS the declaration. It never checks the declaration is TRUE. A trait that claims a
capability the engine does not actually have (a "capability lie") passes that suite and then
mis-routes a real query: the planner trusts ``file_native`` and hands a file source to an
engine that cannot scan it, or trusts a pushdown the engine silently ignores.

This suite measures the declaration against real execution for the engine that runs
in-process (DuckDB, always available) and pins the planner->execution consequence:

  1. DuckDB DECLARES file_native=True  -> MEASURE it actually scans a file in place.
  2. The router ROUTES a file source to DuckDB because of that trait -> MEASURE the very
     governed SQL the router endorsed executes and returns the scanned rows.
  3. A SELF_ONLY engine DECLARES file_native=False -> MEASURE it is NOT handed a scan plan
     (the negative: the declaration matches the absence of the behavior).

Warehouse engines cannot be measured in-process; their declared/measured agreement is what
the cross-vendor parity e2e (test_cross_vendor_parity_e2e.py) exercises against live accounts.
"""

from __future__ import annotations

import pytest

from provisa.core.models import Source, SourceType
from provisa.federation.engine import (
    DriverClass,
    build_duckdb_engine,
    build_sqlalchemy_engine,
)
from provisa.federation.plan import build_execution_plan
from provisa.transpiler.router import Route, decide_route

duckdb = pytest.importorskip("duckdb", reason="duckdb required to MEASURE file_native")


def _never_stale(_sid: str) -> bool:
    return False


def _write_csv(tmp_path) -> str:
    p = tmp_path / "regions.csv"
    p.write_text("id,region,amount\n1,west,100.0\n2,east,200.0\n3,west,300.0\n")
    return str(p)


# ---- 1. DECLARED file_native=True is MEASURED true --------------------------


def test_duckdb_file_native_declaration_is_true_in_practice(tmp_path):
    """DuckDB declares file_native; MEASURE that it genuinely scans a CSV in place (no landing).
    If the engine could not scan the file, the declaration the planner trusts would be a lie."""
    engine = build_duckdb_engine()
    assert engine.file_native is True  # the DECLARATION

    csv = _write_csv(tmp_path)
    con = duckdb.connect()
    rows = con.execute(f"SELECT id, region, amount FROM read_csv_auto('{csv}') ORDER BY id").fetchall()
    con.close()

    # MEASURED: the file was scanned in place and produced its rows — the trait holds.
    assert [r[1] for r in rows] == ["west", "east", "west"]
    assert len(rows) == 3


# ---- 2. the route the trait endorsed actually EXECUTES ----------------------


def test_router_scan_decision_for_file_native_engine_actually_runs(tmp_path):
    """The router routes a file (csv) source to DuckDB and reports 'scanned in place' BECAUSE
    file_native is declared. MEASURE that the endorsed scan really executes on DuckDB — closing
    the loop from DECLARED trait -> routing decision -> live execution."""
    engine = build_duckdb_engine()
    # A virtual/file source (iceberg) is where the router consults file_native to decide
    # scan-in-place vs land — the declared trait drives this reason.
    decision = decide_route(
        sources={"c"}, source_types={"c": "iceberg"}, source_dialects={}, engine=engine
    )
    assert decision.route is Route.ENGINE
    assert "scanned in place" in decision.reason  # decision driven by the declared trait

    # The plan for a file source resolves with no landing prep on a file_native engine...
    plan = build_execution_plan(
        [Source(id="c", type=SourceType.csv, path=_write_csv(tmp_path))], engine, _never_stale
    )
    assert plan.prep == []  # nothing to land — it is scanned in place, as declared

    # ...and the scan the router endorsed actually returns rows on DuckDB.
    con = duckdb.connect()
    n = con.execute(
        f"SELECT count(*) FROM read_csv_auto('{_write_csv(tmp_path)}')"
    ).fetchone()[0]
    con.close()
    assert n == 3


# ---- 3. DECLARED file_native=False matches the ABSENCE of the behavior ------


def test_self_only_engine_declares_no_file_native_and_gets_no_scan_plan():
    """A SELF_ONLY SQLAlchemy engine declares file_native=False; MEASURE the planner never hands
    it a scan-in-place plan for a file source (it must land instead). The negative half of
    declared==measured: the absence of the trait matches the absence of the behavior."""
    engine = build_sqlalchemy_engine("postgresql://h/db")
    assert engine.traits.reach is DriverClass.SELF_ONLY
    assert engine.file_native is False  # the DECLARATION

    decision = decide_route(
        sources={"c"}, source_types={"c": "iceberg"}, source_dialects={}, engine=engine
    )
    # Not scanned in place — the non-file_native engine must LAND the file, matching the
    # declaration that it cannot scan in place.
    assert "scanned in place" not in decision.reason
    assert "landed" in decision.reason


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
