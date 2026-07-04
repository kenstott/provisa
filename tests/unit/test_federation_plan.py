# Copyright (c) 2026 Kenneth Stott
# Canary: 8d2c9a40-6b18-4e75-9f02-1c7a0d4f9b98
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-825: PLAN stage — ordered plan (prep phase + terminal route)."""

from __future__ import annotations

from provisa.core.models import Source, SourceType
from provisa.federation.engine import build_duckdb_engine, build_trino_engine
from provisa.federation.plan import Route, Strategy, build_execution_plan


def _src(sid: str, type_: SourceType, **kw) -> Source:
    return Source(id=sid, type=type_, host="h", port=1, database="d", username="u", **kw)


def _never_stale(_sid: str) -> bool:
    return False


def _always_stale(_sid: str) -> bool:
    return True


# ---- route selection --------------------------------------------------------


def test_single_virtual_source_routes_direct():
    plan = build_execution_plan(
        [_src("pg", SourceType.postgresql)], build_trino_engine(), _never_stale
    )
    assert plan.route is Route.DIRECT
    assert plan.prep == []


def test_multiple_sources_route_to_engine():
    sources = [_src("pg", SourceType.postgresql), _src("my", SourceType.mysql)]
    plan = build_execution_plan(sources, build_trino_engine(), _never_stale)
    assert plan.route is Route.ENGINE


def test_single_scan_source_routes_to_engine():
    # A csv SCAN is not a live native source → engine, not direct.
    plan = build_execution_plan(
        [_src("c", SourceType.csv, path="/c.csv")], build_duckdb_engine(), _never_stale
    )
    assert plan.route is Route.ENGINE


def test_single_materialized_source_routes_to_engine():
    plan = build_execution_plan(
        [_src("api", SourceType.openapi, base_url="http://x")], build_trino_engine(), _never_stale
    )
    assert plan.route is Route.ENGINE


# ---- residency prep phase ---------------------------------------------------


def test_stale_materialized_source_emits_prep_step():
    api = _src("api", SourceType.openapi, base_url="http://x")
    plan = build_execution_plan([api], build_trino_engine(), _always_stale)
    assert [p.source_id for p in plan.prep] == ["api"]
    assert plan.prep[0].strategy is Strategy.MATERIALIZED


def test_fresh_materialized_source_emits_no_prep():
    api = _src("api", SourceType.openapi, base_url="http://x")
    plan = build_execution_plan([api], build_trino_engine(), _never_stale)
    assert plan.prep == []


def test_virtual_and_scan_sources_never_need_prep():
    pg = _src("pg", SourceType.postgresql)
    csv = _src("c", SourceType.csv, path="/c.csv")
    # Even with is_stale=True, VIRTUAL/SCAN carry no residency.
    plan = build_execution_plan([pg, csv], build_duckdb_engine(), _always_stale)
    assert plan.prep == []


def test_mixed_query_preps_only_the_materialized_stale_source():
    pg = _src("pg", SourceType.postgresql)  # VIRTUAL
    api = _src("api", SourceType.openapi, base_url="http://x")  # MATERIALIZED
    plan = build_execution_plan([pg, api], build_trino_engine(), _always_stale)
    assert [p.source_id for p in plan.prep] == ["api"]
    assert plan.route is Route.ENGINE  # >1 source


def test_prep_respects_per_source_staleness():
    api1 = _src("a1", SourceType.openapi, base_url="http://x")
    api2 = _src("a2", SourceType.mongodb)
    plan = build_execution_plan([api1, api2], build_trino_engine(), lambda sid: sid == "a1")
    assert [p.source_id for p in plan.prep] == ["a1"]  # only the stale one
