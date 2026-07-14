# Copyright (c) 2026 Kenneth Stott
# Canary: 7e1a4d92-3c58-4b71-9a02-6d4c1f8b0e35
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-897: federation engines are declared-capability objects; the DECLARED traits
(reach/mpp/file_native/pooled/transactional/streaming + connector-level pushdown) are
first-class planner INPUTS. Each engine DECLARES its traits; the router/planner READS a
trait to decide, and an unset trait fails loud where a decision needs it.
"""

from __future__ import annotations

import pytest

from provisa.core.models import Source, SourceType
from provisa.federation.connector_base import Capability
from provisa.federation.engine import (
    DriverClass,
    EngineTraits,
    FederationEngine,
    UndeclaredTrait,
    build_bigquery_engine,
    build_clickhouse_engine,
    build_databricks_engine,
    build_duckdb_engine,
    build_fabric_engine,
    build_pg_engine,
    build_snowflake_engine,
    build_sqlalchemy_engine,
    build_synapse_engine,
    build_trino_engine,
)
from provisa.federation.plan import UnscannableSource, build_execution_plan
from provisa.transpiler.router import Route, decide_route


def _src(sid: str, type_: SourceType, **kw) -> Source:
    fields = {"host": "h", "port": 5432, "database": "db", "username": "u", **kw}
    return Source(id=sid, type=type_, **fields)


def _never_stale(_sid: str) -> bool:
    return False


_ALL_BUILDERS = [
    build_trino_engine,
    build_duckdb_engine,
    build_pg_engine,
    build_clickhouse_engine,
    build_snowflake_engine,
    build_databricks_engine,
    build_bigquery_engine,
    build_fabric_engine,
    build_synapse_engine,
    lambda: build_sqlalchemy_engine("postgresql://h/db"),
]


# ---- each engine DECLARES a complete trait descriptor (REQ-897) -------------


@pytest.mark.parametrize("build", _ALL_BUILDERS)
def test_every_engine_declares_a_full_traits_descriptor(build):
    traits = build().traits
    assert isinstance(traits, EngineTraits)
    assert isinstance(traits.reach, DriverClass)
    for flag in (
        traits.mpp,
        traits.file_native,
        traits.pooled,
        traits.transactional,
        traits.streaming,
    ):
        assert isinstance(flag, bool)


def test_traits_consolidate_established_reach_and_scale():
    trino = build_trino_engine().traits
    assert trino.reach is DriverClass.BROAD and trino.mpp is True
    duck = build_duckdb_engine().traits
    assert duck.reach is DriverClass.PARTIAL and duck.mpp is False
    sa = build_sqlalchemy_engine("postgresql://h/db").traits
    assert sa.reach is DriverClass.SELF_ONLY


def test_file_native_declared_per_engine():
    # File-scanning engines declare file_native; a land-only self-only engine does not.
    assert build_duckdb_engine().file_native is True
    assert build_trino_engine().file_native is True
    assert build_pg_engine().file_native is True
    assert build_sqlalchemy_engine("postgresql://h/db").file_native is False


def test_streaming_trait_derives_from_arrow_stream_transport():
    assert build_trino_engine().streaming is True  # advertises ARROW_STREAM
    assert build_sqlalchemy_engine("postgresql://h/db").streaming is False  # rows only


# ---- an undeclared trait fails loud where a decision needs it (REQ-897) ------


def test_reading_an_undeclared_trait_fails_loud():
    eng = FederationEngine(
        "adhoc", [], driver_class=DriverClass.PARTIAL
    )  # no storage traits declared
    with pytest.raises(UndeclaredTrait) as exc:
        _ = eng.file_native
    assert "adhoc" in str(exc.value) and "file_native" in str(exc.value)


def test_traits_descriptor_fails_loud_when_a_trait_is_unset():
    eng = FederationEngine(
        "adhoc", [], driver_class=DriverClass.PARTIAL, file_native=True, pooled=True
    )
    with pytest.raises(UndeclaredTrait):
        _ = eng.traits  # transactional never declared


# ---- the router READS the file_native trait to decide (REQ-897) -------------


def test_router_reads_file_native_to_route_a_file_source():
    file_engine = build_duckdb_engine()  # file_native=True
    land_engine = build_sqlalchemy_engine("postgresql://h/db")  # file_native=False
    args = dict(sources={"c"}, source_types={"c": "iceberg"}, source_dialects={})

    scan = decide_route(**args, engine=file_engine)
    land = decide_route(**args, engine=land_engine)

    assert scan.route is Route.ENGINE and land.route is Route.ENGINE
    # A file_native engine routes the file source DIFFERENTLY (scan in place) than a non-file_native
    # engine (must land) — the DECLARED trait drives the decision, not the source type alone.
    assert scan.reason != land.reason
    assert "scanned in place" in scan.reason
    assert "landed" in land.reason


def test_router_engine_agnostic_without_an_engine():
    # No engine bound → engine-agnostic decision (the trait is a planner input, never guessed).
    d = decide_route({"c"}, {"c": "iceberg"}, {})
    assert d.route is Route.ENGINE and d.reason == "virtual source (iceberg)"


# ---- connector-level pushdown trait gates a pushdown decision (REQ-897) ------


def test_connector_pushdown_is_the_declared_capability_input():
    cap = build_trino_engine().connector_pushdown("postgresql")
    assert isinstance(cap, Capability)
    assert cap.predicate_pushdown is True  # Trino pushes predicates to a postgres catalog


def test_pushdown_trait_gates_promotion_decision():
    from provisa.federation.cardinality import Estimate
    from provisa.federation.promote import PushdownDemand, should_promote

    # A reducing predicate the connector CANNOT push down, over a known-large scan → promote.
    weak = Capability(predicate_pushdown=False)
    strong = Capability(predicate_pushdown=True)
    demand = PushdownDemand(predicate=True)
    from provisa.federation.cardinality import CardinalityMethod

    big = Estimate(value=10_000_000, exact=False, method=CardinalityMethod.NATIVE_STAT)
    assert should_promote(weak, demand, big) is True
    assert should_promote(strong, demand, big) is False  # full pushdown → live read stays


# ---- the planner READS file_native live and fails loud on a SCAN gap --------


def test_plan_scan_source_ok_on_file_native_engine():
    # DuckDB (file_native) scanning a csv resolves to a plan with no error and an ENGINE route.
    plan = build_execution_plan(
        [_src("c", SourceType.csv, path="/c.csv")], build_duckdb_engine(), _never_stale
    )
    assert plan.prep == []


def test_plan_scan_on_non_file_native_engine_fails_loud():
    # A synthetic engine that yields a SCAN strategy but does NOT declare file_native must fail loud.
    duck = build_duckdb_engine()
    duck._file_native = None  # simulate a missing declaration on a SCAN-capable engine
    with pytest.raises((UnscannableSource, UndeclaredTrait)):
        build_execution_plan([_src("c", SourceType.csv, path="/c.csv")], duck, _never_stale)


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
