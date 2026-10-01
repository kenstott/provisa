# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""REQ-1911 knob: data-source selection — a weight per registered source (and per table within a
source) from which each request's table is drawn; per-transport source weights."""

from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

import pytest
import yaml

BENCH = Path(__file__).resolve().parents[3] / "demo" / "named" / "perf" / "bench"
sys.path.insert(0, str(BENCH))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import contract_checks  # noqa: E402
import contract_model  # noqa: E402
import deployment_fixture as fx  # noqa: E402
import optimistic_load as ol  # noqa: E402
import request_mix  # noqa: E402
import request_render  # noqa: E402
import setup_contract as sc  # noqa: E402
from queries import OPTIMISTIC  # noqa: E402

PERF_CONTRACT = BENCH / "setups" / "perf-stack.yaml"
ENV = {"PROVISA_HTTP_BASE_URL": "http://localhost:8001"}
TRANSPORTS = list(ol.TRANSPORTS)
SOURCES = ["bench-postgresql", "bench-clickhouse", "bench-mongodb", "bench-neo4j"]
# transports that can express each source's table, from the spellings the contract declares
SQL_AND_GRAPHQL = ["graphql", "data_sql", "pgwire", "flight_sql"]


def _weights(raw: dict[str, Any], **w: float) -> None:
    raw["knobs"]["source_weights"] = {s: w.get(s.split("-")[1], 0.0) for s in SOURCES}


def _only_pg_elsewhere(raw: dict[str, Any], transports: list[str]) -> None:
    """Give each transport an override that sends everything to Postgres."""
    for t in transports:
        raw["transports"][t] = {
            "knobs": {
                "source_weights": {s: 1.0 if s == "bench-postgresql" else 0.0 for s in SOURCES}
            }
        }


def _setup(tmp_path: Path, mutate: Any = None, deployment: Any = None) -> contract_model.Setup:
    raw = json.loads(json.dumps(yaml.safe_load(PERF_CONTRACT.read_text())))
    if mutate:
        mutate(raw)
    path = tmp_path / "s.yaml"
    path.write_text(yaml.safe_dump(raw, sort_keys=False))
    return sc.load_setup(
        path, environ=ENV, known_transports=TRANSPORTS, deployment=deployment or fx.build()
    )


# ------------------------------------------------------------------ the contract


def test_a_table_a_transport_does_not_expose_has_no_spelling_there(tmp_path: Path) -> None:
    setup = _setup(tmp_path, deployment=fx.build(drop={"order_events": {"rest", "grpc"}}))
    events = setup.sources["bench-clickhouse"].tables[0]
    assert events.sql == "perf_bench.order_events" and events.graphql_field == "pb__orderEvents"
    assert events.cypher_label == "PerfBench:OrderEvents"
    assert events.rest_path is None and events.grpc_type_name is None


def test_a_weighted_source_must_be_exposed_on_every_transport(tmp_path: Path) -> None:
    deployment = fx.build(drop={"order_events": {"grpc"}})
    with pytest.raises(
        contract_model.SetupError,
        match="transports.grpc: source bench-clickhouse table order_events is not exposed in grpc by the deployment",
    ):
        _setup(
            tmp_path, lambda r: _weights(r, postgresql=0.5, clickhouse=0.5), deployment=deployment
        )


def test_a_transport_override_can_exclude_the_unreachable_source(tmp_path: Path) -> None:
    def mutate(raw: dict[str, Any]) -> None:
        _weights(raw, postgresql=0.5, clickhouse=0.5)
        _only_pg_elsewhere(raw, ["cypher_http", "bolt", "grpc", "rest", "jsonapi"])

    setup = _setup(tmp_path, mutate)
    assert setup.source_weights("graphql")["bench-clickhouse"] == 0.5
    assert setup.source_weights("rest")["bench-clickhouse"] == 0.0


def test_override_weights_are_validated(tmp_path: Path) -> None:
    def mutate(raw: dict[str, Any]) -> None:
        raw["transports"]["graphql"] = {
            "knobs": {
                "source_weights": {
                    "bench-postgresql": 0.3,
                    "bench-clickhouse": 0.3,
                    "bench-mongodb": 0.0,
                    "bench-neo4j": 0.0,
                }
            }
        }

    with pytest.raises(
        contract_model.SetupError,
        match="transports.graphql.knobs.source_weights: weights sum to 0.6",
    ):
        _setup(tmp_path, mutate)


def test_table_weights_must_sum_to_one_in_a_source(tmp_path: Path) -> None:
    def mutate(raw: dict[str, Any]) -> None:
        raw["sources"]["bench-postgresql"]["tables"][0]["weight"] = 0.5

    with pytest.raises(
        contract_model.SetupError, match="tables: weights sum to 0.5, must sum to 1"
    ):
        _setup(tmp_path, mutate)


def test_a_weighted_source_needs_a_table(tmp_path: Path) -> None:
    def mutate(raw: dict[str, Any]) -> None:
        raw["sources"]["bench-clickhouse"]["tables"] = []
        raw["cross_source_joins"] = []
        _weights(raw, postgresql=0.5, clickhouse=0.5)

    with pytest.raises(
        contract_model.SetupError,
        match="knobs.source_weights: source bench-clickhouse has weight but no table",
    ):
        _setup(tmp_path, mutate)


def test_source_weights_knob_is_no_longer_refused(tmp_path: Path) -> None:
    def mutate(raw: dict[str, Any]) -> None:
        _weights(raw, postgresql=0.5, clickhouse=0.5)
        _only_pg_elsewhere(raw, ["cypher_http", "bolt", "grpc", "rest", "jsonapi"])

    assert _setup(tmp_path, mutate).knobs.source_weights["bench-clickhouse"] == 0.5


# ------------------------------------------------------------------ the draw


def test_zero_knob_always_reads_postgres_orders(tmp_path: Path) -> None:
    gen = request_mix.RequestGenerator(_setup(tmp_path), "graphql", "s", cacheable=True)
    assert {(s.source, s.table) for s in (gen.next() for _ in range(50))} == {
        ("bench-postgresql", "orders")
    }


def _mixed(raw: dict[str, Any]) -> None:
    _weights(raw, postgresql=0.5, clickhouse=0.3, mongodb=0.2)
    _only_pg_elsewhere(raw, ["cypher_http", "bolt", "grpc", "rest", "jsonapi"])


def test_sources_follow_the_weights(tmp_path: Path) -> None:
    gen = request_mix.RequestGenerator(_setup(tmp_path, _mixed), "graphql", "s", cacheable=True)
    counts = Counter(gen.next().source for _ in range(5000))
    assert set(counts) == {"bench-postgresql", "bench-clickhouse", "bench-mongodb"}
    assert abs(counts["bench-postgresql"] / 5000 - 0.5) < 0.04
    assert abs(counts["bench-clickhouse"] / 5000 - 0.3) < 0.04


def test_a_transport_never_draws_a_source_its_override_excludes(tmp_path: Path) -> None:
    gen = request_mix.RequestGenerator(_setup(tmp_path, _mixed), "rest", "s", cacheable=False)
    assert {gen.next().source for _ in range(300)} == {"bench-postgresql"}


def test_tables_within_a_source_follow_the_table_weights(tmp_path: Path) -> None:
    def mutate(raw: dict[str, Any]) -> None:
        tables = raw["sources"]["bench-postgresql"]["tables"]
        tables[0]["weight"] = 0.75  # orders
        tables[1]["weight"] = 0.25  # order_items

    gen = request_mix.RequestGenerator(_setup(tmp_path, mutate), "pgwire", "s", cacheable=True)
    counts = Counter(gen.next().table for _ in range(4000))
    assert abs(counts["orders"] / 4000 - 0.75) < 0.04 and counts["order_items"] > 0


def test_columns_and_filters_come_from_the_drawn_table(tmp_path: Path) -> None:
    def mutate(raw: dict[str, Any]) -> None:
        _mixed(raw)
        raw["knobs"]["fields"] = {
            "probability": 1,
            "distribution": {"kind": "constant", "value": 2},
        }
        raw["knobs"]["filters"] = {
            "probability": 1,
            "distribution": {"kind": "constant", "value": 1},
        }

    gen = request_mix.RequestGenerator(_setup(tmp_path, mutate), "graphql", "s", cacheable=True)
    setup = _setup(tmp_path, mutate)
    tables = {(sid, t.name): t for sid, s in setup.sources.items() for t in s.tables}
    for _ in range(300):
        spec = gen.next()
        table = tables[(spec.source, spec.table)]
        names = {c.name for c in table.columns}
        assert set(spec.columns) <= names
        ((col, value),) = spec.filters
        assert col in {c.name for c in contract_checks.filter_columns(table)}


def test_same_seed_same_sequence_and_independent_of_other_knobs(tmp_path: Path) -> None:
    def with_cache(raw: dict[str, Any]) -> None:
        _mixed(raw)
        raw["knobs"]["cache"]["probability"] = 0.5

    a = request_mix.RequestGenerator(_setup(tmp_path, _mixed), "graphql", "x/1/1", cacheable=True)
    b = request_mix.RequestGenerator(_setup(tmp_path, _mixed), "graphql", "x/1/1", cacheable=True)
    c = request_mix.RequestGenerator(
        _setup(tmp_path, with_cache), "graphql", "x/1/1", cacheable=True
    )
    sa = [(s.source, s.table) for s in (a.next() for _ in range(100))]
    assert sa == [(s.source, s.table) for s in (b.next() for _ in range(100))]
    assert sa == [(s.source, s.table) for s in (c.next() for _ in range(100))]


def test_base_spec_is_per_transport(tmp_path: Path) -> None:
    def mutate(raw: dict[str, Any]) -> None:
        _weights(raw, clickhouse=1.0)
        _only_pg_elsewhere(raw, ["cypher_http", "bolt", "grpc", "rest", "jsonapi"])

    setup = _setup(tmp_path, mutate)
    assert (
        request_mix.RequestGenerator(setup, "graphql", "s", cacheable=True)
        .base_spec(True, "org_admin")
        .source
        == "bench-clickhouse"
    )
    assert (
        request_mix.RequestGenerator(setup, "rest", "s", cacheable=False)
        .base_spec(False, "org_admin")
        .source
        == "bench-postgresql"
    )


# ------------------------------------------------------------------ the text each transport sends


def test_zero_knob_still_equals_optimistic(tmp_path: Path) -> None:
    q = request_render.build_query(_setup(tmp_path), "graphql", cached=True)
    for attr in ("sql", "cypher", "graphql", "grpc", "rest", "jsonapi"):
        assert getattr(q, attr) == getattr(OPTIMISTIC, attr), attr


def test_render_other_sources(tmp_path: Path) -> None:
    r = request_render.Renderer(_setup(tmp_path))
    ev = r.render(
        request_mix.RequestSpec(
            "bench-clickhouse", "order_events", ("event_id", "event_type"), 5, False
        )
    )
    assert ev.sql == "SELECT event_id, event_type FROM perf_bench.order_events LIMIT 5"
    assert ev.graphql == "query { pb__orderEvents(limit: 5) { eventId eventType } }"
    assert (
        ev.cypher
        == "MATCH (o:PerfBench:OrderEvents) RETURN o.eventId AS event_id, o.eventType AS event_type LIMIT 5"
    )
    assert ev.grpc is not None and ev.grpc["type_name"] == "PbOrderEvents"
    assert ev.rest is not None and ev.rest["path"] == "/data/rest/perf-bench/order_events"
    assert ev.jsonapi is not None and ev.jsonapi["path"] == "/data/jsonapi/perf-bench/order_events"
    node = r.render(
        request_mix.RequestSpec("bench-neo4j", "bench_order_node", ("order_id",), 1, True)
    )
    assert (
        node.sql
        == "-- @provisa cache=true\nSELECT order_id FROM perf_bench.bench_order_node LIMIT 1"
    )
    assert node.cypher == (
        "// @provisa cache=true\nMATCH (b:PerfBench:Order) RETURN b.orderId AS order_id LIMIT 1"
    )
    assert node.graphql == "query @cached { pb__order(limit: 1) { orderId } }"


# ------------------------------------------------------------------ the report


def _rec(latency: float, source: str) -> ol.Record:
    return (latency, request_mix.RequestSpec(source, "t", ("a",), 1, False), None, 1, 1)


def test_summary_by_source() -> None:
    out = ol.summarize_requests(
        [_rec(0.001, "bench-postgresql")] * 6 + [_rec(0.030, "bench-clickhouse")] * 4, window_s=2.0
    )
    assert list(out["by_source"]) == ["bench-clickhouse", "bench-postgresql"]
    assert out["by_source"]["bench-clickhouse"]["p50_ms"] == 30.0
    assert out["by_source"]["bench-postgresql"]["requests"] == 6
