# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""REQ-1911 knobs: number of joins, same-source and cross-source, drawn from the contract's join
pairs."""

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
NO_JOINS = ["grpc", "rest", "jsonapi"]  # single-table surfaces: they cannot express a join
ZERO = {"probability": 0, "distribution": {"kind": "constant", "value": 0}}


def _const(n: int, p: float = 1.0) -> dict[str, Any]:
    return {"probability": p, "distribution": {"kind": "constant", "value": n}}


def _no_joins_on_single_table_surfaces(raw: dict[str, Any], **extra: Any) -> None:
    for t in NO_JOINS:
        knobs = {"joins_same_source": ZERO, "joins_cross_source": ZERO, **extra}
        raw["transports"][t] = {"knobs": knobs}


def _same(raw: dict[str, Any], n: int = 1, p: float = 1.0) -> None:
    raw["knobs"]["joins_same_source"] = _const(n, p)
    _no_joins_on_single_table_surfaces(raw)


def _cross(raw: dict[str, Any], n: int = 1, spread: bool = True) -> None:
    """Cross-source joins; sources spread so the join targets have weight."""
    raw["knobs"]["joins_cross_source"] = _const(n)
    if spread:
        raw["knobs"]["source_weights"] = {
            "bench-postgresql": 0.5,
            "bench-clickhouse": 0.25,
            "bench-mongodb": 0.25,
            "bench-neo4j": 0.0,
        }
    pg_only = {s: 1.0 if s == "bench-postgresql" else 0.0 for s in SOURCES}
    _no_joins_on_single_table_surfaces(raw, source_weights=pg_only)


def _setup(tmp_path: Path, mutate: Any = None) -> contract_model.Setup:
    raw = json.loads(json.dumps(yaml.safe_load(PERF_CONTRACT.read_text())))
    if mutate:
        mutate(raw)
    path = tmp_path / "s.yaml"
    path.write_text(yaml.safe_dump(raw, sort_keys=False))
    return sc.load_setup(path, environ=ENV, known_transports=TRANSPORTS, deployment=fx.build())


def _gen(
    setup: contract_model.Setup, transport: str, stream: str = "s"
) -> request_mix.RequestGenerator:
    return request_mix.RequestGenerator(setup, transport, stream, cacheable=transport != "rest")


# ------------------------------------------------------------------ the contract


def test_perf_contract_declares_the_join_pairs(tmp_path: Path) -> None:
    setup = _setup(tmp_path)
    (items,) = setup.sources["bench-postgresql"].joins
    assert (items.left_table, items.right_table) == ("orders", "order_items")
    assert (items.graphql_field, items.cypher_rel) == ("orderItems", "HAS_ITEM")
    assert [(j.right_source, j.graphql_field, j.cypher_rel) for j in setup.cross_source_joins] == [
        ("bench-clickhouse", "orderEvents", "HAS_EVENT"),
        ("bench-mongodb", "orderDoc", "HAS_DOC"),
    ]


def test_join_knobs_are_no_longer_refused(tmp_path: Path) -> None:
    def mutate(raw: dict[str, Any]) -> None:
        _same(raw)
        _cross(raw)

    setup = _setup(tmp_path, mutate)
    assert setup.knobs.distributions["joins_same_source"].probability == 1.0


def test_joins_cannot_be_expressed_on_single_table_surfaces(tmp_path: Path) -> None:
    def mutate(raw: dict[str, Any]) -> None:
        raw["knobs"]["joins_same_source"] = _const(1)

    with pytest.raises(
        contract_model.SetupError, match="transports.grpc: joins cannot be expressed on grpc"
    ):
        _setup(tmp_path, mutate)


def test_a_distribution_beyond_what_the_pairs_reach(tmp_path: Path) -> None:
    with pytest.raises(
        contract_model.SetupError,
        match="knobs.joins_same_source.distribution: draws up to 2 same-source joins; no table "
        "drawn on graphql reaches more than 1",
    ):
        _setup(tmp_path, lambda r: _same(r, n=2))


def test_cross_joins_need_weight_on_the_target_sources(tmp_path: Path) -> None:
    def mutate(raw: dict[str, Any]) -> None:
        _cross(raw, spread=False)

    with pytest.raises(
        contract_model.SetupError,
        match="draws up to 1 cross-source joins; no table drawn on graphql reaches more than 0",
    ):
        _setup(tmp_path, mutate)


def test_per_transport_override_can_disable_joins(tmp_path: Path) -> None:
    def mutate(raw: dict[str, Any]) -> None:
        _same(raw)
        raw["transports"]["bolt"] = {"knobs": {"joins_same_source": ZERO}}

    gen = _gen(_setup(tmp_path, mutate), "bolt")
    assert {s.joins for s in (gen.next() for _ in range(30))} == {()}


def test_join_pairs_of_an_unknown_table(tmp_path: Path) -> None:
    def mutate(raw: dict[str, Any]) -> None:
        raw["cross_source_joins"][0]["right"] = "bench-clickhouse:ghost.order_id"

    with pytest.raises(
        contract_model.SetupError, match="cross_source_joins\\[0\\].right: unknown table 'ghost'"
    ):
        _setup(tmp_path, mutate)


# ------------------------------------------------------------------ the draw


def test_no_knob_no_join(tmp_path: Path) -> None:
    gen = _gen(_setup(tmp_path), "pgwire")
    assert {gen.next().joins for _ in range(50)} == {()}


def test_same_source_join_adds_order_items(tmp_path: Path) -> None:
    gen = _gen(_setup(tmp_path, _same), "pgwire")
    for _ in range(50):
        spec = gen.next()
        (step,) = spec.joins
        assert (spec.table, step.child_table, step.kind, step.parent, step.forward) == (
            "orders",
            "order_items",
            "same",
            0,
            True,
        )
        assert (step.parent_column, step.child_column) == ("order_id", "order_id")
        assert (step.graphql_field, step.cypher_rel) == ("orderItems", "HAS_ITEM")


def test_probability_mixes_joined_and_plain(tmp_path: Path) -> None:
    gen = _gen(_setup(tmp_path, lambda r: _same(r, 1, 0.5)), "pgwire")
    counts = Counter(len(gen.next().joins) for _ in range(2000))
    assert set(counts) == {0, 1} and 900 < counts[1] < 1100


def test_cross_joins_follow_the_child_source_weights(tmp_path: Path) -> None:
    gen = _gen(_setup(tmp_path, _cross), "graphql")
    children = Counter()
    for _ in range(1000):
        spec = gen.next()
        (step,) = spec.joins
        assert spec.table == "orders" and step.kind == "cross" and step.forward
        children[step.child_table] += 1
    assert set(children) == {"order_events", "order_docs"}
    assert 400 < children["order_events"] < 600


def test_two_cross_joins_reach_both_targets_once(tmp_path: Path) -> None:
    gen = _gen(_setup(tmp_path, lambda r: _cross(r, n=2)), "graphql")
    for _ in range(50):
        spec = gen.next()
        assert sorted(j.child_table for j in spec.joins) == ["order_docs", "order_events"]
        assert [j.parent for j in spec.joins] == [0, 0]


def test_sql_may_join_against_the_declared_direction(tmp_path: Path) -> None:
    gen = _gen(_setup(tmp_path, _cross), "pgwire")
    seen = {
        (s.table, s.joins[0].child_table, s.joins[0].forward)
        for s in (gen.next() for _ in range(2000))
    }
    assert ("order_events", "orders", False) in seen
    assert ("orders", "order_events", True) in seen


def test_graphql_and_cypher_only_join_left_to_right(tmp_path: Path) -> None:
    for transport in ("graphql", "bolt", "cypher_http"):
        gen = _gen(_setup(tmp_path, _cross), transport)
        for _ in range(300):
            spec = gen.next()
            assert spec.table == "orders" and all(j.forward for j in spec.joins)


def test_joined_requests_pick_a_base_table_that_can_join(tmp_path: Path) -> None:
    def mutate(raw: dict[str, Any]) -> None:
        _same(raw)
        raw["knobs"]["source_weights"] = {
            "bench-postgresql": 0.5,
            "bench-clickhouse": 0.5,
            "bench-mongodb": 0.0,
            "bench-neo4j": 0.0,
        }
        _no_joins_on_single_table_surfaces(
            raw, source_weights={s: 1.0 if s == "bench-postgresql" else 0.0 for s in SOURCES}
        )

    setup = _setup(tmp_path, mutate)
    gen = _gen(setup, "graphql")
    for _ in range(200):
        spec = gen.next()
        # order_events has no same-source pair, so a request that joins never starts there
        assert spec.table == "orders" or spec.joins == ()


def test_same_seed_same_sequence_and_independent_of_other_knobs(tmp_path: Path) -> None:
    def with_cache(raw: dict[str, Any]) -> None:
        _cross(raw)
        raw["knobs"]["cache"]["probability"] = 0.5

    a = _gen(_setup(tmp_path, _cross), "graphql", "x/1/1")
    b = _gen(_setup(tmp_path, _cross), "graphql", "x/1/1")
    c = _gen(_setup(tmp_path, with_cache), "graphql", "x/1/1")
    pick = lambda g: [(s.table, s.joins) for s in (g.next() for _ in range(100))]  # noqa: E731
    assert pick(a) == pick(b) == pick(c)


# ------------------------------------------------------------------ the text each transport sends


def _spec(base: str, steps: tuple, **kw: Any) -> request_mix.RequestSpec:
    source = {"orders": "bench-postgresql", "order_events": "bench-clickhouse"}[base]
    cols = kw.pop("columns", ("order_id",))
    return request_mix.RequestSpec(
        source, base, cols, kw.pop("rows", 1), kw.pop("cached", False), kw.pop("filters", ()), steps
    )


def _items_step() -> contract_model.JoinStep:
    return contract_model.JoinStep(
        0,
        "same",
        "bench-postgresql",
        "order_items",
        "order_id",
        "order_id",
        True,
        "orderItems",
        "HAS_ITEM",
    )


def _events_step() -> contract_model.JoinStep:
    return contract_model.JoinStep(
        0,
        "cross",
        "bench-clickhouse",
        "order_events",
        "order_id",
        "order_id",
        True,
        "orderEvents",
        "HAS_EVENT",
    )


def _docs_step() -> contract_model.JoinStep:
    return contract_model.JoinStep(
        0,
        "cross",
        "bench-mongodb",
        "order_docs",
        "order_id",
        "order_id",
        True,
        "orderDoc",
        "HAS_DOC",
    )


def test_zero_knob_still_equals_optimistic(tmp_path: Path) -> None:
    q = request_render.build_query(_setup(tmp_path), "graphql", cached=True)
    for attr in ("sql", "cypher", "graphql", "grpc", "rest", "jsonapi"):
        assert getattr(q, attr) == getattr(OPTIMISTIC, attr), attr


def test_render_one_same_source_join(tmp_path: Path) -> None:
    q = request_render.Renderer(_setup(tmp_path)).render(
        _spec("orders", (_items_step(),), cached=True)
    )
    assert q.sql == (
        "-- @provisa cache=true\nSELECT t0.order_id, t1.item_id AS order_items_item_id "
        "FROM perf_bench.orders t0 JOIN perf_bench.order_items t1 ON t1.order_id = t0.order_id LIMIT 1"
    )
    assert q.cypher == (
        "// @provisa cache=true\nMATCH (o:PerfBench:Orders)-[:HAS_ITEM]->(j1:PerfBench:OrderItems) "
        "RETURN o.orderId AS order_id, j1.itemId AS order_items_item_id LIMIT 1"
    )
    assert q.graphql == "query @cached { pb__orders(limit: 1) { orderId orderItems { itemId } } }"
    assert q.grpc is None and q.rest is None and q.jsonapi is None  # single-table surfaces


def test_render_join_with_fields_filter_and_rows(tmp_path: Path) -> None:
    spec = _spec(
        "orders",
        (_items_step(),),
        columns=("order_id", "region"),
        rows=5,
        filters=(("region", "NA"),),
    )
    q = request_render.Renderer(_setup(tmp_path)).render(spec)
    assert q.sql == (
        "SELECT t0.order_id, t0.region, t1.item_id AS order_items_item_id FROM perf_bench.orders t0 "
        "JOIN perf_bench.order_items t1 ON t1.order_id = t0.order_id WHERE t0.region = 'NA' LIMIT 5"
    )
    assert q.cypher == (
        'MATCH (o:PerfBench:Orders)-[:HAS_ITEM]->(j1:PerfBench:OrderItems) WHERE o.region = "NA" '
        "RETURN o.orderId AS order_id, o.region AS region, j1.itemId AS order_items_item_id LIMIT 5"
    )
    assert q.graphql == (
        'query { pb__orders(limit: 5, where: {region: {eq: "NA"}}) '
        "{ orderId region orderItems { itemId } } }"
    )


def test_render_two_cross_source_joins(tmp_path: Path) -> None:
    q = request_render.Renderer(_setup(tmp_path)).render(
        _spec("orders", (_events_step(), _docs_step()))
    )
    assert q.sql == (
        "SELECT t0.order_id, t1.event_id AS order_events_event_id, t2.order_id AS order_docs_order_id "
        "FROM perf_bench.orders t0 JOIN perf_bench.order_events t1 ON t1.order_id = t0.order_id "
        "JOIN perf_bench.order_docs t2 ON t2.order_id = t0.order_id LIMIT 1"
    )
    assert q.cypher == (
        "MATCH (o:PerfBench:Orders)-[:HAS_EVENT]->(j1:PerfBench:OrderEvents), "
        "(o)-[:HAS_DOC]->(j2:PerfBench:OrderDocs) RETURN o.orderId AS order_id, "
        "j1.eventId AS order_events_event_id, j2.orderId AS order_docs_order_id LIMIT 1"
    )
    assert q.graphql == (
        "query { pb__orders(limit: 1) { orderId orderEvents { eventId } orderDoc { orderId } } }"
    )


def test_render_a_join_against_the_declared_direction_is_sql_only(tmp_path: Path) -> None:
    reverse = contract_model.JoinStep(
        0, "cross", "bench-postgresql", "orders", "order_id", "order_id", False, None, None
    )
    q = request_render.Renderer(_setup(tmp_path)).render(
        _spec("order_events", (reverse,), columns=("event_id",))
    )
    assert q.sql == (
        "SELECT t0.event_id, t1.order_id AS orders_order_id FROM perf_bench.order_events t0 "
        "JOIN perf_bench.orders t1 ON t1.order_id = t0.order_id LIMIT 1"
    )
    assert q.graphql is None and q.cypher is None


def test_render_a_chain_nests(tmp_path: Path) -> None:
    """Step 2 hangs off step 1 (parent 1): GraphQL nests it, Cypher chains from the child, SQL
    joins on the child's alias."""
    chain = contract_model.JoinStep(
        1, "same", "bench-postgresql", "orders", "order_id", "order_id", True, "order", "BACK"
    )
    q = request_render.Renderer(_setup(tmp_path)).render(_spec("orders", (_items_step(), chain)))
    assert q.sql.endswith("JOIN perf_bench.orders t2 ON t2.order_id = t1.order_id LIMIT 1")
    assert "orderItems { itemId order { orderId } }" in q.graphql
    assert "(j1)-[:BACK]->(j2:PerfBench:Orders)" in q.cypher


# ------------------------------------------------------------------ the report


def _rec(latency: float, steps: tuple) -> ol.Record:
    return (latency, _spec("orders", steps), None, 1, 1)


def test_summary_reports_same_and_cross_source_joins_separately() -> None:
    reqs = (
        [_rec(0.001, ())] * 4
        + [_rec(0.004, (_items_step(),))] * 3
        + [_rec(0.009, (_events_step(), _docs_step()))] * 2
    )
    out = ol.summarize_requests(reqs, window_s=1.0)
    assert list(out["by_joins_same"]) == ["0", "1"]
    assert out["by_joins_same"]["1"]["requests"] == 3
    assert list(out["by_joins_cross"]) == ["0", "2"]
    assert out["by_joins_cross"]["2"]["p50_ms"] == 9.0
