# Copyright (c) 2026 Kenneth Stott
# Canary: 9b676265-9082-441e-8ace-7c054dfb8199
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""REQ-1911 knob: the route a request asks for — direct to the source or through the engine
(`route=direct` / `route=federated`). It is not the live/replica choice: that is the operator's
replication setting (test_replication.py)."""

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
NO_ROUTE_HINT = ["grpc", "rest", "jsonapi"]


def _mode(raw: dict[str, Any], source: str, mode: str, p: float) -> None:
    raw["knobs"]["route"][source] = {"mode": mode, "federated_probability": p}


def _auto(sources: list[str] = SOURCES) -> dict[str, Any]:
    return {s: {"mode": "auto", "federated_probability": 0} for s in sources}


def _setup(tmp_path: Path, *mutators: Any) -> contract_model.Setup:
    raw = json.loads(json.dumps(yaml.safe_load(PERF_CONTRACT.read_text())))
    for m in mutators:
        m(raw)
    path = tmp_path / "s.yaml"
    path.write_text(yaml.safe_dump(raw, sort_keys=False))
    return sc.load_setup(path, environ=ENV, known_transports=TRANSPORTS, deployment=fx.build())


def _gen(
    setup: contract_model.Setup, transport: str = "graphql", stream: str = "s"
) -> request_mix.RequestGenerator:
    return request_mix.RequestGenerator(setup, transport, stream, cacheable=transport != "rest")


def _without_route_hint_surfaces(raw: dict[str, Any]) -> None:
    """Transports with no route hint read only the sources left on `auto`."""
    for t in NO_ROUTE_HINT:
        raw["transports"][t] = {
            "knobs": {
                "source_weights": {s: 1.0 if s == "bench-postgresql" else 0.0 for s in SOURCES}
            }
        }


def _ch_federated(raw: dict[str, Any]) -> None:
    raw["knobs"]["source_weights"] = dict.fromkeys(SOURCES, 0.0) | {
        "bench-postgresql": 0.5,
        "bench-clickhouse": 0.5,
    }
    _mode(raw, "bench-clickhouse", "federated", 1.0)
    _without_route_hint_surfaces(raw)


# ------------------------------------------------------------------ the contract


def test_the_perf_contract_sends_no_route() -> None:
    setup = sc.load_setup(
        PERF_CONTRACT, environ=ENV, known_transports=TRANSPORTS, deployment=fx.build()
    )
    assert {s: rm.mode for s, rm in setup.knobs.route.items()} == dict.fromkeys(SOURCES, "auto")


@pytest.mark.parametrize(
    "mode,p,msg",
    [
        ("auto", 0.5, "mode auto requires federated_probability 0"),
        ("direct", 0.5, "mode direct requires federated_probability 0"),
        ("federated", 0.5, "mode federated requires federated_probability 1"),
        ("mixed", 0.0, "mode mixed requires federated_probability strictly between 0 and 1"),
        ("materialized", 0.5, "mode: must be one of auto, direct, federated, mixed"),
    ],
)
def test_mode_and_probability_must_agree(tmp_path: Path, mode: str, p: float, msg: str) -> None:
    with pytest.raises(contract_model.SetupError, match=msg):
        _setup(tmp_path, lambda r: _mode(r, "bench-clickhouse", mode, p))


def test_the_old_read_mode_and_ttl_knobs_are_gone(tmp_path: Path) -> None:
    for old in ("read_mode", "ttl_seconds"):

        def mutate(raw: dict[str, Any], old: str = old) -> None:
            raw["knobs"][old] = {}

        with pytest.raises(contract_model.SetupError, match=f"knobs: unknown key '{old}'"):
            _setup(tmp_path, mutate)


def test_a_transport_with_no_route_hint_cannot_read_a_non_auto_source(tmp_path: Path) -> None:
    with pytest.raises(
        contract_model.SetupError, match="transports.grpc: no route hint exists on grpc"
    ):
        _setup(tmp_path, lambda r: _mode(r, "bench-postgresql", "direct", 0))


def test_the_override_that_excludes_the_source_makes_it_valid(tmp_path: Path) -> None:
    assert _setup(tmp_path, _ch_federated).knobs.route["bench-clickhouse"].mode == "federated"


def test_route_can_be_overridden_per_transport(tmp_path: Path) -> None:
    def mutate(raw: dict[str, Any]) -> None:
        _ch_federated(raw)
        raw["transports"]["graphql"] = {
            "knobs": {
                "route": _auto()
                | {"bench-clickhouse": {"mode": "direct", "federated_probability": 0}}
            }
        }

    setup = _setup(tmp_path, mutate)
    assert setup.route_mode("graphql", "bench-clickhouse").mode == "direct"
    assert setup.route_mode("pgwire", "bench-clickhouse").mode == "federated"

    def routes(transport: str) -> set[str | None]:
        gen = _gen(setup, transport)
        return {s.route for s in (gen.next() for _ in range(300)) if s.source == "bench-clickhouse"}

    assert routes("graphql") == {"direct"} and routes("pgwire") == {"federated"}


def test_an_override_must_name_every_source(tmp_path: Path) -> None:
    def mutate(raw: dict[str, Any]) -> None:
        raw["transports"]["graphql"] = {
            "knobs": {"route": {"bench-postgresql": {"mode": "auto", "federated_probability": 0}}}
        }

    with pytest.raises(
        contract_model.SetupError,
        match="transports.graphql.knobs.route: missing source 'bench-clickhouse'",
    ):
        _setup(tmp_path, mutate)


def test_hint_free_transports_can_be_overridden_to_auto(tmp_path: Path) -> None:
    def mutate(raw: dict[str, Any]) -> None:
        _mode(raw, "bench-postgresql", "direct", 0)
        for t in NO_ROUTE_HINT:
            raw["transports"][t] = {"knobs": {"route": _auto()}}

    setup = _setup(tmp_path, mutate)
    grpc = _gen(setup, "grpc")
    assert {grpc.next().route for _ in range(50)} == {None}
    assert setup.route_mode("graphql", "bench-postgresql").mode == "direct"


# ------------------------------------------------------------------ the draw


def test_zero_knob_sends_no_route(tmp_path: Path) -> None:
    gen = _gen(_setup(tmp_path))
    assert {gen.next().route for _ in range(50)} == {None}


def test_direct_and_federated_per_source(tmp_path: Path) -> None:
    gen = _gen(_setup(tmp_path, _ch_federated))
    seen = {(s.source, s.route) for s in (gen.next() for _ in range(400))}
    assert seen == {("bench-postgresql", None), ("bench-clickhouse", "federated")}


def test_direct_mode_sends_route_direct(tmp_path: Path) -> None:
    def mutate(raw: dict[str, Any]) -> None:
        raw["knobs"]["source_weights"] = dict.fromkeys(SOURCES, 0.0) | {"bench-clickhouse": 1.0}
        _mode(raw, "bench-clickhouse", "direct", 0)
        _without_route_hint_surfaces(raw)

    gen = _gen(_setup(tmp_path, mutate))
    assert {gen.next().route for _ in range(50)} == {"direct"}


def test_mixed_share_follows_the_probability(tmp_path: Path) -> None:
    def mutate(raw: dict[str, Any]) -> None:
        raw["knobs"]["source_weights"] = dict.fromkeys(SOURCES, 0.0) | {"bench-clickhouse": 1.0}
        _mode(raw, "bench-clickhouse", "mixed", 0.3)
        _without_route_hint_surfaces(raw)

    gen = _gen(_setup(tmp_path, mutate))
    counts = Counter(gen.next().route for _ in range(5000))
    assert set(counts) == {"direct", "federated"}
    assert abs(counts["federated"] / 5000 - 0.3) < 0.03


def test_the_requests_own_source_decides_the_route_when_it_joins(tmp_path: Path) -> None:
    def mutate(raw: dict[str, Any]) -> None:
        _ch_federated(raw)
        raw["knobs"]["joins_cross_source"] = {
            "probability": 1,
            "distribution": {"kind": "constant", "value": 1},
        }
        for t in NO_ROUTE_HINT:
            raw["transports"][t]["knobs"]["joins_cross_source"] = {
                "probability": 0,
                "distribution": {"kind": "constant", "value": 0},
            }

    gen = _gen(_setup(tmp_path, mutate))
    for _ in range(200):
        spec = gen.next()
        assert spec.route == ("federated" if spec.source == "bench-clickhouse" else None)


def test_same_seed_same_sequence_and_independent(tmp_path: Path) -> None:
    def mixed(raw: dict[str, Any]) -> None:
        raw["knobs"]["source_weights"] = dict.fromkeys(SOURCES, 0.0) | {"bench-clickhouse": 1.0}
        _mode(raw, "bench-clickhouse", "mixed", 0.5)
        _without_route_hint_surfaces(raw)

    def mixed_and_cache(raw: dict[str, Any]) -> None:
        mixed(raw)
        raw["knobs"]["cache"]["probability"] = 0.5

    a, b, c = (_gen(_setup(tmp_path, m), stream="x/1/1") for m in (mixed, mixed, mixed_and_cache))
    ra = [s.route for s in (a.next() for _ in range(100))]
    assert ra == [s.route for s in (b.next() for _ in range(100))]
    assert ra == [s.route for s in (c.next() for _ in range(100))]


# ------------------------------------------------------------------ the text each transport sends


def _spec(route: str | None, cached: bool = False) -> request_mix.RequestSpec:
    return request_mix.RequestSpec(
        "bench-postgresql", "orders", ("order_id",), 1, cached, (), (), 0, route
    )


def test_zero_knob_still_equals_optimistic(tmp_path: Path) -> None:
    q = request_render.build_query(_setup(tmp_path), "graphql", cached=True)
    for attr in ("sql", "cypher", "graphql", "grpc", "rest", "jsonapi"):
        assert getattr(q, attr) == getattr(OPTIMISTIC, attr), attr


def test_render_the_route_hint_in_each_language(tmp_path: Path) -> None:
    r = request_render.Renderer(_setup(tmp_path))
    q = r.render(_spec("direct", cached=True))
    assert (
        q.sql
        == "-- @provisa cache=true\n-- @provisa route=direct\nSELECT order_id FROM perf_bench.orders LIMIT 1"
    )
    assert q.cypher == (
        "// @provisa cache=true\n// @provisa route=direct\nMATCH (o:PerfBench:Orders) "
        "RETURN o.orderId AS order_id LIMIT 1"
    )
    assert q.graphql == "query @cached @route(engine: DIRECT) { pb__orders(limit: 1) { orderId } }"
    assert q.grpc is None and q.rest is None and q.jsonapi is None  # no route hint there
    q = r.render(_spec("federated"))
    assert q.sql == "-- @provisa route=federated\nSELECT order_id FROM perf_bench.orders LIMIT 1"
    assert q.graphql == "query @route(engine: FEDERATED) { pb__orders(limit: 1) { orderId } }"


# ------------------------------------------------------------------ the report


def _rec(latency: float, route: str | None) -> ol.Record:
    return (latency, _spec(route), None, 1, 1)


def test_summary_by_route() -> None:
    out = ol.summarize_requests(
        [_rec(0.001, None)] * 3 + [_rec(0.002, "direct")] * 4 + [_rec(0.009, "federated")] * 2,
        window_s=1.0,
    )
    assert list(out["by_route"]) == ["auto", "direct", "federated"]
    assert out["by_route"]["federated"]["p50_ms"] == 9.0
    assert out["by_route"]["direct"]["requests"] == 4


def test_the_transport_report_names_what_the_run_was_measured_under(tmp_path: Path) -> None:
    out = ol.replication_report(_setup(tmp_path, _ch_federated), "graphql")
    assert out["bench-clickhouse"] == {
        "replication": "replica",
        "ttl_seconds": 300,
        "route": "federated",
        "federated_probability": 1.0,
    }
    assert (
        out["bench-postgresql"]["replication"] == "live"
        and out["bench-postgresql"]["route"] == "auto"
    )
