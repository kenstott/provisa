# Copyright (c) 2026 Kenneth Stott
# Canary: 33081f17-b716-44cd-89cc-5563329cfd9f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""REQ-1911 knob: cached vs not cached — the probability that a request opts into the response
cache. 1.0 is the optimistic test, 0.0 sends every request uncached."""

from __future__ import annotations

import json
import sys
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
TRANSPORT = "graphql"  # any: the base request reads Postgres orders on all of them


def _raw() -> dict[str, Any]:
    return json.loads(json.dumps(yaml.safe_load(PERF_CONTRACT.read_text())))


def _load(raw: dict[str, Any], tmp_path: Path) -> contract_model.Setup:
    path = tmp_path / "s.yaml"
    path.write_text(yaml.safe_dump(raw, sort_keys=False))
    return sc.load_setup(path, environ=ENV, known_transports=TRANSPORTS, deployment=fx.build())


# ------------------------------------------------------------------ the draw


def test_same_seed_same_stream_same_sequence() -> None:
    a = request_mix.CacheDraw(0.5, seed=7, stream="graphql/0/1")
    b = request_mix.CacheDraw(0.5, seed=7, stream="graphql/0/1")
    assert [a.next() for _ in range(200)] == [b.next() for _ in range(200)]


def test_different_seed_or_stream_differs() -> None:
    base = [request_mix.CacheDraw(0.5, 7, "a").next() for _ in range(1)]  # noqa: F841
    seq = lambda seed, stream: [  # noqa: E731
        d.next() for d in [request_mix.CacheDraw(0.5, seed, stream)] for _ in range(200)
    ]
    assert seq(7, "a") != seq(8, "a")
    assert seq(7, "a") != seq(7, "b")


def test_probability_zero_never_and_one_always() -> None:
    never = request_mix.CacheDraw(0.0, 1, "x")
    always = request_mix.CacheDraw(1.0, 1, "x")
    assert not any(never.next() for _ in range(1000))
    assert all(always.next() for _ in range(1000))


EXPECTED_HALF_SHARE = 528  # 1000 draws, seed 0, stream 'share'


def test_probability_half_share_is_exact_for_a_fixed_seed() -> None:
    draw = request_mix.CacheDraw(0.5, seed=0, stream="share")
    n = sum(draw.next() for _ in range(1000))
    assert n == EXPECTED_HALF_SHARE
    assert 440 <= n <= 560  # and it is a half


# ------------------------------------------------------------------ the contract


def test_zero_knob_contract_still_equals_todays_requests() -> None:
    setup = sc.load_setup(
        PERF_CONTRACT, environ=ENV, known_transports=TRANSPORTS, deployment=fx.build()
    )
    assert setup.knobs.cache_probability == 1.0
    q = request_render.build_query(setup, TRANSPORT, cached=True)
    for attr in ("sql", "cypher", "graphql", "grpc", "rest", "jsonapi"):
        assert getattr(q, attr) == getattr(OPTIMISTIC, attr), attr


def test_uncached_request_drops_exactly_the_opt_in() -> None:
    setup = sc.load_setup(
        PERF_CONTRACT, environ=ENV, known_transports=TRANSPORTS, deployment=fx.build()
    )
    q = request_render.build_query(setup, TRANSPORT, cached=False)
    assert q.sql == "SELECT order_id FROM perf_bench.orders LIMIT 1"
    assert q.cypher == "MATCH (o:PerfBench:Orders) RETURN o.orderId AS order_id LIMIT 1"
    assert q.graphql == "query { pb__orders(limit: 1) { orderId } }"
    assert q.grpc is not None and q.grpc["metadata"] == []
    assert q.rest == OPTIMISTIC.rest and q.jsonapi == OPTIMISTIC.jsonapi  # no opt-in to drop


def test_per_transport_override_wins(tmp_path: Path) -> None:
    raw = _raw()
    raw["transports"]["pgwire"] = {"knobs": {"cache": {"probability": 0.25}}}
    setup = _load(raw, tmp_path)
    assert setup.cache_probability("pgwire") == 0.25
    assert setup.cache_probability("graphql") == 1.0


@pytest.mark.parametrize(
    "bad,msg",
    [(1.5, "knobs.cache.probability: must be between 0 and 1"), ("x", "must be a number")],
)
def test_cache_probability_validated(tmp_path: Path, bad: Any, msg: str) -> None:
    raw = _raw()
    raw["knobs"]["cache"]["probability"] = bad
    with pytest.raises(contract_model.SetupError, match=msg):
        _load(raw, tmp_path)


def test_cache_knob_is_required(tmp_path: Path) -> None:
    raw = _raw()
    del raw["knobs"]["cache"]
    with pytest.raises(contract_model.SetupError, match="knobs.cache: required"):
        _load(raw, tmp_path)


def test_cache_repetition_is_its_own_knob(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """`cache` (opt in or not) and `cache_repetition` (repeat an earlier request) are separate:
    with repetition unbuilt it is refused by name while the cache knob works."""
    monkeypatch.setattr(contract_checks, "IMPLEMENTED_KNOBS", frozenset())
    raw = _raw()
    raw["knobs"]["cache_repetition"] = {
        "probability": 0.5,
        "distribution": {"kind": "constant", "value": 3},
    }
    with pytest.raises(
        contract_model.SetupError, match="not implemented yet: knobs.cache_repetition"
    ):
        _load(raw, tmp_path)
    raw = _raw()
    raw["knobs"]["cache"]["probability"] = 0.5
    assert _load(raw, tmp_path).knobs.cache_probability == 0.5


def test_endpoints_carry_the_contract(tmp_path: Path) -> None:
    raw = _raw()
    raw["transports"]["bolt"] = {"knobs": {"cache": {"probability": 0.0}}}
    setup = _load(raw, tmp_path)
    ep = ol.endpoints_from_setup(setup)
    assert ep.setup is setup
    assert setup.cache_probability("bolt") == 0.0 and setup.cache_probability("graphql") == 1.0


# ------------------------------------------------------------------ the report


def _spec(cached: bool, n_cols: int = 1) -> request_mix.RequestSpec:
    return request_mix.RequestSpec("bench-postgresql", "orders", tuple("abcde"[:n_cols]), 1, cached)


def _rec(latency: float, cached: bool, hit: bool | None, n_cols: int = 1) -> ol.Record:
    return (latency, _spec(cached, n_cols), hit, 1, n_cols)


def test_summary_splits_by_opt_in_and_by_outcome() -> None:
    reqs = (
        [_rec(0.001, True, True)] * 8
        + [_rec(0.010, True, False)] * 2
        + [_rec(0.020, False, False)] * 10
    )
    out = ol.summarize_requests(reqs, window_s=2.0)
    assert out["cache_opt_in_ratio"] == 0.5
    assert out["hit_ratio"] == 0.4
    assert out["by_opt_in"]["cached"]["requests"] == 10
    assert out["by_opt_in"]["uncached"] == {
        "requests": 10,
        "req_per_s": 5.0,
        "p50_ms": 20.0,
        "p99_ms": 20.0,
    }
    assert out["by_outcome"]["hit"]["requests"] == 8
    assert out["by_outcome"]["hit"]["p50_ms"] == 1.0
    assert out["by_outcome"]["miss"]["requests"] == 12


def test_summary_without_a_hit_header_has_no_outcome_split() -> None:
    reqs = [_rec(0.001, True, None)] * 4 + [_rec(0.002, False, None)] * 4
    out = ol.summarize_requests(reqs, window_s=1.0)
    assert out["hit_ratio"] is None
    assert out["by_outcome"] is None
    assert out["cache_opt_in_ratio"] == 0.5


def test_a_class_with_no_requests_reports_zero_not_a_percentile() -> None:
    out = ol.summarize_requests([_rec(0.001, True, None)] * 3, window_s=1.0)
    assert out["by_opt_in"]["uncached"] == {
        "requests": 0,
        "req_per_s": 0.0,
        "p50_ms": None,
        "p99_ms": None,
    }


# ------------------------------------------------------------------ the client loop


class _Recorder:
    def __init__(self, ep: Any) -> None:
        self.sent: list[Any] = []
        _Recorder.last = self

    def call(self, query: Any, role: str) -> tuple[int, int, bool | None]:
        self.sent.append(query)
        return 1, 1, None

    def close(self) -> None:
        pass

    last: Any = None


def _ep(tmp_path: Path, mutate: Any = None) -> ol.Endpoints:
    raw = _raw()
    if mutate:
        mutate(raw)
    return ol.endpoints_from_setup(_load(raw, tmp_path))


def _run_loop(
    monkeypatch: pytest.MonkeyPatch, transport: str, ep: ol.Endpoints, index: int = 3
) -> tuple[list[request_mix.RequestSpec], list[Any]]:
    import queue
    import threading

    monkeypatch.setitem(ol.CLIENTS, transport, _Recorder)
    go = threading.Event()
    go.set()
    ready: queue.Queue = queue.Queue()
    out: queue.Queue = queue.Queue()
    ol._client_process(transport, ep, index, 1, 0.05, ready, go, out)  # noqa: SLF001
    assert ready.get() is None
    records = out.get()["records"]
    return [r[1] for r in records], _Recorder.last.sent


def _cache_p(p: float) -> Any:
    def mutate(raw: dict[str, Any]) -> None:
        raw["knobs"]["cache"]["probability"] = p

    return mutate


def test_loop_p1_sends_only_opted_in_and_p0_only_plain(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    specs, _ = _run_loop(monkeypatch, "graphql", _ep(tmp_path, _cache_p(1.0)))
    assert specs and all(s.cached for s in specs)
    specs, _ = _run_loop(monkeypatch, "graphql", _ep(tmp_path, _cache_p(0.0)))
    assert specs and not any(s.cached for s in specs)


def test_loop_sequence_is_the_seeded_stream(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def mutate(raw: dict[str, Any]) -> None:
        raw["knobs"]["cache"]["probability"] = 0.5
        raw["seed"] = 11

    specs, _ = _run_loop(monkeypatch, "graphql", _ep(tmp_path, mutate))
    draw = request_mix.CacheDraw(0.5, 11, "graphql/3/0")
    assert [s.cached for s in specs] == [draw.next() for _ in specs]
    assert {s.cached for s in specs} == {True, False}


def test_transport_without_opt_in_never_draws(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    specs, _ = _run_loop(monkeypatch, "rest", _ep(tmp_path, _cache_p(1.0)))
    assert specs and not any(s.cached for s in specs)


def test_warmup_covers_every_cache_variant_the_client_can_send(tmp_path: Path) -> None:
    def variants(p: float, opt_in: bool) -> list[bool]:
        raw = _raw()
        raw["knobs"]["cache"]["probability"] = p
        setup = _load(raw, tmp_path)
        gen = request_mix.RequestGenerator(setup, "graphql", "w", cacheable=True)
        return [s.cached for s in ol._warmup_specs(gen, setup, "graphql", opt_in)]  # noqa: SLF001

    assert variants(1.0, True) == [True]
    assert variants(0.0, True) == [False]
    assert variants(0.5, True) == [True, False]
    assert variants(1.0, False) == [False]
