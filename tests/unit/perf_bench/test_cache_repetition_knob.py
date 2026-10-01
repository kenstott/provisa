# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""REQ-1911 knob: cache repetition — the probability that a request repeats an earlier one of the
same client, at a distance drawn from a distribution. Separate from the `cache` knob (whether a
request opts into the response cache)."""

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


def _rep(raw: dict[str, Any], p: float, dist: dict[str, Any]) -> None:
    raw["knobs"]["cache_repetition"] = {"probability": p, "distribution": dist}


def _const(n: int) -> dict[str, Any]:
    return {"kind": "constant", "value": n}


def _varied(raw: dict[str, Any]) -> None:
    """Requests that differ from one another, so a repeat is distinguishable from a coincidence."""
    raw["knobs"]["filters"] = {"probability": 1, "distribution": _const(1)}
    raw["knobs"]["fields"] = {
        "probability": 1,
        "distribution": {"kind": "uniform", "min": 1, "max": 5},
    }


def _setup(tmp_path: Path, *mutators: Any) -> contract_model.Setup:
    raw = json.loads(json.dumps(yaml.safe_load(PERF_CONTRACT.read_text())))
    for m in mutators:
        m(raw)
    path = tmp_path / "s.yaml"
    path.write_text(yaml.safe_dump(raw, sort_keys=False))
    return sc.load_setup(path, environ=ENV, known_transports=TRANSPORTS, deployment=fx.build())


def _gen(
    setup: contract_model.Setup, transport: str = "pgwire", stream: str = "s"
) -> request_mix.RequestGenerator:
    return request_mix.RequestGenerator(setup, transport, stream, cacheable=True)


# ------------------------------------------------------------------ the contract


def test_knob_is_no_longer_refused(tmp_path: Path) -> None:
    setup = _setup(tmp_path, lambda r: _rep(r, 0.3, _const(2)))
    assert setup.knobs.distributions["cache_repetition"].probability == 0.3


def test_lookback_must_be_at_least_one(tmp_path: Path) -> None:
    with pytest.raises(
        contract_model.SetupError,
        match="knobs.cache_repetition.distribution: a lookback distance must be >= 1",
    ):
        _setup(tmp_path, lambda r: _rep(r, 1.0, {"kind": "uniform", "min": 0, "max": 5}))


def test_lookback_window_is_bounded(tmp_path: Path) -> None:
    with pytest.raises(
        contract_model.SetupError,
        match="draws up to 100001 requests back; the window is limited to 100000",
    ):
        _setup(tmp_path, lambda r: _rep(r, 1.0, _const(100001)))


def test_transport_override_is_checked(tmp_path: Path) -> None:
    def mutate(raw: dict[str, Any]) -> None:
        raw["transports"]["bolt"] = {
            "knobs": {"cache_repetition": {"probability": 1, "distribution": _const(0)}}
        }

    with pytest.raises(
        contract_model.SetupError, match="transports.bolt.knobs.cache_repetition.distribution"
    ):
        _setup(tmp_path, mutate)


# ------------------------------------------------------------------ the draw


def test_no_knob_never_repeats(tmp_path: Path) -> None:
    gen = _gen(_setup(tmp_path, _varied))
    assert {gen.next().repeat for _ in range(200)} == {0}


def test_probability_one_repeats_the_previous_request(tmp_path: Path) -> None:
    gen = _gen(_setup(tmp_path, _varied, lambda r: _rep(r, 1.0, _const(1))))
    specs = [gen.next() for _ in range(50)]
    assert specs[0].repeat == 0  # nothing earlier to repeat
    assert all(s.repeat == 1 for s in specs[1:])
    assert all(s == specs[0] for s in specs)


def test_the_distance_is_how_far_back(tmp_path: Path) -> None:
    gen = _gen(_setup(tmp_path, _varied, lambda r: _rep(r, 1.0, _const(3))))
    specs = [gen.next() for _ in range(40)]
    assert [s.repeat for s in specs[:3]] == [0, 0, 0]  # fewer than 3 earlier requests
    assert all(s.repeat == 3 for s in specs[3:])
    for i in range(3, 40):
        assert specs[i] == specs[i - 3]


def test_share_follows_the_probability(tmp_path: Path) -> None:
    gen = _gen(
        _setup(tmp_path, _varied, lambda r: _rep(r, 0.5, {"kind": "uniform", "min": 1, "max": 4}))
    )
    specs = [gen.next() for _ in range(4000)]
    share = sum(s.repeat > 0 for s in specs) / len(specs)
    assert 0.46 < share < 0.54
    assert {s.repeat for s in specs} == {0, 1, 2, 3, 4}


def test_fresh_requests_are_what_the_other_knobs_drew(tmp_path: Path) -> None:
    """The repetition knob replaces a request; it never changes what the others would draw."""
    base = _gen(_setup(tmp_path, _varied))
    rep = _gen(
        _setup(tmp_path, _varied, lambda r: _rep(r, 0.5, {"kind": "uniform", "min": 1, "max": 4}))
    )
    for _ in range(500):
        b, r = base.next(), rep.next()
        if r.repeat == 0:
            assert r == b


def test_a_repeat_keeps_the_cache_opt_in_of_the_request_it_repeats(tmp_path: Path) -> None:
    def cache_half(raw: dict[str, Any]) -> None:
        raw["knobs"]["cache"]["probability"] = 0.5

    gen = _gen(_setup(tmp_path, _varied, cache_half, lambda r: _rep(r, 1.0, _const(2))))
    specs = [gen.next() for _ in range(40)]
    for i in range(2, 40):
        assert specs[i].cached == specs[i - 2].cached


def test_repeats_render_to_the_same_query_object(tmp_path: Path) -> None:
    setup = _setup(tmp_path, _varied, lambda r: _rep(r, 1.0, _const(1)))
    gen, renderer = _gen(setup), request_render.Renderer(setup)
    first, second = gen.next(), gen.next()
    assert second.repeat == 1 and renderer.render(first) is renderer.render(second)


def test_same_seed_same_sequence(tmp_path: Path) -> None:
    setup = _setup(
        tmp_path, _varied, lambda r: _rep(r, 0.5, {"kind": "geometric", "p": 0.5, "max": 6})
    )
    a, b = _gen(setup, stream="x/1/1"), _gen(setup, stream="x/1/1")
    assert [a.next() for _ in range(200)] == [b.next() for _ in range(200)]


def test_per_transport_override(tmp_path: Path) -> None:
    def mutate(raw: dict[str, Any]) -> None:
        raw["transports"]["rest"] = {
            "knobs": {"cache_repetition": {"probability": 1, "distribution": _const(1)}}
        }

    setup = _setup(tmp_path, _varied, mutate)
    rest = request_mix.RequestGenerator(setup, "rest", "s", cacheable=False)
    assert all(s.repeat == 1 for s in [rest.next() for _ in range(10)][1:])
    graphql = _gen(setup, "graphql")
    assert {graphql.next().repeat for _ in range(10)} == {0}


def test_zero_knob_still_equals_optimistic(tmp_path: Path) -> None:
    q = request_render.build_query(_setup(tmp_path), "graphql", cached=True)
    for attr in ("sql", "cypher", "graphql", "grpc", "rest", "jsonapi"):
        assert getattr(q, attr) == getattr(OPTIMISTIC, attr), attr


# ------------------------------------------------------------------ the report


def _rec(latency: float, repeat: int, hit: bool | None) -> ol.Record:
    spec = request_mix.RequestSpec("s", "t", ("a",), 1, True, (), (), repeat)
    return (latency, spec, hit, 1, 1)


def test_summary_separates_fresh_from_repeated_requests() -> None:
    reqs = [_rec(0.010, 0, False)] * 5 + [_rec(0.001, 2, True)] * 3 + [_rec(0.010, 2, False)]
    out = ol.summarize_requests(reqs, window_s=1.0)
    assert out["repeat_ratio"] == round(4 / 9, 4)
    assert out["by_repeat"]["fresh"]["requests"] == 5
    assert out["by_repeat"]["repeat"]["requests"] == 4
    assert out["by_repeat"]["fresh"]["hit_ratio"] == 0.0
    assert out["by_repeat"]["repeat"]["hit_ratio"] == 0.75


def test_summary_hit_ratio_is_null_when_the_transport_does_not_say() -> None:
    out = ol.summarize_requests([_rec(0.001, 1, None)] * 3, window_s=1.0)
    assert out["by_repeat"]["repeat"]["hit_ratio"] is None
