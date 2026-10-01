# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""REQ-1911 knob: number of selected fields."""

from __future__ import annotations

import json
import random
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
TRANSPORT = "graphql"  # any: the base request reads Postgres orders on all of them
COLUMNS = ["order_id", "customer_id", "region", "status", "amount"]


def _fields(raw: dict[str, Any], p: float, dist: dict[str, Any]) -> None:
    raw["knobs"]["fields"] = {"probability": p, "distribution": dist}


def _setup(tmp_path: Path, mutate: Any = None) -> contract_model.Setup:
    raw = json.loads(json.dumps(yaml.safe_load(PERF_CONTRACT.read_text())))
    if mutate:
        mutate(raw)
    path = tmp_path / "s.yaml"
    path.write_text(yaml.safe_dump(raw, sort_keys=False))
    return sc.load_setup(path, environ=ENV, known_transports=TRANSPORTS, deployment=fx.build())


# ------------------------------------------------------------------ distributions


def _sampler(kind: str, **params: Any) -> request_mix.Sampler:
    return request_mix.Sampler(contract_model.Distribution(kind, params), random.Random(5))


def test_constant_and_uniform_bounds() -> None:
    assert {_sampler("constant", value=3).next() for _ in range(50)} == {3}
    s = _sampler("uniform", min=2, max=4)
    assert {s.next() for _ in range(300)} == {2, 3, 4}


def test_zipf_is_skewed_to_the_low_end_and_bounded() -> None:
    s = _sampler("zipf", exponent=1.5, max=5)
    counts = Counter(s.next() for _ in range(5000))
    assert set(counts) == {1, 2, 3, 4, 5}
    assert counts[1] > counts[2] > counts[5]


def test_geometric_is_skewed_to_the_low_end_and_bounded() -> None:
    s = _sampler("geometric", p=0.5, max=4)
    counts = Counter(s.next() for _ in range(5000))
    assert set(counts) == {1, 2, 3, 4}
    assert counts[1] > counts[2] > counts[4]


def test_knob_draw_probability_zero_and_one() -> None:
    knob = lambda p: contract_model.Knob(p, contract_model.Distribution("constant", {"value": 2}))  # noqa: E731
    assert not any(request_mix.KnobDraw(knob(0.0), 1, "s", "fields").next() for _ in range(100))
    assert {request_mix.KnobDraw(knob(1.0), 1, "s", "fields").next() for _ in range(100)} == {2}


def test_knob_draw_applies_with_the_probability() -> None:
    d = request_mix.KnobDraw(
        contract_model.Knob(0.5, contract_model.Distribution("constant", {"value": 2})),
        0,
        "share",
        "fields",
    )
    n = sum(d.next() is not None for _ in range(1000))
    assert n == EXPECTED_APPLIED and 440 <= n <= 560


EXPECTED_APPLIED = 530  # 1000 draws, seed 0, stream "share"


# ------------------------------------------------------------------ the request


def test_no_knob_selects_the_base_column(tmp_path: Path) -> None:
    gen = request_mix.RequestGenerator(_setup(tmp_path), "pgwire", "s", cacheable=True)
    assert {gen.next().columns for _ in range(50)} == {("order_id",)}


def test_fields_knob_selects_that_many_distinct_declared_columns_in_order(tmp_path: Path) -> None:
    setup = _setup(tmp_path, lambda r: _fields(r, 1.0, {"kind": "constant", "value": 3}))
    gen = request_mix.RequestGenerator(setup, "pgwire", "s", cacheable=True)
    seen = set()
    for _ in range(200):
        cols = gen.next().columns
        assert len(cols) == len(set(cols)) == 3
        assert list(cols) == sorted(cols, key=COLUMNS.index)
        seen.add(cols)
    assert len(seen) > 1  # the choice varies


def test_fields_count_follows_the_distribution(tmp_path: Path) -> None:
    setup = _setup(tmp_path, lambda r: _fields(r, 1.0, {"kind": "uniform", "min": 1, "max": 5}))
    gen = request_mix.RequestGenerator(setup, "graphql", "s", cacheable=True)
    assert {len(gen.next().columns) for _ in range(500)} == {1, 2, 3, 4, 5}


def test_probability_mixes_base_and_drawn(tmp_path: Path) -> None:
    setup = _setup(tmp_path, lambda r: _fields(r, 0.5, {"kind": "constant", "value": 4}))
    gen = request_mix.RequestGenerator(setup, "graphql", "s", cacheable=True)
    counts = Counter(len(gen.next().columns) for _ in range(1000))
    assert set(counts) == {1, 4} and 400 < counts[4] < 600


def test_same_seed_same_sequence(tmp_path: Path) -> None:
    setup = _setup(tmp_path, lambda r: _fields(r, 1.0, {"kind": "uniform", "min": 1, "max": 5}))
    a = request_mix.RequestGenerator(setup, "grpc", "x/1/2", cacheable=True)
    b = request_mix.RequestGenerator(setup, "grpc", "x/1/2", cacheable=True)
    assert [a.next() for _ in range(100)] == [b.next() for _ in range(100)]


def test_knobs_draw_independently(tmp_path: Path) -> None:
    """Turning the cache knob must not change which columns are drawn."""

    def fields(raw: dict[str, Any]) -> None:
        _fields(raw, 1.0, {"kind": "uniform", "min": 1, "max": 5})

    def fields_and_cache(raw: dict[str, Any]) -> None:
        fields(raw)
        raw["knobs"]["cache"]["probability"] = 0.5

    a = request_mix.RequestGenerator(_setup(tmp_path, fields), "grpc", "s", cacheable=True)
    b = request_mix.RequestGenerator(
        _setup(tmp_path, fields_and_cache), "grpc", "s", cacheable=True
    )
    assert [a.next().columns for _ in range(100)] == [b.next().columns for _ in range(100)]


def test_per_transport_override(tmp_path: Path) -> None:
    def mutate(raw: dict[str, Any]) -> None:
        raw["transports"]["rest"] = {
            "knobs": {
                "fields": {"probability": 1, "distribution": {"kind": "constant", "value": 2}}
            }
        }

    setup = _setup(tmp_path, mutate)
    rest = request_mix.RequestGenerator(setup, "rest", "s", cacheable=False)
    other = request_mix.RequestGenerator(setup, "graphql", "s", cacheable=True)
    assert len(rest.next().columns) == 2
    assert len(other.next().columns) == 1


# ------------------------------------------------------------------ the text each transport sends


def test_render_zero_knob_still_equals_optimistic(tmp_path: Path) -> None:
    q = request_render.build_query(_setup(tmp_path), TRANSPORT, cached=True)
    for attr in ("sql", "cypher", "graphql", "grpc", "rest", "jsonapi"):
        assert getattr(q, attr) == getattr(OPTIMISTIC, attr), attr


def test_render_three_fields(tmp_path: Path) -> None:
    setup = _setup(tmp_path)
    spec = request_mix.RequestSpec(
        "bench-postgresql", "orders", ("order_id", "region", "amount"), 1, True
    )
    q = request_render.Renderer(setup).render(spec)
    assert (
        q.sql
        == "-- @provisa cache=true\nSELECT order_id, region, amount FROM perf_bench.orders LIMIT 1"
    )
    assert q.cypher == (
        "// @provisa cache=true\nMATCH (o:PerfBench:Orders) RETURN o.orderId AS order_id, "
        "o.region AS region, o.amount AS amount LIMIT 1"
    )
    assert q.graphql == "query @cached { pb__orders(limit: 1) { orderId region amount } }"
    assert q.grpc is not None and q.grpc["read_mask"] == ["order_id", "region", "amount"]
    assert q.rest == {
        "path": "/data/rest/perf-bench/orders",
        "params": {"limit": 1, "fields": "orderId,region,amount"},
    }
    assert q.jsonapi == {
        "path": "/data/jsonapi/perf-bench/orders",
        "params": {"page[size]": 1, "fields[orders]": "order_id,region,amount"},
    }


def test_render_is_memoized(tmp_path: Path) -> None:
    r = request_render.Renderer(_setup(tmp_path))
    spec = request_mix.RequestSpec("bench-postgresql", "orders", ("order_id",), 1, False)
    assert r.render(spec) is r.render(spec)


# ------------------------------------------------------------------ validation


def test_fields_may_not_draw_zero(tmp_path: Path) -> None:
    with pytest.raises(
        contract_model.SetupError, match="knobs.fields.distribution: a field count must be >= 1"
    ):
        _setup(tmp_path, lambda r: _fields(r, 1.0, {"kind": "uniform", "min": 0, "max": 3}))


def test_fields_may_not_exceed_the_table(tmp_path: Path) -> None:
    with pytest.raises(
        contract_model.SetupError, match="draws up to 6 fields, a usable table declares 5 columns"
    ):
        _setup(tmp_path, lambda r: _fields(r, 1.0, {"kind": "zipf", "exponent": 1.2, "max": 6}))


def test_transport_override_range_is_checked(tmp_path: Path) -> None:
    def mutate(raw: dict[str, Any]) -> None:
        raw["transports"]["bolt"] = {
            "knobs": {
                "fields": {"probability": 1, "distribution": {"kind": "constant", "value": 9}}
            }
        }

    with pytest.raises(
        contract_model.SetupError, match="transports.bolt.knobs.fields.distribution"
    ):
        _setup(tmp_path, mutate)


def test_fields_is_no_longer_refused(tmp_path: Path) -> None:
    setup = _setup(tmp_path, lambda r: _fields(r, 0.4, {"kind": "uniform", "min": 1, "max": 5}))
    assert setup.knobs.distributions["fields"].probability == 0.4


# ------------------------------------------------------------------ the report


def _rec(latency: float, n_cols: int, returned: int | None = None) -> ol.Record:
    spec = request_mix.RequestSpec("s", "t", tuple("abcde"[:n_cols]), 1, False)
    return (latency, spec, None, 1, n_cols if returned is None else returned)


def test_summary_by_fields() -> None:
    reqs = [_rec(0.001, 1)] * 6 + [_rec(0.003, 3)] * 4
    out = ol.summarize_requests(reqs, window_s=2.0)
    assert list(out["by_fields"]) == ["1", "3"]
    assert out["by_fields"]["1"]["requests"] == 6
    assert out["by_fields"]["3"]["p50_ms"] == 3.0
    assert out["avg_columns_returned"] == 1.8


def test_summary_reports_columns_returned_when_the_transport_ignores_the_selection() -> None:
    out = ol.summarize_requests([_rec(0.001, 2, returned=26)] * 2, window_s=1.0)
    assert out["avg_columns_returned"] == 26.0
