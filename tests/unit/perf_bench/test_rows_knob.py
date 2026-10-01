# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""REQ-1911 knob: number of rows returned."""

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
TRANSPORT = "graphql"  # any: the base request reads Postgres orders on all of them


def _rows(raw: dict[str, Any], p: float, dist: dict[str, Any]) -> None:
    raw["knobs"]["rows"] = {"probability": p, "distribution": dist}


def _setup(tmp_path: Path, mutate: Any = None) -> contract_model.Setup:
    raw = json.loads(json.dumps(yaml.safe_load(PERF_CONTRACT.read_text())))
    if mutate:
        mutate(raw)
    path = tmp_path / "s.yaml"
    path.write_text(yaml.safe_dump(raw, sort_keys=False))
    return sc.load_setup(path, environ=ENV, known_transports=TRANSPORTS, deployment=fx.build())


def test_rows_knob_is_no_longer_refused(tmp_path: Path) -> None:
    setup = _setup(tmp_path, lambda r: _rows(r, 0.5, {"kind": "constant", "value": 10}))
    assert setup.knobs.distributions["rows"].probability == 0.5


def test_rows_may_not_draw_zero(tmp_path: Path) -> None:
    with pytest.raises(
        contract_model.SetupError, match="knobs.rows.distribution: a row count must be >= 1"
    ):
        _setup(tmp_path, lambda r: _rows(r, 1.0, {"kind": "uniform", "min": 0, "max": 10}))


def test_transport_override_is_checked(tmp_path: Path) -> None:
    def mutate(raw: dict[str, Any]) -> None:
        raw["transports"]["grpc"] = {
            "knobs": {"rows": {"probability": 1, "distribution": {"kind": "constant", "value": 0}}}
        }

    with pytest.raises(contract_model.SetupError, match="transports.grpc.knobs.rows.distribution"):
        _setup(tmp_path, mutate)


def test_no_knob_means_the_base_rows(tmp_path: Path) -> None:
    gen = request_mix.RequestGenerator(_setup(tmp_path), "pgwire", "s", cacheable=True)
    assert {gen.next().rows for _ in range(50)} == {1}


def test_rows_follow_the_distribution(tmp_path: Path) -> None:
    setup = _setup(tmp_path, lambda r: _rows(r, 1.0, {"kind": "uniform", "min": 5, "max": 9}))
    gen = request_mix.RequestGenerator(setup, "pgwire", "s", cacheable=True)
    assert {gen.next().rows for _ in range(300)} == {5, 6, 7, 8, 9}


def test_probability_mixes_base_and_drawn(tmp_path: Path) -> None:
    setup = _setup(tmp_path, lambda r: _rows(r, 0.25, {"kind": "constant", "value": 100}))
    gen = request_mix.RequestGenerator(setup, "pgwire", "s", cacheable=True)
    counts = Counter(gen.next().rows for _ in range(2000))
    assert set(counts) == {1, 100} and 400 < counts[100] < 600


def test_zipf_rows_are_skewed_to_small_results(tmp_path: Path) -> None:
    setup = _setup(
        tmp_path, lambda r: _rows(r, 1.0, {"kind": "zipf", "exponent": 1.3, "max": 1000})
    )
    gen = request_mix.RequestGenerator(setup, "pgwire", "s", cacheable=True)
    rows = sorted(gen.next().rows for _ in range(2000))
    assert rows[0] >= 1 and rows[-1] <= 1000 and rows[1000] < 50  # median is small


def test_same_seed_same_sequence_and_independent_of_other_knobs(tmp_path: Path) -> None:
    def only_rows(raw: dict[str, Any]) -> None:
        _rows(raw, 1.0, {"kind": "uniform", "min": 1, "max": 50})

    def rows_and_cache(raw: dict[str, Any]) -> None:
        only_rows(raw)
        raw["knobs"]["cache"]["probability"] = 0.5
        raw["knobs"]["fields"] = {
            "probability": 1,
            "distribution": {"kind": "constant", "value": 2},
        }

    a = request_mix.RequestGenerator(_setup(tmp_path, only_rows), "grpc", "x/1/1", cacheable=True)
    a2 = request_mix.RequestGenerator(_setup(tmp_path, only_rows), "grpc", "x/1/1", cacheable=True)
    b = request_mix.RequestGenerator(
        _setup(tmp_path, rows_and_cache), "grpc", "x/1/1", cacheable=True
    )
    ra, ra2, rb = ([a.next().rows for _ in range(100)] for a in (a, a2, b))
    assert ra == ra2 == rb


def test_per_transport_override(tmp_path: Path) -> None:
    def mutate(raw: dict[str, Any]) -> None:
        raw["transports"]["rest"] = {
            "knobs": {"rows": {"probability": 1, "distribution": {"kind": "constant", "value": 25}}}
        }

    setup = _setup(tmp_path, mutate)
    assert request_mix.RequestGenerator(setup, "rest", "s", cacheable=False).next().rows == 25
    assert request_mix.RequestGenerator(setup, "graphql", "s", cacheable=True).next().rows == 1


def test_zero_knob_still_equals_optimistic(tmp_path: Path) -> None:
    q = request_render.build_query(_setup(tmp_path), TRANSPORT, cached=True)
    for attr in ("sql", "cypher", "graphql", "grpc", "rest", "jsonapi"):
        assert getattr(q, attr) == getattr(OPTIMISTIC, attr), attr


def test_render_rows_on_every_transport(tmp_path: Path) -> None:
    spec = request_mix.RequestSpec("bench-postgresql", "orders", ("order_id",), 250, False)
    q = request_render.Renderer(_setup(tmp_path)).render(spec)
    assert q.sql.endswith("LIMIT 250") and q.cypher.endswith("LIMIT 250")
    assert "pb__orders(limit: 250)" in q.graphql
    assert q.grpc is not None and q.grpc["limit"] == 250
    assert q.rest is not None and q.rest["params"]["limit"] == 250
    assert q.jsonapi is not None and q.jsonapi["params"]["page[size]"] == 250


def _rec(latency: float, rows_asked: int, rows_returned: int) -> ol.Record:
    spec = request_mix.RequestSpec("s", "t", ("a",), rows_asked, False)
    return (latency, spec, None, rows_returned, 1)


def test_summary_by_rows_reports_what_came_back() -> None:
    reqs = [_rec(0.001, 1, 1)] * 4 + [_rec(0.020, 100, 100)] * 3 + [_rec(0.005, 1000, 40)] * 2
    out = ol.summarize_requests(reqs, window_s=1.0)
    assert list(out["by_rows"]) == ["1", "100", "1000"]
    assert out["by_rows"]["100"]["p50_ms"] == 20.0
    assert (
        out["by_rows"]["1000"]["avg_rows_returned"] == 40.0
    )  # fewer than asked: the table ran out
    assert out["by_rows"]["1"]["avg_rows_returned"] == 1.0
