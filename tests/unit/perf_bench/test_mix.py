# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""REQ-1911: the seeded randomized mix of every knob at once, with per-transport overrides."""

from __future__ import annotations

import json
import queue
import sys
import threading
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

PERF_CONTRACT = BENCH / "setups" / "perf-stack.yaml"
ENV = {"PROVISA_HTTP_BASE_URL": "http://localhost:8001"}
TRANSPORTS = list(ol.TRANSPORTS)
SOURCES = ["bench-postgresql", "bench-clickhouse", "bench-mongodb", "bench-neo4j"]
HINT_FREE = ["grpc", "rest", "jsonapi"]  # single table, no route hint
ZERO = {"probability": 0, "distribution": {"kind": "constant", "value": 0}}
N = 3000


def full_mix(raw: dict[str, Any]) -> None:
    k = raw["knobs"]
    k["cache"]["probability"] = 0.6
    k["fields"] = {"probability": 0.5, "distribution": {"kind": "uniform", "min": 1, "max": 4}}
    k["filters"] = {"probability": 0.5, "distribution": {"kind": "uniform", "min": 0, "max": 2}}
    k["rows"] = {"probability": 0.3, "distribution": {"kind": "zipf", "exponent": 1.3, "max": 50}}
    k["joins_same_source"] = {"probability": 0.3, "distribution": {"kind": "constant", "value": 1}}
    k["joins_cross_source"] = {
        "probability": 0.3,
        "distribution": {"kind": "uniform", "min": 0, "max": 2},
    }
    k["cache_repetition"] = {
        "probability": 0.2,
        "distribution": {"kind": "uniform", "min": 1, "max": 5},
    }
    k["source_weights"] = {
        "bench-postgresql": 0.55,
        "bench-clickhouse": 0.25,
        "bench-mongodb": 0.2,
        "bench-neo4j": 0.0,
    }
    k["route"]["bench-clickhouse"] = {"mode": "federated", "federated_probability": 1}
    k["route"]["bench-mongodb"] = {"mode": "mixed", "federated_probability": 0.5}
    pg_only = {s: 1.0 if s == "bench-postgresql" else 0.0 for s in SOURCES}
    auto = {s: {"mode": "auto", "federated_probability": 0} for s in SOURCES}
    for t in HINT_FREE:
        raw["transports"][t] = {
            "knobs": {
                "joins_same_source": ZERO,
                "joins_cross_source": ZERO,
                "source_weights": pg_only,
                "route": auto,
            }
        }
    raw["transports"]["pgwire"] = {"knobs": {"cache": {"probability": 0.9}}}
    raw["transports"]["bolt"] = {
        "knobs": {"rows": {"probability": 1, "distribution": {"kind": "constant", "value": 3}}}
    }
    raw["seed"] = 42


def _setup(tmp_path: Path, mutate: Any = full_mix) -> contract_model.Setup:
    raw = json.loads(json.dumps(yaml.safe_load(PERF_CONTRACT.read_text())))
    mutate(raw)
    path = tmp_path / "s.yaml"
    path.write_text(yaml.safe_dump(raw, sort_keys=False))
    return sc.load_setup(path, environ=ENV, known_transports=TRANSPORTS, deployment=fx.build())


@pytest.fixture(scope="module")
def mix(tmp_path_factory: pytest.TempPathFactory) -> contract_model.Setup:
    return _setup(tmp_path_factory.mktemp("mix"))


def _specs(
    setup: contract_model.Setup, transport: str, n: int = N, stream: str = "mix/0/0"
) -> list[request_mix.RequestSpec]:
    gen = request_mix.RequestGenerator(
        setup, transport, stream, cacheable=ol.TRANSPORTS[transport][1] is not None
    )
    return [gen.next() for _ in range(n)]


def _share(specs: list[Any], pred: Any) -> float:
    return sum(1 for s in specs if pred(s)) / len(specs)


# ------------------------------------------------------------------ every transport gets a sendable request


@pytest.mark.parametrize("transport", TRANSPORTS)
def test_every_request_renders_for_its_transport(mix: contract_model.Setup, transport: str) -> None:
    renderer = request_render.Renderer(mix)
    language = contract_model.TRANSPORT_LANGUAGE[transport]
    for spec in _specs(mix, transport, 1500):
        assert getattr(renderer.render(spec), language) is not None, spec


@pytest.mark.parametrize("transport", TRANSPORTS)
def test_same_seed_same_sequence_per_transport(mix: contract_model.Setup, transport: str) -> None:
    assert _specs(mix, transport, 300) == _specs(mix, transport, 300)


def test_streams_and_seeds_differ(tmp_path: Path, mix: contract_model.Setup) -> None:
    assert _specs(mix, "data_sql", 200) != _specs(mix, "data_sql", 200, stream="mix/0/1")

    def other_seed(raw: dict[str, Any]) -> None:
        full_mix(raw)
        raw["seed"] = 43

    assert _specs(mix, "data_sql", 200) != _specs(_setup(tmp_path, other_seed), "data_sql", 200)


# ------------------------------------------------------------------ the mix follows the knobs


def test_marginals_on_a_transport_with_no_override(mix: contract_model.Setup) -> None:
    specs = _specs(mix, "data_sql")
    assert abs(_share(specs, lambda s: s.cached) - 0.6) < 0.06
    assert abs(_share(specs, lambda s: len(s.columns) > 1) - 0.375) < 0.06
    assert abs(_share(specs, lambda s: len(s.filters) > 0) - 0.5 * 2 / 3) < 0.06
    assert 0.1 < _share(specs, lambda s: s.rows > 1) < 0.3
    assert abs(_share(specs, lambda s: any(j.kind == "same" for j in s.joins)) - 0.3) < 0.07
    assert (
        abs(_share(specs, lambda s: any(j.kind == "cross" for j in s.joins)) - 0.3 * 2 / 3) < 0.07
    )
    assert (
        abs(_share(specs, lambda s: s.repeat > 0) - 0.2 * 0.9) < 0.07
    )  # the first few cannot repeat
    assert {s.source for s in specs} == {"bench-postgresql", "bench-clickhouse", "bench-mongodb"}


def test_route_modes_follow_the_source(mix: contract_model.Setup) -> None:
    routes: dict[str, Counter[str | None]] = {}
    for s in _specs(mix, "data_sql"):
        routes.setdefault(s.source, Counter())[s.route] += 1
    assert set(routes["bench-postgresql"]) == {None}
    assert set(routes["bench-clickhouse"]) == {"federated"}
    assert set(routes["bench-mongodb"]) == {"direct", "federated"}


# ------------------------------------------------------------------ per-transport overrides


def test_overrides_change_only_their_transport(mix: contract_model.Setup) -> None:
    assert abs(_share(_specs(mix, "pgwire"), lambda s: s.cached) - 0.9) < 0.05
    assert abs(_share(_specs(mix, "flight_sql"), lambda s: s.cached) - 0.6) < 0.06
    assert {s.rows for s in _specs(mix, "bolt")} == {3}
    assert {s.rows for s in _specs(mix, "cypher_http")} != {3}


@pytest.mark.parametrize("transport", HINT_FREE)
def test_single_table_surfaces_get_plain_postgres_requests(
    mix: contract_model.Setup, transport: str
) -> None:
    specs = _specs(mix, transport)
    assert {s.source for s in specs} == {"bench-postgresql"}
    assert {s.joins for s in specs} == {()}
    assert {s.route for s in specs} == {None}
    assert {len(s.columns) > 1 for s in specs} == {True, False}  # the other knobs still apply


# ------------------------------------------------------------------ through the client loop


class _Recorder:
    last: Any = None

    def __init__(self, ep: Any) -> None:
        self.sent: list[Any] = []
        _Recorder.last = self

    def call(self, query: Any, role: str) -> tuple[int, int, bool | None]:
        self.sent.append(query)
        return 1, 1, None

    def close(self) -> None:
        pass


def test_the_loop_sends_the_mix_and_records_what_it_sent(
    monkeypatch: pytest.MonkeyPatch, mix: contract_model.Setup
) -> None:
    monkeypatch.setitem(ol.CLIENTS, "graphql", _Recorder)
    ep = ol.endpoints_from_setup(mix)
    go = threading.Event()
    go.set()
    ready: queue.Queue = queue.Queue()
    out: queue.Queue = queue.Queue()
    ol._client_process("graphql", ep, 0, 1, 0.3, ready, go, out)  # noqa: SLF001
    assert ready.get() is None
    records = out.get()["records"]
    sent = _Recorder.last.sent
    assert len(records) > 200
    assert all(q.graphql is not None for q in sent)
    assert len({q.graphql for q in sent}) > 50  # a mix, not one request
    summary = ol.summarize_requests(records, window_s=0.3)
    assert set(summary["by_source"]) >= {"bench-postgresql", "bench-clickhouse"}
    assert set(summary["by_route"]) == {"auto", "direct", "federated"}
    assert set(summary["by_repeat"]) == {"fresh", "repeat"}
    assert len(summary["by_fields"]) > 1 and len(summary["by_filters"]) > 1
    assert "1" in summary["by_joins_same"] and "1" in summary["by_joins_cross"]
