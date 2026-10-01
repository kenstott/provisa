# Copyright (c) 2026 Kenneth Stott
# Canary: b5fa11cd-039e-4022-a91b-066923c55cac
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""REQ-1911: the offered-rate load. Requests arrive on a schedule that does not wait for the
previous answer, so a slow server shows as latency and as lag, not as a lower offered rate."""

from __future__ import annotations

import json
import queue
import sys
import threading
import time
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
import setup_contract as sc  # noqa: E402

PERF_CONTRACT = BENCH / "setups" / "perf-stack.yaml"
ENV = {"PROVISA_HTTP_BASE_URL": "http://localhost:8001"}
TRANSPORTS = list(ol.TRANSPORTS)


def _open(rate: float = 200, connections: int = 2, window: float = 0.5) -> Any:
    def mutate(raw: dict[str, Any]) -> None:
        raw["load"] = {
            "mode": "open_loop",
            "rate_per_s": rate,
            "connections": connections,
            "window_s": window,
            "processes": 1,
            "profile_requests": 0,
            "idle_window_s": 0,
        }

    return mutate


def _setup(tmp_path: Path, mutate: Any = None) -> contract_model.Setup:
    raw = json.loads(json.dumps(yaml.safe_load(PERF_CONTRACT.read_text())))
    if mutate:
        mutate(raw)
    path = tmp_path / "s.yaml"
    path.write_text(yaml.safe_dump(raw, sort_keys=False))
    return sc.load_setup(path, environ=ENV, known_transports=TRANSPORTS, deployment=fx.build())


# ------------------------------------------------------------------ the contract


def test_open_loop_is_no_longer_refused(tmp_path: Path) -> None:
    load = _setup(tmp_path, _open()).load
    assert (load.mode, load.rate_per_s, load.steps, load.window_s) == ("open_loop", 200, (2,), 0.5)


@pytest.mark.parametrize(
    "change,msg",
    [
        ({"connections": 0}, "load.connections: value must be >= 1"),
        ({"rate_per_s": 0}, "load.rate_per_s: must be > 0"),
        ({"rate_per_s": "fast"}, "load.rate_per_s: must be a number"),
    ],
)
def test_open_loop_values_are_validated(tmp_path: Path, change: dict, msg: str) -> None:
    def mutate(raw: dict[str, Any]) -> None:
        _open()(raw)
        raw["load"].update(change)

    with pytest.raises(contract_model.SetupError, match=msg):
        _setup(tmp_path, mutate)


def test_open_loop_needs_connections(tmp_path: Path) -> None:
    def mutate(raw: dict[str, Any]) -> None:
        _open()(raw)
        del raw["load"]["connections"]

    with pytest.raises(contract_model.SetupError, match="load.connections: required"):
        _setup(tmp_path, mutate)


def test_closed_loop_does_not_take_a_rate(tmp_path: Path) -> None:
    def mutate(raw: dict[str, Any]) -> None:
        raw["load"]["rate_per_s"] = 5

    with pytest.raises(contract_model.SetupError, match="load: unknown key 'rate_per_s'"):
        _setup(tmp_path, mutate)


# ------------------------------------------------------------------ the schedule


def test_each_client_sends_every_interval_from_its_phase() -> None:
    times = ol.open_loop_schedule(start=100.0, interval=0.5, phase=0.25, stop_at=102.0)
    assert [round(t, 6) for t in times] == [100.125, 100.625, 101.125, 101.625]


def test_phases_spread_the_clients_across_the_interval() -> None:
    assert [ol.open_loop_phase(client, 4) for client in range(4)] == [0.0, 0.25, 0.5, 0.75]


def test_the_schedule_does_not_wait_for_answers() -> None:
    # a schedule is a function of time only
    a = list(ol.open_loop_schedule(0.0, 0.1, 0.0, 1.0))
    b = list(ol.open_loop_schedule(0.0, 0.1, 0.0, 1.0))
    assert a == b and len(a) == 10


# ------------------------------------------------------------------ the loop


class _Recorder:
    delay = 0.0
    last: Any = None

    def __init__(self, ep: Any) -> None:
        self.sent: list[float] = []
        _Recorder.last = self

    def call(self, query: Any, role: str) -> tuple[int, int, bool | None]:
        self.sent.append(time.perf_counter())
        if _Recorder.delay:
            time.sleep(_Recorder.delay)
        return 1, 1, None

    def close(self) -> None:
        pass


def _run(
    monkeypatch: pytest.MonkeyPatch, setup: contract_model.Setup, threads: int = 2
) -> dict[str, Any]:
    monkeypatch.setitem(ol.CLIENTS, "graphql", _Recorder)
    ep = ol.endpoints_from_setup(setup)
    go = threading.Event()
    go.set()
    ready: queue.Queue = queue.Queue()
    out: queue.Queue = queue.Queue()
    ol._client_process("graphql", ep, 0, threads, setup.load.window_s, ready, go, out)  # noqa: SLF001
    assert ready.get() is None
    return out.get()


def test_the_loop_offers_the_contracts_rate(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _Recorder.delay = 0.0
    result = _run(monkeypatch, _setup(tmp_path, _open(rate=200, connections=2, window=0.5)))
    n = len(result["records"])
    assert 80 <= n <= 105  # 200/s for 0.5 s: about 100


def test_a_slow_server_shows_as_latency_not_as_a_lower_offered_rate(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _Recorder.delay = 0.02  # each answer takes 20 ms; 2 clients at 100/s each want one every 10 ms
    try:
        result = _run(monkeypatch, _setup(tmp_path, _open(rate=200, connections=2, window=0.4)))
    finally:
        _Recorder.delay = 0.0
    latencies = sorted(r[0] for r in result["records"])
    # latency runs from the scheduled arrival, so the queue behind a slow answer is in it
    assert latencies[-1] > 0.05
    assert result["max_lag_s"] > 0.05


def test_latency_is_measured_from_the_scheduled_time(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _Recorder.delay = 0.0
    result = _run(
        monkeypatch, _setup(tmp_path, _open(rate=100, connections=1, window=0.3)), threads=1
    )
    assert all(r[0] >= 0 for r in result["records"])
    assert result["max_lag_s"] >= 0


def test_closed_loop_results_carry_no_lag(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _Recorder.delay = 0.0
    result = _run(monkeypatch, _setup(tmp_path))
    assert result["max_lag_s"] is None


def test_run_all_runs_the_open_loop_at_its_connection_count(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    seen: list[Any] = []
    monkeypatch.setattr(ol, "run_all", lambda *a, **k: seen.append(a) or [])
    monkeypatch.setenv("PROVISA_HTTP_BASE_URL", "http://localhost:8001")
    path = tmp_path / "s.yaml"
    raw = json.loads(json.dumps(yaml.safe_load(PERF_CONTRACT.read_text())))
    _open(rate=300, connections=6, window=7)(raw)
    path.write_text(yaml.safe_dump(raw, sort_keys=False))
    from types import SimpleNamespace

    import lookup

    names = tmp_path / "names.json"
    unbound = sc.load_setup(path, environ=ENV, known_transports=TRANSPORTS)
    lookup.write_resolved(lookup.resolve(*sc.identities(unbound), fx.build()), names)
    args = SimpleNamespace(
        setup=str(path),
        server_pid=None,
        resolved_names=str(names),
        apply_replication=False,
        skip_route_verification=True,
    )
    ol.run_from_args(args, tmp_path, argv=["x"])
    ep, out, transports, steps, window, procs, pid = seen[0]
    assert steps == (6,) and window == 7


def test_the_step_reports_target_and_achieved_rate() -> None:
    summary = ol.summarize_requests([], window_s=2.0)
    fields = ol.open_loop_fields(rate_per_s=300.0, requests=540, window_s=2.0, max_lag_s=0.012)
    assert fields == {
        "load_mode": "open_loop",
        "target_rate_per_s": 300.0,
        "achieved_rate_per_s": 270.0,
        "schedule_lag_ms_max": 12.0,
    }
    assert summary["requests"] == 0
