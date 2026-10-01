# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""REQ-1911 report: what a step measures beyond latency — the mean of each knob, source CPU from
the source containers, and the per-request time split from the server's own stats."""

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
import measurements  # noqa: E402
import optimistic_load as ol  # noqa: E402
import request_mix  # noqa: E402
import setup_contract as sc  # noqa: E402

PERF_CONTRACT = BENCH / "setups" / "perf-stack.yaml"
ENV = {"PROVISA_HTTP_BASE_URL": "http://localhost:8001"}
TRANSPORTS = list(ol.TRANSPORTS)


def _setup(tmp_path: Path, mutate: Any = None) -> contract_model.Setup:
    raw = json.loads(json.dumps(yaml.safe_load(PERF_CONTRACT.read_text())))
    if mutate:
        mutate(raw)
    path = tmp_path / "s.yaml"
    path.write_text(yaml.safe_dump(raw, sort_keys=False))
    return sc.load_setup(path, environ=ENV, known_transports=TRANSPORTS, deployment=fx.build())


# ------------------------------------------------------------------ knob means in a step


def _rec(
    n_cols: int = 1,
    n_filters: int = 0,
    rows: int = 1,
    joins: tuple = (),
    repeat: int = 0,
    cached: bool = False,
    route: str | None = None,
) -> ol.Record:
    spec = request_mix.RequestSpec(
        "bench-postgresql",
        "orders",
        tuple("abcde"[:n_cols]),
        rows,
        cached,
        tuple(("c", 1) for _ in range(n_filters)),
        joins,
        repeat,
        route,
    )
    return (0.001, spec, None, rows, n_cols)


def _step(kind: str) -> contract_model.JoinStep:
    return contract_model.JoinStep(0, kind, "s", "t", "a", "b", True, None, None)


def test_summary_reports_the_mean_of_every_knob() -> None:
    reqs = [
        _rec(n_cols=1),
        _rec(
            n_cols=3, n_filters=2, rows=10, joins=(_step("same"),), cached=True, route="federated"
        ),
        _rec(n_cols=2, rows=4, joins=(_step("cross"), _step("cross")), repeat=1),
        _rec(n_cols=2, route="direct"),
    ]
    means = ol.summarize_requests(reqs, window_s=1.0)["knob_means"]
    assert means == {
        "fields": 2.0,
        "filters": 0.5,
        "rows": 4.0,
        "joins_same": 0.25,
        "joins_cross": 0.5,
        "cache_opt_in": 0.25,
        "repeat": 0.25,
        "federated": 0.25,
        "direct": 0.25,
    }


def test_summary_of_nothing_has_no_means() -> None:
    assert ol.summarize_requests([], window_s=1.0)["knob_means"] is None


# ------------------------------------------------------------------ docker stats


DOCKER_OUT = """perf-postgresql-1 12.50%
perf-clickhouse-1 150.00%
perf-mongodb-1 0.00%
unrelated-1 99.00%
"""


def test_parse_docker_stats_gives_cores() -> None:
    assert measurements.parse_docker_stats(DOCKER_OUT) == {
        "perf-postgresql-1": 0.125,
        "perf-clickhouse-1": 1.5,
        "perf-mongodb-1": 0.0,
        "unrelated-1": 0.99,
    }


@pytest.mark.parametrize("bad", ["perf-x", "perf-x abc%", "perf-x 12"])
def test_parse_docker_stats_refuses_a_malformed_line(bad: str) -> None:
    with pytest.raises(ValueError, match="docker stats line"):
        measurements.parse_docker_stats(bad)


def test_sampler_averages_each_monitored_container() -> None:
    outputs = iter(["a 100.00%\nb 50.00%\n", "a 200.00%\nb 50.00%\n"])
    s = measurements.DockerStatsSampler(
        {"src-a": "a", "src-b": "b", "src-c": "c"}, run=lambda: next(outputs)
    )
    s.sample_once()
    s.sample_once()
    result = s.result()
    assert result.cores == {"src-a": 1.5, "src-b": 0.5, "src-c": None}
    assert result.samples == 2 and result.error is None


def test_sampler_reports_a_runner_failure_instead_of_hiding_it() -> None:
    def boom() -> str:
        raise FileNotFoundError("docker")

    s = measurements.DockerStatsSampler({"src-a": "a"}, run=boom)
    s.sample_once()
    result = s.result()
    assert result.cores == {"src-a": None}
    assert result.error == "FileNotFoundError: docker"


def test_sampler_thread_samples_until_stopped() -> None:
    n = {"calls": 0}

    def run() -> str:
        n["calls"] += 1
        return "a 100.00%\n"

    s = measurements.DockerStatsSampler({"src-a": "a"}, run=run, pause_s=0.01)
    s.start()
    while n["calls"] < 3:
        pass
    s.stop()
    assert s.result().samples >= 3 and s.result().cores["src-a"] == 1.0


def test_source_cpu_per_request() -> None:
    out = measurements.source_cpu_fields(
        {"bench-postgresql": 0.5, "bench-clickhouse": None},
        window_s=10.0,
        attempts=1000,
        requests_by_source={"bench-postgresql": 800, "bench-clickhouse": 200},
    )
    assert out == {
        "source_cpu_cores": {"bench-postgresql": 0.5, "bench-clickhouse": None},
        # 0.5 cores x 10 s = 5 cpu-s over 1000 requests
        "source_cpu_ms_per_request": {"bench-postgresql": 5.0, "bench-clickhouse": None},
        # ... and per request that read the source (800 of them)
        "source_cpu_ms_per_source_request": {"bench-postgresql": 6.25, "bench-clickhouse": None},
    }


def test_source_without_requests_has_no_per_source_request_cost() -> None:
    out = measurements.source_cpu_fields(
        {"bench-neo4j": 0.1}, window_s=10.0, attempts=100, requests_by_source={}
    )
    assert out["source_cpu_ms_per_source_request"] == {"bench-neo4j": None}


# ------------------------------------------------------------------ the contract names the containers


def test_perf_contract_names_the_monitored_containers(tmp_path: Path) -> None:
    setup = _setup(tmp_path)
    assert {s: src.container for s, src in setup.sources.items()} == {
        "bench-postgresql": "perf-postgresql-1",
        "bench-clickhouse": "perf-clickhouse-1",
        "bench-mongodb": "perf-mongodb-1",
        "bench-neo4j": "perf-neo4j-1",
    }


def test_monitor_is_optional(tmp_path: Path) -> None:
    def mutate(raw: dict[str, Any]) -> None:
        del raw["sources"]["bench-neo4j"]["monitor"]

    assert _setup(tmp_path, mutate).sources["bench-neo4j"].container is None


def test_malformed_monitor(tmp_path: Path) -> None:
    def mutate(raw: dict[str, Any]) -> None:
        raw["sources"]["bench-neo4j"]["monitor"] = {"container": ""}

    with pytest.raises(
        contract_model.SetupError, match="monitor.container: must be a non-empty string"
    ):
        _setup(tmp_path, mutate)


def test_with_source_cpu_names_samples_and_errors() -> None:
    sampled = measurements.SampleResult({"bench-postgresql": 0.5}, samples=7, error=None)
    out = ol.with_source_cpu(
        sampled, {"by_source": {"bench-postgresql": {"requests": 400}}}, window_s=10.0, attempts=500
    )
    assert out["source_cpu_samples"] == 7 and out["source_cpu_error"] is None
    assert out["source_cpu_ms_per_request"] == {"bench-postgresql": 10.0}
    assert out["source_cpu_ms_per_source_request"] == {"bench-postgresql": 12.5}
    failed = measurements.SampleResult(
        {"bench-postgresql": None}, samples=0, error="FileNotFoundError: docker"
    )
    out = ol.with_source_cpu(failed, {"by_source": {}}, window_s=10.0, attempts=0)
    assert out["source_cpu_error"] == "FileNotFoundError: docker"
    assert out["source_cpu_ms_per_request"] == {"bench-postgresql": None}


# ------------------------------------------------------------------ the time split from the server's stats


def _stats(total: float, *entries: tuple[str, float, bool]) -> dict[str, Any]:
    return {
        "total_elapsed_ms": total,
        "sources": [
            {
                "field": "f",
                "source": "s",
                "strategy": strat,
                "elapsed_ms": ms,
                "rows": 1,
                **({"cache_hit": True} if hit else {}),
            }
            for strat, ms, hit in entries
        ],
    }


def test_time_split_separates_provisa_engine_and_source() -> None:
    split = measurements.time_split(
        _stats(
            10.0,
            ("direct:postgresql", 3.0, False),
            ("federated:pg", 2.0, False),
            ("api:openapi", 1.0, False),
        )
    )
    assert split == {
        "total_ms": 10.0,
        "source_ms": 4.0,
        "engine_ms": 2.0,
        "provisa_ms": 4.0,
        "cache_hit": False,
        "entries_exceed_total": False,
    }


def test_time_split_of_a_cache_hit_is_all_provisa() -> None:
    split = measurements.time_split(_stats(1.5, ("cache", 0.2, True)))
    assert split["provisa_ms"] == 1.5 and split["source_ms"] == 0.0 and split["cache_hit"] is True


def test_time_split_flags_overlapping_entries() -> None:
    split = measurements.time_split(_stats(5.0, ("direct:a", 4.0, False), ("direct:b", 4.0, False)))
    assert split["entries_exceed_total"] is True and split["provisa_ms"] == 0.0


def test_time_split_refuses_an_unknown_strategy() -> None:
    with pytest.raises(ValueError, match="unknown strategy 'warp'"):
        measurements.time_split(_stats(1.0, ("warp", 0.5, False)))


@pytest.mark.parametrize(
    "transport,body",
    [
        (
            "graphql",
            {"data": {}, "extensions": {"provisa_stats": {"total_elapsed_ms": 1.0, "sources": []}}},
        ),
        ("data_sql", {"data": {}, "provisa_stats": {"total_elapsed_ms": 1.0, "sources": []}}),
        ("cypher_http", {"rows": [], "provisa_stats": {"total_elapsed_ms": 1.0, "sources": []}}),
    ],
)
def test_stats_are_found_where_each_transport_puts_them(transport: str, body: dict) -> None:
    assert measurements.extract_stats(transport, body) == {"total_elapsed_ms": 1.0, "sources": []}


def test_a_response_without_stats_is_an_error() -> None:
    with pytest.raises(ValueError, match="no provisa_stats in the graphql response"):
        measurements.extract_stats("graphql", {"data": {}})


def test_the_stats_header_profile_covers_http_transports_only() -> None:
    assert measurements.PROFILE_TRANSPORTS == ("graphql", "data_sql", "cypher_http")


def test_profile_sends_the_header_and_aggregates(tmp_path: Path) -> None:
    import httpx

    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        stats = _stats(10.0, ("direct:postgresql", 6.0, False))
        return httpx.Response(
            200,
            json={
                "data": {"sql": [{"order_id": 1}]},
                "columns": ["order_id"],
                "provisa_stats": stats,
            },
        )

    setup = _setup(tmp_path)
    client = httpx.Client(base_url="http://x", transport=httpx.MockTransport(handler))
    out = measurements.profile_transport(setup, "data_sql", client, role="org_admin", requests=20)
    assert len(seen) == 20
    assert all(r.headers["x-provisa-stats"] == "true" for r in seen)
    group = out["by_source"]["bench-postgresql"]
    assert group == {
        "requests": 20,
        "total_ms": 10.0,
        "provisa_ms": 4.0,
        "engine_ms": 0.0,
        "source_ms": 6.0,
        "cache_hits": 0,
        "entries_exceed_total": 0,
    }
    assert out["requests"] == 20 and out["transport"] == "data_sql"


def test_profile_refuses_a_transport_without_stats() -> None:
    with pytest.raises(ValueError, match="no per-request time split on pgwire"):
        measurements.profile_transport(None, "pgwire", None, role="r", requests=1)  # type: ignore[arg-type]


# ------------------------------------------------------------------ the profile is part of a run


def test_load_names_how_many_profile_requests(tmp_path: Path) -> None:
    assert _setup(tmp_path).load.profile_requests == 0
    assert (
        _setup(tmp_path, lambda r: r["load"].update({"profile_requests": 50})).load.profile_requests
        == 50
    )


def test_profile_requests_is_required_and_not_negative(tmp_path: Path) -> None:
    def drop(raw: dict[str, Any]) -> None:
        del raw["load"]["profile_requests"]

    with pytest.raises(contract_model.SetupError, match="load.profile_requests: required"):
        _setup(tmp_path, drop)
    with pytest.raises(
        contract_model.SetupError, match="load.profile_requests: value must be >= 0"
    ):
        _setup(tmp_path, lambda r: r["load"].update({"profile_requests": -1}))


def _run_transport(
    monkeypatch: pytest.MonkeyPatch, setup: contract_model.Setup, transport: str
) -> dict:
    monkeypatch.setattr(
        ol, "verify", lambda t, ep: {"rows": 1, "columns": 1, "second_request_hit": None}
    )
    monkeypatch.setattr(ol, "run_step", lambda *a: {"concurrency": a[2]})
    ep = ol.endpoints_from_setup(setup)
    return ol.run_transport(transport, ep, (1,), 1.0, 1, None)


def test_run_transport_adds_the_time_split_for_http_surfaces(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    calls: list[Any] = []
    monkeypatch.setattr(
        measurements,
        "profile_transport",
        lambda setup, t, client, role, requests: (
            calls.append((t, role, requests)) or {"transport": t}
        ),
    )
    setup = _setup(tmp_path, lambda r: r["load"].update({"profile_requests": 7}))
    report = _run_transport(monkeypatch, setup, "graphql")
    assert report["time_split"] == {"transport": "graphql"} and calls == [
        ("graphql", "org_admin", 7)
    ]
    assert _run_transport(monkeypatch, setup, "pgwire")["time_split"] is None  # no stats there


def test_run_transport_without_profile_requests_has_no_time_split(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    assert _run_transport(monkeypatch, _setup(tmp_path), "graphql")["time_split"] is None


def test_a_failing_profile_is_reported(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    def boom(*a: Any, **k: Any) -> Any:
        raise ValueError("no provisa_stats in the graphql response")

    monkeypatch.setattr(measurements, "profile_transport", boom)
    setup = _setup(tmp_path, lambda r: r["load"].update({"profile_requests": 3}))
    report = _run_transport(monkeypatch, setup, "graphql")
    assert report["time_split"] is None
    assert report["time_split_error"] == "ValueError: no provisa_stats in the graphql response"


# ------------------------------------------------------------------ the idle window (the refresh cost)


def test_measure_idle_reports_the_cpu_of_an_idle_window() -> None:
    cpu = iter([100.0, 103.0])  # the server used 3 CPU-s over the window
    slept: list[float] = []
    sampler = measurements.DockerStatsSampler({"bench-clickhouse": "ch"}, run=lambda: "ch 40.00%\n")
    out = measurements.measure_idle(
        window_s=60.0,
        server_cpu=lambda: next(cpu),
        sampler=sampler,
        sleep=lambda s: slept.append(s) or sampler.sample_once(),
    )
    assert slept == [60.0]
    assert out == {
        "window_s": 60.0,
        "server_cpu_cores": 0.05,
        "source_cpu_cores": {"bench-clickhouse": 0.4},
        "source_cpu_error": None,
    }


def test_measure_idle_reports_a_failing_docker() -> None:
    def boom() -> str:
        raise FileNotFoundError("docker")

    sampler = measurements.DockerStatsSampler({"bench-clickhouse": "ch"}, run=boom)
    cpu = iter([0.0, 0.6])
    out = measurements.measure_idle(
        window_s=60.0,
        server_cpu=lambda: next(cpu),
        sampler=sampler,
        sleep=lambda s: sampler.sample_once(),
    )
    assert out["source_cpu_error"] == "FileNotFoundError: docker"
    assert out["source_cpu_cores"] == {"bench-clickhouse": None}


def test_load_names_the_idle_window(tmp_path: Path) -> None:
    assert _setup(tmp_path).load.idle_window_s == 0
    assert (
        _setup(tmp_path, lambda r: r["load"].update({"idle_window_s": 90})).load.idle_window_s == 90
    )


def test_idle_window_is_required(tmp_path: Path) -> None:
    def drop(raw: dict[str, Any]) -> None:
        del raw["load"]["idle_window_s"]

    with pytest.raises(contract_model.SetupError, match="load.idle_window_s: required"):
        _setup(tmp_path, drop)


def _fake_report(*a: Any) -> dict[str, Any]:
    return {
        "transport": a[0],
        "cached": True,
        "cache_probability": 1.0,
        "shape": {"rows": 1, "columns": 1},
        "steps": [],
    }


def test_run_all_writes_the_idle_measurement(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    calls: list[Any] = []
    monkeypatch.setattr(ol, "run_transport", _fake_report)
    monkeypatch.setattr(ol, "_server_cpu_seconds", lambda pid: 0.0)
    monkeypatch.setattr(
        measurements,
        "measure_idle",
        lambda **kw: (
            calls.append(kw["window_s"])
            or {
                "window_s": kw["window_s"],
                "server_cpu_cores": 0.1,
                "source_cpu_cores": {},
                "source_cpu_error": None,
            }
        ),
    )
    setup = _setup(tmp_path, lambda r: r["load"].update({"idle_window_s": 5}))
    ol.run_all(ol.endpoints_from_setup(setup), tmp_path / "out", ["graphql"], (1,), 1.0, 1, 123)
    assert calls == [5]
    assert json.loads((tmp_path / "out" / "idle.json").read_text())["server_cpu_cores"] == 0.1


def test_run_all_without_an_idle_window_writes_nothing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(ol, "run_transport", _fake_report)
    ol.run_all(
        ol.endpoints_from_setup(_setup(tmp_path)), tmp_path / "out", ["graphql"], (1,), 1.0, 1, None
    )
    assert not (tmp_path / "out" / "idle.json").exists()


def test_an_idle_window_needs_the_server_pid(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(ol, "run_transport", _fake_report)
    setup = _setup(tmp_path, lambda r: r["load"].update({"idle_window_s": 5}))
    with pytest.raises(contract_model.SetupError, match="load.idle_window_s needs --server-pid"):
        ol.run_all(
            ol.endpoints_from_setup(setup), tmp_path / "out", ["graphql"], (1,), 1.0, 1, None
        )
