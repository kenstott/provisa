# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""REQ-1911: the report derived from a set of sweep runs — cost models per transport and source
(Provisa CPU and source CPU kept apart), the check of a mix against the model, and the break-even
between live and replica reads."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pytest

BENCH = Path(__file__).resolve().parents[3] / "demo" / "named" / "perf" / "bench"
sys.path.insert(0, str(BENCH))

import bench_report as br  # noqa: E402
import cost_model as cm  # noqa: E402

PROVISA = {
    "base": 0.20,
    "fields": 0.010,
    "filters": 0.030,
    "rows": 0.0010,
    "joins_same": 0.50,
    "joins_cross": 1.20,
}
SOURCE = {
    "base": 0.05,
    "fields": 0.002,
    "filters": 0.010,
    "rows": 0.0030,
    "joins_same": 0.10,
    "joins_cross": 0.00,
}
ZERO = {"fields": 1.0, "filters": 0.0, "rows": 1.0, "joins_same": 0.0, "joins_cross": 0.0}


def _cpu(model: dict[str, float], means: dict[str, float]) -> float:
    return model["base"] + sum(model[k] * v for k, v in means.items())


def _write_run(
    root: Path,
    name: str,
    transport: str,
    source: str,
    knob: str,
    value: Any,
    means: dict[str, float],
    *,
    provisa: float | None = None,
    src: float | None = None,
    ttl: int = 0,
    extra: dict | None = None,
) -> dict[str, Any]:
    step = {
        "concurrency": 1,
        "knob_means": means,
        "server_cpu_ms_per_request": _cpu(PROVISA, means) if provisa is None else provisa,
        "source_cpu_ms_per_request": {source: _cpu(SOURCE, means) if src is None else src},
        **(extra or {}),
    }
    busy = {**step, "concurrency": 16, "server_cpu_ms_per_request": 99.0}  # not the one costed
    out = root / "results" / name / "optimistic"
    out.mkdir(parents=True)
    (out / f"{transport}.json").write_text(
        json.dumps({"transport": transport, "steps": [busy, step]})
    )
    return {
        "name": name,
        "transport": transport,
        "source": source,
        "knob": knob,
        "value": value,
        "contract": f"{name}.yaml",
        "skipped": None,
        "ttl": ttl,
    }


def _sweep(
    root: Path, transport: str = "graphql", source: str = "bench-postgresql"
) -> list[dict[str, Any]]:
    runs = [
        _write_run(
            root, f"{transport}.{source}.baseline", transport, source, "baseline", None, ZERO
        )
    ]
    plan = {
        "fields": [2, 4],
        "filters": [1, 2],
        "rows": [10, 100],
        "joins_same_source": [1, 2],
        "joins_cross_source": [1, 2],
    }
    key = {"joins_same_source": "joins_same", "joins_cross_source": "joins_cross"}
    for knob, vals in plan.items():
        for v in vals:
            means = dict(ZERO)
            means[key.get(knob, knob)] = float(v)
            runs.append(
                _write_run(
                    root, f"{transport}.{source}.{knob}.{v}", transport, source, knob, v, means
                )
            )
    return runs


def _manifest(root: Path, runs: list[dict[str, Any]]) -> None:
    (root / "manifest.json").write_text(
        json.dumps({"label": "internal: not for publication", "runs": runs})
    )


def test_the_report_recovers_the_cost_of_each_knob_for_provisa_and_the_source(
    tmp_path: Path,
) -> None:
    _manifest(tmp_path, _sweep(tmp_path))
    report = br.build_report(tmp_path)
    cell = report["transports"]["graphql"]["bench-postgresql"]
    assert cell["provisa_cpu"]["base"] == pytest.approx(PROVISA["base"])
    for knob in cm.KNOBS:
        assert cell["provisa_cpu"]["per_unit"][knob] == pytest.approx(PROVISA[knob]), knob
        assert cell["source_cpu"]["per_unit"][knob] == pytest.approx(SOURCE[knob], abs=1e-9), knob
    assert cell["provisa_cpu"]["r2"] == pytest.approx(1.0)
    assert cell["not_modelled"] == {}


def test_the_report_is_labelled_internal(tmp_path: Path) -> None:
    _manifest(tmp_path, _sweep(tmp_path))
    assert br.build_report(tmp_path)["label"] == "internal: not for publication"


def test_a_knob_that_could_not_be_swept_is_named_not_modelled(tmp_path: Path) -> None:
    runs = [
        r for r in _sweep(tmp_path) if r["knob"] not in ("joins_same_source", "joins_cross_source")
    ]
    _manifest(tmp_path, runs)
    cell = br.build_report(tmp_path)["transports"]["graphql"]["bench-postgresql"]
    assert set(cell["not_modelled"]) == {"joins_same", "joins_cross"}
    assert "does not vary" in cell["not_modelled"]["joins_same"]
    assert set(cell["provisa_cpu"]["per_unit"]) == {"fields", "filters", "rows"}


def test_skipped_runs_are_listed_not_hidden(tmp_path: Path) -> None:
    runs = _sweep(tmp_path)
    runs.append(
        {
            "name": "rest.bench-clickhouse.baseline",
            "transport": "rest",
            "source": "bench-clickhouse",
            "knob": "baseline",
            "value": None,
            "contract": None,
            "skipped": "transports.rest: has no rest naming",
        }
    )
    _manifest(tmp_path, runs)
    report = br.build_report(tmp_path)
    assert report["skipped"] == [
        {"name": "rest.bench-clickhouse.baseline", "reason": "transports.rest: has no rest naming"}
    ]


def test_a_run_without_server_cpu_is_an_error(tmp_path: Path) -> None:
    runs = _sweep(tmp_path)
    path = tmp_path / "results" / runs[0]["name"] / "optimistic" / "graphql.json"
    data = json.loads(path.read_text())
    del data["steps"][1]["server_cpu_ms_per_request"]
    path.write_text(json.dumps(data))
    _manifest(tmp_path, runs)
    with pytest.raises(
        ValueError,
        match="graphql.bench-postgresql.baseline: no server_cpu_ms_per_request at concurrency 1",
    ):
        br.build_report(tmp_path)


def test_a_run_whose_transport_failed_is_an_error(tmp_path: Path) -> None:
    runs = _sweep(tmp_path)
    path = tmp_path / "results" / runs[0]["name"] / "optimistic" / "graphql.json"
    path.write_text(json.dumps({"transport": "graphql", "error": "ConnectError: refused"}))
    _manifest(tmp_path, runs)
    with pytest.raises(
        ValueError,
        match="graphql.bench-postgresql.baseline: the transport reported ConnectError: refused",
    ):
        br.build_report(tmp_path)


def test_a_mix_run_is_checked_against_the_model(tmp_path: Path) -> None:
    runs = _sweep(tmp_path)
    mix = {"fields": 2.5, "filters": 0.7, "rows": 30.0, "joins_same": 0.3, "joins_cross": 0.6}
    runs.append(
        _write_run(
            tmp_path,
            "graphql.bench-postgresql.mix",
            "graphql",
            "bench-postgresql",
            "mix",
            None,
            mix,
            provisa=_cpu(PROVISA, mix) * 1.2,
            src=_cpu(SOURCE, mix),
        )
    )
    _manifest(tmp_path, runs)
    checks = br.build_report(tmp_path)["transports"]["graphql"]["bench-postgresql"]["mix_checks"]
    (check,) = checks
    assert check["name"] == "graphql.bench-postgresql.mix"
    assert check["provisa_cpu"]["relative_error"] == pytest.approx(0.2 / 1.2)
    assert check["source_cpu"]["relative_error"] == pytest.approx(0.0, abs=1e-9)


def test_break_even_between_live_and_replica(tmp_path: Path) -> None:
    runs = _sweep(tmp_path, "data_sql", "bench-clickhouse")
    direct = _write_run(
        tmp_path,
        "data_sql.bench-clickhouse.replication.live",
        "data_sql",
        "bench-clickhouse",
        "replication",
        "live",
        ZERO,
        provisa=0.4,
        src=1.6,
    )
    mat = _write_run(
        tmp_path,
        "data_sql.bench-clickhouse.replication.replica",
        "data_sql",
        "bench-clickhouse",
        "replication",
        "replica",
        ZERO,
        provisa=0.4,
        src=0.1,
        ttl=60,
    )
    (tmp_path / "results" / mat["name"] / "idle.json").write_text(
        json.dumps(
            {
                "window_s": 120.0,
                "server_cpu_cores": 0.02,
                "source_cpu_cores": {"bench-clickhouse": 0.03},
                "source_cpu_error": None,
            }
        )
    )
    _manifest(tmp_path, runs + [direct, mat])
    be = br.build_report(tmp_path)["transports"]["data_sql"]["bench-clickhouse"]["break_even"]
    # live costs 2.0 ms per request, a replica 0.5; idle 0.05 cores x 60 s = 3 CPU-s per refresh
    assert be["live_ms"] == pytest.approx(2.0) and be["replica_ms"] == pytest.approx(0.5)
    assert be["refresh_cpu_s"] == pytest.approx(3.0) and be["ttl_s"] == 60
    assert be["requests_per_s"] == pytest.approx(3.0 / 60 * 1000 / 1.5)


def test_break_even_needs_the_idle_measurement(tmp_path: Path) -> None:
    runs = _sweep(tmp_path, "data_sql", "bench-clickhouse")
    direct = _write_run(
        tmp_path,
        "data_sql.bench-clickhouse.replication.live",
        "data_sql",
        "bench-clickhouse",
        "replication",
        "live",
        ZERO,
    )
    mat = _write_run(
        tmp_path,
        "data_sql.bench-clickhouse.replication.replica",
        "data_sql",
        "bench-clickhouse",
        "replication",
        "replica",
        ZERO,
        ttl=60,
    )
    _manifest(tmp_path, runs + [direct, mat])
    with pytest.raises(
        ValueError, match="data_sql.bench-clickhouse.replication.replica: no idle.json"
    ):
        br.build_report(tmp_path)


def test_sizing_inputs_come_from_the_baseline(tmp_path: Path) -> None:
    _manifest(tmp_path, _sweep(tmp_path))
    cell = br.build_report(tmp_path)["transports"]["graphql"]["bench-postgresql"]
    baseline_ms = _cpu(PROVISA, ZERO)
    assert cell["sizing"]["provisa_cpu_ms_per_request"] == pytest.approx(baseline_ms)
    assert cell["sizing"]["requests_per_s_per_core"] == pytest.approx(1000 / baseline_ms)
