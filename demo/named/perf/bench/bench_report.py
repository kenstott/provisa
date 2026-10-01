# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""The report derived from a set of sweep runs (REQ-1911): per transport and source, a cost model
for Provisa's CPU per request and one for the source's, the check of a mixed run against them, the
break-even between live and replica reads, and the per-core sizing inputs.

Reads what ``sweeps.py`` planned and ``run_benchmark.py --optimistic`` produced:

    <root>/manifest.json
    <root>/results/<run name>/optimistic/<transport>.json     (every step, server and source CPU)
    <root>/results/<run name>/idle.json                       (replica runs: the refresh cost)

    python bench_report.py --root results/sweeps --out results/sweeps/report.json

The report is internal analysis: it is labelled so and is not for publication.
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import cost_model as cm

LABEL = "internal: not for publication"
_MODEL_RUNS = (
    "baseline",
    "fields",
    "filters",
    "rows",
    "joins_same_source",
    "joins_cross_source",
    "cache",
)


def _step(root: Path, run: Mapping[str, Any]) -> dict[str, Any]:
    """The run's lowest-concurrency step: the cost of a request, not of queueing."""
    path = root / "results" / run["name"] / "optimistic" / f"{run['transport']}.json"
    report = json.loads(path.read_text())
    if "error" in report:
        raise ValueError(f"{run['name']}: the transport reported {report['error']}")
    steps = [s for s in report["steps"] if "error" not in s]
    if not steps:
        raise ValueError(f"{run['name']}: no step completed")
    return min(steps, key=lambda s: s["concurrency"])


def _provisa_ms(run: Mapping[str, Any], step: Mapping[str, Any]) -> float:
    if "server_cpu_ms_per_request" not in step:
        raise ValueError(
            f"{run['name']}: no server_cpu_ms_per_request at concurrency {step['concurrency']} "
            "(was --server-pid given?)"
        )
    return step["server_cpu_ms_per_request"]


def _source_ms(run: Mapping[str, Any], step: Mapping[str, Any]) -> float | None:
    return (step.get("source_cpu_ms_per_request") or {}).get(run["source"])


def _model(
    observations: list[cm.Observation],
) -> tuple[dict[str, Any], dict[str, str], cm.CostModel]:
    """A model fitted over the knobs that vary: as a dict, the knobs that could not be costed
    with the reason, and the model itself."""
    varying = cm.varying_knobs(observations)
    not_modelled = {
        k: f"knob {k!r} does not vary across the observations" for k in cm.KNOBS if k not in varying
    }
    model = cm.fit(observations, varying)
    as_dict = {"base": model.base, "per_unit": model.marginal(), "r2": model.r2, "n": model.n}
    return as_dict, not_modelled, model


def _check(model: cm.CostModel, obs: cm.Observation) -> dict[str, float]:
    c = model.check(obs)
    return {
        "predicted_ms": c.predicted_ms,
        "measured_ms": c.measured_ms,
        "relative_error": c.relative_error,
    }


def _break_even(
    root: Path, cell_runs: list[Mapping[str, Any]], transport: str, source: str
) -> dict[str, Any] | None:
    by_mode = {r["value"]: r for r in cell_runs if r["knob"] == "replication"}
    if not {"live", "replica"} <= set(by_mode):
        return None

    def total_ms(run: Mapping[str, Any]) -> float:
        step = _step(root, run)
        source_ms = _source_ms(run, step)
        if source_ms is None:
            raise ValueError(f"{run['name']}: no source CPU (is the source container monitored?)")
        return _provisa_ms(run, step) + source_ms

    mat = by_mode["replica"]
    idle_path = root / "results" / mat["name"] / "idle.json"
    if not idle_path.exists():
        raise ValueError(f"{mat['name']}: no idle.json (the refresh cost needs an idle window)")
    idle = json.loads(idle_path.read_text())
    if idle.get("source_cpu_error"):
        raise ValueError(
            f"{mat['name']}: the idle window's source CPU failed: {idle['source_cpu_error']}"
        )
    ttl = mat["ttl"]
    idle_cores = idle["server_cpu_cores"] + idle["source_cpu_cores"][source]
    live_ms, replica_ms = total_ms(by_mode["live"]), total_ms(mat)
    refresh = idle_cores * ttl
    be = cm.break_even(live_ms=live_ms, replica_ms=replica_ms, refresh_cpu_s=refresh, ttl_s=ttl)
    return {
        "live_ms": live_ms,
        "replica_ms": replica_ms,
        "refresh_cpu_s": refresh,
        "ttl_s": ttl,
        "requests_per_s": be.requests_per_s,
        "reason": be.reason,
    }


def build_report(root: Path) -> dict[str, Any]:
    manifest = json.loads((root / "manifest.json").read_text())
    runs = manifest["runs"]
    report: dict[str, Any] = {
        "label": LABEL,
        "transports": {},
        "skipped": [{"name": r["name"], "reason": r["skipped"]} for r in runs if r["skipped"]],
    }
    cells: dict[tuple[str, str], list[Mapping[str, Any]]] = {}
    for r in runs:
        if r["contract"] is not None:
            cells.setdefault((r["transport"], r["source"]), []).append(r)
    for (transport, source), cell_runs in cells.items():
        model_runs = [r for r in cell_runs if r["knob"] in _MODEL_RUNS]
        steps = {r["name"]: _step(root, r) for r in cell_runs if r["knob"] in (*_MODEL_RUNS, "mix")}
        provisa_obs = [
            cm.Observation(steps[r["name"]]["knob_means"], _provisa_ms(r, steps[r["name"]]))
            for r in model_runs
        ]
        source_pairs = [(r, _source_ms(r, steps[r["name"]])) for r in model_runs]
        cell: dict[str, Any] = {}
        cell["provisa_cpu"], not_modelled, provisa_model = _model(provisa_obs)
        cell["not_modelled"] = not_modelled
        have = [(r, ms) for r, ms in source_pairs if ms is not None]
        source_model = None
        if have and len(have) != len(source_pairs):
            raise ValueError(f"{transport}.{source}: source CPU is missing from some runs")
        if have:
            source_obs = [cm.Observation(steps[r["name"]]["knob_means"], ms) for r, ms in have]
            cell["source_cpu"], _unused, source_model = _model(source_obs)
        else:
            cell["source_cpu"] = None  # no container monitored: source CPU was not measured
        cell["mix_checks"] = []
        for r in (r for r in cell_runs if r["knob"] == "mix"):
            step = steps[r["name"]]
            means = step["knob_means"]
            check: dict[str, Any] = {
                "name": r["name"],
                "provisa_cpu": _check(provisa_model, cm.Observation(means, _provisa_ms(r, step))),
            }
            src_ms = _source_ms(r, step)
            check["source_cpu"] = (
                _check(source_model, cm.Observation(means, src_ms))
                if source_model is not None and src_ms is not None
                else None
            )
            cell["mix_checks"].append(check)
        baseline = next(r for r in cell_runs if r["knob"] == "baseline")
        base_ms = _provisa_ms(baseline, steps[baseline["name"]])
        cell["sizing"] = {
            "provisa_cpu_ms_per_request": base_ms,
            "requests_per_s_per_core": 1000 / base_ms,
        }
        cell["break_even"] = _break_even(root, cell_runs, transport, source)
        report["transports"].setdefault(transport, {})[source] = cell
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="Report derived from knob sweep runs (internal)")
    parser.add_argument("--root", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    Path(args.out).write_text(json.dumps(build_report(Path(args.root)), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
