# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""A soak (REQ-1911): hours of the closed-loop request mix, the server sampled throughout, and a
report of what it holds more of as it goes. There is no throughput floor; the question is only
whether a figure keeps rising.

Rounds of ``optimistic_load`` (every transport in turn, one step each, under the contract's
governance) run for ``--duration-s``. Every ``--sample-s`` the device agent's ``/process`` is read
(host_agent.py: the server's process tree, its ``/debug/leak-probe`` and Redis) and written to
``samples.jsonl``. The report fits each series' least-squares slope after ``--warmup-s`` and flags
a series as rising when its fitted rise over that window exceeds twice the spread of its samples
about the fit and the mean of each third of the window is above the one before.

    python soak.py --setup setups/soak.yaml --agent http://10.90.0.2:9555 --server-pid 12345 \\
        --duration-s 14400 --warmup-s 1800 --output-dir results/soak
"""

# Requirements: REQ-1911

from __future__ import annotations

import argparse
import json
import statistics
import sys
import threading
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

SAMPLE_S = 30


def flatten(sample: Mapping[str, Any], prefix: str = "") -> dict[str, float]:
    """One number per series, by its dotted path. A pool list is keyed by each pool's name; other
    lists (what the probe says grew), flags and error strings are not series."""
    out: dict[str, float] = {}
    for key, value in sample.items():
        name = f"{prefix}{key}"
        if isinstance(value, bool) or isinstance(value, str):
            continue
        if isinstance(value, int | float):
            out[name] = value
        elif isinstance(value, Mapping):
            out.update(flatten(value, f"{name}."))
        elif key == "pools":
            for pool in value:
                counts = {k: v for k, v in pool.items() if k not in ("name", "maxsize")}
                out.update(flatten(counts, f"{name}.{pool['name']}."))
    return out


def _fit(points: list[tuple[float, float]]) -> tuple[float, float]:
    """Least-squares slope (per second) and the spread of the points about the line."""
    ts = [t for t, _ in points]
    vs = [v for _, v in points]
    t0, v0 = statistics.fmean(ts), statistics.fmean(vs)
    var_t = sum((t - t0) ** 2 for t in ts)
    slope = sum((t - t0) * (v - v0) for t, v in points) / var_t if var_t else 0.0
    residuals = [v - (v0 + slope * (t - t0)) for t, v in points]
    return slope, statistics.pstdev(residuals)


def _series(points: list[tuple[float, float]]) -> dict[str, Any]:
    slope, spread = _fit(points)
    n = len(points)
    thirds = [
        statistics.fmean(v for _, v in points[i * n // 3 : (i + 1) * n // 3]) for i in range(3)
    ]
    rise = slope * (points[-1][0] - points[0][0])
    return {
        "samples": n,
        "first": points[0][1],
        "last": points[-1][1],
        "slope_per_hour": slope * 3600,
        "spread": spread,
        "thirds": thirds,
        "rising": rise > 2 * spread and thirds[0] < thirds[1] < thirds[2],
    }


def report(samples: Sequence[Mapping[str, Any]], warmup_s: float) -> dict[str, Any]:
    """Each series' fit after ``warmup_s`` (``samples``: ``{"t": seconds, "series": {...}}``) and
    the names of the rising ones."""
    by_name: dict[str, list[tuple[float, float]]] = {}
    for sample in samples:
        if sample["t"] >= warmup_s:
            for name, value in sample["series"].items():
                by_name.setdefault(name, []).append((sample["t"], value))
    if not by_name or max(len(p) for p in by_name.values()) < 3:
        raise ValueError(f"fewer than 3 samples after the warm-up ({warmup_s}s) to fit")
    series = {name: _series(pts) for name, pts in sorted(by_name.items()) if len(pts) >= 3}
    return {
        "warmup_s": warmup_s,
        "series": series,
        "rising": [name for name, s in series.items() if s["rising"]],
    }


class Sampler:
    """Reads ``<agent>/process`` every ``every_s`` seconds on a thread of its own, each sample
    appended to ``out`` as one JSON line and kept for the report."""

    def __init__(self, agent: str, every_s: float, out: Path) -> None:
        self._agent, self._every_s, self._out = agent, every_s, out
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="soak-sampler", daemon=True)
        self.samples: list[dict[str, Any]] = []
        self.failure: BaseException | None = None

    def _run(self) -> None:
        import httpx

        start = time.monotonic()
        try:
            with self._out.open("a") as out:
                while not self._stop.is_set():
                    t = time.monotonic() - start
                    raw = httpx.get(f"{self._agent}/process", timeout=60).raise_for_status().json()
                    out.write(json.dumps({"t": t, "raw": raw}) + "\n")
                    out.flush()
                    self.samples.append({"t": t, "series": flatten(raw)})
                    self._stop.wait(self._every_s)
        except BaseException as exc:  # raised again on the soak's thread when it ends
            self.failure = exc

    def __enter__(self) -> Sampler:
        self._thread.start()
        return self

    def __exit__(self, *_: Any) -> None:
        self._stop.set()
        self._thread.join(120)
        if self.failure is not None:
            raise RuntimeError("the soak's sampling stopped") from self.failure


def main() -> int:
    import contract_model
    import optimistic_load

    parser = argparse.ArgumentParser(description="Hours of the request mix; what keeps rising")
    parser.add_argument("--output-dir", default="results/soak")
    parser.add_argument("--agent", required=True, help="the server device's host_agent.py URL")
    parser.add_argument("--duration-s", type=float, required=True)
    parser.add_argument("--warmup-s", type=float, required=True)
    parser.add_argument("--sample-s", type=float, default=SAMPLE_S)
    optimistic_load.add_arguments(parser)
    args = parser.parse_args()
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    start = time.monotonic()
    try:
        with Sampler(args.agent, args.sample_s, out / "samples.jsonl") as sampler:
            n = 0
            while time.monotonic() - start < args.duration_s and sampler.failure is None:
                optimistic_load.run_from_args(args, out / f"round-{n:03d}")
                n += 1
    except contract_model.SetupError as exc:
        raise SystemExit(f"setup contract: {exc}") from exc
    result = report(sampler.samples, args.warmup_s)
    (out / "soak-report.json").write_text(json.dumps(result, indent=2))
    print(f"soak: {n} rounds, {len(sampler.samples)} samples", flush=True)
    for name in result["rising"]:
        s = result["series"][name]
        print(
            f"RISING {name}: {s['first']} -> {s['last']}, {s['slope_per_hour']:.1f}/h", flush=True
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
