# Copyright (c) 2026 Kenneth Stott
# Canary: 073f31f5-09ac-45a3-b197-552aa59f405c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""The timing sanity report of one matrix run (REQ-1911, METHODOLOGY.md), and the rule that timings
from two VMs are never compared.

    python bench_sanity.py --report results/pg/new.json --baseline results/pg/old.json --out s.json

Flags, for a person to explain, each with the figures that raised it:

- ``over_2x``: a cell moved by 2x or more against the baseline, either way.
- ``cached_slower``: a cached cell is slower than the same cell uncached.
- ``large_faster``: a large query is faster than its small counterpart on one transport.
- ``cold_faster``: a cold query is faster than its warm counterpart.

Two VMs of one machine type and one CPU platform differed by 1.29x to 1.61x, more than the change
then under test, so ``over_2x`` runs only between two runs of one VM. Any other baseline is refused
and the report says why; the checks within the run still stand.
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

# The queries that read the same thing at two sizes or two temperatures.
SIZE_PAIRS = (("federated_join", "large_federated_join"), ("point_lookup", "large_scan"))
TEMPERATURE_PAIRS = (("neo4j_materialize_warm", "neo4j_materialize_cold"),)
OVER = 2.0
# Two cells this close are the same figure: a cached read of a small row sits at the noise.
CACHED_TOLERANCE = 1.1


class CrossVmComparison(ValueError):
    """Two sets of timings that are not from one VM."""


def vm_identity(host: Mapping[str, Any], *, label: str) -> tuple[str, str]:
    """(instance id, CPU platform) from a report's host facts (host_agent.host_facts)."""
    instance, platform = host.get("instance_id"), host.get("cpu_platform")
    if not instance or not platform:
        raise CrossVmComparison(f"{label} does not record its VM and CPU platform")
    return str(instance), str(platform)


def refuse_cross_vm(
    a: Mapping[str, Any], b: Mapping[str, Any], *, labels: tuple[str, str] = ("a", "b")
) -> None:
    """Raise unless ``a`` and ``b`` are the same VM on the same CPU platform."""
    first, second = vm_identity(a, label=labels[0]), vm_identity(b, label=labels[1])
    if first != second:
        raise CrossVmComparison(
            f"{labels[0]} ran on {first[0]} ({first[1]}), {labels[1]} on {second[0]} ({second[1]}): "
            "timings from two VMs are not compared"
        )


def _cells(report: Mapping[str, Any]) -> dict[tuple[str, str, str], float]:
    """p50 by (query, cell label, cache pass), for the cells that answered."""
    return {
        (r["query_id"], f"{r['transport']}({r.get('dialect', '')})", r["cache_pass"]): r[
            "latency_ms_p50"
        ]
        for r in report["results"]
        if "latency_ms_p50" in r
    }


def _flag(kind: str, cell: str, **figures: Any) -> dict[str, Any]:
    return {"kind": kind, "cell": cell, **figures}


def within_run(report: Mapping[str, Any]) -> list[dict[str, Any]]:
    """The relationships a run's cells must keep among themselves."""
    cells = _cells(report)
    flags = []
    for (query, label, cache_pass), p50 in sorted(cells.items()):
        if cache_pass != "cache":
            continue
        uncached = cells.get((query, label, "nocache"))
        if uncached is not None and p50 > uncached * CACHED_TOLERANCE:
            flags.append(
                _flag("cached_slower", f"{query} {label}", cached_ms=p50, uncached_ms=uncached)
            )
    # Each pair: the cheap query, then the one that must take longer.
    ordered = [("large_faster", small, large) for small, large in SIZE_PAIRS]
    ordered += [("cold_faster", warm, cold) for warm, cold in TEMPERATURE_PAIRS]
    for kind, cheap, dear in ordered:
        for (query, label, cache_pass), dear_ms in sorted(cells.items()):
            cheap_ms = cells.get((cheap, label, cache_pass))
            if query == dear and cheap_ms is not None and dear_ms < cheap_ms:
                flags.append(
                    _flag(kind, f"{dear} {label} {cache_pass}", cheap_ms=cheap_ms, dear_ms=dear_ms)
                )
    return flags


def against_baseline(
    report: Mapping[str, Any], baseline: Mapping[str, Any], *, over: float = OVER
) -> list[dict[str, Any]]:
    """Every cell that moved by ``over`` or more against the baseline, either way."""
    now, then = _cells(report), _cells(baseline)
    flags = []
    for key in sorted(now.keys() & then.keys()):
        if not then[key] or not now[key]:
            continue
        ratio = now[key] / then[key]
        if ratio >= over or ratio <= 1 / over:
            flags.append(
                _flag(
                    "over_2x",
                    " ".join(key),
                    baseline_ms=then[key],
                    now_ms=now[key],
                    ratio=round(ratio, 2),
                )
            )
    return flags


def sanity(report: Mapping[str, Any], baseline: Mapping[str, Any] | None) -> dict[str, Any]:
    """The report on ``report``, against ``baseline`` when it is from the same VM."""
    flags = within_run(report)
    if baseline is None:
        comparison = "no baseline"
    else:
        try:
            refuse_cross_vm(
                baseline.get("host") or {},
                report.get("host") or {},
                labels=("the baseline", "this run"),
            )
        except CrossVmComparison as exc:
            comparison = f"refused: {exc}"
        else:
            comparison = "same VM"
            flags += against_baseline(report, baseline)
    return {
        "engine": report.get("engine"),
        "cpu_platform": (report.get("host") or {}).get("cpu_platform"),
        "instance_id": (report.get("host") or {}).get("instance_id"),
        "comparison": comparison,
        "flags": flags,
    }


def report_files(current: Path, baseline: Path | None) -> dict[str, Any]:
    prior = json.loads(baseline.read_text()) if baseline is not None else None
    return sanity(json.loads(current.read_text()), prior)


def main() -> int:
    parser = argparse.ArgumentParser(description="Timing sanity report of one matrix run")
    parser.add_argument("--report", required=True)
    parser.add_argument("--baseline")
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    out = report_files(Path(args.report), Path(args.baseline) if args.baseline else None)
    Path(args.out).write_text(json.dumps(out, indent=2))
    print(f"{out['comparison']}; {len(out['flags'])} flag(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
