# Copyright (c) 2026 Kenneth Stott
# Canary: 7d67e18f-65de-4534-a7a4-d83bffdec10d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""Run one contract live and from replicas, and diff the two (REQ-1911).

    python bench_compare.py --setup setups/perf-stack.yaml --output-dir results/compare \\
        --replica-ttl 300 --server-pid 1234

Runs the contract twice with every source (and no table override) under one replication setting,
each time with ``--apply-replication`` (the setting is applied for the run and restored after it)
and with the route verification on, then writes ``compare.json`` with the two runs side by side per
transport and step. A run whose routes were not verified is refused: a figure for requests that
may have been served the other way is not a comparison. Internal analysis; not for publication.
"""

from __future__ import annotations

import argparse
import copy
import json
from pathlib import Path
from typing import Any

import yaml

LABEL = "internal: not for publication"
SETTINGS = ("live", "replica")


def under_setting(raw: dict[str, Any], setting: str, *, ttl_seconds: int) -> dict[str, Any]:
    """The contract with every source under ``setting``; table overrides are dropped so the two
    runs differ in nothing else."""
    out = copy.deepcopy(raw)
    for src in out["sources"].values():
        src["replication"] = {
            "setting": setting,
            "ttl_seconds": ttl_seconds if setting == "replica" else 0,
        }
        for table in src["tables"]:
            table.pop("replication", None)
    return out


def _ratio(live: float, replica: float) -> float | None:
    return None if not live else replica / live


def _pair(live: float, replica: float) -> dict[str, Any]:
    return {"live": live, "replica": replica, "ratio": _ratio(live, replica)}


def _steps(directory: Path, name: str, which: str) -> list[dict[str, Any]]:
    data = json.loads((directory / f"{name}.json").read_text())
    if "error" in data:
        raise ValueError(f"{name}: the {which} run's transport reported {data['error']}")
    steps = data["steps"]
    for step in steps:
        if not step.get("route_verification"):
            raise ValueError(
                f"{name} step c={step['concurrency']} of the {which} run has no verified routes"
            )
    return steps


def compare_dirs(live_dir: Path, replica_dir: Path) -> dict[str, Any]:
    """The two runs' per-transport results, step by step."""
    out: dict[str, Any] = {"label": LABEL, "transports": {}}
    for path in sorted(live_dir.glob("*.json")):
        name = path.stem
        if not (replica_dir / path.name).exists():
            raise ValueError(f"{name}: the replica run has no result")
        live, replica = _steps(live_dir, name, "live"), _steps(replica_dir, name, "replica")
        if [s["concurrency"] for s in live] != [s["concurrency"] for s in replica]:
            raise ValueError(f"{name}: the runs have different steps")
        rows = []
        for a, b in zip(live, replica):
            row: dict[str, Any] = {
                "concurrency": a["concurrency"],
                "req_per_s": _pair(a["req_per_s"], b["req_per_s"]),
                "p50_ms": _pair(a["p50_ms"], b["p50_ms"]),
                "p99_ms": _pair(a["p99_ms"], b["p99_ms"]),
            }
            if "server_cpu_ms_per_request" in a and "server_cpu_ms_per_request" in b:
                row["provisa_cpu_ms_per_request"] = _pair(
                    a["server_cpu_ms_per_request"], b["server_cpu_ms_per_request"]
                )
            sources = {}
            for sid, live_ms in (a.get("source_cpu_ms_per_request") or {}).items():
                replica_ms = (b.get("source_cpu_ms_per_request") or {}).get(sid)
                if live_ms is not None and replica_ms is not None:
                    sources[sid] = _pair(live_ms, replica_ms)
            row["source_cpu_ms_per_request"] = sources
            rows.append(row)
        out["transports"][name] = {"steps": rows}
    return out


def compare(args: Any, output_dir: Path, *, replica_ttl_s: int) -> dict[str, Any]:
    """Run ``args.setup`` live and from replicas under ``output_dir`` and diff them."""
    import optimistic_load

    raw = yaml.safe_load(Path(args.setup).read_text())
    output_dir.mkdir(parents=True, exist_ok=True)
    for setting in SETTINGS:
        contract = output_dir / f"{setting}.yaml"
        contract.write_text(
            yaml.safe_dump(under_setting(raw, setting, ttl_seconds=replica_ttl_s), sort_keys=False)
        )
        run_args = argparse.Namespace(
            setup=str(contract),
            server_pid=args.server_pid,
            resolved_names=args.resolved_names,
            apply_replication=True,
            skip_route_verification=args.skip_route_verification,
        )
        optimistic_load.run_from_args(run_args, output_dir / setting / "optimistic")
    result = compare_dirs(output_dir / "live" / "optimistic", output_dir / "replica" / "optimistic")
    (output_dir / "compare.json").write_text(json.dumps(result, indent=2))
    return result


def main() -> int:
    import optimistic_load

    parser = argparse.ArgumentParser(description="Run a contract live and from replicas and diff")
    parser.add_argument("--setup", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument(
        "--replica-ttl", type=int, required=True, help="the replica refresh TTL, seconds"
    )
    parser.add_argument("--resolved-names", default=None)
    parser.add_argument("--server-pid", type=int, default=None)
    args = parser.parse_args()
    args.skip_route_verification = False  # a comparison needs the routes verified
    try:
        compare(args, Path(args.output_dir), replica_ttl_s=args.replica_ttl)
    except optimistic_load.contract_model.SetupError as exc:
        raise SystemExit(f"setup contract: {exc}") from exc
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
