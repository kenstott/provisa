# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""Knob sweeps (REQ-1911): each knob alone, per transport and per source, as setup contracts.

A sweep run is the base contract with every knob at zero, one transport, all weight on one source
and one knob set. ``plan_sweeps`` builds every run and validates it with the contract loader; a run
the contract cannot serve (a transport that cannot spell the source's tables, joins on gRPC, more
fields than the table has) is listed with the reason, never dropped. ``write_plan`` writes the
contracts and a manifest; ``run_command`` is the command that runs one.

    python sweeps.py plan --setup setups/perf-stack.yaml --out results/sweeps
"""

from __future__ import annotations

import argparse
import copy
import json
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import contract_model
import lookup
import setup_contract as sc
import yaml

LABEL = "internal: not for publication"
DEFAULT_VALUES: dict[str, list[int]] = {
    "fields": [1, 2, 4],
    "filters": [1, 2],
    "rows": [1, 10, 100, 1000],
    "joins_same_source": [1],
    "joins_cross_source": [1, 2],
}
DEFAULT_CACHE = [0.0, 1.0]
_ZERO_KNOB = {"probability": 0, "distribution": {"kind": "constant", "value": 0}}


@dataclass(frozen=True)
class SweepRun:
    name: str
    transport: str
    source: str
    knob: str  # baseline | a distribution knob | cache | replication
    value: Any
    contract: dict[str, Any] | None  # the loadable contract, or None
    skipped: str | None  # why there is no contract


def _constant(n: int) -> dict[str, Any]:
    return {"probability": 1, "distribution": {"kind": "constant", "value": n}}


def _baseline(
    raw: dict[str, Any], transport: str, source: str, spread: Sequence[str]
) -> dict[str, Any]:
    """The base contract with every knob at zero, one transport, weight on ``source`` (and on
    ``spread``, equally, when a knob needs other sources to reach)."""
    out = copy.deepcopy(raw)
    k = out["knobs"]
    for name in contract_model.DISTRIBUTION_KNOBS:
        k[name] = copy.deepcopy(_ZERO_KNOB)
    sources = list(out["sources"])
    on = {source, *spread}
    k["source_weights"] = {s: (1.0 / len(on) if s in on else 0.0) for s in sources}
    for s in sources:
        k["route"][s] = {"mode": "auto", "federated_probability": 0}
    out["transports"] = {transport: {}}
    return out


def _cross_sources(raw: Mapping[str, Any]) -> list[str]:
    ends: list[str] = []
    for j in raw.get("cross_source_joins", []):
        for side in ("left", "right"):
            s = j[side].partition(":")[0]
            if s not in ends:
                ends.append(s)
    return ends


def _validate(
    contract: dict[str, Any],
    known_transports: Sequence[str],
    environ: Mapping[str, str],
    deployment: Any,
) -> str | None:
    try:
        # a replication sweep is run with --apply-replication, so the deployment need not have it
        sc.setup_from_dict(
            contract,
            environ=environ,
            known_transports=known_transports,
            deployment=deployment,
            verify_replication=False,
        )
    except contract_model.SetupError as exc:
        return str(exc)
    return None


def plan_sweeps(
    base: Mapping[str, Any],
    *,
    known_transports: Sequence[str],
    deployment: Any,
    environ: Mapping[str, str] | None = None,
    transports: Sequence[str] | None = None,
    sources: Sequence[str] | None = None,
    values: Mapping[str, Sequence[int]] | None = None,
    cache: Sequence[float] | None = None,
    replication: Sequence[str] = (),
    ttl_s: int = 0,
) -> list[SweepRun]:
    env = os.environ if environ is None else environ
    wanted_values = DEFAULT_VALUES if values is None else values
    wanted_cache = DEFAULT_CACHE if cache is None else cache
    runs: list[SweepRun] = []
    for transport in transports or list(base["transports"]):
        for source in sources or list(base["sources"]):
            variants: list[tuple[str, Any, dict[str, Any]]] = []
            plain = _baseline(dict(base), transport, source, [])
            variants.append(("baseline", None, plain))
            for knob, vals in wanted_values.items():
                spread = _cross_sources(base) if knob == "joins_cross_source" else []
                for v in vals:
                    c = _baseline(dict(base), transport, source, spread)
                    c["knobs"][knob] = _constant(v)
                    variants.append((knob, v, c))
            for p in wanted_cache:
                c = _baseline(dict(base), transport, source, [])
                c["knobs"]["cache"] = {"probability": p}
                variants.append(("cache", p, c))
            for setting in replication:
                c = _baseline(dict(base), transport, source, [])
                c["sources"][source]["replication"] = {
                    "setting": setting,
                    "ttl_seconds": ttl_s if setting == "replica" else 0,
                }
                variants.append(("replication", setting, c))
            for knob, value, contract in variants:
                name = f"{transport}.{source}.{knob}" + ("" if value is None else f".{value}")
                reason = _validate(contract, known_transports, env, deployment)
                runs.append(
                    SweepRun(
                        name, transport, source, knob, value, None if reason else contract, reason
                    )
                )
    return runs


def write_plan(runs: Sequence[SweepRun], out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest = []
    for r in runs:
        file = None
        if r.contract is not None:
            file = f"{r.name}.yaml"
            (out_dir / file).write_text(yaml.safe_dump(r.contract, sort_keys=False))
        manifest.append(
            {
                "name": r.name,
                "transport": r.transport,
                "source": r.source,
                "knob": r.knob,
                "value": r.value,
                "contract": file,
                "skipped": r.skipped,
            }
        )
    (out_dir / "manifest.json").write_text(json.dumps({"label": LABEL, "runs": manifest}, indent=2))


def run_command(
    contract: str, *, engine: str, output_dir: str, python: str, server_pid: int | None
) -> list[str]:
    """The command that runs one sweep contract (from the bench directory)."""
    cmd = [python, "run_benchmark.py", "--engine", engine, "--optimistic", "--setup", contract]
    if server_pid is not None:
        cmd += ["--server-pid", str(server_pid)]
    return cmd + ["--output-dir", output_dir]


def main() -> int:
    import optimistic_load

    parser = argparse.ArgumentParser(description="Plan knob sweeps as setup contracts")
    parser.add_argument("command", choices=["plan"])
    parser.add_argument("--setup", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument(
        "--resolved-names",
        required=True,
        help="resolved-names.json a run wrote: what the deployment calls the contract's tables",
    )
    parser.add_argument("--replication", default="", help="live,replica: sweep the setting")
    parser.add_argument("--ttl", type=int, default=0)
    args = parser.parse_args()
    base = yaml.safe_load(Path(args.setup).read_text())
    runs = plan_sweeps(
        base,
        known_transports=list(optimistic_load.TRANSPORTS),
        deployment=lookup.read_resolved(Path(args.resolved_names)),
        replication=[m for m in args.replication.split(",") if m],
        ttl_s=args.ttl,
    )
    write_plan(runs, Path(args.out))
    runnable = sum(1 for r in runs if r.contract is not None)
    print(f"{runnable} runnable, {len(runs) - runnable} skipped (reasons in manifest.json)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
