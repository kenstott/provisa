# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""The local pass (REQ-2092): what each governance knob costs a request, per transport, against
the request with every knob at zero. Run it through the machine-wide one-heavy-job lock:

    ~/.provisa-agent/heavy.sh .venv/bin/python3 demo/named/perf/bench/local_pass.py \\
        --output-dir ~/bench_local_pass

It brings up local_proof's stack (its own compose project, the four sources on leased ports, tiny
data) and one isolated Provisa server on it with telemetry on, exporting to a trace receiver that
tallies the request spans' stages (noop_otlp_receiver.py --tally). Then, for each governance
variant and each cache setting, it runs the contract (governance.py sets the variant up in the
deployment and removes it after) and reads, per step, the server's CPU per request (its process
tree), each stage's time and thread CPU per request, and the paging counters.

Every request reads every column of the Postgres orders table, so a masked column is in it. The
figures are local working data, never published.
"""

# Requirements: REQ-2092

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import yaml

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[3]
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(REPO))

import local_proof  # noqa: E402
import optimistic_load as ol  # noqa: E402
import contract_model  # noqa: E402
import three_device  # noqa: E402

TRANSPORTS = ("graphql", "data_sql", "pgwire", "flight_sql", "grpc", "rest", "jsonapi", "mcp")
ZERO = {
    "row_rules_per_table": 0,
    "masked_column_share": 0,
    "central_redaction": False,
    "role_depth": 0,
    "derived_roles": 0,
}
# One knob at a time, then all together: the difference from "zero" is that knob's cost.
VARIANTS: dict[str, dict[str, Any]] = {
    "zero": {},
    "row_rules_2": {"row_rules_per_table": 2},
    "masked_quarter": {"masked_column_share": 0.25},
    "redaction": {"central_redaction": True},
    "role_depth_2": {"role_depth": 2},
    "derived_roles_2": {"derived_roles": 2},
    "all": {
        "row_rules_per_table": 2,
        "masked_column_share": 0.25,
        "central_redaction": True,
        "role_depth": 2,
        "derived_roles": 2,
    },
    # zero again, last: how far the server drifted over the pass
    "zero_end": {},
}
CACHE = {"cached": 1.0, "uncached": 0.0}
WARMUP_S = 5


def contract(
    base: dict[str, Any], variant: dict[str, Any], cache: float, steps: list[int], window_s: float
) -> dict[str, Any]:
    raw = json.loads(json.dumps(base))
    raw["sources"] = {"bench-postgresql": raw["sources"]["bench-postgresql"]}
    raw["cross_source_joins"] = []  # their other ends are not in the pass
    raw["knobs"]["source_weights"] = {"bench-postgresql": 1.0}
    raw["knobs"]["route"] = {"bench-postgresql": raw["knobs"]["route"]["bench-postgresql"]}
    orders = next(t for t in raw["sources"]["bench-postgresql"]["tables"] if t["table"] == "orders")
    raw["knobs"]["fields"] = {
        "probability": 1.0,
        "distribution": {"kind": "constant", "value": len(orders["columns"])},
    }
    raw["knobs"]["cache"] = {"probability": cache}
    raw["knobs"]["governance"] = {**ZERO, **variant, "telemetry": True}
    raw["transports"] = {t: {} for t in TRANSPORTS}
    raw["load"]["steps"] = steps
    raw["load"]["window_s"] = window_s
    raw["load"]["warmup_s"] = WARMUP_S  # each transport's plans and caches warm before a step
    return raw


def start_receiver(port: int, log_dir: Path) -> subprocess.Popen:
    proc = subprocess.Popen(
        [sys.executable, str(HERE / "noop_otlp_receiver.py"), str(port), "--tally"],
        stdout=(log_dir / "receiver.log").open("w"),
        stderr=subprocess.STDOUT,
    )
    import httpx

    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        try:
            httpx.get(f"http://127.0.0.1:{port}/stages", timeout=1).raise_for_status()
            return proc
        except httpx.HTTPError:
            time.sleep(0.2)
    proc.terminate()
    raise RuntimeError(f"the trace receiver did not answer on {port}")


def mcp_roles(role: str) -> str:
    """The roles the MCP server may act as over loopback (``mcp.role``, a pin: a local call with
    no bearer names only these): the contract's role and every role a variant's request names."""
    deepest = max(v.get("role_depth", 0) for v in VARIANTS.values())
    derived = max(v.get("derived_roles", 0) for v in VARIANTS.values())
    return ",".join(
        [
            *(contract_model.chain_role(role, level) for level in range(deepest + 1)),
            *(
                f"{contract_model.derived_attribute(i)}__{contract_model.DERIVED_VALUE}"
                for i in range(derived)
            ),
        ]
    )


def start_server(ports: dict[str, int], tmp: Path, receiver_port: int, mcp_port: int) -> Any:
    """The server, its control plane on the stack's Postgres (local_proof.control_plane_on_stack)."""
    local_proof.control_plane_on_stack(ports)
    config = tmp / "perf-telemetry-on.yaml"
    config.write_text(
        three_device.run_config(
            (local_proof.PERF / "config.yaml").read_text(),
            telemetry="on",
            collector_url=f"http://127.0.0.1:{receiver_port}",
        )
    )
    server = local_proof._proof_server_class()(
        f"benchpass{os.getpid()}",
        engine="duckdb",
        control_plane="own",
        enable_pgwire=True,
        enable_bolt=True,
        await_flight=True,
        await_grpc=True,
        config=str(config),
        env={
            **local_proof._source_env(ports),
            "OTEL_SDK_DISABLED": "false",
            "PROVISA_MCP_PORT": str(mcp_port),
            "PROVISA_MCP_HOST": "127.0.0.1",
            "PROVISA_MCP_ROLE": mcp_roles("org_admin"),
        },
    )
    server.start(timeout=600)
    return server


def run_variant(path: Path, out: Path, server: Any, receiver_port: int) -> list[dict]:
    args = SimpleNamespace(
        setup=str(path),
        server_pid=server._proc.pid,
        server_cpu_url=None,
        source_cpu_url=None,
        backend_container=[],
        device=[],
        stage_tally_url=f"http://127.0.0.1:{receiver_port}",
        resolved_names=None,
        apply_replication=False,
        skip_route_verification=True,
    )
    return ol.run_from_args(args, out, argv=["local_pass"])


def table(results: dict[str, list[dict]]) -> list[dict[str, Any]]:
    """One row per variant, cache setting, transport and step: server CPU per request, its
    difference from the zero variant, the stages' thread CPU per request and the paging."""
    rows = []
    for key, reports in results.items():
        variant, cache = key.split("/")
        for report in reports:
            for step in report.get("steps", []):
                if "error" in step:
                    rows.append(
                        {
                            "variant": variant,
                            "cache": cache,
                            "transport": report["transport"],
                            "error": step["error"],
                        }
                    )
                    continue
                stages = {
                    f"{t}.{name}": s["cpu_ms_per_request"]
                    for t, v in step.get("stages", {}).items()
                    for name, s in v["stages"].items()
                }
                rows.append(
                    {
                        "variant": variant,
                        "cache": cache,
                        "transport": report["transport"],
                        "concurrency": step["concurrency"],
                        "req_per_s": step["req_per_s"],
                        "errors": step["errors"],
                        "server_cpu_ms_per_request": step.get("server_cpu_ms_per_request"),
                        "stage_cpu_ms_per_request": stages,
                        "paging": step["paging"],
                    }
                )
    zero = {
        (r["cache"], r["transport"], r.get("concurrency")): r.get("server_cpu_ms_per_request")
        for r in rows
        if r["variant"] == "zero"
    }
    for r in rows:
        base = zero.get((r["cache"], r["transport"], r.get("concurrency")))
        cpu = r.get("server_cpu_ms_per_request")
        r["over_zero_ms"] = round(cpu - base, 3) if cpu is not None and base is not None else None
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Governed vs zero, per request, locally")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--steps", default="1,8")
    parser.add_argument("--window-s", type=float, default=10.0)
    parser.add_argument("--variants", default=",".join(VARIANTS))
    parser.add_argument("--cache", default=",".join(CACHE))
    args = parser.parse_args()
    out = Path(args.output_dir).expanduser()
    out.mkdir(parents=True, exist_ok=True)
    steps = [int(s) for s in args.steps.split(",")]

    from tests.port_lease import lease_ports

    leased = lease_ports(7)
    ports = {
        "bench-postgresql": leased[0],
        "bench-clickhouse": leased[1],
        "bench-mongodb": leased[2],
        "neo4j-http": leased[3],
        "neo4j-bolt": leased[4],
    }
    receiver_port, mcp_port = leased[5], leased[6]
    project = f"provisa-benchpass-{os.getpid()}"
    tmp = Path(tempfile.mkdtemp(prefix="benchpass-"))
    compose = ["docker", "compose", "-p", project, "-f", str(local_proof.compose_file(ports, tmp))]
    receiver = server = None
    results: dict[str, list[dict]] = {}
    try:
        for service in ("postgresql", "mongodb", "clickhouse", "neo4j"):
            local_proof.run([*compose, "up", "-d", "--wait", service])
        local_proof.seed(ports)
        receiver = start_receiver(receiver_port, out)
        server = start_server(ports, tmp, receiver_port, mcp_port)
        base = local_proof.proof_contract(server, project)
        base["deployment"]["endpoints"]["mcp"] = {"url": f"http://127.0.0.1:{mcp_port}/mcp"}
        for variant in args.variants.split(","):
            for cache in args.cache.split(","):
                key = f"{variant}/{cache}"
                run_dir = out / variant / cache
                run_dir.mkdir(parents=True, exist_ok=True)
                path = run_dir / "setup.yaml"
                raw = contract(base, VARIANTS[variant], CACHE[cache], steps, args.window_s)
                path.write_text(yaml.safe_dump(raw, sort_keys=False))
                local_proof.log(f"== {key}")
                results[key] = run_variant(path, run_dir, server, receiver_port)
    finally:
        if server is not None:
            (out / "server-stderr-tail.txt").write_text(server.dump_stderr_debug()[-20000:])
            server.stop_process()
        if receiver is not None:
            receiver.terminate()
        subprocess.run([*compose, "down", "-v", "--remove-orphans"], check=False)
    rows = table(results)
    (out / "local-pass.json").write_text(json.dumps(rows, indent=2))
    cols = [
        "variant",
        "cache",
        "transport",
        "concurrency",
        "req_per_s",
        "errors",
        "server_cpu_ms_per_request",
        "over_zero_ms",
    ]
    print(local_proof.render_table(rows, cols))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
