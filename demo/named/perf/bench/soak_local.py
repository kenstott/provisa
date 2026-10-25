# Copyright (c) 2026 Kenneth Stott
# Canary: 96132c88-3120-47d7-a0f8-d5f364cce998
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""A short soak against local_proof.py's small stack (REQ-1911): it proves the soak's path end to
end before a real one runs on the perf VM. It is not a measurement. Run it through the
machine-wide one-heavy-job lock:

    ~/.provisa-agent/heavy.sh .venv/bin/python3 demo/named/perf/bench/soak_local.py \\
        --output-dir ~/bench_soak_local --duration-s 600 --warmup-s 120

The stack is local_proof's own compose project, plus a Redis container for the response cache. The
server is its own isolated subprocess, with ``PROVISA_LEAK_PROBE=1`` set and that Redis. A
host_agent thread serves the server's ``/process``, and soak.py samples it every 30 s at a low
concurrency (``--concurrency``) over setups/soak.yaml. Everything is torn down at the end.
"""

# Requirements: REQ-1911

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

import httpx
import yaml

import contract_model as cm
import host_agent
import local_proof as lp


def main() -> int:
    parser = argparse.ArgumentParser(description="A short soak against a small local stack")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--duration-s", type=float, default=600)
    parser.add_argument("--warmup-s", type=float, default=120)
    parser.add_argument("--concurrency", type=int, default=2)
    parser.add_argument("--window-s", type=float, default=20)
    args = parser.parse_args()
    out = Path(args.output_dir).expanduser()
    out.mkdir(parents=True, exist_ok=True)
    lp.wait_for_load(3600)

    from tests.port_lease import lease_ports

    leased = lease_ports(7)
    ports = {
        "bench-postgresql": leased[0],
        "bench-clickhouse": leased[1],
        "bench-mongodb": leased[2],
        "neo4j-http": leased[3],
        "neo4j-bolt": leased[4],
        "redis": leased[5],
        "mcp": leased[6],
    }
    project = f"provisa-benchsoak-{os.getpid()}"
    tmp = Path(tempfile.mkdtemp(prefix="benchsoak-"))
    compose_path = lp.compose_file(ports, tmp)
    stack = yaml.safe_load(compose_path.read_text())
    stack["services"]["redis"] = {
        "image": "redis:7.4-alpine",
        "command": ["redis-server", "--save", "", "--appendonly", "no"],
        "ports": [f"127.0.0.1:{ports['redis']}:6379"],
        "labels": {"com.provisa.benchproof": "1"},
        "healthcheck": {"test": ["CMD", "redis-cli", "ping"], "interval": "5s", "retries": 10},
    }
    compose_path.write_text(yaml.safe_dump(stack, sort_keys=False))
    compose = ["docker", "compose", "-p", project, "-f", str(compose_path)]
    server = agent = None
    code = 1
    try:
        for service in ("redis", "postgresql", "mongodb", "clickhouse", "neo4j"):
            lp.run([*compose, "up", "-d", "--wait", service])
        lp.seed(ports)
        lp.control_plane_on_stack(ports)
        g = yaml.safe_load((lp.HERE / "setups" / "soak.yaml").read_text())["knobs"]["governance"]
        # loopback MCP acts only as its pinned roles (mcp.role): every role a soak request names
        mcp_roles = ",".join(
            [
                *(cm.chain_role("org_admin", level) for level in range(g["role_depth"] + 1)),
                *(
                    f"{cm.derived_attribute(i)}__{cm.DERIVED_VALUE}"
                    for i in range(1, g["derived_roles"] + 1)
                ),
            ]
        )
        server = lp._proof_server_class()(
            f"benchsoak{os.getpid()}",
            engine="duckdb",
            control_plane="own",
            enable_pgwire=True,
            enable_bolt=True,
            await_flight=True,
            await_grpc=True,
            config=str(lp.PERF / "config.yaml"),
            env={
                **lp._source_env(ports),
                "PROVISA_LEAK_PROBE": "1",
                "PROVISA_REDIS_EMBEDDED": "0",
                "REDIS_URL": f"redis://127.0.0.1:{ports['redis']}/0",
                "PROVISA_MCP_PORT": str(ports["mcp"]),
                "PROVISA_MCP_HOST": "127.0.0.1",
                "PROVISA_MCP_ROLE": mcp_roles,
            },
        )
        server.start(timeout=600)
        deadline = time.monotonic() + 120
        while True:  # the MCP server binds beside the app, after the app answers
            try:
                httpx.get(f"http://127.0.0.1:{ports['mcp']}/mcp", timeout=5)
                break
            except httpx.TransportError:
                if time.monotonic() > deadline:
                    raise RuntimeError("the MCP server did not come up") from None
                time.sleep(1)
        raw = lp.proof_contract(server, project, "soak.yaml")
        raw["deployment"]["endpoints"]["mcp"]["url"] = f"http://127.0.0.1:{ports['mcp']}/mcp"
        raw["load"]["steps"] = [args.concurrency]
        raw["load"]["window_s"] = args.window_s
        raw["load"]["processes"] = 1
        contract = tmp / "soak.yaml"
        contract.write_text(yaml.safe_dump(raw, sort_keys=False))
        agent = host_agent.serve(
            ("127.0.0.1", 0),
            pid=server._proc.pid,  # noqa: SLF001 - the server subprocess this run started
            facts={},
            rtt_targets={},
            leak_probe_url=f"{server.base_url}/debug/leak-probe",
            redis=("127.0.0.1", ports["redis"]),
        )
        threading.Thread(target=agent.serve_forever, daemon=True).start()
        code = subprocess.run(
            [
                sys.executable,
                str(lp.HERE / "soak.py"),
                "--setup",
                str(contract),
                "--agent",
                f"http://127.0.0.1:{agent.server_address[1]}",
                "--server-pid",
                str(server._proc.pid),  # noqa: SLF001
                "--duration-s",
                str(args.duration_s),
                "--warmup-s",
                str(args.warmup_s),
                "--skip-route-verification",
                "--output-dir",
                str(out),
            ],
            check=False,
            cwd=str(lp.HERE),
            env={**os.environ, "PYTHONPATH": f"{lp.REPO}:{lp.HERE}"},
        ).returncode
    finally:
        if agent is not None:
            agent.shutdown()
            agent.server_close()
        if server is not None:
            if code != 0:
                (out / "server-stderr-tail.txt").write_text(server.dump_stderr_debug()[-8000:])
            server.stop_process()
        subprocess.run([*compose, "down", "-v", "--remove-orphans"], check=False)
    return code


if __name__ == "__main__":
    sys.exit(main())
