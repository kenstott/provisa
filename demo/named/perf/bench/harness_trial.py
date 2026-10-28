# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""Run run_benchmark.py's query matrix once per engine against a small isolated local stack, to
check the harness's own code path (every transport x dialect cell answers, row counts agree, the
report parses). It measures nothing worth publishing. Run it through the machine-wide lock:

    ~/.provisa-agent/heavy.sh .venv/bin/python demo/named/perf/bench/harness_trial.py \\
        --engine duckdb --output-dir ~/bench_trial

The stack is local_proof.py's: its own compose project (``provisa-benchtrial-<pid>``) with the four
sources on leased loopback ports, seeded at ``--orders``, and an isolated Provisa server
(tests/integration/isolated_server.py) on demo/named/perf/config.yaml. ``--engine trino`` adds a
Trino coordinator to the same project. Everything is torn down (``compose down -v``) on exit.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

import yaml

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[3]
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(REPO))

import local_proof as lp  # noqa: E402

# The smallest data at which every query in queries.QUERIES has rows: point_lookup reads order
# 12345, cypher_single_source and cypher_cross_engine read customer 4242, and each generator makes
# orders // 8 customers.
MIN_ORDERS = 33_936

ENGINES = ("duckdb", "pg", "trino")


# demo/named/perf/trino/config.properties sizes query memory for the 32 GB perf VM, which Trino
# refuses to start with on a small heap.
_TRINO_SMALL_MEMORY = {"query.max-memory": "1GB", "query.max-memory-per-node": "1GB"}


def trino_service(port: int, tmp: Path) -> dict[str, Any]:
    """The perf demo's Trino coordinator (demo/named/perf/trino) with a 2 GB heap, on a leased
    port."""
    raw = yaml.safe_load((lp.PERF / "docker-compose.yml").read_text())
    svc = dict(raw["services"]["trino"])
    for key in ("profiles", "restart"):
        svc.pop(key, None)
    trino_dir = lp.PERF / "trino"
    lines = []
    for line in (trino_dir / "config.properties").read_text().splitlines():
        key = line.split("=", 1)[0].strip()
        lines.append(f"{key}={_TRINO_SMALL_MEMORY[key]}" if key in _TRINO_SMALL_MEMORY else line)
    config = tmp / "trino-config.properties"
    config.write_text("\n".join(lines) + "\n")
    jvm = tmp / "trino-jvm.config"
    jvm.write_text((trino_dir / "jvm.config").read_text().replace("-Xmx64G", "-Xmx2G"))
    svc["volumes"] = [
        f"{config}:/etc/trino/config.properties:ro",
        f"{jvm}:/etc/trino/jvm.config:ro",
        f"{trino_dir / 'node.properties'}:/etc/trino/node.properties:ro",
    ]
    svc["ports"] = [f"127.0.0.1:{port}:8080"]
    svc["labels"] = {"com.provisa.benchproof": "1"}
    return svc


def compose_file(ports: dict[str, int], tmp: Path, engine: str) -> Path:
    path = lp.compose_file(ports, tmp)
    if engine != "trino":
        return path
    doc = yaml.safe_load(path.read_text())
    doc["services"]["trino"] = trino_service(ports["trino"], tmp)
    path.write_text(yaml.safe_dump(doc, sort_keys=False))
    return path


def engine_env(engine: str, ports: dict[str, int]) -> dict[str, str]:
    if engine == "pg":
        # orchestrate.sh's engine_env: the pg engine federates through the source's own Postgres
        return {
            "PROVISA_ENGINE_URL": (
                f"postgresql://provisa:provisa@localhost:{ports['bench-postgresql']}/provisa_bench"
            )
        }
    if engine == "trino":
        # Trino runs in a container: every address it dials must resolve there, so the sources and
        # the control plane are named host.docker.internal (which /etc/hosts maps to 127.0.0.1 on
        # the host, so the server's own direct connections still reach them).
        env = {
            "TRINO_HOST": "127.0.0.1",
            "TRINO_PORT": str(ports["trino"]),
            "PROVISA_ENGINE_CONTROL_PLANE_HOST": "host.docker.internal",
        }
        for key in (
            "PROVISA_BENCH_POSTGRESQL_HOST",
            "PROVISA_BENCH_CLICKHOUSE_HOST",
            "PROVISA_BENCH_MONGO_HOST",
            "PROVISA_BENCH_NEO4J_HOST",
        ):
            env[key] = "host.docker.internal"
        return env
    return {}


def start_server(engine: str, ports: dict[str, int]) -> Any:
    server = lp._proof_server_class()(  # noqa: SLF001
        f"benchtrial{os.getpid()}{engine}",
        engine=engine,
        control_plane="own",
        enable_pgwire=True,
        enable_bolt=True,
        await_flight=True,
        await_grpc=True,
        config=str(lp.PERF / "config.yaml"),
        env={**lp._server_env(ports), **engine_env(engine, ports)},  # noqa: SLF001
    )
    server.start(timeout=600)
    return server


def run_matrix(server: Any, engine: str, out: Path) -> int:
    cmd = [
        sys.executable,
        str(HERE / "run_benchmark.py"),
        "--engine",
        engine,
        "--pgwire-host",
        "127.0.0.1",
        "--pgwire-port",
        str(server.pgwire_port),
        "--bolt-host",
        "127.0.0.1",
        "--bolt-port",
        str(server.bolt_port),
        "--http-base-url",
        server.base_url,
        "--flight-host",
        "127.0.0.1",
        "--flight-port",
        str(server.flight_port),
        "--grpc-host",
        "127.0.0.1",
        "--grpc-port",
        str(server.grpc_port),
        "--output-dir",
        str(out),
        "--skip-saturation",
    ]
    lp.log("$ " + " ".join(cmd))
    return subprocess.run(cmd, cwd=str(HERE), check=False).returncode


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the query matrix on a small isolated stack")
    parser.add_argument("--engine", action="append", choices=ENGINES, required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--orders", type=int, default=40_000)
    args = parser.parse_args()
    if args.orders < MIN_ORDERS:
        parser.error(f"--orders below {MIN_ORDERS} leaves queries with no rows to compare")
    out = Path(args.output_dir).expanduser()
    out.mkdir(parents=True, exist_ok=True)

    from tests.port_lease import lease_ports

    leased = lease_ports(6)
    ports = {
        "bench-postgresql": leased[0],
        "bench-clickhouse": leased[1],
        "bench-mongodb": leased[2],
        "neo4j-http": leased[3],
        "neo4j-bolt": leased[4],
        "trino": leased[5],
    }
    lp.ORDERS = lp.NEO4J_ORDERS = args.orders
    project = f"provisa-benchtrial-{os.getpid()}"
    tmp = Path(tempfile.mkdtemp(prefix="benchtrial-"))
    with_trino = "trino" in args.engine
    compose = [
        "docker",
        "compose",
        "-p",
        project,
        "-f",
        str(compose_file(ports, tmp, "trino" if with_trino else "duckdb")),
    ]
    failed: list[str] = []
    try:
        services = ["postgresql", "mongodb", "clickhouse", "neo4j"]
        if with_trino:
            services.append("trino")
        for service in services:
            lp.run([*compose, "up", "-d", "--wait", service])
        lp.seed(ports)
        lp.control_plane_on_stack(ports)
        for engine in args.engine:
            server = None
            try:
                server = start_server(engine, ports)
                if run_matrix(server, engine, out) != 0:
                    failed.append(engine)
            except Exception as exc:  # noqa: BLE001 - one engine's failure is reported, not fatal
                lp.log(f"{engine}: {exc}")
                failed.append(engine)
            finally:
                if server is not None:
                    (out / f"{engine}-server.stderr").write_text(server.dump_stderr_debug())
                    server.stop_process()
    finally:
        subprocess.run([*compose, "down", "-v", "--remove-orphans"], check=False)
    lp.log(f"project {project} down; failed engines: {failed or 'none'}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
