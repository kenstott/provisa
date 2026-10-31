# Copyright (c) 2026 Kenneth Stott
# Canary: 6acaeca4-3de9-42a5-a40b-afd6ad1852a2
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
(tests/integration/isolated_server.py) on demo/named/perf/config.yaml. Everything is torn down
(``compose down -v``) on exit.

``--engine pg`` and ``--engine trino`` run the engine in a container that dials the sources by the
addresses the server knows, loopback ones, so those containers take the host's network. That is a
Linux host's network: the trial refuses these engines on another platform. ``--engine pg`` builds
the perf Postgres image (Dockerfile.postgres). ``--engine trino`` adds a Trino coordinator, the
Zaychik Flight proxy built from ./zaychik and a MinIO for its OTel store.

An engine fails the trial when any cell failed, even once, when two transports answered one query
with different row counts, or when a transport was unavailable: run_benchmark.py records those and
exits 0. ``<output-dir>/trial-summary.json`` lists them.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
from collections import defaultdict
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


#: Engines in a container, which reach the sources on the host's loopback.
CONTAINER_ENGINES = {"pg", "trino"}
#: run_benchmark.py's transports: a trial missing one has not run its cells.
TRANSPORTS = {"sql", "flight", "cypher", "http", "graphql", "grpc"}

PERF_PG_IMAGE = "provisa-perf-postgres:16"

# The OTel store's bucket and login in the trial's own MinIO. Nothing else reads them.
OTEL_BUCKET = "provisa-otel"
MINIO_USER, MINIO_PASSWORD = "minioadmin", "minioadmin"

# demo/named/perf/trino/config.properties sizes query memory for the 32 GB perf VM, which Trino
# refuses to start with on a small heap.
_TRINO_SMALL_MEMORY = {"query.max-memory": "1GB", "query.max-memory-per-node": "1GB"}


def tcp_healthcheck(port: int) -> dict[str, Any]:
    return {
        "test": ["CMD-SHELL", f"timeout 2 bash -c '</dev/tcp/127.0.0.1/{port}' || exit 1"],
        "interval": "5s",
        "timeout": "3s",
        "retries": 24,
        "start_period": "20s",
    }


def trino_service(port: int, tmp: Path) -> dict[str, Any]:
    """The perf demo's Trino coordinator (demo/named/perf/trino) with a 2 GB heap, on the host's
    network and a leased port."""
    raw = yaml.safe_load((lp.PERF / "docker-compose.yml").read_text())
    svc = dict(raw["services"]["trino"])
    for key in ("profiles", "restart", "ports"):
        svc.pop(key, None)
    trino_dir = lp.PERF / "trino"
    lines = []
    for line in (trino_dir / "config.properties").read_text().splitlines():
        key = line.split("=", 1)[0].strip()
        lines.append(f"{key}={_TRINO_SMALL_MEMORY[key]}" if key in _TRINO_SMALL_MEMORY else line)
    config = tmp / "trino-config.properties"
    config.write_text(
        ("\n".join(lines) + "\n")
        .replace("http-server.http.port=8080", f"http-server.http.port={port}")
        .replace("localhost:8080", f"localhost:{port}")
    )
    jvm = tmp / "trino-jvm.config"
    jvm.write_text((trino_dir / "jvm.config").read_text().replace("-Xmx64G", "-Xmx2G"))
    svc["volumes"] = [
        f"{config}:/etc/trino/config.properties:ro",
        f"{jvm}:/etc/trino/jvm.config:ro",
        f"{trino_dir / 'node.properties'}:/etc/trino/node.properties:ro",
    ]
    svc["network_mode"] = "host"
    # node.id is the hostname, which a cloud VM's dots make invalid
    svc["environment"] = {"HOSTNAME": "benchtrino"}
    svc["healthcheck"] = {
        **svc["healthcheck"],
        "test": ["CMD-SHELL", f"trino --server localhost:{port} --execute 'SELECT 1' || exit 1"],
    }
    svc["labels"] = {"com.provisa.benchproof": "1"}
    return svc


def zaychik_service(ports: dict[str, int]) -> dict[str, Any]:
    """The Flight proxy the trino engine reads results through. There is no other path (REQ-146 is
    withdrawn), and no published image: the root stack builds it from ./zaychik."""
    return {
        "image": "zaychik:latest",
        "build": {"context": str(REPO / "zaychik")},
        "network_mode": "host",
        "environment": {
            "TF_TRINO_HOST": "127.0.0.1",
            "TF_TRINO_PORT": str(ports["trino"]),
            "TF_TRINO_SSL": "false",
            "TF_FLIGHT_HOST": "127.0.0.1",
            "TF_FLIGHT_PORT": str(ports["zaychik"]),
            "TF_FLIGHT_SSL": "false",
            "TF_FLIGHT_AUTH_TYPE": "trino",
            "TF_FLIGHT_BATCH_SIZE": "10000",
        },
        "healthcheck": tcp_healthcheck(ports["zaychik"]),
        "labels": {"com.provisa.benchproof": "1"},
    }


def minio_service(ports: dict[str, int]) -> dict[str, Any]:
    """The trino engine's OTel store, the root stack's image on leased ports."""
    core = yaml.safe_load((REPO / "docker-compose.core.yml").read_text())
    return {
        "image": core["services"]["minio"]["image"],
        "network_mode": "host",
        "environment": {
            "MINIO_ROOT_USER": MINIO_USER,
            "MINIO_ROOT_PASSWORD": MINIO_PASSWORD,
            "MINIO_API_PORT_NUMBER": str(ports["minio"]),
            "MINIO_CONSOLE_PORT_NUMBER": str(ports["minio-console"]),
        },
        "healthcheck": tcp_healthcheck(ports["minio"]),
        "labels": {"com.provisa.benchproof": "1"},
    }


def put_engine_postgres_on_host(pg: dict[str, Any], port: int, image: str) -> None:
    """The pg engine runs inside this Postgres and dials the other sources on loopback. It needs
    the extensions of the perf image; ``image`` names another only to see a trial fail without
    them."""
    pg.pop("ports")
    pg["network_mode"] = "host"
    pg["image"] = image
    pg["command"] = [*pg["command"], "-c", f"port={port}"]
    if image == PERF_PG_IMAGE:
        pg["build"] = {"context": str(lp.PERF), "dockerfile": "Dockerfile.postgres"}
        pg["command"] += ["-c", "shared_preload_libraries=pg_duckdb"]
    pg["healthcheck"] = {
        **pg["healthcheck"],
        "test": ["CMD-SHELL", f"pg_isready -p {port} -U provisa -d provisa_bench"],
    }


def compose_file(ports: dict[str, int], tmp: Path, engines: list[str], pg_image: str) -> Path:
    path = lp.compose_file(ports, tmp)
    if not CONTAINER_ENGINES & set(engines):
        return path
    doc = yaml.safe_load(path.read_text())
    services = doc["services"]
    if "pg" in engines:
        put_engine_postgres_on_host(services["postgresql"], ports["bench-postgresql"], pg_image)
        # P5: the pg engine's ClickHouse attach dials the native port's default whatever the source
        # declares. Published so the cells behind that attach run; goes with the fix (J306).
        services["clickhouse"]["ports"].append("127.0.0.1:9000:9000")
    if "trino" in engines:
        services["trino"] = trino_service(ports["trino"], tmp)
        services["minio"] = minio_service(ports)
        services["zaychik"] = zaychik_service(ports)
    path.write_text(yaml.safe_dump(doc, sort_keys=False))
    return path


def create_otel_bucket(ports: dict[str, int]) -> None:
    import boto3

    s3 = boto3.client(
        "s3",
        endpoint_url=f"http://127.0.0.1:{ports['minio']}",
        aws_access_key_id=MINIO_USER,
        aws_secret_access_key=MINIO_PASSWORD,
        region_name="us-east-1",
    )
    s3.create_bucket(Bucket=OTEL_BUCKET)


def engine_env(engine: str, ports: dict[str, int]) -> dict[str, str]:
    if engine == "pg":
        # orchestrate.sh's engine_env: the pg engine federates through the source's own Postgres,
        # and row-level replicas land there, where it reads them
        url = f"postgresql://provisa:provisa@localhost:{ports['bench-postgresql']}/provisa_bench"
        return {"PROVISA_ENGINE_URL": url, "PROVISA_MATERIALIZE_URL": url}
    if engine == "trino":
        return {
            "TRINO_HOST": "127.0.0.1",
            "TRINO_PORT": str(ports["trino"]),
            "ZAYCHIK_HOST": "127.0.0.1",
            "ZAYCHIK_PORT": str(ports["zaychik"]),
            "PROVISA_OTEL_S3_ENDPOINT": f"http://127.0.0.1:{ports['minio']}",
            "PROVISA_OTEL_S3_ACCESS_KEY": MINIO_USER,
            "PROVISA_OTEL_S3_SECRET_KEY": MINIO_PASSWORD,
        }
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
        env={
            **lp._server_env(ports),  # noqa: SLF001
            **lp._mcp_env(ports),  # noqa: SLF001
            **engine_env(engine, ports),
        },
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


def run_optimistic(server: Any, engine: str, out: Path, ports: dict[str, int]) -> int:
    """The 11-transport optimistic ramp on the proof contract, written to <out>/<engine>/optimistic."""
    contract = lp.proof_contract(server, f"provisa-benchtrial-{os.getpid()}", mcp_port=ports["mcp"])
    path = out / f"{engine}-optimistic-contract.yaml"
    path.write_text(yaml.safe_dump(contract, sort_keys=False))
    cmd = [
        sys.executable,
        str(HERE / "run_benchmark.py"),
        "--engine",
        engine,
        "--optimistic",
        "--setup",
        str(path),
        "--output-dir",
        str(out / engine),
    ]
    lp.log("$ " + " ".join(cmd))
    env = {**os.environ, "PROVISA_HTTP_BASE_URL": server.base_url}
    return subprocess.run(cmd, cwd=str(HERE), check=False, env=env).returncode


def matrix_verdict(report: dict[str, Any]) -> dict[str, Any]:
    """What a matrix report says of an engine: its cells, each that failed, each query whose
    transports disagree on the rows, and any transport that did not run."""
    results = report["results"]
    failed = [
        {
            "query": r["query_id"],
            "pass": r["cache_pass"],
            "transport": f"{r['transport']}({r['dialect']})",
            "errors": r["errors"],
            "all_failed": r.get("status") == "all_failed",
            "first_error": r["first_error"],
        }
        for r in results
        if r["errors"]
    ]
    rows: dict[tuple[str, str], dict[str, float]] = defaultdict(dict)
    for r in results:
        if r.get("n"):
            cell = f"{r['transport']}({r['dialect']})"
            rows[(r["query_id"], r["cache_pass"])][cell] = r["total_rows"] / r["n"]
    disagree = [
        {"query": query, "pass": cache_pass, "rows": cells}
        for (query, cache_pass), cells in rows.items()
        if len(set(cells.values())) > 1
    ]
    return {
        "cells": len(results),
        "failed": failed,
        "row_disagreements": disagree,
        "unavailable_transports": sorted(TRANSPORTS - set(report["transports_tested"])),
    }


def optimistic_verdict(reports: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """The same for the optimistic ramp, one report per transport."""
    failed = []
    for transport, report in sorted(reports.items()):
        problems = [{"error": report["error"]}] if "error" in report else []
        for step in report.get("steps", []):
            if step.get("error") or step.get("errors"):
                problems.append(
                    {
                        "errors": step.get("errors"),
                        "error": step.get("error") or step["first_error"],
                    }
                )
        failed += [{"transport": transport, **p} for p in problems]
    return {
        "cells": len(reports),
        "failed": failed,
        "row_disagreements": [],
        "unavailable_transports": sorted(set(lp.ol.TRANSPORTS) - set(reports)),
    }


def engine_failed(verdict: dict[str, Any]) -> bool:
    return bool(
        not verdict["cells"]
        or verdict["failed"]
        or verdict["row_disagreements"]
        or verdict["unavailable_transports"]
    )


def read_verdict(out: Path, engine: str, optimistic: bool, before: set[Path]) -> dict[str, Any]:
    """The verdict from what this run wrote; a run that wrote nothing has no cells."""
    if optimistic:
        files = (out / engine / "optimistic").glob("*.json")
        reports = {p.stem: json.loads(p.read_text()) for p in files if p not in before}
        return optimistic_verdict(reports)
    written = sorted(set((out / engine).glob("*.json")) - before)
    if not written:
        return {"cells": 0, "failed": [], "row_disagreements": [], "unavailable_transports": []}
    return matrix_verdict(json.loads(written[-1].read_text()))


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the query matrix on a small isolated stack")
    parser.add_argument("--engine", action="append", choices=ENGINES, required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--orders", type=int, default=40_000)
    parser.add_argument(
        "--optimistic",
        action="store_true",
        help="Run the optimistic ramp on all 11 transports in place of the query matrix",
    )
    parser.add_argument(
        "--pg-image",
        default=PERF_PG_IMAGE,
        help="The Postgres image of --engine pg. postgres:16 has none of the engine's extensions: "
        "the trial must then report the engine failed",
    )
    args = parser.parse_args()
    if args.orders < MIN_ORDERS:
        parser.error(f"--orders below {MIN_ORDERS} leaves queries with no rows to compare")
    if CONTAINER_ENGINES & set(args.engine) and sys.platform != "linux":
        parser.error("--engine pg and trino need a Linux host's network; run them on a VM")
    out = Path(args.output_dir).expanduser()
    out.mkdir(parents=True, exist_ok=True)

    from tests.port_lease import lease_ports

    names = (
        "bench-postgresql",
        "bench-clickhouse",
        "bench-mongodb",
        "neo4j-http",
        "neo4j-bolt",
        "trino",
        "minio",
        "minio-console",
        "zaychik",
        "mcp",
    )
    ports = dict(zip(names, lease_ports(len(names))))
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
        str(compose_file(ports, tmp, args.engine, args.pg_image)),
    ]
    verdicts: dict[str, dict[str, Any]] = {}
    try:
        services = ["postgresql", "mongodb", "clickhouse", "neo4j"]
        if with_trino:
            services += ["trino", "zaychik", "minio"]
        for service in services:
            lp.run([*compose, "up", "-d", "--wait", service])
        if with_trino:
            create_otel_bucket(ports)
        lp.seed(ports)
        lp.control_plane_on_stack(ports)
        for engine in args.engine:
            server = None
            before = set((out / engine).glob("*.json")) | set(
                (out / engine / "optimistic").glob("*.json")
            )
            verdict: dict[str, Any] = {}
            try:
                server = start_server(engine, ports)
                if args.optimistic:
                    code = run_optimistic(server, engine, out, ports)
                else:
                    code = run_matrix(server, engine, out)
                verdict = read_verdict(out, engine, args.optimistic, before)
                verdict["exit_code"] = code
            except Exception as exc:  # noqa: BLE001 - one engine's failure is reported, not fatal
                lp.log(f"{engine}: {exc}")
                verdict = {"exception": str(exc)}
            finally:
                verdicts[engine] = verdict
                if server is not None:
                    (out / f"{engine}-server.stderr").write_text(server.dump_stderr_debug())
                    server.stop_process()
    finally:
        subprocess.run([*compose, "down", "-v", "--remove-orphans"], check=False)
    failed = [
        engine
        for engine, verdict in verdicts.items()
        if "exception" in verdict or verdict["exit_code"] != 0 or engine_failed(verdict)
    ]
    (out / "trial-summary.json").write_text(
        json.dumps({"failed_engines": failed, "engines": verdicts}, indent=2, default=str)
    )
    for engine, verdict in verdicts.items():
        lp.log(f"{engine}: {json.dumps(verdict, default=str)[:4000]}")
    lp.log(f"project {project} down; failed engines: {failed or 'none'}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
