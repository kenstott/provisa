# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""The kill-a-node check (REQ-1911, METHODOLOGY.md): two Provisa nodes behind a TCP load balancer,
one killed mid-run. It measures nothing: it says whether the survivor kept answering correctly.

    python chaos_check.py --setup setups/three-device.yaml --out results/chaos \\
        --node a=10.90.0.2 --node b=10.90.0.5 --kill 'gcloud compute ssh ... -- sudo pkill -9 uvicorn'

Each published transport runs a loop of one fixed request through the load balancer and checks each
answer against the first. Every outcome falls in one class:

- ``ok``: the first answer again.
- ``dropped``: the connection failed. A client reconnects and goes on.
- ``wrong``: an answer that differs from the first.
- ``error``: the server answered with a failure.

The check passes when nothing is ``wrong`` or ``error`` anywhere, nothing failed before the kill,
nothing failed once ``--grace-s`` had passed after it, and every transport answered in that last
stretch. ``harness_trial.py --chaos`` runs the same check on two local nodes.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import threading
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import optimistic_load as ol
import request_render
import setup_contract
import yaml

TRANSPORTS = ("pgwire", "flight_sql", "graphql", "bolt", "grpc", "rest", "jsonapi")
LISTENERS = ("http", "pgwire", "bolt", "flight", "grpc")
HAPROXY_IMAGE = "haproxy:3.0.5"
_RECONNECT_S = 0.2
# Exception class names, anywhere in the class's bases, that mean the connection went away.
_DROPPED_NAMES = {
    "OSError",  # ConnectionResetError, BrokenPipeError and socket timeouts are under it
    "TransportError",  # httpx
    "OperationalError",  # psycopg
    "InterfaceError",  # psycopg
    "ServiceUnavailable",  # neo4j
    "SessionExpired",  # neo4j
    "DriverError",  # neo4j
    "FlightUnavailableError",
    "ArrowIOError",
}


@dataclass(frozen=True)
class Node:
    name: str
    host: str
    ports: Mapping[str, int]  # listener -> port


def is_dropped(exc: BaseException) -> bool:
    """Whether ``exc`` is the connection failing, as against the server answering with a failure."""
    code = getattr(exc, "code", None)
    if callable(code):  # a grpc.RpcError
        try:
            return getattr(code(), "name", "") == "UNAVAILABLE"
        except TypeError:
            return False
    return any(c.__name__ in _DROPPED_NAMES for c in type(exc).__mro__)


def haproxy_config(nodes: Sequence[Node], front: Mapping[str, int]) -> str:
    """TCP frontends, one per listener, each over every node. A node's HTTP port is the health
    check of all its listeners: a killed node leaves them all together."""
    lines = [
        "global",
        "  maxconn 4096",
        "defaults",
        "  mode tcp",
        "  retries 3",
        "  option redispatch",
        "  timeout connect 2s",
        "  timeout client 120s",
        "  timeout server 120s",
    ]
    for listener in LISTENERS:
        lines += [
            f"frontend f_{listener}",
            f"  bind *:{front[listener]}",
            f"  default_backend b_{listener}",
            f"backend b_{listener}",
            "  balance roundrobin",
        ]
        lines += [
            f"  server {n.name} {n.host}:{n.ports[listener]} check port {n.ports['http']} "
            "inter 500ms fall 2 rise 2"
            for n in nodes
        ]
    return "\n".join(lines) + "\n"


def start_balancer(config: Path, name: str) -> Callable[[], None]:
    """HAProxy in a container on this host's network. Returns its stop."""
    subprocess.run(
        [
            "sudo",
            "docker",
            "run",
            "-d",
            "--rm",
            "--name",
            name,
            "--network",
            "host",
            "-v",
            f"{config}:/usr/local/etc/haproxy/haproxy.cfg:ro",
            HAPROXY_IMAGE,
        ],
        check=True,
    )

    def stop() -> None:
        subprocess.run(["sudo", "docker", "rm", "-f", name], check=False)

    return stop


def _canon(answer: Any) -> str:
    return json.dumps(answer[:2], sort_keys=True, default=str)


def _loop(
    transport: str,
    ep: ol.Endpoints,
    stop: threading.Event,
    record: Callable[[str, str, str], None],
) -> None:
    """One request, repeated until ``stop``, each outcome recorded."""
    query = request_render.build_query(ep.setup, transport, cached=False)
    role = ep.setup.request_role(transport, ep.role)
    expected: str | None = None
    client = None
    while not stop.is_set():
        try:
            if client is None:
                client = ol.CLIENTS[transport](ep)
            answer = _canon(client.rows(query, role))
        except Exception as exc:  # noqa: BLE001 - every failure is classified and recorded
            outcome = "dropped" if is_dropped(exc) else "error"
            record(transport, outcome, ol._brief(exc))  # noqa: SLF001
            client = _closed(client)
            time.sleep(_RECONNECT_S)
            continue
        if expected is None:
            expected = answer
        record(transport, "ok" if answer == expected else "wrong", "")
    _closed(client)


def _closed(client: Any) -> None:
    if client is not None:
        try:
            client.close()
        except Exception:  # noqa: BLE001, S110 - the connection is gone already
            pass


def verdict(
    records: Iterable[tuple[float, str, str, str]],
    *,
    transports: Sequence[str],
    killed_at: float,
    grace_s: float,
) -> dict[str, Any]:
    """The check's result from ``(time, transport, outcome, detail)`` records."""
    out: dict[str, Any] = {
        t: {"before": {}, "during": {}, "after": {}, "first_failure": None} for t in transports
    }
    problems: list[str] = []
    for at, transport, outcome, detail in records:
        phase = "before" if at < killed_at else ("during" if at < killed_at + grace_s else "after")
        row = out[transport]
        row[phase][outcome] = row[phase].get(outcome, 0) + 1
        if outcome != "ok" and row["first_failure"] is None:
            row["first_failure"] = f"{outcome}: {detail}"
        if outcome in ("wrong", "error") or (outcome == "dropped" and phase != "during"):
            problems.append(f"{transport}: {outcome} {phase}: {detail}")
    for transport in transports:
        if not out[transport]["after"].get("ok"):
            problems.append(f"{transport}: answered nothing after the grace period")
        if not out[transport]["before"].get("ok"):
            problems.append(f"{transport}: answered nothing before the kill")
    return {"passed": not problems, "problems": sorted(set(problems))[:50], "transports": out}


def run_check(
    ep: ol.Endpoints,
    kill: Callable[[], None],
    *,
    transports: Sequence[str] = TRANSPORTS,
    workers: int = 2,
    warm_s: float = 20,
    grace_s: float = 15,
    after_s: float = 20,
) -> dict[str, Any]:
    """Load on every transport, ``kill()`` after ``warm_s``, and the verdict."""
    records: list[tuple[float, str, str, str]] = []
    lock = threading.Lock()

    def record(transport: str, outcome: str, detail: str) -> None:
        with lock:
            records.append((time.monotonic(), transport, outcome, detail))

    stop = threading.Event()
    threads = [
        threading.Thread(target=_loop, args=(t, ep, stop, record), daemon=True)
        for t in transports
        for _ in range(workers)
    ]
    for t in threads:
        t.start()
    time.sleep(warm_s)
    killed_at = time.monotonic()
    kill()
    time.sleep(grace_s + after_s)
    stop.set()
    for t in threads:
        t.join(30)
    return verdict(records, transports=transports, killed_at=killed_at, grace_s=grace_s)


def _node(value: str, ports: Mapping[str, int]) -> Node:
    name, _, host = value.partition("=")
    if not host:
        raise SystemExit(f"--node needs NAME=HOST, got {value!r}")
    return Node(name, host, ports)


def main() -> int:
    parser = argparse.ArgumentParser(description="Kill one of two Provisa nodes under load")
    parser.add_argument("--setup", required=True, help="the contract; endpoints name this host")
    parser.add_argument("--out", required=True)
    parser.add_argument("--node", action="append", required=True, help="NAME=HOST, twice")
    parser.add_argument("--kill", required=True, help="the shell command that kills one node")
    parser.add_argument("--warm-s", type=float, default=20)
    parser.add_argument("--grace-s", type=float, default=15)
    parser.add_argument("--after-s", type=float, default=20)
    args = parser.parse_args()
    raw = yaml.safe_load(Path(args.setup).read_text())
    e = raw["deployment"]["endpoints"]
    ports = {
        "http": 8001,
        "pgwire": e["pgwire"]["port"],
        "bolt": e["bolt"]["port"],
        "flight": e["flight"]["port"],
        "grpc": e["grpc"]["port"],
    }
    nodes = [_node(v, ports) for v in args.node]
    out = Path(args.out).expanduser()
    out.mkdir(parents=True, exist_ok=True)
    config = out / "haproxy.cfg"
    config.write_text(haproxy_config(nodes, ports))
    stop_balancer = start_balancer(config.resolve(), "provisa-bench-lb")
    try:
        time.sleep(3)  # its first health checks
        ep = endpoints(Path(args.setup), out)
        result = run_check(
            ep,
            lambda: subprocess.run(args.kill, shell=True, check=True),  # noqa: S602  # nosec B602
            warm_s=args.warm_s,
            grace_s=args.grace_s,
            after_s=args.after_s,
        )
    finally:
        stop_balancer()
    (out / "chaos.json").write_text(json.dumps(result, indent=2))
    print(json.dumps({"passed": result["passed"], "problems": result["problems"]}, indent=2))
    return 0 if result["passed"] else 1


def endpoints(contract: Path, out: Path) -> ol.Endpoints:
    """The contract's endpoints, its names resolved through the load balancer."""
    unbound = setup_contract.load_setup(str(contract), known_transports=list(ol.TRANSPORTS))
    resolved = ol.resolve_names(argparse.Namespace(resolved_names=None), unbound, out)
    setup = setup_contract.load_setup(
        str(contract),
        known_transports=list(ol.TRANSPORTS),
        deployment=resolved,
        verify_replication=False,
    )
    return ol.endpoints_from_setup(setup)


if __name__ == "__main__":
    raise SystemExit(main())
