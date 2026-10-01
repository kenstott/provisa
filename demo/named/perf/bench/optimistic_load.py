# Copyright (c) 2026 Kenneth Stott
# Canary: 3f6a1d84-2c7e-4b59-9a10-6e8d4c2b7f35
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""The most-optimistic request on every transport, and a load generator that is not the bottleneck.

``queries.OPTIMISTIC`` is one request reduced as far as it goes — one row, one column, no
predicate, served from the response cache where the transport has a per-request opt-in — so what
is left to measure on each transport is the transport itself: its protocol handling and whatever
processing it does that the request did not need.

The ramp is CLOSED-LOOP: ``concurrency`` clients each hold one persistent connection and send the
next request when the previous one answers. Clients are spread over several PROCESSES (one Python
process executes on one core at a time, so a single process of N threads measures the client), and
each step reports the client's own CPU next to the result: a step whose client processes are
saturated is marked ``client_limited`` and is a measurement of this tool, not of the server.

Server CPU per request is reported when ``--server-pid`` names the server's process (its children
— uvicorn workers — are included): at concurrency 1 it is the per-transport overhead ranking.

    python optimistic_load.py --http-base-url http://localhost:8001 --server-pid 1234 \\
        --output-dir results/optimistic
"""

from __future__ import annotations

import argparse
import json
import math
import multiprocessing
import os
import resource
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from queries import OPTIMISTIC

# transport -> (what carries the request, the per-request cache opt-in it uses or None)
TRANSPORTS: dict[str, tuple[str, str | None]] = {
    "graphql": ("POST /data/graphql", "@cached"),
    "data_sql": ("POST /data/sql", "-- @provisa cache=true"),
    "cypher_http": ("POST /data/cypher", "// @provisa cache=true"),
    "pgwire": ("PostgreSQL wire protocol, prepared statement", "-- @provisa cache=true"),
    "flight_sql": ("Arrow Flight ticket, SQL", "-- @provisa cache=true"),
    "flight_graphql": ("Arrow Flight ticket, GraphQL", "@cached"),
    "bolt": ("Bolt, Cypher", "// @provisa cache=true"),
    "grpc": ("gRPC ProvisaService typed RPC", "x-provisa-cache: true metadata"),
    # REST and JSON:API have no per-request cache opt-in (both pass NO_CACHE_HINT to the
    # pipeline), so they run uncached and are labelled so.
    "rest": ("GET /data/rest/{domain}/{table}", None),
    "jsonapi": ("GET /data/jsonapi/{domain}/{table}", None),
}

DEFAULT_STEPS = (1, 16, 64, 256)
# What the server does with the request beyond what was asked — verified against a server
# registered with the perf demo's orders table, and reported with each transport's result.
NOTES: dict[str, str] = {
    "cypher_http": "served from the cache, but the X-Provisa-Cache header says MISS on every response",
    "data_sql": "served from the cache; /data/sql sends no X-Provisa-Cache header",
    "grpc": (
        "read_mask is not applied (every column of the row is returned) and the cache opt-in has "
        "no effect on a DIRECT-route statement (the answer follows the source)"
    ),
    "rest": "no per-request cache opt-in exists: uncached",
    "jsonapi": "no per-request cache opt-in exists: uncached; also computes meta.total per request",
}

# A client process whose CPU use over the window is at or above this share of one core is the
# limit of what it could send.
_CLIENT_SATURATED = 0.9


@dataclass(frozen=True)
class Endpoints:
    http_base_url: str
    pgwire_host: str
    pgwire_port: int
    bolt_host: str
    bolt_port: int
    flight_host: str
    flight_port: int
    grpc_host: str
    grpc_port: int
    role: str


# --------------------------------------------------------------------------------------------
# One client per connection. ``call()`` returns (rows, columns, cache_hit) — cache_hit is None
# when the transport's response does not say.
# --------------------------------------------------------------------------------------------


class _HttpClient:
    def __init__(self, ep: Endpoints) -> None:
        import httpx

        self._http = httpx.Client(
            base_url=ep.http_base_url, timeout=60, headers={"X-Provisa-Role": ep.role}
        )
        self._role = ep.role

    @staticmethod
    def _hit(resp: Any) -> bool | None:
        status = resp.headers.get("x-provisa-cache")
        return None if status is None else status == "HIT"

    def close(self) -> None:
        self._http.close()


class GraphqlClient(_HttpClient):
    def call(self) -> tuple[int, int, bool | None]:
        resp = self._http.post("/data/graphql", json={"query": OPTIMISTIC.graphql})
        resp.raise_for_status()
        body = resp.json()
        if body.get("errors"):
            raise RuntimeError(str(body["errors"])[:300])
        (rows,) = body["data"].values()
        return len(rows), len(rows[0]), self._hit(resp)


class DataSqlClient(_HttpClient):
    def call(self) -> tuple[int, int, bool | None]:
        resp = self._http.post("/data/sql", json={"sql": OPTIMISTIC.sql, "role": self._role})
        resp.raise_for_status()
        body = resp.json()
        (rows,) = body["data"].values()
        return len(rows), len(body["columns"]), self._hit(resp)


class CypherHttpClient(_HttpClient):
    def call(self) -> tuple[int, int, bool | None]:
        resp = self._http.post("/data/cypher", json={"query": OPTIMISTIC.cypher, "params": {}})
        resp.raise_for_status()
        body = resp.json()
        # /data/cypher answers ``X-Provisa-Cache: MISS`` on every response, served from the cache
        # or not (cypher_router builds the header from no cache result), so the header says
        # nothing here.
        return len(body["rows"]), len(body["columns"]), None


class RestClient(_HttpClient):
    def call(self) -> tuple[int, int, bool | None]:
        spec = OPTIMISTIC.rest
        resp = self._http.get(spec["path"], params=spec["params"])
        resp.raise_for_status()
        rows = resp.json()["data"]
        return len(rows), len(rows[0]), self._hit(resp)


class JsonApiClient(_HttpClient):
    def call(self) -> tuple[int, int, bool | None]:
        spec = OPTIMISTIC.jsonapi
        resp = self._http.get(
            spec["path"], params=spec["params"], headers={"Accept": "application/vnd.api+json"}
        )
        resp.raise_for_status()
        rows = resp.json()["data"]
        return len(rows), len(rows[0]["attributes"]), self._hit(resp)


class PgwireClient:
    """One connection, the statement prepared once and executed by Bind/Execute after that — what
    a driver does with a statement it sends repeatedly."""

    def __init__(self, ep: Endpoints) -> None:
        import psycopg

        self._conn = psycopg.connect(
            host=ep.pgwire_host,
            port=ep.pgwire_port,
            user=ep.role,
            password="unused",  # --demo perf runs with auth.provider: none
            dbname="provisa",
            autocommit=True,
            prepare_threshold=0,
        )

    def call(self) -> tuple[int, int, bool | None]:
        cur = self._conn.execute(OPTIMISTIC.sql)
        rows = cur.fetchall()
        return len(rows), len(cur.description or ()), None

    def close(self) -> None:
        self._conn.close()


class _FlightClient:
    query = ""

    def __init__(self, ep: Endpoints) -> None:
        import pyarrow.flight as fl

        self._client = fl.FlightClient(f"grpc://{ep.flight_host}:{ep.flight_port}")
        self._ticket = fl.Ticket(json.dumps({"query": self.query, "role": ep.role}).encode())

    def call(self) -> tuple[int, int, bool | None]:
        table = self._client.do_get(self._ticket).read_all()
        return table.num_rows, table.num_columns, None

    def close(self) -> None:
        self._client.close()


class FlightSqlClient(_FlightClient):
    query = OPTIMISTIC.sql or ""


class FlightGraphqlClient(_FlightClient):
    query = OPTIMISTIC.graphql or ""


class BoltClient:
    def __init__(self, ep: Endpoints) -> None:
        from neo4j import GraphDatabase

        self._driver = GraphDatabase.driver(
            f"bolt://{ep.bolt_host}:{ep.bolt_port}", auth=(ep.role, "unused")
        )
        self._session = self._driver.session()

    def call(self) -> tuple[int, int, bool | None]:
        records = list(self._session.run(OPTIMISTIC.cypher))
        return len(records), len(records[0].keys()), None

    def close(self) -> None:
        self._session.close()
        self._driver.close()


_grpc_lock = threading.Lock()
_grpc_shared: Any = None  # one compiled descriptor pool + channel per client PROCESS


class GrpcClient:
    def __init__(self, ep: Endpoints) -> None:
        global _grpc_shared
        from run_benchmark import GrpcTransport

        with _grpc_lock:
            if _grpc_shared is None:
                transport = GrpcTransport(ep.grpc_host, ep.grpc_port, ep.http_base_url, ep.role)
                if not transport.available():
                    raise RuntimeError(f"gRPC unavailable: {getattr(transport, '_error', '')}")
                _grpc_shared = transport
        transport = _grpc_shared
        spec = OPTIMISTIC.grpc
        assert spec is not None
        type_name = spec["type_name"]
        request_cls = transport._resolve_message_class(f"provisa.v1.{type_name}Request")  # noqa: SLF001
        response_cls = transport._resolve_message_class(f"provisa.v1.{type_name}")  # noqa: SLF001
        self._request = request_cls(limit=spec["limit"])
        self._request.read_mask.paths.extend(spec["read_mask"])
        self._metadata = [("x-provisa-role", ep.role), *spec["metadata"]]
        self._stream = transport._channel.unary_stream(  # noqa: SLF001
            f"/provisa.v1.ProvisaService/Query{type_name}",
            request_serializer=request_cls.SerializeToString,
            response_deserializer=response_cls.FromString,
        )

    def call(self) -> tuple[int, int, bool | None]:
        rows = list(self._stream(self._request, metadata=self._metadata))
        return len(rows), len(rows[0].ListFields()), None

    def close(self) -> None:
        pass  # the channel is the process's, closed when the process exits


CLIENTS: dict[str, Any] = {
    "graphql": GraphqlClient,
    "data_sql": DataSqlClient,
    "cypher_http": CypherHttpClient,
    "pgwire": PgwireClient,
    "flight_sql": FlightSqlClient,
    "flight_graphql": FlightGraphqlClient,
    "bolt": BoltClient,
    "grpc": GrpcClient,
    "rest": RestClient,
    "jsonapi": JsonApiClient,
}


# --------------------------------------------------------------------------------------------
# The ramp
# --------------------------------------------------------------------------------------------


def _brief(exc: BaseException) -> str:
    """An error as one short line (a gRPC error carries its whole debug context)."""
    return f"{type(exc).__name__}: {' '.join(str(exc).split())[:140]}"


def _cpu_seconds() -> float:
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return usage.ru_utime + usage.ru_stime


def _client_process(
    transport: str,
    ep: Endpoints,
    threads: int,
    window_s: float,
    ready: Any,
    go: Any,
    out: Any,
) -> None:
    """One client process: ``threads`` closed-loop clients, each on its own connection."""
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    latencies: list[list[float]] = [[] for _ in range(threads)]
    errors = [0] * threads
    first_error: list[str] = []
    hits = [0] * threads
    hit_known = [0] * threads
    connected = threading.Barrier(threads + 1)

    def _loop(i: int) -> None:
        try:
            client = CLIENTS[transport](ep)
        except Exception as exc:  # noqa: BLE001 - reported in the step's result
            first_error.append(f"connect: {_brief(exc)}")
            connected.abort()
            return
        try:
            client.call()  # the connection is open and the statement is prepared / cached
        except Exception as exc:  # noqa: BLE001 - a refused warm-up is the server's answer at
            # this concurrency (a stream cap, say): counted like any other failed request.
            errors[i] += 1
            if not first_error:
                first_error.append(_brief(exc))
        try:
            connected.wait()
        except threading.BrokenBarrierError:
            client.close()
            return
        go.wait()
        stop_at = time.perf_counter() + window_s
        mine = latencies[i]
        while True:
            t0 = time.perf_counter()
            if t0 >= stop_at:
                break
            try:
                _rows, _cols, hit = client.call()
            except Exception as exc:  # noqa: BLE001 - counted, first one kept
                errors[i] += 1
                if not first_error:
                    first_error.append(_brief(exc))
                continue
            mine.append(time.perf_counter() - t0)
            if hit is not None:
                hit_known[i] += 1
                hits[i] += hit
        client.close()

    workers = [threading.Thread(target=_loop, args=(i,), daemon=True) for i in range(threads)]
    for w in workers:
        w.start()
    try:
        connected.wait(timeout=120)
    except threading.BrokenBarrierError:
        ready.put(first_error[0] if first_error else "client did not connect")
        return
    ready.put(None)
    go.wait()
    cpu0 = _cpu_seconds()
    for w in workers:
        w.join()
    out.put(
        {
            "latencies": [x for per in latencies for x in per],
            "errors": sum(errors),
            "first_error": first_error[0] if first_error else None,
            "hits": sum(hits),
            "hit_known": sum(hit_known),
            "cpu_s": _cpu_seconds() - cpu0,
        }
    )


def _server_cpu_seconds(server_pid: int | None) -> float | None:
    """CPU the server has used so far: the named process and every descendant (its workers)."""
    if server_pid is None:
        return None
    import psutil

    root = psutil.Process(server_pid)
    total = 0.0
    for proc in [root, *root.children(recursive=True)]:
        try:
            times = proc.cpu_times()
        except psutil.NoSuchProcess:
            continue
        total += times.user + times.system
    return total


def _percentile(sorted_values: list[float], q: float) -> float:
    return sorted_values[min(len(sorted_values) - 1, int(q * len(sorted_values)))]


def run_step(
    transport: str,
    ep: Endpoints,
    concurrency: int,
    window_s: float,
    max_processes: int,
    server_pid: int | None,
) -> dict:
    """One closed-loop step at ``concurrency`` clients for ``window_s`` seconds."""
    ctx = multiprocessing.get_context("spawn")
    n_procs = min(concurrency, max_processes)
    per_proc = [
        concurrency // n_procs + (1 if i < concurrency % n_procs else 0) for i in range(n_procs)
    ]
    ready, out, go = ctx.Queue(), ctx.Queue(), ctx.Event()
    procs = [
        ctx.Process(target=_client_process, args=(transport, ep, n, window_s, ready, go, out))
        for n in per_proc
    ]
    for p in procs:
        p.start()
    failures = [msg for msg in (ready.get(timeout=180) for _ in procs) if msg is not None]
    if failures:
        go.set()
        for p in procs:
            p.join(10)
            if p.is_alive():
                p.terminate()
        return {"concurrency": concurrency, "error": failures[0]}
    server_cpu0 = _server_cpu_seconds(server_pid)
    go.set()
    results = [out.get(timeout=window_s + 120) for _ in procs]
    server_cpu1 = _server_cpu_seconds(server_pid)
    for p in procs:
        p.join(30)
    latencies = sorted(x for r in results for x in r["latencies"])
    requests = len(latencies)
    hit_known = sum(r["hit_known"] for r in results)
    client_cpu = [r["cpu_s"] / window_s for r in results]
    step: dict[str, Any] = {
        "concurrency": concurrency,
        "client_processes": n_procs,
        "window_s": window_s,
        "requests": requests,
        "req_per_s": round(requests / window_s, 1),
        "p50_ms": round(_percentile(latencies, 0.50) * 1000, 3) if latencies else None,
        "p99_ms": round(_percentile(latencies, 0.99) * 1000, 3) if latencies else None,
        "errors": sum(r["errors"] for r in results),
        "first_error": next((r["first_error"] for r in results if r["first_error"]), None),
        # None: this transport's response does not say whether it was served from the cache.
        "hit_ratio": round(sum(r["hits"] for r in results) / hit_known, 4) if hit_known else None,
        "client_cpu_cores": round(sum(client_cpu), 2),
        "client_cpu_max_core": round(max(client_cpu), 2),
        "client_limited": max(client_cpu) >= _CLIENT_SATURATED,
    }
    attempts = requests + step["errors"]  # a refused request cost the server CPU too
    if server_cpu0 is not None and server_cpu1 is not None and attempts:
        step["server_cpu_ms_per_request"] = round((server_cpu1 - server_cpu0) / attempts * 1000, 3)
        step["server_cpu_cores"] = round((server_cpu1 - server_cpu0) / window_s, 2)
    return step


def verify(transport: str, ep: Endpoints) -> dict:
    """The request's shape on this transport: rows and columns of one call."""
    client = CLIENTS[transport](ep)
    try:
        client.call()
        rows, cols, hit = client.call()
    finally:
        client.close()
    return {"rows": rows, "columns": cols, "second_request_hit": hit}


def run_transport(
    transport: str,
    ep: Endpoints,
    steps: tuple[int, ...],
    window_s: float,
    max_processes: int,
    server_pid: int | None,
) -> dict:
    carrier, opt_in = TRANSPORTS[transport]
    report: dict[str, Any] = {
        "transport": transport,
        "carrier": carrier,
        "cache_opt_in": opt_in,
        "cached": opt_in is not None,
        "note": NOTES.get(transport),
    }
    try:
        report["shape"] = verify(transport, ep)
    except Exception as exc:  # noqa: BLE001 - an unreachable transport is reported, not fatal
        report["error"] = _brief(exc)
        return report
    report["steps"] = [
        run_step(transport, ep, c, window_s, max_processes, server_pid) for c in steps
    ]
    return report


def summary_line(report: dict) -> str:
    name = report["transport"]
    label = "cached" if report["cached"] else "UNCACHED (no opt-in)"
    if "error" in report:
        return f"optimistic {name:15s} {label}: UNAVAILABLE — {report['error']}"
    shape = report["shape"]
    parts = []
    for step in report["steps"]:
        if "error" in step:
            parts.append(f"c={step['concurrency']}: {step['error']}")
            continue
        text = f"c={step['concurrency']}: {step['req_per_s']:.0f}/s p50={step['p50_ms']}ms p99={step['p99_ms']}ms"
        if step["errors"]:
            text += f" errors={step['errors']} ({step['first_error']})"
        if step["hit_ratio"] is not None:
            text += f" hit={step['hit_ratio']:.2f}"
        if "server_cpu_ms_per_request" in step:
            text += f" srv_cpu={step['server_cpu_ms_per_request']}ms/req"
        text += f" client_cpu={step['client_cpu_cores']}"
        if step["client_limited"]:
            text += " CLIENT-LIMITED"
        parts.append(text)
    return (
        f"optimistic {name:15s} {label} rows={shape['rows']} cols={shape['columns']} | "
        + " | ".join(parts)
    )


def run_all(
    ep: Endpoints,
    output_dir: Path,
    transports: list[str],
    steps: tuple[int, ...],
    window_s: float,
    max_processes: int,
    server_pid: int | None,
) -> list[dict]:
    output_dir.mkdir(parents=True, exist_ok=True)
    reports = []
    for transport in transports:
        report = run_transport(transport, ep, steps, window_s, max_processes, server_pid)
        (output_dir / f"{transport}.json").write_text(json.dumps(report, indent=2))
        print(summary_line(report), flush=True)
        reports.append(report)
    ranked = sorted(
        (
            (r["steps"][0]["server_cpu_ms_per_request"], r["transport"])
            for r in reports
            if r.get("steps") and "server_cpu_ms_per_request" in r["steps"][0]
        ),
    )
    if ranked:
        first = steps[0]
        print(
            f"optimistic server CPU per request at c={first}: "
            + ", ".join(f"{name} {ms}ms" for ms, name in ranked),
            flush=True,
        )
    return reports


def add_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--optimistic-transports",
        default=",".join(TRANSPORTS),
        help="Comma-separated subset of: " + ", ".join(TRANSPORTS),
    )
    parser.add_argument(
        "--optimistic-steps",
        default=",".join(str(s) for s in DEFAULT_STEPS),
        help="Closed-loop concurrency steps",
    )
    parser.add_argument("--optimistic-window", type=float, default=20.0, help="Seconds per step")
    parser.add_argument(
        "--optimistic-processes",
        type=int,
        default=max(1, math.ceil((os.cpu_count() or 2) / 2)),
        help="Client processes per step (default: half the cores, leaving the rest to the server)",
    )
    parser.add_argument(
        "--server-pid",
        type=int,
        default=None,
        help="The server's process id (its children are included): reports server CPU per request",
    )


def run_from_args(args: argparse.Namespace, ep: Endpoints, output_dir: Path) -> list[dict]:
    transports = [t for t in args.optimistic_transports.split(",") if t]
    unknown = [t for t in transports if t not in TRANSPORTS]
    if unknown:
        raise SystemExit(f"unknown optimistic transport(s) {unknown}; known: {list(TRANSPORTS)}")
    steps = tuple(int(s) for s in args.optimistic_steps.split(","))
    return run_all(
        ep,
        output_dir,
        transports,
        steps,
        args.optimistic_window,
        args.optimistic_processes,
        args.server_pid,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--http-base-url", default="http://localhost:8001")
    parser.add_argument("--pgwire-host", default="localhost")
    parser.add_argument("--pgwire-port", type=int, default=5439)
    parser.add_argument("--bolt-host", default="localhost")
    parser.add_argument("--bolt-port", type=int, default=17687)
    parser.add_argument("--flight-host", default="localhost")
    parser.add_argument("--flight-port", type=int, default=8815)
    parser.add_argument("--grpc-host", default="localhost")
    parser.add_argument("--grpc-port", type=int, default=50051)
    parser.add_argument("--role", default="org_admin")
    parser.add_argument("--output-dir", default="results/optimistic")
    add_arguments(parser)
    args = parser.parse_args()
    ep = Endpoints(
        http_base_url=args.http_base_url,
        pgwire_host=args.pgwire_host,
        pgwire_port=args.pgwire_port,
        bolt_host=args.bolt_host,
        bolt_port=args.bolt_port,
        flight_host=args.flight_host,
        flight_port=args.flight_port,
        grpc_host=args.grpc_host,
        grpc_port=args.grpc_port,
        role=args.role,
    )
    run_from_args(args, ep, Path(args.output_dir))
    return 0


if __name__ == "__main__":
    sys.exit(main())
