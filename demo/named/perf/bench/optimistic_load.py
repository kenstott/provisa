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

    PROVISA_HTTP_BASE_URL=http://localhost:8001 python optimistic_load.py \\
        --setup setups/perf-stack.yaml --server-pid 1234 --output-dir results/optimistic
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
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import contract_model
import credential as credential_module
import lookup
import measurements
import replication
import request_mix
import request_render
import setup_contract
from queries import Query

# transport -> (what carries the request, the per-request cache opt-in it uses or None)
TRANSPORTS: dict[str, tuple[str, str | None]] = {
    "graphql": ("POST /data/graphql", "@cached"),
    "data_sql": ("POST /data/sql", "-- @provisa cache=true"),
    "cypher_http": ("POST /data/cypher", "// @provisa cache=true"),
    "pgwire": ("PostgreSQL wire protocol, prepared statement", "-- @provisa cache=true"),
    "flight_sql": ("Arrow Flight ticket, SQL", "-- @provisa cache=true"),
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
    "jsonapi": (
        "no per-request cache opt-in exists: uncached; meta.total is counted only for a request "
        "that sends page[total]=true (REQ-1197)"
    ),
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
    setup: contract_model.Setup  # the contract: knobs, tables and seed behind every request
    # What the deployment's auth requires (credential.py; the secret is read from the environment
    # variable the contract names); None when the contract says credentials mode none.
    credential: credential_module.Credential | None


# --------------------------------------------------------------------------------------------
# One client per connection. ``call(query, role)`` sends the request ``query`` carries for this
# transport as ``role`` and returns (rows, columns, cache_hit) — cache_hit is None when the
# transport's response does not say.
# --------------------------------------------------------------------------------------------


class _HttpClient:
    def __init__(self, ep: Endpoints) -> None:
        import httpx

        self._cred = ep.credential
        self._http = httpx.Client(base_url=ep.http_base_url, timeout=60)

    def _headers(self, role: str, extra: dict[str, str] | None = None) -> dict[str, str]:
        """The request's headers: its role, the credential (a fresh bearer token: a login's expires)
        and ``extra``."""
        auth = self._cred.auth_header() if self._cred else {}
        return {"X-Provisa-Role": role, **auth, **(extra or {})}

    @staticmethod
    def _hit(resp: Any) -> bool | None:
        status = resp.headers.get("x-provisa-cache")
        return None if status is None else status == "HIT"

    def close(self) -> None:
        self._http.close()


class GraphqlClient(_HttpClient):
    def call(self, query: Query, role: str) -> tuple[int, int, bool | None]:
        resp = self._http.post(
            "/data/graphql", json={"query": query.graphql}, headers=self._headers(role)
        )
        resp.raise_for_status()
        body = resp.json()
        if body.get("errors"):
            raise RuntimeError(str(body["errors"])[:300])
        (rows,) = body["data"].values()
        return len(rows), len(rows[0]) if rows else 0, self._hit(resp)


class DataSqlClient(_HttpClient):
    def call(self, query: Query, role: str) -> tuple[int, int, bool | None]:
        resp = self._http.post(
            "/data/sql", json={"sql": query.sql, "role": role}, headers=self._headers(role)
        )
        resp.raise_for_status()
        body = resp.json()
        (rows,) = body["data"].values()
        return len(rows), len(body["columns"]), self._hit(resp)


class CypherHttpClient(_HttpClient):
    def call(self, query: Query, role: str) -> tuple[int, int, bool | None]:
        resp = self._http.post(
            "/data/cypher",
            json={"query": query.cypher, "params": {}},
            headers=self._headers(role),
        )
        resp.raise_for_status()
        body = resp.json()
        # /data/cypher answers ``X-Provisa-Cache: MISS`` on every response, served from the cache
        # or not (cypher_router builds the header from no cache result), so the header says
        # nothing here.
        return len(body["rows"]), len(body["columns"]), None


class RestClient(_HttpClient):
    def call(self, query: Query, role: str) -> tuple[int, int, bool | None]:
        spec = query.rest
        assert spec is not None  # the generator only sends REST a table REST exposes
        resp = self._http.get(spec["path"], params=spec["params"], headers=self._headers(role))
        resp.raise_for_status()
        rows = resp.json()["data"]
        return len(rows), len(rows[0]) if rows else 0, self._hit(resp)


class JsonApiClient(_HttpClient):
    def call(self, query: Query, role: str) -> tuple[int, int, bool | None]:
        spec = query.jsonapi
        assert spec is not None  # the generator only sends JSON:API a table it exposes
        resp = self._http.get(
            spec["path"],
            params=spec["params"],
            headers=self._headers(role, {"Accept": "application/vnd.api+json"}),
        )
        resp.raise_for_status()
        rows = resp.json()["data"]
        return len(rows), len(rows[0]["attributes"]) if rows else 0, self._hit(resp)


class PgwireClient:
    """One connection per role, the statement prepared once and executed by Bind/Execute after
    that — what a driver does with a statement it sends repeatedly. A role's connection is opened
    the first time a request is sent as it."""

    def __init__(self, ep: Endpoints) -> None:
        import psycopg

        self._psycopg = psycopg
        self._ep = ep
        self._conns: dict[str, Any] = {}

    def _conn(self, role: str) -> Any:
        conn = self._conns.get(role)
        if conn is None:
            cred = self._ep.credential
            # the user and password when the deployment has auth; --demo perf runs auth.provider: none
            user, password = cred.basic(role) if cred else (role, "unused")
            conn = self._conns[role] = self._psycopg.connect(
                host=self._ep.pgwire_host,
                port=self._ep.pgwire_port,
                user=user,
                password=password,
                dbname="provisa",
                autocommit=True,
                prepare_threshold=0,
            )
        return conn

    def call(self, query: Query, role: str) -> tuple[int, int, bool | None]:
        cur = self._conn(role).execute(query.sql)
        rows = cur.fetchall()
        return len(rows), len(cur.description or ()), None

    def close(self) -> None:
        for conn in self._conns.values():
            conn.close()


class _FlightClient:
    @staticmethod
    def _statement(query: Query) -> str:
        raise NotImplementedError

    def __init__(self, ep: Endpoints) -> None:
        import pyarrow.flight as fl

        self._fl = fl
        self._cred = ep.credential
        self._client = fl.FlightClient(f"grpc://{ep.flight_host}:{ep.flight_port}")

    def call(self, query: Query, role: str) -> tuple[int, int, bool | None]:
        # the server reads the credential from the ticket (flight/server.py do_get: request["token"])
        body: dict[str, Any] = {"query": self._statement(query), "role": role}
        if self._cred:
            body["token"] = self._cred.bearer()
        table = self._client.do_get(self._fl.Ticket(json.dumps(body).encode())).read_all()
        return table.num_rows, table.num_columns, None

    def close(self) -> None:
        self._client.close()


class FlightSqlClient(_FlightClient):
    @staticmethod
    def _statement(query: Query) -> str:
        assert query.sql is not None
        return query.sql


class BoltClient:
    """One driver and session per role, opened the first time a request is sent as it."""

    def __init__(self, ep: Endpoints) -> None:
        from neo4j import GraphDatabase

        self._graph = GraphDatabase
        self._ep = ep
        self._sessions: dict[str, tuple[Any, Any]] = {}

    def _session(self, role: str) -> Any:
        known = self._sessions.get(role)
        if known is None:
            cred = self._ep.credential
            if cred is not None and cred.kind == "token":
                import neo4j

                auth: Any = neo4j.bearer_auth(cred.secret)
            else:
                auth = cred.basic(role) if cred else (role, "unused")
            driver = self._graph.driver(
                f"bolt://{self._ep.bolt_host}:{self._ep.bolt_port}", auth=auth
            )
            known = self._sessions[role] = (driver, driver.session())
        return known[1]

    def call(self, query: Query, role: str) -> tuple[int, int, bool | None]:
        records = list(self._session(role).run(query.cypher))
        return len(records), len(records[0].keys()) if records else 0, None

    def close(self) -> None:
        for driver, session in self._sessions.values():
            session.close()
            driver.close()


_grpc_lock = threading.Lock()
_grpc_shared: Any = None  # one compiled descriptor pool + channel per client PROCESS


class GrpcClient:
    def __init__(self, ep: Endpoints) -> None:
        global _grpc_shared
        from run_benchmark import GrpcTransport

        with _grpc_lock:
            if _grpc_shared is None:
                transport = GrpcTransport(
                    ep.grpc_host,
                    ep.grpc_port,
                    ep.http_base_url,
                    ep.role,
                    ep.credential.auth_header() if ep.credential else None,
                )
                if not transport.available():
                    raise RuntimeError(f"gRPC unavailable: {getattr(transport, '_error', '')}")
                _grpc_shared = transport
        transport = _grpc_shared
        self._transport = transport
        self._cred = ep.credential
        self._types: dict[str, tuple[Any, Any]] = {}
        # a built request per distinct spec: the message is immutable once sent
        self._built: dict[Any, tuple[Any, list]] = {}

    def _stream_for(self, type_name: str) -> tuple[Any, Any]:
        """(request class, stream callable) of one message type, compiled once per client."""
        known = self._types.get(type_name)
        if known is None:
            transport = self._transport
            request_cls = transport._resolve_message_class(f"provisa.v1.{type_name}Request")  # noqa: SLF001
            response_cls = transport._resolve_message_class(f"provisa.v1.{type_name}")  # noqa: SLF001
            stream = transport._channel.unary_stream(  # noqa: SLF001
                f"/provisa.v1.ProvisaService/Query{type_name}",
                request_serializer=request_cls.SerializeToString,
                response_deserializer=response_cls.FromString,
            )
            known = self._types[type_name] = (request_cls, stream)
        return known

    def call(self, query: Query, role: str) -> tuple[int, int, bool | None]:
        spec = query.grpc
        assert spec is not None
        request_cls, stream = self._stream_for(spec["type_name"])
        filters = spec.get("filter")
        key = (
            role,
            spec["type_name"],
            spec["limit"],
            tuple(spec["read_mask"]),
            tuple(spec["metadata"]),
            tuple(filters.items()) if filters else None,
        )
        built = self._built.get(key)
        if built is None:
            request = request_cls(limit=spec["limit"])
            request.read_mask.paths.extend(spec["read_mask"])
            if filters:
                filter_cls = self._transport._resolve_message_class(  # noqa: SLF001
                    f"provisa.v1.{spec['type_name']}Filter"
                )
                request.filter.CopyFrom(filter_cls(**filters))
            built = self._built[key] = (
                request,
                [("x-provisa-role", role), *spec["metadata"]],
            )
        request, base_metadata = built
        # the bearer token is added per call, not cached with the request: a login's token expires
        metadata = (
            [*base_metadata, ("authorization", f"Bearer {self._cred.bearer()}")]
            if self._cred
            else base_metadata
        )
        rows = list(stream(request, metadata=metadata))
        return len(rows), len(rows[0].ListFields()) if rows else 0, None

    def close(self) -> None:
        pass  # the channel is the process's, closed when the process exits


CLIENTS: dict[str, Any] = {
    "graphql": GraphqlClient,
    "data_sql": DataSqlClient,
    "cypher_http": CypherHttpClient,
    "pgwire": PgwireClient,
    "flight_sql": FlightSqlClient,
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


# One answered request: (latency_s, the request spec, hit or None, rows returned, columns returned).
# hit is None when the transport's response does not say.
Record = tuple[float, request_mix.RequestSpec, bool | None, int, int]


def replication_report(setup: contract_model.Setup, transport: str) -> dict[str, dict[str, Any]]:
    """Per source, what the run was measured under: the replication setting and its TTL, and the
    route mode the transport asked for."""
    return {
        sid: {
            "replication": src.replication.setting,
            "ttl_seconds": src.replication.ttl_seconds,
            "route": setup.route_mode(transport, sid).mode,
            "federated_probability": setup.route_mode(transport, sid).federated_probability,
        }
        for sid, src in setup.sources.items()
    }


def knob_means(records: list[Record]) -> dict[str, float] | None:
    """The mean of each knob over the answered requests: what the step's mix actually was (the
    cost model regresses CPU per request on these)."""
    if not records:
        return None
    n = len(records)

    def mean(value: Any) -> float:
        return round(sum(value(r[1]) for r in records) / n, 4)

    return {
        "fields": mean(lambda s: len(s.columns)),
        "filters": mean(lambda s: len(s.filters)),
        "rows": mean(lambda s: s.rows),
        "joins_same": mean(lambda s: sum(1 for j in s.joins if j.kind == "same")),
        "joins_cross": mean(lambda s: sum(1 for j in s.joins if j.kind == "cross")),
        "cache_opt_in": mean(lambda s: s.cached),
        "repeat": mean(lambda s: bool(s.repeat)),
        "federated": mean(lambda s: s.route == "federated"),
        "direct": mean(lambda s: s.route == "direct"),
    }


def summarize_requests(records: list[Record], window_s: float) -> dict[str, Any]:
    """Throughput and latency of the answered requests, overall and split by what the knobs
    varied: whether the request opted into the response cache, hit or miss (where the transport's
    response says), and the number of selected fields."""

    def stats(group: list[Record]) -> dict[str, Any]:
        latencies = sorted(r[0] for r in group)
        return {
            "requests": len(latencies),
            "req_per_s": round(len(latencies) / window_s, 1),
            "p50_ms": round(_percentile(latencies, 0.50) * 1000, 3) if latencies else None,
            "p99_ms": round(_percentile(latencies, 0.99) * 1000, 3) if latencies else None,
        }

    def hit_ratio(group: list[Record]) -> float | None:
        known_in_group = [r for r in group if r[2] is not None]
        if not known_in_group:
            return None
        return round(sum(1 for r in known_in_group if r[2]) / len(known_in_group), 4)

    def grouped(
        key: Any, *, returned_rows: bool = False, hits: bool = False
    ) -> dict[str, dict[str, Any]]:
        out: dict[str, dict[str, Any]] = {}
        for k in sorted({key(r) for r in records}):
            group = [r for r in records if key(r) == k]
            out[str(k)] = stats(group)
            if returned_rows:
                out[str(k)]["avg_rows_returned"] = round(sum(r[3] for r in group) / len(group), 2)
            if hits:
                out[str(k)]["hit_ratio"] = hit_ratio(group)
        return out

    known = [r for r in records if r[2] is not None]
    return {
        **stats(records),
        # None: this transport's response does not say whether it was served from the cache.
        "hit_ratio": round(sum(1 for r in known if r[2]) / len(known), 4) if known else None,
        "cache_opt_in_ratio": (
            round(sum(1 for r in records if r[1].cached) / len(records), 4) if records else None
        ),
        "by_opt_in": {
            "cached": stats([r for r in records if r[1].cached]),
            "uncached": stats([r for r in records if not r[1].cached]),
        },
        "by_outcome": (
            {
                "hit": stats([r for r in known if r[2]]),
                "miss": stats([r for r in known if not r[2]]),
            }
            if known
            else None
        ),
        # Latency by the number of fields the request selected, with the columns the transport
        # actually returned (a transport that does not apply the selection returns them all).
        # Fresh vs repeated requests (cache repetition), with the hit ratio of each where the
        # transport's response says: the cache effect, kept apart from the other knobs.
        "repeat_ratio": (
            round(sum(1 for r in records if r[1].repeat) / len(records), 4) if records else None
        ),
        "by_repeat": grouped(lambda r: "repeat" if r[1].repeat else "fresh", hits=True),
        "knob_means": knob_means(records),
        "by_source": grouped(lambda r: r[1].source),
        "by_role": grouped(lambda r: r[1].role),
        # Latency by the route hint the request carried: auto (none), direct, or federated.
        "by_route": grouped(lambda r: r[1].route or "auto"),
        # Latency by the number of joins, same-source and cross-source reported separately.
        "by_joins_same": grouped(lambda r: sum(1 for j in r[1].joins if j.kind == "same")),
        "by_joins_cross": grouped(lambda r: sum(1 for j in r[1].joins if j.kind == "cross")),
        "by_fields": grouped(lambda r: len(r[1].columns)),
        # Latency by the number of equality filters the request carried.
        "by_filters": grouped(lambda r: len(r[1].filters)),
        # Latency by the number of rows asked for, with the rows that came back (fewer than asked
        # when the table or the filter ran out).
        "by_rows": grouped(lambda r: r[1].rows, returned_rows=True),
        "avg_columns_returned": (
            round(sum(r[4] for r in records) / len(records), 2) if records else None
        ),
    }


def _warmup_specs(
    gen: request_mix.RequestGenerator,
    setup: contract_model.Setup,
    transport: str,
    opt_in: bool,
) -> list[request_mix.RequestSpec]:
    """The base request in every cache variant and as every role a client can send: with the
    opt-in if any request draws it, without it if any does not (a transport with no opt-in sends
    only the plain one)."""
    p_cached = setup.cache_probability(transport)
    variants = ([True] if p_cached > 0 and opt_in else []) + (
        [False] if p_cached < 1 or not opt_in else []
    )
    return [
        gen.base_spec(cached, r.role) for r in setup.roles if r.weight > 0 for cached in variants
    ]


def open_loop_schedule(
    start: float, interval: float, phase: float, stop_at: float
) -> Iterator[float]:
    """The times one client sends at: every ``interval`` from ``start``, shifted by ``phase`` of an
    interval, until ``stop_at``. A function of time only: the server's speed does not move it."""
    k = 0
    while (t := start + (phase + k) * interval) < stop_at:  # not accumulated: no drift
        yield t
        k += 1


def open_loop_phase(client: int, clients: int) -> float:
    """Where in the interval client number ``client`` of ``clients`` starts, spreading the clients
    so they do not all send at once."""
    return (client % clients) / clients


def open_loop_fields(
    *, rate_per_s: float, requests: int, window_s: float, max_lag_s: float
) -> dict[str, Any]:
    return {
        "load_mode": "open_loop",
        "target_rate_per_s": rate_per_s,
        "achieved_rate_per_s": round(requests / window_s, 1),
        "schedule_lag_ms_max": round(max_lag_s * 1000, 3),
    }


def _client_process(
    transport: str,
    ep: Endpoints,
    index: int,
    threads: int,
    window_s: float,
    ready: Any,
    go: Any,
    out: Any,
) -> None:
    """One client process: ``threads`` closed-loop clients, each on its own connection."""
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    records: list[list[Record]] = [[] for _ in range(threads)]
    opt_in = TRANSPORTS[transport][1] is not None
    renderer = request_render.Renderer(ep.setup)
    errors = [0] * threads
    first_error: list[str] = []
    open_loop = ep.setup.load.mode == "open_loop"
    lag = [0.0] * threads  # open loop: the most any request was sent after its scheduled time
    connected = threading.Barrier(threads + 1)

    def _loop(i: int) -> None:
        try:
            client = CLIENTS[transport](ep)
        except Exception as exc:  # noqa: BLE001 - reported in the step's result
            first_error.append(f"connect: {_brief(exc)}")
            connected.abort()
            return
        gen = request_mix.RequestGenerator(
            ep.setup, transport, f"{transport}/{index}/{i}", cacheable=opt_in
        )
        try:
            # the connection is open and the statement is prepared / cached: the request no knob
            # applies to, with the opt-in if any request carries it
            for warm in _warmup_specs(gen, ep.setup, transport, opt_in):
                client.call(renderer.render(warm), warm.role)
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
        mine = records[i]
        if open_loop:
            clients = ep.setup.load.steps[0]
            rate = ep.setup.load.rate_per_s
            assert rate is not None  # open_loop always carries a rate
            interval = clients / rate
            for scheduled in open_loop_schedule(
                time.perf_counter(),
                interval,
                open_loop_phase(index * threads + i, clients),
                stop_at,
            ):
                spec = gen.next()
                query = renderer.render(spec)
                wait = scheduled - time.perf_counter()
                if wait > 0:
                    time.sleep(wait)
                sent = time.perf_counter()
                lag[i] = max(lag[i], sent - scheduled)
                try:
                    rows, cols, hit = client.call(query, spec.role)
                except Exception as exc:  # noqa: BLE001 - counted, first one kept
                    errors[i] += 1
                    if not first_error:
                        first_error.append(_brief(exc))
                    continue
                # an open loop's latency runs from the scheduled arrival: the wait behind a slow
                # answer is part of what a request that arrived on time experienced
                mine.append((time.perf_counter() - scheduled, spec, hit, rows, cols))
            client.close()
            return
        while True:
            spec = gen.next()
            query = renderer.render(spec)  # drawn and rendered before the clock starts
            t0 = time.perf_counter()
            if t0 >= stop_at:
                break
            try:
                rows, cols, hit = client.call(query, spec.role)
            except Exception as exc:  # noqa: BLE001 - counted, first one kept
                errors[i] += 1
                if not first_error:
                    first_error.append(_brief(exc))
                continue
            mine.append((time.perf_counter() - t0, spec, hit, rows, cols))
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
            "records": [x for per in records for x in per],
            "errors": sum(errors),
            "first_error": first_error[0] if first_error else None,
            "cpu_s": _cpu_seconds() - cpu0,
            "max_lag_s": max(lag) if open_loop else None,
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
        ctx.Process(target=_client_process, args=(transport, ep, i, n, window_s, ready, go, out))
        for i, n in enumerate(per_proc)
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
    monitored = {sid: s.container for sid, s in ep.setup.sources.items() if s.container}
    sampler = measurements.DockerStatsSampler(monitored) if monitored else None
    if sampler is not None:
        sampler.start()
    go.set()
    results = [out.get(timeout=window_s + 120) for _ in procs]
    server_cpu1 = _server_cpu_seconds(server_pid)
    if sampler is not None:
        sampler.stop()
    for p in procs:
        p.join(30)
    all_records = [x for r in results for x in r["records"]]
    summary = summarize_requests(all_records, window_s)
    requests = summary["requests"]
    client_cpu = [r["cpu_s"] / window_s for r in results]
    step: dict[str, Any] = {
        "concurrency": concurrency,
        "client_processes": n_procs,
        "window_s": window_s,
        **summary,
        "errors": sum(r["errors"] for r in results),
        "first_error": next((r["first_error"] for r in results if r["first_error"]), None),
        "client_cpu_cores": round(sum(client_cpu), 2),
        "client_cpu_max_core": round(max(client_cpu), 2),
        "client_limited": max(client_cpu) >= _CLIENT_SATURATED,
        # what the step sent, by the table read and the route its replication setting implies: the
        # data the route verification compares with the audit log
        "route_classes": replication.classes_of(ep.setup, [r[1] for r in all_records]),
    }
    attempts = requests + step["errors"]  # a refused request cost the server CPU too
    if server_cpu0 is not None and server_cpu1 is not None and attempts:
        step["server_cpu_ms_per_request"] = round((server_cpu1 - server_cpu0) / attempts * 1000, 3)
        step["server_cpu_cores"] = round((server_cpu1 - server_cpu0) / window_s, 2)
    if sampler is not None:
        step.update(with_source_cpu(sampler.result(), summary, window_s, attempts))
    if ep.setup.load.mode == "open_loop":
        assert ep.setup.load.rate_per_s is not None
        step.update(
            open_loop_fields(
                rate_per_s=ep.setup.load.rate_per_s,
                requests=requests,
                window_s=window_s,
                max_lag_s=max(r["max_lag_s"] for r in results),
            )
        )
    return step


def with_source_cpu(
    sampled: measurements.SampleResult, summary: dict[str, Any], window_s: float, attempts: int
) -> dict[str, Any]:
    """The source-CPU fields of a step: per source, from the sampled containers."""
    by_source = {sid: g["requests"] for sid, g in summary["by_source"].items()}
    fields = measurements.source_cpu_fields(
        sampled.cores, window_s=window_s, attempts=attempts, requests_by_source=by_source
    )
    fields["source_cpu_samples"] = sampled.samples
    fields["source_cpu_error"] = sampled.error  # a failing `docker stats` is reported, not hidden
    return fields


def verify(transport: str, ep: Endpoints) -> dict:
    """The request's shape on this transport: rows and columns of one call."""
    client = CLIENTS[transport](ep)
    try:
        query = request_render.build_query(ep.setup, transport, cached=True)
        client.call(query, ep.role)
        rows, cols, hit = client.call(query, ep.role)
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
    audit: Any = None,
) -> dict:
    """One transport's report. ``audit`` (``replication.audit_reader``): every step is checked
    against the route the audit log recorded for its requests, and a step served by a route other
    than its declared replication implies raises ``replication.RouteMismatch``; None: not checked,
    and the report says so."""
    carrier, opt_in = TRANSPORTS[transport]
    report: dict[str, Any] = {
        "transport": transport,
        "carrier": carrier,
        "cache_opt_in": opt_in,
        "cached": opt_in is not None,
        # Share of requests that carry the opt-in (REQ-1911 knob `cache`); None: no opt-in exists.
        "cache_probability": ep.setup.cache_probability(transport) if opt_in is not None else None,
        "note": NOTES.get(transport),
        "replication": replication_report(ep.setup, transport),
    }
    try:
        report["shape"] = verify(transport, ep)
    except Exception as exc:  # noqa: BLE001 - an unreachable transport is reported, not fatal
        report["error"] = _brief(exc)
        return report
    report["steps"] = []
    for c in steps:
        baseline = audit.baseline() if audit is not None else 0
        step = run_step(transport, ep, c, window_s, max_processes, server_pid)
        step["route_verification"] = (
            replication.verify_routes(ep.setup, step["route_classes"], audit, baseline)
            if audit is not None and "route_classes" in step
            else None
        )
        report["steps"].append(step)
    report["route_verification"] = (
        "verified" if audit is not None else "skipped (--skip-route-verification)"
    )
    # Whether this transport's responses say hit or miss (hit_ratio / by_outcome are null if not).
    report["hit_observable"] = any(s.get("hit_ratio") is not None for s in report["steps"])
    report["time_split"] = None
    if ep.setup.load.profile_requests and transport in measurements.PROFILE_TRANSPORTS:
        try:
            import httpx

            headers = {
                "X-Provisa-Role": ep.role,
                **(ep.credential.auth_header() if ep.credential else {}),
            }
            with httpx.Client(base_url=ep.http_base_url, timeout=60, headers=headers) as client:
                report["time_split"] = measurements.profile_transport(
                    ep.setup,
                    transport,
                    client,
                    role=ep.role,
                    requests=ep.setup.load.profile_requests,
                )
        except Exception as exc:  # noqa: BLE001 - reported with the transport, like a failed step
            report["time_split_error"] = _brief(exc)
    return report


def summary_line(report: dict) -> str:
    name = report["transport"]
    label = f"cache={report['cache_probability']:g}" if report["cached"] else "UNCACHED (no opt-in)"
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
        if report["cached"] and report["cache_probability"] not in (0, 1):
            split = step["by_outcome"] or step["by_opt_in"]
            text += " " + " ".join(
                f"{k}={v['req_per_s']:.0f}/s p50={v['p50_ms']}ms" for k, v in split.items()
            )
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
    audit: Any = None,
) -> list[dict]:
    output_dir.mkdir(parents=True, exist_ok=True)
    if ep.setup.load.idle_window_s and server_pid is None:
        raise contract_model.SetupError("load.idle_window_s needs --server-pid")
    reports = []
    for transport in transports:
        report = run_transport(transport, ep, steps, window_s, max_processes, server_pid, audit)
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
    if ep.setup.load.idle_window_s:
        monitored = {sid: s.container for sid, s in ep.setup.sources.items() if s.container}
        sampler = measurements.DockerStatsSampler(monitored)

        def server_cpu() -> float:
            cpu = _server_cpu_seconds(server_pid)
            assert cpu is not None  # server_pid was required above
            return cpu

        idle = measurements.measure_idle(
            window_s=ep.setup.load.idle_window_s,
            server_cpu=server_cpu,
            sampler=sampler,
            sleep=lambda seconds: measurements.sample_during(sampler, seconds),
        )
        (output_dir / "idle.json").write_text(json.dumps(idle, indent=2))
        print(
            f"idle CPU over {idle['window_s']}s: server {idle['server_cpu_cores']} cores",
            flush=True,
        )
    return reports


# Flags the setup contract owns: a run with --setup refuses them rather than letting a second
# source disagree with the contract (matched as an argparse abbreviation too).
_CONTRACT_OWNED_FLAGS = (
    "--http-base-url",
    "--pgwire-host",
    "--pgwire-port",
    "--bolt-host",
    "--bolt-port",
    "--flight-host",
    "--flight-port",
    "--grpc-host",
    "--grpc-port",
    "--role",
    "--bypass-relationship-guard",
    "--optimistic-transports",
    "--optimistic-steps",
    "--optimistic-window",
    "--optimistic-processes",
)


def add_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--setup",
        default=None,
        help="Setup data contract (setups/*.yaml, REQ-1911): the deployment, the request and the "
        "load. Required for an optimistic run.",
    )
    parser.add_argument(
        "--resolved-names",
        default=None,
        help="Replay the resolved-names.json an earlier run wrote instead of asking the deployment "
        "(an offline or repeat run). Without it the run asks the deployment what each transport "
        "calls the contract's tables and writes resolved-names.json next to the results.",
    )
    parser.add_argument(
        "--apply-replication",
        action="store_true",
        help="Set the replication the contract declares through the admin API before the run and "
        "restore the previous values afterwards (also if the run fails). Without it a deployment "
        "that is not as declared is refused, never changed.",
    )
    parser.add_argument(
        "--skip-route-verification",
        action="store_true",
        help="Do not check the audit log for the route that served each step. The report says the "
        "routes are unverified.",
    )
    parser.add_argument(
        "--server-pid",
        type=int,
        default=None,
        help="The server's process id (its children are included): reports server CPU per request",
    )


def _credential(setup: contract_model.Setup) -> credential_module.Credential | None:
    """The credential the contract's credentials block describes, its secret read from the named
    variable (None: credentials mode none). setup_contract checked the variable is set."""
    c = setup.credentials
    if c.mode == "none":
        return None
    assert c.kind is not None and c.env is not None
    return credential_module.Credential(
        kind=c.kind,
        user=c.user,
        secret=os.environ[c.env],
        base_url=setup.endpoints.http_base_url,
    )


def resolve_names(
    args: argparse.Namespace, unbound: contract_model.Setup, output_dir: Path
) -> lookup.Resolved:
    """What the deployment calls the contract's tables: replayed from ``--resolved-names``, else
    asked of the deployment (every endpoint ``lookup.py`` lists). The answer is written to
    ``resolved-names.json`` in ``output_dir`` either way, and the raw answers to
    ``lookup-responses.json`` when asked."""
    output_dir.mkdir(parents=True, exist_ok=True)
    if args.resolved_names:
        resolved = lookup.read_resolved(Path(args.resolved_names))
        print(f"names: replayed from {args.resolved_names}", flush=True)
    else:
        import httpx

        credential = _credential(unbound)
        headers = credential.auth_header() if credential else {}
        role = next(r.role for r in unbound.roles if r.weight > 0)
        try:
            with httpx.Client(
                base_url=unbound.endpoints.http_base_url, timeout=120, headers=headers
            ) as client:
                raw = lookup.fetch(client, role)
            resolved = lookup.resolve(*setup_contract.identities(unbound), raw)
        except lookup.LookupError_ as exc:
            raise contract_model.SetupError(str(exc)) from exc
        (output_dir / "lookup-responses.json").write_text(lookup.raw_to_json(raw))
        print(f"names: asked {unbound.endpoints.http_base_url} as {role}", flush=True)
    lookup.write_resolved(resolved, output_dir / "resolved-names.json")
    return resolved


def endpoints_from_setup(setup: contract_model.Setup) -> Endpoints:
    credential = _credential(setup)
    e = setup.endpoints
    return Endpoints(
        http_base_url=e.http_base_url,
        pgwire_host=e.pgwire.host,
        pgwire_port=e.pgwire.port,
        bolt_host=e.bolt.host,
        bolt_port=e.bolt.port,
        flight_host=e.flight.host,
        flight_port=e.flight.port,
        grpc_host=e.grpc.host,
        grpc_port=e.grpc.port,
        role=setup.roles[0].role,
        setup=setup,
        credential=credential,
    )


def _refuse_contract_owned_flags(argv: list[str]) -> None:
    for token in argv:
        if not token.startswith("--"):
            continue
        name = token.split("=", 1)[0]
        # An exact flag, or an abbreviation of exactly one (argparse accepts an unambiguous
        # prefix; `--optimistic` is a prefix of four of these and is itself a different flag).
        if name in _CONTRACT_OWNED_FLAGS or (
            sum(flag.startswith(name) for flag in _CONTRACT_OWNED_FLAGS) == 1
        ):
            raise contract_model.SetupError(
                f"{name} is set by the setup contract; a run with --setup does not take it"
            )


def run_from_args(
    args: argparse.Namespace, output_dir: Path, argv: list[str] | None = None
) -> list[dict]:
    if args.setup is None:
        raise contract_model.SetupError("--setup is required for an optimistic run")
    _refuse_contract_owned_flags(sys.argv[1:] if argv is None else argv[1:])
    unbound = setup_contract.load_setup(args.setup, known_transports=list(TRANSPORTS))
    resolved = resolve_names(args, unbound, output_dir)
    # the replication check is made here, with the flag in hand, not while binding
    setup = setup_contract.load_setup(
        args.setup, known_transports=list(TRANSPORTS), deployment=resolved, verify_replication=False
    )
    for warning in replication.require_declared_or_apply(
        setup,
        resolved,
        apply=args.apply_replication,
        routes_verified=not args.skip_route_verification,
    ):
        print(f"replication WARNING (the audit log decides): {warning}", flush=True)
    processes = setup.load.processes or max(1, math.ceil((os.cpu_count() or 2) / 2))
    ep = endpoints_from_setup(setup)
    audit = None
    if args.skip_route_verification:
        print("routes: NOT verified (--skip-route-verification)", flush=True)
    else:
        import psycopg

        user, password = ep.credential.basic(ep.role) if ep.credential else (ep.role, "unused")
        conn = psycopg.connect(
            host=ep.pgwire_host,
            port=ep.pgwire_port,
            user=user,
            password=password,
            dbname="provisa",
            autocommit=True,
        )
        domains = sorted({d for d in resolved_domains(setup, resolved)})
        audit = replication.audit_reader(conn, domains)

    def run() -> list[dict]:
        return run_all(
            ep,
            output_dir,
            list(setup.transports),
            setup.load.steps,
            setup.load.window_s,
            processes,
            args.server_pid,
            audit=audit,
        )

    if not args.apply_replication:
        return run()
    import httpx

    headers = ep.credential.auth_header() if ep.credential else {}
    with httpx.Client(base_url=ep.http_base_url, timeout=120, headers=headers) as client:
        with replication.AdminReplication(client, ep.role).applied(setup, resolved):
            print("replication: set as declared for this run (restored afterwards)", flush=True)
            return run()


def resolved_domains(setup: contract_model.Setup, resolved: lookup.Resolved) -> list[str]:
    """The registered domains of the contract's tables (what the audit log is filtered by)."""
    return [
        d
        for sid, src in setup.sources.items()
        for t in src.tables
        if (d := resolved.tables[setup_contract.table_identity(setup, sid, t).key].domain_id)
    ]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="The optimistic request on every transport, closed or open loop, from a setup contract"
    )
    parser.add_argument("--output-dir", default="results/optimistic")
    add_arguments(parser)
    # run_benchmark.py's `--setup` is optional (it also runs the query matrix); here it is not.
    parser.set_defaults(setup=None)
    for action in parser._actions:  # noqa: SLF001 - make --setup required for this entry point
        if "--setup" in action.option_strings:
            action.required = True
    args = parser.parse_args()
    try:
        run_from_args(args, Path(args.output_dir))
    except contract_model.SetupError as exc:
        raise SystemExit(f"setup contract: {exc}") from exc
    return 0


if __name__ == "__main__":
    sys.exit(main())
