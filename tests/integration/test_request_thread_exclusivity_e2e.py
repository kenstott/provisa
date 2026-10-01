# Copyright (c) 2026 Kenneth Stott
# Canary: 8a3f6d21-0c7e-4b94-b5d2-7e1c4a9f3b60
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1882 (amended 2026-09-29): every request runs entirely on its own thread — on a real server.

One real server process, started exactly as deployed (``main:app``, uvloop) with the test-only
thread tracer (``tests.thread_trace``): DuckDB engine over a SQLite source, with pgwire, Bolt,
Arrow Flight, gRPC, HTTP and MCP listening. Each request carries a ``91NNNNN`` tag in its query;
the tracer records the OS thread of every pipeline stage that sees the tag — transport dispatch,
governance, routing, engine execution, result encode and send — and every hand-off to another
thread or loop.

Per transport:

- one request: every stage ran on ONE thread, that thread is one a transport handed the request
  to (never the ASGI front loop's), and nothing was handed to another thread — except the HTTP
  response relay to the front loop, where the server's socket lives;
- concurrent requests: each one stays on one thread, and requests that overlap in time are on
  different threads.

A landed (``prefer_materialized``) source on a DuckDB-file store is read through a land the
request itself triggers; that land must run on the request's thread too.
"""

# Requirements: REQ-1882

from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import threading
import time
from pathlib import Path

import pytest
import yaml

pytestmark = [pytest.mark.integration]

_REPO = Path(__file__).resolve().parents[2]
_ROLE = "org_admin"
_ORG = "thread_exclusive_e2e"
_ROWS = 5_000
_CONCURRENT = 6

# Thread starts and hand-offs a request may make without its work leaving its thread:
# - the ASGI receive/send relay to the front loop, where the server's socket transport lives
#   (provisa/core/request_thread.py);
# - the request's deadline watchdog, a timer thread that runs none of the request's work — it only
#   calls the in-flight statement's cancel at expiry (provisa/core/request_deadline.py).
# - work that OUTLIVES the request, detached onto the background pool and never awaited by it
#   (provisa/core/connection_loop.py spawn_background/spawn_after: a hot-cache promote, a TTL drop).
# Each entry: (file, function) of the frame that makes the hand-off.
_SANCTIONED_HOPS = (
    ("provisa/core/request_thread.py", "thread_send"),
    ("provisa/core/request_thread.py", "thread_receive"),
    ("provisa/core/request_deadline.py", "__init__"),
    ("provisa/core/connection_loop.py", "_submit"),
)


def _seed(db: Path, rows: int) -> None:
    con = sqlite3.connect(db)
    try:
        con.execute("CREATE TABLE events (id INTEGER, amount TEXT, ts TEXT)")
        con.executemany(
            "INSERT INTO events VALUES (?, ?, ?)",
            ((i, f"{i}.50", "2026-01-02 03:04:05") for i in range(1, rows + 1)),
        )
        con.commit()
    finally:
        con.close()


class _Server:
    """The isolated server plus its trace file."""

    def __init__(self, srv, trace: Path, mcp_port: int) -> None:
        self.srv = srv
        self.trace = trace
        self.mcp_port = mcp_port

    def records(self) -> list[dict]:
        return [json.loads(line) for line in self.trace.read_text().splitlines() if line]

    def tagged(self, tag: int) -> list[dict]:
        return [r for r in self.records() if r.get("tag") == str(tag)]


def _start(work: Path, org: str, source_extra: dict, *, store_url: str | None = None) -> _Server:
    from tests.integration.isolated_server import IsolatedServer, free_port

    db = work / "events.sqlite"
    _seed(db, _ROWS)
    with open(_REPO / "tests/fixtures/sample_config.yaml") as f:
        base = yaml.safe_load(f)
    cfg: dict = {"naming": base["naming"], "roles": base["roles"], "relationships": []}
    cfg["sources"] = [{"id": "tx-sqlite", "type": "sqlite", "path": str(db), **source_extra}]
    cfg["domains"] = [{"id": "thread-x", "description": "request-thread exclusivity e2e"}]
    cfg["tables"] = [
        {
            "source_id": "tx-sqlite",
            "domain_id": "thread-x",
            "schema": "default",
            "table": "events",
            "columns": [
                {"name": n, "data_type": t, "visible_to": [_ROLE]}
                for n, t in (("id", "integer"), ("amount", "varchar"), ("ts", "varchar"))
            ],
        }
    ]
    cfg_path = work / "config.yaml"
    cfg_path.write_text(yaml.safe_dump(cfg))
    trace = work / "threads.jsonl"
    mcp_port = free_port()
    overrides = {
        "PROVISA_TEST_THREAD_TRACE": str(trace),
        "PROVISA_MCP_PORT": str(mcp_port),
        "PROVISA_MCP_HOST": "127.0.0.1",
        "PROVISA_MCP_ROLE": _ROLE,
        # Room for the concurrent cases: the default Flight ceiling is below _CONCURRENT, and the
        # default inline-result threshold is below _ROWS.
        "FLIGHT_MAX_CONCURRENT_STREAMS": "64",
        "PROVISA_REDIRECT_THRESHOLD": "1000000",
    }
    saved = {k: os.environ.get(k) for k in overrides}
    os.environ.update(overrides)
    try:
        srv = IsolatedServer(
            org,
            engine="duckdb",
            enable_pgwire=True,
            enable_bolt=True,
            await_flight=True,
            await_grpc=True,
            config=str(cfg_path),
            control_plane="postgres",
            materialize_store_url=store_url,
            app="tests.integration.thread_trace_app:app",
        )
        srv.start(timeout=240)
    finally:
        for k, v in saved.items():
            if v is None:
                del os.environ[k]
            else:
                os.environ[k] = v
    return _Server(srv, trace, mcp_port)


def _stop(server: _Server, org: str) -> None:
    from tests.integration.isolated_server import drop_org_schema

    server.srv.stop_process()
    asyncio.run(drop_org_schema(org))


@pytest.fixture(scope="module")
def server(tmp_path_factory):
    s = _start(tmp_path_factory.mktemp("threadx"), _ORG, {})
    try:
        yield s
    finally:
        _stop(s, _ORG)


@pytest.fixture(scope="module")
def landed_server(tmp_path_factory):
    """The source must be read from its landed copy, held in a DuckDB-file store."""
    org = _ORG + "_landed"
    work = tmp_path_factory.mktemp("threadx_landed")
    s = _start(
        work,
        org,
        {"prefer_materialized": True, "cache_ttl": 1},
        store_url=f"duckdb:///{work / 'materialize.duckdb'}",
    )
    try:
        yield s
    finally:
        _stop(s, org)


# -- clients (one request each; the tag rides in the query) ------------------------------------


def _sql(tag: int) -> str:
    return f"SELECT id, amount, ts FROM thread_x.events WHERE id <> {tag}"


def _pgwire(server: _Server, tag: int) -> int:
    import psycopg2

    conn = psycopg2.connect(
        host="127.0.0.1",
        port=server.srv.pgwire_port,
        dbname="provisa",
        user=_ROLE,
        password="provisa",
    )
    try:
        cur = conn.cursor()
        cur.execute(_sql(tag))
        return len(cur.fetchall())
    finally:
        conn.close()


def _flight(server: _Server, query: str) -> int:
    import pyarrow.flight as flight

    client = flight.FlightClient(f"grpc://127.0.0.1:{server.srv.flight_port}")
    try:
        ticket = flight.Ticket(json.dumps({"query": query, "role": _ROLE}).encode())
        return client.do_get(ticket).read_all().num_rows
    finally:
        client.close()


def _flight_sql(server: _Server, tag: int) -> int:
    return _flight(server, _sql(tag))


def _gql(tag: int) -> str:
    return f"query {{ tx__events(where: {{id: {{lt: {tag}}}}}) {{ id amount ts }} }}"


def _cypher(tag: int) -> str:
    return f"MATCH (e:events) WHERE e.id < {tag} RETURN e.id AS id, e.amount AS amount"


def _flight_graphql(server: _Server, tag: int) -> int:
    return _flight(server, _gql(tag))


def _flight_cypher(server: _Server, tag: int) -> int:
    return _flight(server, _cypher(tag))


def _http_graphql(server: _Server, tag: int) -> int:
    import httpx

    resp = httpx.post(
        f"{server.srv.base_url}/data/graphql",
        json={"query": _gql(tag), "role": _ROLE},
        headers={"X-Provisa-Role": _ROLE},
        timeout=120,
    )
    assert resp.status_code == 200, resp.text
    return len(resp.json()["data"]["tx__events"])


def _http_sql(server: _Server, tag: int) -> int:
    import httpx

    resp = httpx.post(
        f"{server.srv.base_url}/data/sql",
        json={"sql": _sql(tag), "role": _ROLE},
        headers={"X-Provisa-Role": _ROLE},
        timeout=120,
    )
    assert resp.status_code == 200, resp.text
    return len(resp.json()["data"]["sql"])


def _http_cypher(server: _Server, tag: int) -> int:
    import httpx

    resp = httpx.post(
        f"{server.srv.base_url}/data/cypher",
        json={"query": _cypher(tag), "params": {}},
        headers={"X-Provisa-Role": _ROLE},
        timeout=120,
    )
    assert resp.status_code == 200, resp.text
    return len(resp.json()["rows"])


def _bolt(server: _Server, tag: int) -> int:
    from neo4j import GraphDatabase

    driver = GraphDatabase.driver(f"bolt://127.0.0.1:{server.srv.bolt_port}", auth=(_ROLE, ""))
    try:
        with driver.session() as sess:
            return len(list(sess.run(_cypher(tag))))
    finally:
        driver.close()


def _grpc(server: _Server, tag: int) -> int:
    import grpc
    from google.protobuf.message_factory import GetMessageClass

    from tests.grpc_proto_client import role_descriptor_pool

    _pool, svc = role_descriptor_pool(server.srv.base_url, _ROLE)
    method = next(
        m
        for m in svc.methods
        if m.name.startswith("Query")
        and "vents" in m.name
        and not m.name.endswith(("Aggregate", "GroupBy", "Batch"))
    )
    req_cls = GetMessageClass(method.input_type)
    resp_cls = GetMessageClass(method.output_type)
    channel = grpc.insecure_channel(f"127.0.0.1:{server.srv.grpc_port}")
    try:
        rpc = channel.unary_stream(
            f"/{svc.full_name}/{method.name}",
            request_serializer=req_cls.SerializeToString,
            response_deserializer=resp_cls.FromString,
        )
        # The tag is the row limit: it reaches the compiled SQL, where the tracer reads it.
        request = req_cls(limit=tag)
        return len(list(rpc(request, metadata=(("x-provisa-role", _ROLE),), timeout=120)))
    finally:
        channel.close()


def _mcp(server: _Server, tag: int) -> int:
    from mcp import ClientSession
    from mcp.client.streamable_http import streamablehttp_client

    async def _call() -> int:
        url = f"http://127.0.0.1:{server.mcp_port}/mcp"
        async with streamablehttp_client(url) as (read, write, _):
            async with ClientSession(read, write) as session:
                await session.initialize()
                result = await session.call_tool("run_sql", {"sql": _sql(tag), "limit": 5})
                assert not result.isError, result.content
                return len(result.content)

    return asyncio.run(_call())


def _sse(server: _Server, tag: int) -> int:
    """Open the SSE subscription and read until the stream's first bytes (or 3 s of silence)."""
    import httpx

    del tag  # an SSE subscription has no query text; its records are read by transport, not tag
    with httpx.stream(
        "GET",
        f"{server.srv.base_url}/data/subscribe/tx__events",
        headers={"X-Provisa-Role": _ROLE},
        timeout=httpx.Timeout(10, read=3),
    ) as resp:
        assert resp.status_code == 200, resp.read()
        try:
            for _chunk in resp.iter_raw():
                break
        except httpx.ReadTimeout:
            pass
    return 0


_TRANSPORTS = {
    "pgwire": (_pgwire, 9100100, "pgwire"),
    "flight-sql": (_flight_sql, 9100200, "flight.do_get"),
    "flight-graphql": (_flight_graphql, 9100300, "flight.do_get"),
    "flight-cypher": (_flight_cypher, 9100400, "flight.do_get"),
    "grpc": (_grpc, 9100500, "grpc._streaming"),
    "http-graphql": (_http_graphql, 9100600, "http"),
    "http-sql": (_http_sql, 9100700, "http"),
    "http-cypher": (_http_cypher, 9100800, "http"),
    "bolt": (_bolt, 9100900, "bolt"),
    "mcp": (_mcp, 9101000, "http"),
}


# -- trace assertions ---------------------------------------------------------------------------


def _unsanctioned(hops: list[dict]) -> list[dict]:
    def _site(hop: dict) -> tuple[str, str]:
        path, _line, func = hop["at"].split(" <- ")[0].rsplit(":", 2)
        return path, func

    return [h for h in hops if _site(h) not in _SANCTIONED_HOPS]


def _assert_exclusive(server: _Server, name: str, tag: int, entry_transport: str) -> int:
    """Every stage of the request tagged ``tag`` ran on one thread; returns that thread."""
    records = server.tagged(tag)
    stages = [r for r in records if r["kind"] == "stage"]
    assert stages, f"{name}: no pipeline stage saw tag {tag}"
    kinds = {s["stage"].split(":")[0] for s in stages}
    assert {"govern", "execute"} <= kinds, f"{name}: stages seen {sorted(kinds)}"
    if name.split("[")[0] != "mcp":
        # An MCP tool returns its result to the MCP session, which encodes and sends it on the
        # MCP transport's loop (provisa/api/mcp/server.py: the transport only relays); every
        # other transport encodes and sends on the request's thread.
        assert kinds & {"send", "encode"}, f"{name}: no encode/send stage seen: {sorted(kinds)}"
    by_thread: dict[int, set[str]] = {}
    for s in stages:
        by_thread.setdefault(s["ident"], set()).add(s["stage"])
    assert len(by_thread) == 1, f"{name}: stages ran on {len(by_thread)} threads: {by_thread}"
    (ident,) = by_thread
    entries = [
        r
        for r in server.records()
        if r["kind"] == "entry" and r["ident"] == ident and r["transport"] == entry_transport
    ]
    assert entries, f"{name}: thread {ident} was never handed a {entry_transport} request"
    hops = _unsanctioned([r for r in records if r["kind"] == "hop"])
    assert not hops, f"{name}: the request handed work to another thread: {hops}"
    return ident


@pytest.mark.parametrize("name", list(_TRANSPORTS))
def test_one_request_runs_every_stage_on_its_own_thread(server, name):
    call, tag, entry_transport = _TRANSPORTS[name]
    assert call(server, tag) >= 1
    _assert_exclusive(server, name, tag, entry_transport)


@pytest.mark.parametrize("name", list(_TRANSPORTS))
def test_concurrent_requests_each_stay_on_their_own_thread(server, name):
    call, base, entry_transport = _TRANSPORTS[name]
    tags = [base + 10 + i for i in range(_CONCURRENT)]
    start = threading.Barrier(_CONCURRENT)
    errors: list[BaseException] = []

    def _client(tag: int) -> None:
        try:
            start.wait(timeout=30)
            call(server, tag)
        except BaseException as exc:  # reported by the assertion below with the real cause
            errors.append(exc)

    threads = [threading.Thread(target=_client, args=(t,)) for t in tags]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=180)
    assert not errors, errors

    spans: dict[int, tuple[int, float, float]] = {}
    for tag in tags:
        ident = _assert_exclusive(server, f"{name}[{tag}]", tag, entry_transport)
        times = [r["t"] for r in server.tagged(tag)]
        spans[tag] = (ident, min(times), max(times))
    overlapping = 0
    for a in tags:
        for b in tags:
            if a >= b:
                continue
            ia, a0, a1 = spans[a]
            ib, b0, b1 = spans[b]
            if a0 <= b1 and b0 <= a1:
                overlapping += 1
                assert ia != ib, f"{name}: requests {a} and {b} overlapped on thread {ia}"
    assert overlapping, f"{name}: no two of the {_CONCURRENT} requests overlapped in time"


def test_sse_subscription_is_served_on_its_request_thread(server):
    before = len(server.records())
    _sse(server, 0)
    time.sleep(0.5)
    records = server.records()[before:]
    stages = [r for r in records if r["kind"] == "stage" and "sse" in r["stage"]]
    handler = [r for r in records if r["kind"] == "stage" and r["stage"].endswith("subscribe")]
    assert handler and stages, [r.get("stage") for r in records]
    idents = {r["ident"] for r in handler + stages}
    assert len(idents) == 1, f"the SSE stream left its request thread: {handler + stages}"
    (ident,) = idents
    assert any(
        r["kind"] == "entry" and r["ident"] == ident and r["transport"] == "http" for r in records
    )
    hops = _unsanctioned([r for r in records if r["kind"] == "hop" and r["ident"] == ident])
    assert not hops, f"the SSE stream handed work to another thread: {hops}"


def test_a_read_triggered_land_runs_on_the_request_thread(landed_server):
    """The first read of a prefer_materialized source lands it (REQ-1661). The request triggers
    that land, so the land — the store write included — is part of the request."""
    tag = 9102000
    time.sleep(2)  # past the source's 1 s cache_ttl: this read finds the copy stale and re-lands it
    assert _http_sql(landed_server, tag) >= 1
    records = landed_server.records()
    request_threads = {r["ident"] for r in records if r.get("tag") == str(tag)}
    lands = [r for r in records if r["kind"] == "stage" and r["stage"].startswith("land:")]
    assert lands, "the read did not land the source"
    hops = _unsanctioned([r for r in records if r["kind"] == "hop" and r.get("tag") == str(tag)])
    assert not hops, f"the land was handed to another thread: {hops}"
    stage_threads = {
        r["ident"] for r in records if r["kind"] == "stage" and r.get("tag") == str(tag)
    }
    assert len(stage_threads) == 1, f"the request ran on {len(request_threads)} threads"
