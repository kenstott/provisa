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

A landed (``replicate``) source on a DuckDB-file store is read through a land the
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
#   (provisa/core/request_thread.py): the complete response in one relay, and a relay per message
#   for a streamed response, a large/chunked body, a receive() after the body and a WebSocket;
# - the deadline watchdog, ONE thread for the process, started by the first deadline it is asked
#   to watch. It runs none of any request's work — it only calls the in-flight statement's cancel
#   at expiry (provisa/core/request_deadline.py).
# - work that OUTLIVES the request, detached onto the background pool and never awaited by it
#   (provisa/core/connection_loop.py spawn_background/spawn_after: a hot-cache promote, a TTL drop).
# Each entry: (file, function) of the frame that makes the hand-off.
_SANCTIONED_HOPS = (
    ("provisa/core/request_thread.py", "thread_send"),
    ("provisa/core/request_thread.py", "thread_receive"),
    ("provisa/core/request_thread.py", "relayed_receive"),
    ("provisa/core/request_deadline.py", "watch"),
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
        self.airport_port = 0

    def records(self) -> list[dict]:
        return [json.loads(line) for line in self.trace.read_text().splitlines() if line]

    def tagged(self, tag: int) -> list[dict]:
        return [r for r in self.records() if r.get("tag") == str(tag)]


def _sqlite_config(work: Path, source_extra: dict) -> Path:
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
            # REQ-336: a subscription reads each change back by the table's key.
            "columns": [
                {"name": n, "data_type": t, "visible_to": [_ROLE], "is_primary_key": n == "id"}
                for n, t in (("id", "integer"), ("amount", "varchar"), ("ts", "varchar"))
            ],
        }
    ]
    cfg_path = work / "config.yaml"
    cfg_path.write_text(yaml.safe_dump(cfg))
    return cfg_path


def _start(
    work: Path,
    org: str,
    source_extra: dict | None = None,
    *,
    store_url: str | None = None,
    engine: str = "duckdb",
    config: Path | None = None,
) -> _Server:
    from tests.integration.isolated_server import IsolatedServer
    from tests.port_lease import lease_ports

    cfg_path = config if config is not None else _sqlite_config(work, source_extra or {})
    trace = work / "threads.jsonl"
    # Pinned leases, never reissued: these servers bind the wildcard address, which a loopback
    # bind probe does not see, and two traced servers run at once in this module.
    mcp_port, airport_port = lease_ports(2)
    overrides = {
        "PROVISA_TEST_THREAD_TRACE": str(trace),
        "PROVISA_MCP_PORT": str(mcp_port),
        "PROVISA_MCP_HOST": "127.0.0.1",
        "PROVISA_MCP_ROLE": _ROLE,
        "PROVISA_AIRPORT_PORT": str(airport_port),
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
            engine=engine,
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
    server = _Server(srv, trace, mcp_port)
    server.airport_port = airport_port
    return server


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
        {"replicate": 0, "cache_ttl": 1},
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
        if r["kind"] == "entry"
        and r["ident"] == ident
        and r["transport"].startswith(entry_transport)
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


def test_http_request_threads_are_reused_one_request_at_a_time(server):
    """REQ-1882 (amended 2026-10-01): an HTTP request thread serves one request start to finish
    and then the next. Requests sent one after another are each exclusive to a thread, and the
    server did not need a new thread for each of them."""
    call, base, entry_transport = _TRANSPORTS["http-graphql"]
    tags = [base + 40 + i for i in range(6)]
    idents = []
    for tag in tags:
        assert call(server, tag) >= 1
        idents.append(_assert_exclusive(server, f"http-graphql[{tag}]", tag, entry_transport))
    assert len(set(idents)) < len(idents), f"no request thread was reused: {idents}"
    # A reused thread ran its requests one after another, never interleaved.
    spans = {}
    for tag, ident in zip(tags, idents, strict=True):
        times = [r["t"] for r in server.tagged(tag)]
        spans[tag] = (ident, min(times), max(times))
    for a in tags:
        for b in tags:
            if a < b and spans[a][0] == spans[b][0]:
                assert spans[a][2] <= spans[b][1] or spans[b][2] <= spans[a][1], (a, b, spans)


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
    """The first read of a replicate source lands it (REQ-1661). The request triggers
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


# -- Bolt over WebSocket (the Neo4j Browser transport) -------------------------------------------


def _bolt_ws(server: _Server, tag: int) -> int:
    """One Cypher query over Bolt 4.4 framed in WebSocket binary messages; returns the row count."""
    import struct

    from websockets.sync.client import connect

    from provisa.bolt import messages as msg
    from provisa.bolt.packstream import pack_message, unpack_fields

    def _chunked(data: bytes) -> bytes:
        return struct.pack("!H", len(data)) + data + b"\x00\x00"

    with connect(f"ws://127.0.0.1:{server.srv.bolt_port}", max_size=None) as ws:
        buf = bytearray()

        def _read(n: int) -> bytes:
            while len(buf) < n:
                frame = ws.recv(timeout=60)
                assert isinstance(frame, bytes)
                buf.extend(frame)
            out = bytes(buf[:n])
            del buf[:n]
            return out

        def _message() -> tuple[int, list]:
            parts = []
            while True:
                (size,) = struct.unpack("!H", _read(2))
                if size == 0:
                    break
                parts.append(_read(size))
            data = b"".join(parts)
            # A message is a PackStream structure: marker (0xB0 | field count), tag, fields.
            return data[1], unpack_fields(data[2:]) if len(data) > 2 else []

        ws.send(msg.MAGIC + bytes([0, 0, 4, 4]) + b"\x00" * 12)
        assert _read(4) == bytes([0, 0, 4, 4])
        hello = {"user_agent": "thread-x/1", "scheme": "basic", "principal": _ROLE}
        ws.send(_chunked(pack_message(msg.HELLO, {**hello, "credentials": ""})))
        tag_, fields = _message()
        assert tag_ == msg.SUCCESS, fields
        ws.send(_chunked(pack_message(msg.RUN, _cypher(tag), {}, {})))
        tag_, fields = _message()
        assert tag_ == msg.SUCCESS, fields
        ws.send(_chunked(pack_message(msg.PULL, {"n": -1})))
        rows = 0
        while True:
            tag_, fields = _message()
            if tag_ == msg.RECORD:
                rows += 1
                continue
            assert tag_ == msg.SUCCESS, fields
            return rows


def test_bolt_over_websocket_runs_every_stage_on_its_connection_thread(server):
    tag = 9103000
    assert _bolt_ws(server, tag) >= 1
    _assert_exclusive(server, "bolt-ws", tag, "bolt")


# -- a Postgres source: the DIRECT route, the Trino engine route, REST, JSON:API, airport --------

_PG_ORG = _ORG + "_pg"


@pytest.fixture(scope="module")
def pg_server(tmp_path_factory):
    """The sample catalog over the stack's Postgres, on the Trino engine: a single-source read
    takes the DIRECT route to the source, and ``route=federated`` sends it through Trino."""
    s = _start(
        tmp_path_factory.mktemp("threadx_pg"),
        _PG_ORG,
        engine="trino",
        config=_REPO / "tests/fixtures/sample_config.yaml",
    )
    try:
        yield s
    finally:
        _stop(s, _PG_ORG)


def _pg_sql(tag: int) -> str:
    return f"SELECT id, region FROM sales_analytics.orders WHERE id <> {tag}"


def _pgwire_text(server: _Server, sql: str) -> int:
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
        cur.execute(sql)
        return len(cur.fetchall())
    finally:
        conn.close()


def _pgwire_binary(server: _Server, sql: str) -> int:
    """asyncpg reads results in binary — the client the raw-DataRow passthrough serves."""
    import asyncpg

    async def _run() -> int:
        conn = await asyncpg.connect(
            host="127.0.0.1",
            port=server.srv.pgwire_port,
            user=_ROLE,
            password="provisa",
            database="provisa",
            statement_cache_size=0,
        )
        try:
            return len(await conn.fetch(sql))
        finally:
            await conn.close()

    return asyncio.run(_run())


def _http_sql_text(server: _Server, sql: str) -> int:
    import httpx

    resp = httpx.post(
        f"{server.srv.base_url}/data/sql",
        json={"sql": sql, "role": _ROLE},
        headers={"X-Provisa-Role": _ROLE},
        timeout=120,
    )
    assert resp.status_code == 200, resp.text
    return len(resp.json()["data"]["sql"])


def _http_graphql_text(server: _Server, query: str, field: str) -> int:
    import httpx

    resp = httpx.post(
        f"{server.srv.base_url}/data/graphql",
        json={"query": query, "role": _ROLE},
        headers={"X-Provisa-Role": _ROLE},
        timeout=120,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert "errors" not in body, body
    return len(body["data"][field])


def _rest(server: _Server, tag: int) -> int:
    import httpx

    resp = httpx.get(
        f"{server.srv.base_url}/data/rest/sales-analytics/orders",
        params={"limit": str(tag)},  # the tag is the row limit: it reaches the compiled SQL
        headers={"X-Provisa-Role": _ROLE},
        timeout=120,
    )
    assert resp.status_code == 200, resp.text
    return len(resp.json()["data"])


def _jsonapi(server: _Server, tag: int) -> int:
    import httpx

    resp = httpx.get(
        f"{server.srv.base_url}/data/jsonapi/sales-analytics/orders",
        params={"filter[id][lt]": str(tag)},
        headers={"X-Provisa-Role": _ROLE, "Accept": "application/vnd.api+json"},
        timeout=120,
    )
    assert resp.status_code == 200, resp.text
    return len(resp.json()["data"])


_AIRPORT_CLIENT = r"""
import sys

import duckdb

port, role, tag = int(sys.argv[1]), sys.argv[2], int(sys.argv[3])
conn = duckdb.connect()
conn.execute("INSTALL airport FROM community")
conn.execute("LOAD airport")
loc = f"grpc://localhost:{port}"
conn.execute("CREATE SECRET airport_sec (TYPE airport, auth_token ?, scope ?)", [role, loc])
conn.execute(f"ATTACH '{loc}' AS provisa (TYPE AIRPORT)")
rows = conn.execute(
    f'SELECT id FROM provisa."sales_analytics"."orders" WHERE id < {tag}'
).fetchall()
print(len(rows))
"""


def _airport(server: _Server, tag: int) -> int:
    import subprocess
    import sys

    result = subprocess.run(
        [sys.executable, "-c", _AIRPORT_CLIENT, str(server.airport_port), _ROLE, str(tag)],
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    return int(result.stdout.strip().splitlines()[-1])


_PG_PATHS = {
    # name: (call, tag, entry transport, a stage that proves the route taken)
    "direct-pgwire-text": (lambda s, t: _pgwire_text(s, _pg_sql(t)), 9104000, "pgwire", "pg_"),
    "direct-pgwire-binary": (lambda s, t: _pgwire_binary(s, _pg_sql(t)), 9104100, "pgwire", "pg_"),
    "direct-flight": (lambda s, t: _flight(s, _pg_sql(t)), 9104200, "flight.do_get", "pg_"),
    "direct-http-sql": (lambda s, t: _http_sql_text(s, _pg_sql(t)), 9104300, "http", "pg_"),
    "direct-http-graphql": (
        lambda s, t: _http_graphql_text(
            s, f"query {{ sa__orders(where: {{id: {{lt: {t}}}}}) {{ id region }} }}", "sa__orders"
        ),
        9104400,
        "http",
        "pg_",
    ),
    "rest": (_rest, 9104500, "http", "transport:http.rest"),
    "jsonapi": (_jsonapi, 9104600, "http", "transport:http.jsonapi"),
    # A GraphQL ticket over Flight: the Flight server does not pass a request's @route directive
    # to routing, so this single-source read takes the DIRECT route (buffered, then one Arrow table).
    "direct-flight-graphql": (
        lambda s, t: _flight(
            s, f"query {{ sa__orders(where: {{id: {{lt: {t}}}}}) {{ id region }} }}"
        ),
        9104700,
        "flight.do_get",
        "pg_",
    ),
    # The Trino engine route: a GraphQL request over HTTP with @route(engine: FEDERATED). The
    # raw-SQL pipeline (pgwire, Flight SQL, /data/sql) does not read a `-- @provisa route=`
    # comment, so a single-source raw-SQL statement on this catalog always takes DIRECT.
    "trino-http-graphql": (
        lambda s, t: _http_graphql_text(
            s,
            "query @route(engine: FEDERATED) "
            f"{{ sa__orders(where: {{id: {{lt: {t}}}}}) {{ id region }} }}",
            "sa__orders",
        ),
        9105000,
        "http",
        "trino",
    ),
    "airport": (_airport, 9105100, "airport.", "airport"),
}


@pytest.mark.parametrize("name", list(_PG_PATHS))
def test_postgres_source_paths_run_every_stage_on_the_request_thread(pg_server, name):
    call, tag, entry_transport, route_marker = _PG_PATHS[name]
    assert call(pg_server, tag) >= 1
    _assert_exclusive(pg_server, name, tag, entry_transport)
    stages = {r["stage"] for r in pg_server.tagged(tag) if r["kind"] == "stage"}
    assert any(route_marker in s for s in stages), f"{name}: not the route under test: {stages}"
