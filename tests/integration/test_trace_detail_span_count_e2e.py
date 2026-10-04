# Copyright (c) 2026 Kenneth Stott
# Canary: c2d81f06-7a4e-4b39-9e52-0d6b3a8f1c47
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E (REQ-1910): how many spans one request exports, in normal and in debug trace detail.

Two real servers (DuckDB engine over a SQLite source, SQLite control plane, embedded Redis, pgwire
listening), one per trace detail, each exporting OTLP to a receiver inside this process. A request
is sent on its own, the export is left to settle, and every span the server put on the wire for
that request is counted:

- normal detail: ONE span per request, carrying the route, the cache result, the row count and the
  stage durations as attributes, and no SQL text;
- debug detail: the request span plus a span per stage and per Redis command, with SQL text;
- either detail: the request counter and the latency histogram record the request.
"""

# Requirements: REQ-1910

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path

import httpx
import pytest
import yaml

from tests.otlp_receiver import OtlpReceiver, ReceivedSpan

pytestmark = [pytest.mark.integration]

pytest.importorskip("duckdb")

_REPO = Path(__file__).resolve().parents[2]
_ROLE = "org_admin"
_GQL = "query @cached { td__events(where: {id: {eq: 7}}) { id amount } }"
_SQL = "SELECT id, amount FROM trace_detail.events WHERE id = 7"
# Attributes that carry statement text; normal detail records none of them.
_SQL_TEXT_ATTRS = ("db.statement", "provisa.query_text", "flight.sql", "db.query.text")


@dataclass
class _Traced:
    srv: object
    receiver: OtlpReceiver
    mcp_port: int

    @property
    def base_url(self) -> str:
        return self.srv.base_url  # type: ignore[attr-defined]

    @property
    def pgwire_port(self) -> int:
        return self.srv.pgwire_port  # type: ignore[attr-defined]


def _config(work: Path) -> Path:
    db = work / "events.sqlite"
    con = sqlite3.connect(db)
    try:
        con.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, amount TEXT)")
        con.executemany("INSERT INTO events VALUES (?, ?)", ((i, f"{i}.50") for i in range(1, 51)))
        con.commit()
    finally:
        con.close()
    with open(_REPO / "tests/fixtures/sample_config.yaml") as f:
        base = yaml.safe_load(f)
    cfg = {
        "naming": base["naming"],
        "roles": base["roles"],
        "relationships": [],
        "sources": [{"id": "td-sqlite", "type": "sqlite", "path": str(db)}],
        "domains": [{"id": "trace-detail", "description": "trace detail span count e2e"}],
        "tables": [
            {
                "source_id": "td-sqlite",
                "domain_id": "trace-detail",
                "schema": "default",
                "table": "events",
                "columns": [
                    {"name": n, "data_type": t, "visible_to": [_ROLE]}
                    for n, t in (("id", "integer"), ("amount", "varchar"))
                ],
            }
        ],
    }
    path = work / "config.yaml"
    path.write_text(yaml.safe_dump(cfg))
    return path


def _start(work: Path, org: str, detail: str | None) -> _Traced:
    """An isolated ``test``-instance server (own ports, own SQLite control plane) exporting OTLP
    to a receiver in this process. ``detail`` None leaves the trace detail at its default."""
    from tests.integration.isolated_server import IsolatedServer
    from tests.port_lease import lease_ports

    # Pinned leases, never reissued: the MCP port is bound by the server only once it has started,
    # and a transient lease would be handed out again to one of the server's own listeners.
    receiver_port, mcp_port = lease_ports(2)
    receiver = OtlpReceiver(port=receiver_port)
    receiver.start()
    env = {
        "PROVISA_MCP_PORT": str(mcp_port),
        "PROVISA_MCP_HOST": "127.0.0.1",
        "PROVISA_MCP_ROLE": _ROLE,
        "OTEL_SDK_DISABLED": "false",
        "OTEL_EXPORTER_OTLP_ENDPOINT": receiver.endpoint,
        "OTEL_EXPORTER_OTLP_PROTOCOL": "http/protobuf",
        "OTEL_SPAN_EXPORT_DELAY_MILLIS": "200",
        "OTEL_METRIC_EXPORT_INTERVAL": "1000",
    }
    if detail is not None:
        env["PROVISA_TRACE_DETAIL"] = detail
    srv = IsolatedServer(
        org,
        engine="duckdb",
        enable_pgwire=True,
        enable_bolt=True,
        await_flight=True,
        await_grpc=True,
        config=str(_config(work)),
        control_plane="sqlite",
        env=env,
    )
    try:
        srv.start(timeout=240)
    except BaseException:
        receiver.stop()
        raise
    return _Traced(srv, receiver, mcp_port)


def _stop(server: _Traced) -> None:
    server.srv.stop_process()  # type: ignore[attr-defined]
    server.receiver.stop()


@pytest.fixture(scope="module")
def normal_server(tmp_path_factory):
    s = _start(tmp_path_factory.mktemp("trace_normal"), "trace_detail_normal_e2e", None)
    try:
        yield s
    finally:
        _stop(s)


@pytest.fixture(scope="module")
def debug_server(tmp_path_factory):
    s = _start(tmp_path_factory.mktemp("trace_debug"), "trace_detail_debug_e2e", "debug")
    try:
        yield s
    finally:
        _stop(s)


@pytest.fixture(autouse=True)
def _requests_sent_here_start_no_trace():
    """The requests these tests send carry no trace context of this process.

    An application built in this process by another test file (``create_app`` -> ``setup_otel``)
    instruments httpx and gRPC clients process-wide, and nothing removes that when its lifespan
    ends — some such applications are session-scoped and still running. A client instrumented that
    way opens a span of its own and sends its ``traceparent``, so the server under test exports
    its request span as the CHILD of a span that exists only in this process: the request is no
    longer a trace of its own, which is what these tests count. Seen as ``child spans [...]`` on
    the http-sql, http-cypher and grpc requests whenever any such file ran first."""
    from opentelemetry.instrumentation.utils import suppress_instrumentation

    with suppress_instrumentation():
        yield


# -- one request, and the spans it exported --------------------------------------------------------


def _graphql(server: _Traced) -> None:
    resp = httpx.post(
        f"{server.base_url}/data/graphql",
        json={"query": _GQL, "role": _ROLE},
        headers={"X-Provisa-Role": _ROLE},
        timeout=120,
    )
    assert resp.status_code == 200 and not resp.json().get("errors"), resp.text
    assert resp.json()["data"]["td__events"] == [{"id": 7, "amount": "7.50"}], resp.text


def _spans_of_traces_started_in(
    server: _Traced, before: set[str], t0_ns: int, t1_ns: int
) -> list[ReceivedSpan]:
    """Every span of every trace whose first span started inside [t0, t1] — one request's spans
    when nothing else was sent in that window. A trace that began before ``t0`` (a scheduler tick
    already running) is not the request's."""
    spans = server.receiver.settle()
    by_trace: dict[str, list[ReceivedSpan]] = {}
    for s in spans:
        by_trace.setdefault(s.trace_id, []).append(s)
    return [
        s
        for tid, group in by_trace.items()
        if tid not in before and t0_ns <= min(g.start_ns for g in group) <= t1_ns
        for s in group
    ]


def _cached_graphql_spans(server: _Traced) -> list[ReceivedSpan]:
    """The spans of ONE cached GraphQL request: the trace of its HTTP server span."""
    _graphql(server)  # executes and stores
    _graphql(server)  # a hit; also warms every lazy import on the hit path
    before = {s.trace_id for s in server.receiver.settle()}
    _graphql(server)
    spans = [s for s in server.receiver.settle() if s.trace_id not in before]
    roots = [s for s in spans if s.name == "POST /data/graphql"]
    assert len(roots) == 1, sorted(s.name for s in spans)
    return [s for s in spans if s.trace_id == roots[0].trace_id]


def _pgwire_lookup_spans(server: _Traced) -> list[ReceivedSpan]:
    """The spans of ONE pgwire point lookup on an already-open, already-used connection."""
    import psycopg

    conn = psycopg.connect(
        host="127.0.0.1",
        port=server.pgwire_port,
        dbname="provisa",
        user=_ROLE,
        password="provisa",
        autocommit=True,
    )
    try:
        assert conn.execute(_SQL).fetchall() == [(7, "7.50")]
        before = {s.trace_id for s in server.receiver.settle()}
        t0 = time.time_ns()
        assert conn.execute(_SQL).fetchall() == [(7, "7.50")]
        t1 = time.time_ns()
        return _spans_of_traces_started_in(server, before, t0, t1)
    finally:
        conn.close()


def _names(spans: list[ReceivedSpan]) -> list[str]:
    return sorted(f"{s.name} [{s.scope}]" for s in spans)


def _sql_text(spans: list[ReceivedSpan]) -> list[tuple[str, str]]:
    return [(s.name, k) for s in spans for k in _SQL_TEXT_ATTRS if k in s.attrs]


# -- normal detail: one span per request -----------------------------------------------------------


def test_normal_cached_graphql_request_exports_one_span(normal_server):
    spans = _cached_graphql_spans(normal_server)
    assert len(spans) == 1, f"{len(spans)} spans: {_names(spans)}"
    attrs = spans[0].attrs
    assert attrs["provisa.transport"] == "graphql", attrs
    assert attrs["provisa.route"] == "cache", attrs
    assert attrs["cache.hit"] is True, attrs
    assert attrs["db.row_count"] == 1, attrs
    # The statement's role, table and text live in its audit row (the ops `queries` report), not
    # on the span.
    assert not {"provisa.role", "provisa.table", "provisa.domain"} & set(attrs), attrs
    # A hit on a plan the server has already compiled and governed is answered before routing
    # (REQ-1897): the stages that run are the cache read and the response encoding.
    for stage in ("cache", "encode"):
        assert attrs[f"stage.{stage}.ms"] >= 0, (stage, attrs)
    assert "stage.route.ms" not in attrs, attrs
    assert _sql_text(spans) == []


def test_normal_pgwire_point_lookup_exports_one_span(normal_server):
    spans = _pgwire_lookup_spans(normal_server)
    assert len(spans) == 1, f"{len(spans)} spans: {_names(spans)}"
    attrs = spans[0].attrs
    assert attrs["provisa.transport"] == "pgwire", attrs
    assert attrs["provisa.route"] in ("direct", "engine"), attrs
    assert attrs["db.row_count"] == 1, attrs
    # The statement's role, table and text live in its audit row (the ops `queries` report), not
    # on the span.
    assert not {"provisa.role", "provisa.table", "provisa.domain"} & set(attrs), attrs
    for stage in ("govern", "execute", "encode"):
        assert attrs[f"stage.{stage}.ms"] >= 0, (stage, attrs)
    assert _sql_text(spans) == []


# -- debug detail: the span waterfall, with SQL text -----------------------------------------------


def test_debug_cached_graphql_request_exports_stage_and_redis_spans(debug_server):
    spans = _cached_graphql_spans(debug_server)
    names = _names(spans)
    assert any(n.startswith("cache.get") for n in names), names
    assert any("instrumentation.redis" in n for n in names), names
    assert any(n.startswith("POST /data/graphql http send") for n in names), names
    assert any(n.startswith("POST /data/graphql http receive") for n in names), names
    # One trace: every span hangs off the request span.
    assert len({s.trace_id for s in spans}) == 1, names
    redis = next(s for s in spans if "instrumentation.redis" in s.scope)
    assert "db.statement" in redis.attrs, redis.attrs


def test_debug_pgwire_point_lookup_records_the_statement_text(debug_server):
    spans = _pgwire_lookup_spans(debug_server)
    names = _names(spans)
    request = [s for s in spans if s.name == "pgwire.query"]
    assert len(request) == 1, names
    # Debug detail: a span per stage of the statement, under the request span.
    assert {"pgwire.govern", "pgwire.execute", "pgwire.encode"} <= {s.name for s in spans}, names
    assert all(s.parent_span_id for s in spans if s is not request[0]), names
    # The statement text is recorded in debug detail (and only there).
    assert request[0].attrs["db.statement"] == _SQL, request[0].attrs
    # One trace per request: anything else recorded hangs off the request span.
    assert len({s.trace_id for s in spans}) == 1, names


# -- every transport: one request span ------------------------------------------------------------


def _flight(server: _Traced, query: str) -> int:
    import json

    import pyarrow.flight as flight

    client = flight.FlightClient(f"grpc://127.0.0.1:{server.srv.flight_port}")  # type: ignore[attr-defined]
    try:
        ticket = flight.Ticket(json.dumps({"query": query, "role": _ROLE}).encode())
        return client.do_get(ticket).read_all().num_rows
    finally:
        client.close()


def _http(server: _Traced, path: str, body: dict) -> None:
    resp = httpx.post(
        f"{server.base_url}{path}", json=body, headers={"X-Provisa-Role": _ROLE}, timeout=120
    )
    assert resp.status_code == 200, resp.text


def _bolt(server: _Traced) -> int:
    from neo4j import GraphDatabase

    port = server.srv.bolt_port  # type: ignore[attr-defined]
    driver = GraphDatabase.driver(f"bolt://127.0.0.1:{port}", auth=(_ROLE, ""))
    try:
        with driver.session() as sess:
            return len(list(sess.run(_CYPHER)))
    finally:
        driver.close()


def _grpc(server: _Traced) -> int:
    import grpc
    from google.protobuf.message_factory import GetMessageClass

    from tests.grpc_proto_client import role_descriptor_pool

    _pool, svc = role_descriptor_pool(server.base_url, _ROLE)
    method = next(
        m
        for m in svc.methods
        if m.name.startswith("Query")
        and "vents" in m.name
        and not m.name.endswith(("Aggregate", "GroupBy", "Batch"))
    )
    req_cls = GetMessageClass(method.input_type)
    resp_cls = GetMessageClass(method.output_type)
    channel = grpc.insecure_channel(f"127.0.0.1:{server.srv.grpc_port}")  # type: ignore[attr-defined]
    try:
        rpc = channel.unary_stream(
            f"/{svc.full_name}/{method.name}",
            request_serializer=req_cls.SerializeToString,
            response_deserializer=resp_cls.FromString,
        )
        return len(list(rpc(req_cls(limit=3), metadata=(("x-provisa-role", _ROLE),), timeout=120)))
    finally:
        channel.close()


def _mcp(server: _Traced) -> int:
    import asyncio

    from mcp import ClientSession
    from mcp.client.streamable_http import streamablehttp_client

    async def _call() -> int:
        url = f"http://127.0.0.1:{server.mcp_port}/mcp"
        async with streamablehttp_client(url) as (read, write, _):
            async with ClientSession(read, write) as session:
                await session.initialize()
                result = await session.call_tool("run_sql", {"sql": _SQL, "limit": 5})
                assert not result.isError, result.content
                return len(result.content)

    return asyncio.run(_call())


_CYPHER = "MATCH (e:events) WHERE e.id = 7 RETURN e.id AS id, e.amount AS amount"
_PLAIN_GQL = "query { td__events(where: {id: {eq: 7}}) { id amount } }"

# transport -> (one request, the provisa.transport its request span reports)
_REQUESTS = {
    "http-sql": (lambda s: _http(s, "/data/sql", {"sql": _SQL, "role": _ROLE}), "sql"),
    "http-cypher": (lambda s: _http(s, "/data/cypher", {"query": _CYPHER, "params": {}}), "cypher"),
    "flight-sql": (lambda s: _flight(s, _SQL), "flight"),
    "flight-graphql": (lambda s: _flight(s, _PLAIN_GQL), "flight"),
    "bolt": (_bolt, "bolt"),
    "grpc": (_grpc, "grpc"),
    "mcp": (_mcp, "mcp"),
}


def _one_request_spans(server: _Traced, send) -> list[ReceivedSpan]:
    send(server)  # warm: connections, lazy imports, the plan caches
    before = {s.trace_id for s in server.receiver.settle()}
    t0 = time.time_ns()
    send(server)
    t1 = time.time_ns()
    return _spans_of_traces_started_in(server, before, t0, t1)


def _request_spans(spans: list[ReceivedSpan], transport: str) -> list[ReceivedSpan]:
    return [s for s in spans if s.attrs.get("provisa.transport") == transport]


@pytest.mark.parametrize("name", sorted(_REQUESTS))
def test_normal_detail_exports_one_span_per_request_on_every_transport(normal_server, name):
    send, transport = _REQUESTS[name]
    spans = _one_request_spans(normal_server, send)
    # MCP and gRPC clients make protocol calls of their own around the request (session
    # initialise, reflection); each of those is its own one-span HTTP request.
    ours = _request_spans(spans, transport)
    assert len(ours) == 1, f"{name}: {_names(spans)}"
    assert all(not s.parent_span_id for s in spans), f"{name}: child spans {_names(spans)}"
    assert len({s.trace_id for s in spans}) == len(spans), f"{name}: {_names(spans)}"
    assert _sql_text(spans) == []
    assert ours[0].attrs["provisa.route"] in ("direct", "engine", "cache"), ours[0].attrs


@pytest.mark.parametrize("name", sorted(_REQUESTS))
def test_debug_detail_keeps_a_request_in_one_trace_on_every_transport(debug_server, name):
    send, transport = _REQUESTS[name]
    spans = _one_request_spans(debug_server, send)
    ours = _request_spans(spans, transport)
    assert len(ours) == 1, f"{name}: {_names(spans)}"
    # Every span that is not a request span of its own hangs off one: no orphan stage roots.
    roots = [s for s in spans if not s.parent_span_id]
    assert all("provisa.transport" in s.attrs for s in roots), f"{name}: {_names(roots)}"


# -- metrics: every request, either detail ---------------------------------------------------------


def _await_points(server: _Traced, metric: str, transport: str) -> int:
    """The latest exported count of ``metric`` for ``transport`` (cumulative), once it appears."""
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        counts = [
            p.count
            for p in server.receiver.metric_points()
            if p.metric == metric and p.attrs.get("transport") == transport
        ]
        if counts:
            return max(counts)
        time.sleep(0.2)
    names = sorted({p.metric for p in server.receiver.metric_points()})
    raise AssertionError(f"no {metric} point for transport={transport}; exported: {names}")


@pytest.mark.parametrize("which", ["normal_server", "debug_server"])
def test_request_counter_and_latency_histogram_record_every_request(which, request):
    server = request.getfixturevalue(which)
    _cached_graphql_spans(server)
    _pgwire_lookup_spans(server)
    for transport in ("graphql", "pgwire"):
        assert _await_points(server, "provisa.query.executed", transport) >= 2
        assert _await_points(server, "provisa.query.duration_ms", transport) >= 2
