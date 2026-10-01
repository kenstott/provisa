# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""REQ-1911: optimistic_load takes its setup from the contract; with the zero-knob contract each
transport sends exactly the request the optimistic test sent before (same statement text, same
headers/hints)."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pyarrow as pa
import pytest

BENCH = Path(__file__).resolve().parents[3] / "demo" / "named" / "perf" / "bench"
sys.path.insert(0, str(BENCH))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import contract_model  # noqa: E402
import deployment_fixture as fx  # noqa: E402
import optimistic_load as ol  # noqa: E402
import request_render  # noqa: E402
import setup_contract as sc  # noqa: E402
from queries import OPTIMISTIC  # noqa: E402

PERF_CONTRACT = BENCH / "setups" / "perf-stack.yaml"
ENV = {"PROVISA_HTTP_BASE_URL": "http://localhost:8001"}

SQL = "-- @provisa cache=true\nSELECT order_id FROM perf_bench.orders LIMIT 1"
CYPHER = "// @provisa cache=true\nMATCH (o:PerfBench:Orders) RETURN o.orderId AS order_id LIMIT 1"
GRAPHQL = "query @cached { pb__orders(limit: 1) { orderId } }"
ROLE = {"x-provisa-role": "org_admin"}

# What each transport sent before the contract (read from the pre-contract clients).
GOLDEN: dict[str, Any] = {
    "graphql": ("POST", "http://localhost:8001/data/graphql", {"query": GRAPHQL}, None),
    "data_sql": ("POST", "http://localhost:8001/data/sql", {"sql": SQL, "role": "org_admin"}, None),
    "cypher_http": (
        "POST",
        "http://localhost:8001/data/cypher",
        {"query": CYPHER, "params": {}},
        None,
    ),
    "rest": (
        "GET",
        "http://localhost:8001/data/rest/perf-bench/orders?limit=1&fields=orderId",
        None,
        None,
    ),
    "jsonapi": (
        "GET",
        "http://localhost:8001/data/jsonapi/perf-bench/orders?page%5Bsize%5D=1&fields%5Borders%5D=order_id",
        None,
        "application/vnd.api+json",
    ),
    "pgwire": SQL,
    "flight_sql": {"query": SQL, "role": "org_admin"},
    "bolt": CYPHER,
    "grpc": (
        "/provisa.v1.ProvisaService/QueryPbOrders",
        1,
        ["order_id"],
        [("x-provisa-role", "org_admin"), ("x-provisa-cache", "true")],
    ),
}


def _endpoints(setup: contract_model.Setup) -> ol.Endpoints:
    return ol.Endpoints(
        http_base_url="http://localhost:8001",
        pgwire_host="localhost",
        pgwire_port=5439,
        bolt_host="localhost",
        bolt_port=17687,
        flight_host="localhost",
        flight_port=8815,
        grpc_host="localhost",
        grpc_port=50051,
        role="org_admin",
        setup=setup,
        credential=None,
    )


def _setup() -> contract_model.Setup:
    return sc.load_setup(
        PERF_CONTRACT, environ=ENV, known_transports=list(ol.TRANSPORTS), deployment=fx.build()
    )


@pytest.fixture
def captured(monkeypatch: pytest.MonkeyPatch) -> list[Any]:
    """Fakes for every wire client library; every request lands in the returned list."""
    sent: list[Any] = []
    real_client = httpx.Client

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content) if request.content else None
        sent.append(
            (
                request.method,
                str(request.url),
                body,
                request.headers.get("accept") if "jsonapi" in request.url.path else None,
                request.headers.get("x-provisa-role"),
            )
        )
        path = request.url.path
        if path.endswith("/graphql"):
            return httpx.Response(200, json={"data": {"pb__orders": [{"orderId": 1}]}})
        if path.endswith("/sql"):
            return httpx.Response(200, json={"data": {"orders": [[1]]}, "columns": ["order_id"]})
        if path.endswith("/cypher"):
            return httpx.Response(200, json={"rows": [[1]], "columns": ["order_id"]})
        if "jsonapi" in path:
            return httpx.Response(200, json={"data": [{"attributes": {"order_id": 1}}]})
        return httpx.Response(200, json={"data": [{"orderId": 1}]})

    monkeypatch.setattr(
        httpx, "Client", lambda **kw: real_client(transport=httpx.MockTransport(handler), **kw)
    )

    import neo4j
    import psycopg
    import pyarrow.flight as fl

    class FakeConn:
        def execute(self, sql: str) -> Any:
            sent.append(sql)
            return SimpleNamespace(fetchall=lambda: [(1,)], description=[("order_id",)])

        def close(self) -> None:
            pass

    monkeypatch.setattr(psycopg, "connect", lambda **kw: FakeConn())

    class FakeFlight:
        def __init__(self, location: str) -> None:
            pass

        def do_get(self, ticket: Any, options: Any = None) -> Any:
            sent.append(json.loads(ticket.ticket))
            return SimpleNamespace(read_all=lambda: pa.table({"order_id": [1]}))

        def close(self) -> None:
            pass

    monkeypatch.setattr(fl, "FlightClient", FakeFlight)

    class FakeSession:
        def run(self, text: str) -> Any:
            sent.append(text)
            return [SimpleNamespace(keys=lambda: ["order_id"])]

        def close(self) -> None:
            pass

    class FakeDriver:
        def session(self) -> FakeSession:
            return FakeSession()

        def close(self) -> None:
            pass

    monkeypatch.setattr(neo4j.GraphDatabase, "driver", lambda *a, **kw: FakeDriver())

    class Req:
        def __init__(self, limit: int) -> None:
            self.limit = limit
            self.read_mask = SimpleNamespace(paths=[])

        SerializeToString = FromString = None  # noqa: N815 - the stream below is faked

        def ListFields(self) -> list[int]:  # noqa: N802
            return [1]

    class FakeGrpc:
        _channel = SimpleNamespace()

        def _resolve_message_class(self, name: str) -> Any:
            return Req

    def unary_stream(path: str, request_serializer: Any, response_deserializer: Any) -> Any:
        def call(request: Req, metadata: list) -> list:
            sent.append((path, request.limit, list(request.read_mask.paths), list(metadata)))
            return [request]

        return call

    FakeGrpc._channel.unary_stream = unary_stream  # type: ignore[attr-defined]
    monkeypatch.setattr(ol, "_grpc_shared", FakeGrpc())
    return sent


def _one_request(transport: str, ep: ol.Endpoints, query: Any, sent: list[Any]) -> Any:
    sent.clear()
    client = ol.CLIENTS[transport](ep)
    try:
        client.call(query, "org_admin")
    finally:
        client.close()
    assert len(sent) == 1, sent
    return sent[0]


def _expected(transport: str) -> Any:
    g = GOLDEN[transport]
    if transport in ("graphql", "data_sql", "cypher_http", "rest", "jsonapi"):
        method, url, body, accept = g
        return (method, url, body, accept, "org_admin")
    return g


@pytest.mark.parametrize("transport", list(GOLDEN))
def test_contract_request_equals_the_pre_contract_request(
    transport: str, captured: list[Any]
) -> None:
    ep = _endpoints(_setup())
    before = _one_request(transport, ep, OPTIMISTIC, captured)
    after = _one_request(
        transport, ep, request_render.build_query(_setup(), transport, cached=True), captured
    )
    assert after == before
    assert after == _expected(transport)


def test_every_client_transport_has_a_golden_request() -> None:
    assert set(ol.CLIENTS) == set(GOLDEN)


def test_endpoints_come_from_the_contract() -> None:
    ep = ol.endpoints_from_setup(_setup())
    assert (ep.http_base_url, ep.pgwire_host, ep.pgwire_port) == (
        "http://localhost:8001",
        "localhost",
        5439,
    )
    assert (ep.bolt_port, ep.flight_port, ep.grpc_port, ep.role) == (
        17687,
        8815,
        50051,
        "org_admin",
    )
    assert ep.setup == _setup()


# credentials are covered by test_credentials.py


# ---------------------------------------------------------------- the CLI


def test_setup_flag_is_required_by_the_optimistic_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "argv", ["optimistic_load.py"])
    with pytest.raises(SystemExit) as exc:
        ol.main()
    assert exc.value.code == 2


def test_run_from_args_without_setup_is_an_error(tmp_path: Path) -> None:
    with pytest.raises(contract_model.SetupError, match="--setup is required"):
        ol.run_from_args(
            SimpleNamespace(
                setup=None,
                server_pid=None,
                resolved_names=None,
                apply_replication=False,
                skip_route_verification=True,
            ),
            tmp_path,
            argv=[],
        )


@pytest.mark.parametrize(
    "flag",
    [
        "--http-base-url=http://x",
        "--role",
        "--optimistic-steps",
        "--bypass-relationship-guard",
        "--http-base",
    ],
)
def test_flags_the_contract_owns_are_refused(flag: str, tmp_path: Path) -> None:
    args = SimpleNamespace(
        setup=str(PERF_CONTRACT),
        server_pid=None,
        resolved_names=str(_resolved_file(tmp_path)),
        apply_replication=False,
        skip_route_verification=True,
    )
    with pytest.raises(contract_model.SetupError, match="set by the setup contract"):
        ol.run_from_args(
            args, tmp_path, argv=["run_benchmark.py", "--setup", str(PERF_CONTRACT), flag]
        )


def _resolved_file(tmp_path: Path) -> Path:
    import lookup

    unbound = sc.load_setup(PERF_CONTRACT, environ=ENV, known_transports=list(ol.TRANSPORTS))
    path = tmp_path / "given.json"
    lookup.write_resolved(lookup.resolve(*sc.identities(unbound), fx.build()), path)
    return path


def test_run_from_args_runs_the_contract(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("PROVISA_HTTP_BASE_URL", "http://localhost:8001")
    calls: list[Any] = []
    monkeypatch.setattr(ol, "run_all", lambda *a, **k: calls.append(a) or [])
    args = SimpleNamespace(
        setup=str(PERF_CONTRACT),
        server_pid=7,
        resolved_names=str(_resolved_file(tmp_path)),
        apply_replication=False,
        skip_route_verification=True,
    )
    ol.run_from_args(
        args, tmp_path / "out", argv=["run_benchmark.py", "--setup", str(PERF_CONTRACT)]
    )
    ep, out, transports, steps, window, procs, pid = calls[0]
    assert transports == list(ol.TRANSPORTS)
    assert steps == ol.DEFAULT_STEPS and window == 20.0 and pid == 7 and out == tmp_path / "out"
    assert procs >= 1 and ep.setup == _setup()
    assert (tmp_path / "out" / "resolved-names.json").exists()  # written whether asked or replayed


def test_run_from_args_asks_the_deployment_when_no_names_are_given(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("PROVISA_HTTP_BASE_URL", "http://localhost:8001")
    import lookup

    asked: list[Any] = []
    monkeypatch.setattr(
        lookup,
        "fetch",
        lambda client, role: asked.append((str(client.base_url), role)) or fx.build(),
    )
    monkeypatch.setattr(ol, "run_all", lambda *a, **k: [])
    args = SimpleNamespace(
        setup=str(PERF_CONTRACT),
        server_pid=None,
        resolved_names=None,
        apply_replication=False,
        skip_route_verification=True,
    )
    ol.run_from_args(args, tmp_path, argv=["x"])
    assert asked == [("http://localhost:8001", "org_admin")]
    assert (tmp_path / "resolved-names.json").exists() and (
        tmp_path / "lookup-responses.json"
    ).exists()


def test_a_lookup_that_cannot_match_the_contract_is_a_setup_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("PROVISA_HTTP_BASE_URL", "http://localhost:8001")
    import lookup

    monkeypatch.setattr(
        lookup, "fetch", lambda client, role: fx.build(tables=fx.TABLES[1:], relationships=[])
    )
    args = SimpleNamespace(
        setup=str(PERF_CONTRACT),
        server_pid=None,
        resolved_names=None,
        apply_replication=False,
        skip_route_verification=True,
    )
    with pytest.raises(
        contract_model.SetupError, match="bench-postgresql/public.orders is not registered"
    ):
        ol.run_from_args(args, tmp_path, argv=["x"])


def test_the_optimistic_flag_itself_is_not_refused(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("PROVISA_HTTP_BASE_URL", "http://localhost:8001")
    monkeypatch.setattr(ol, "run_all", lambda *a, **k: [])
    argv = [
        "run_benchmark.py",
        "--engine",
        "pg",
        "--optimistic",
        "--setup",
        str(PERF_CONTRACT),
        "--output-dir",
        "r",
    ]
    args = SimpleNamespace(
        setup=str(PERF_CONTRACT),
        server_pid=None,
        resolved_names=str(_resolved_file(tmp_path)),
        apply_replication=False,
        skip_route_verification=True,
    )
    ol.run_from_args(args, tmp_path, argv=argv)


GOLDEN_UNCACHED: dict[str, Any] = {
    "graphql": "query { pb__orders(limit: 1) { orderId } }",
    "data_sql": "SELECT order_id FROM perf_bench.orders LIMIT 1",
    "cypher_http": "MATCH (o:PerfBench:Orders) RETURN o.orderId AS order_id LIMIT 1",
}


@pytest.mark.parametrize("transport", list(GOLDEN))
def test_uncached_request_drops_only_the_opt_in(transport: str, captured: list[Any]) -> None:
    setup = _setup()
    sent = _one_request(
        transport,
        _endpoints(setup),
        request_render.build_query(setup, transport, cached=False),
        captured,
    )
    if transport == "graphql":
        assert sent[2] == {"query": GOLDEN_UNCACHED["graphql"]}
    elif transport == "data_sql":
        assert sent[2] == {"sql": GOLDEN_UNCACHED["data_sql"], "role": "org_admin"}
    elif transport == "cypher_http":
        assert sent[2]["query"] == GOLDEN_UNCACHED["cypher_http"]
    elif transport in ("pgwire", "flight_sql"):
        text = sent if transport == "pgwire" else sent["query"]
        assert text == "SELECT order_id FROM perf_bench.orders LIMIT 1"
    elif transport == "bolt":
        assert sent == GOLDEN_UNCACHED["cypher_http"]
    elif transport == "grpc":
        assert sent[3] == [("x-provisa-role", "org_admin")]
    else:  # rest, jsonapi: no opt-in, the same request
        assert sent == _expected(transport)
