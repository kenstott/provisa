# Copyright (c) 2026 Kenneth Stott
# Canary: 6f3674fb-cbda-4071-a71b-559b6072aeaf
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""REQ-1911: the role mix — each request is sent as a role drawn from the contract's weighted
roles."""

from __future__ import annotations

import json
import queue
import sys
import threading
from collections import Counter
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pyarrow as pa
import pytest
import yaml

BENCH = Path(__file__).resolve().parents[3] / "demo" / "named" / "perf" / "bench"
sys.path.insert(0, str(BENCH))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import contract_model  # noqa: E402
import deployment_fixture as fx  # noqa: E402
import optimistic_load as ol  # noqa: E402
import request_mix  # noqa: E402
import request_render  # noqa: E402
import setup_contract as sc  # noqa: E402

PERF_CONTRACT = BENCH / "setups" / "perf-stack.yaml"
ENV = {"PROVISA_HTTP_BASE_URL": "http://localhost:8001"}
TRANSPORTS = list(ol.TRANSPORTS)


def _roles(raw: dict[str, Any], *pairs: tuple[str, float]) -> None:
    raw["deployment"]["roles"] = [{"role": r, "weight": w} for r, w in pairs]


def _mix(raw: dict[str, Any]) -> None:
    _roles(raw, ("org_admin", 0.7), ("analyst", 0.3))


def _setup(tmp_path: Path, *mutators: Any) -> contract_model.Setup:
    raw = json.loads(json.dumps(yaml.safe_load(PERF_CONTRACT.read_text())))
    for m in mutators:
        m(raw)
    path = tmp_path / "s.yaml"
    path.write_text(yaml.safe_dump(raw, sort_keys=False))
    return sc.load_setup(path, environ=ENV, known_transports=TRANSPORTS, deployment=fx.build())


def _gen(
    setup: contract_model.Setup, transport: str = "pgwire", stream: str = "s"
) -> request_mix.RequestGenerator:
    return request_mix.RequestGenerator(setup, transport, stream, cacheable=transport != "rest")


# ------------------------------------------------------------------ the contract


def test_a_role_mix_is_no_longer_refused(tmp_path: Path) -> None:
    assert [r.role for r in _setup(tmp_path, _mix).roles] == ["org_admin", "analyst"]


def test_duplicate_roles_are_refused(tmp_path: Path) -> None:
    with pytest.raises(
        contract_model.SetupError, match="deployment.roles: duplicate role 'analyst'"
    ):
        _setup(tmp_path, lambda r: _roles(r, ("analyst", 0.5), ("analyst", 0.5)))


# ------------------------------------------------------------------ the draw


def test_one_role_is_every_requests_role(tmp_path: Path) -> None:
    gen = _gen(_setup(tmp_path))
    assert {gen.next().role for _ in range(50)} == {"org_admin"}


def test_roles_follow_the_weights(tmp_path: Path) -> None:
    gen = _gen(_setup(tmp_path, _mix))
    counts = Counter(gen.next().role for _ in range(5000))
    assert abs(counts["org_admin"] / 5000 - 0.7) < 0.03


def test_a_zero_weight_role_is_never_drawn(tmp_path: Path) -> None:
    gen = _gen(_setup(tmp_path, lambda r: _roles(r, ("org_admin", 1.0), ("analyst", 0.0))))
    assert {gen.next().role for _ in range(300)} == {"org_admin"}


def test_same_seed_same_sequence_and_independent_of_other_knobs(tmp_path: Path) -> None:
    def with_cache(raw: dict[str, Any]) -> None:
        _mix(raw)
        raw["knobs"]["cache"]["probability"] = 0.5

    a, b, c = (_gen(_setup(tmp_path, m), stream="x/1/1") for m in (_mix, _mix, with_cache))
    ra = [s.role for s in (a.next() for _ in range(100))]
    assert ra == [s.role for s in (b.next() for _ in range(100))]
    assert ra == [s.role for s in (c.next() for _ in range(100))]


def test_a_repeat_keeps_the_role(tmp_path: Path) -> None:
    def mutate(raw: dict[str, Any]) -> None:
        _mix(raw)
        raw["knobs"]["cache_repetition"] = {
            "probability": 1,
            "distribution": {"kind": "constant", "value": 2},
        }

    gen = _gen(_setup(tmp_path, mutate))
    specs = [gen.next() for _ in range(40)]
    assert all(specs[i].role == specs[i - 2].role for i in range(2, 40))


def test_the_role_is_part_of_the_requests_identity() -> None:
    a = request_mix.RequestSpec("s", "t", ("c",), 1, False, role="org_admin")
    assert a != request_mix.RequestSpec("s", "t", ("c",), 1, False, role="analyst")


def test_the_role_does_not_change_the_text_sent(tmp_path: Path) -> None:
    r = request_render.Renderer(_setup(tmp_path, _mix))
    base = request_mix.RequestSpec("bench-postgresql", "orders", ("order_id",), 1, False)
    assert (
        r.render(base).sql
        == r.render(request_mix.RequestSpec(**{**base.__dict__, "role": "analyst"})).sql
    )


# ------------------------------------------------------------------ the clients send each request as its role


@pytest.fixture
def wire(monkeypatch: pytest.MonkeyPatch) -> dict[str, list[Any]]:
    log: dict[str, list[Any]] = {
        "http": [],
        "pgwire_connect": [],
        "pgwire": [],
        "flight": [],
        "bolt_auth": [],
        "grpc": [],
    }
    real = httpx.Client

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content) if request.content else None
        log["http"].append((request.url.path, request.headers.get("x-provisa-role"), body))
        path = request.url.path
        if path.endswith("/graphql"):
            return httpx.Response(200, json={"data": {"f": [{"a": 1}]}})
        if path.endswith("/sql"):
            return httpx.Response(200, json={"data": {"t": [[1]]}, "columns": ["a"]})
        if path.endswith("/cypher"):
            return httpx.Response(200, json={"rows": [[1]], "columns": ["a"]})
        if "jsonapi" in path:
            return httpx.Response(200, json={"data": [{"attributes": {"a": 1}}]})
        return httpx.Response(200, json={"data": [{"a": 1}]})

    monkeypatch.setattr(
        httpx, "Client", lambda **kw: real(transport=httpx.MockTransport(handler), **kw)
    )

    import neo4j
    import psycopg
    import pyarrow.flight as fl

    class Conn:
        def __init__(self, user: str) -> None:
            self.user = user

        def execute(self, sql: str) -> Any:
            log["pgwire"].append((self.user, sql))
            return SimpleNamespace(fetchall=lambda: [(1,)], description=[("a",)])

        def close(self) -> None:
            pass

    def connect(**kw: Any) -> Conn:
        log["pgwire_connect"].append(kw["user"])
        return Conn(kw["user"])

    monkeypatch.setattr(psycopg, "connect", connect)

    class Flight:
        def __init__(self, location: str) -> None:
            pass

        def do_get(self, ticket: Any, options: Any = None) -> Any:
            log["flight"].append(json.loads(ticket.ticket)["role"])
            return SimpleNamespace(read_all=lambda: pa.table({"a": [1]}))

        def close(self) -> None:
            pass

    monkeypatch.setattr(fl, "FlightClient", Flight)

    class Session:
        def __init__(self, user: str) -> None:
            self.user = user

        def run(self, text: str) -> Any:
            log["bolt"] = log.get("bolt", []) + [(self.user, text)]
            return [SimpleNamespace(keys=lambda: ["a"])]

        def close(self) -> None:
            pass

    class Driver:
        def __init__(self, user: str) -> None:
            self.user = user

        def session(self) -> Session:
            return Session(self.user)

        def close(self) -> None:
            pass

    def driver(uri: str, auth: tuple[str, str]) -> Driver:
        log["bolt_auth"].append(auth[0])
        return Driver(auth[0])

    monkeypatch.setattr(neo4j.GraphDatabase, "driver", driver)

    class Msg:
        def __init__(self, limit: int) -> None:
            self.limit = limit
            self.read_mask = SimpleNamespace(paths=[])
            self.filter = SimpleNamespace(CopyFrom=lambda m: None)

        SerializeToString = FromString = None  # noqa: N815

        def ListFields(self) -> list[int]:  # noqa: N802
            return [1]

    class Grpc:
        _channel = SimpleNamespace(
            unary_stream=lambda path, **kw: (
                lambda req, metadata: log["grpc"].append(list(metadata)) or [req]
            )
        )

        def _resolve_message_class(self, name: str) -> Any:
            return Msg

    monkeypatch.setattr(ol, "_grpc_shared", Grpc())
    return log


def _client(transport: str, setup: contract_model.Setup) -> Any:
    return ol.CLIENTS[transport](ol.endpoints_from_setup(setup))


def _q(setup: contract_model.Setup, transport: str) -> Any:
    return request_render.build_query(setup, transport, cached=False)


def test_http_sends_the_requests_role(tmp_path: Path, wire: dict[str, list[Any]]) -> None:
    setup = _setup(tmp_path, _mix)
    for transport in ("graphql", "data_sql", "cypher_http", "rest", "jsonapi"):
        wire["http"].clear()
        client = _client(transport, setup)
        client.call(_q(setup, transport), "analyst")
        client.call(_q(setup, transport), "org_admin")
        assert [h[1] for h in wire["http"]] == ["analyst", "org_admin"], transport
    wire["http"].clear()
    _client("data_sql", setup).call(_q(setup, "data_sql"), "analyst")
    assert wire["http"][0][2]["role"] == "analyst"


def test_pgwire_opens_one_connection_per_role(tmp_path: Path, wire: dict[str, list[Any]]) -> None:
    setup = _setup(tmp_path, _mix)
    client = _client("pgwire", setup)
    q = _q(setup, "pgwire")
    for role in ("analyst", "org_admin", "analyst", "analyst", "org_admin"):
        client.call(q, role)
    assert wire["pgwire_connect"] == ["analyst", "org_admin"]
    assert [u for u, _ in wire["pgwire"]] == [
        "analyst",
        "org_admin",
        "analyst",
        "analyst",
        "org_admin",
    ]


def test_flight_ticket_carries_the_role(tmp_path: Path, wire: dict[str, list[Any]]) -> None:
    setup = _setup(tmp_path, _mix)
    client = _client("flight_sql", setup)
    client.call(_q(setup, "flight_sql"), "analyst")
    client.call(_q(setup, "flight_sql"), "org_admin")
    assert wire["flight"] == ["analyst", "org_admin"]


def test_bolt_opens_one_driver_per_role(tmp_path: Path, wire: dict[str, list[Any]]) -> None:
    setup = _setup(tmp_path, _mix)
    client = _client("bolt", setup)
    q = _q(setup, "bolt")
    for role in ("analyst", "org_admin", "analyst"):
        client.call(q, role)
    assert wire["bolt_auth"] == ["analyst", "org_admin"]
    assert [u for u, _ in wire["bolt"]] == ["analyst", "org_admin", "analyst"]


def test_grpc_metadata_carries_the_role(tmp_path: Path, wire: dict[str, list[Any]]) -> None:
    setup = _setup(tmp_path, _mix)
    client = _client("grpc", setup)
    client.call(_q(setup, "grpc"), "analyst")
    client.call(_q(setup, "grpc"), "org_admin")
    assert [m[0] for m in wire["grpc"]] == [
        ("x-provisa-role", "analyst"),
        ("x-provisa-role", "org_admin"),
    ]


# ------------------------------------------------------------------ through the client loop


class _Recorder:
    last: Any = None

    def __init__(self, ep: Any) -> None:
        self.sent: list[tuple[Any, str]] = []
        _Recorder.last = self

    def call(self, query: Any, role: str) -> tuple[int, int, bool | None]:
        self.sent.append((query, role))
        return 1, 1, None

    def close(self) -> None:
        pass


def test_the_loop_sends_each_request_as_its_role_and_warms_up_every_role(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setitem(ol.CLIENTS, "graphql", _Recorder)
    ep = ol.endpoints_from_setup(_setup(tmp_path, _mix))
    go = threading.Event()
    go.set()
    ready: queue.Queue = queue.Queue()
    out: queue.Queue = queue.Queue()
    ol._client_process("graphql", ep, 0, 1, 0.2, ready, go, out)  # noqa: SLF001
    assert ready.get() is None
    records = out.get()["records"]
    sent = _Recorder.last.sent
    warm = len(sent) - len(records)
    assert {role for _, role in sent[:warm]} == {"org_admin", "analyst"}  # every role warmed
    assert [role for _, role in sent[warm:]] == [r[1].role for r in records]
    assert {r[1].role for r in records} == {"org_admin", "analyst"}


# ------------------------------------------------------------------ the report


def _rec(latency: float, role: str) -> ol.Record:
    return (latency, request_mix.RequestSpec("s", "t", ("a",), 1, False, role=role), None, 1, 1)


def test_summary_by_role() -> None:
    out = ol.summarize_requests(
        [_rec(0.001, "org_admin")] * 7 + [_rec(0.004, "analyst")] * 3, window_s=1.0
    )
    assert list(out["by_role"]) == ["analyst", "org_admin"]
    assert out["by_role"]["analyst"]["p50_ms"] == 4.0
    assert out["by_role"]["org_admin"]["requests"] == 7
