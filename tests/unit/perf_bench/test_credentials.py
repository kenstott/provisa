# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""REQ-1911: a deployment with auth. The env var holds the secret, the contract names the user.

How each transport authenticates (provisa/auth/providers/simple.py, middleware.py, grpc/auth.py,
api/flight/server.py, bolt/session.py): HTTP, gRPC and Flight take a bearer token (the simple
provider's ``POST /auth/login`` mints a 30-minute JWT from username and password); pgwire and Bolt
take the username and password directly.

FIXTURE: the login endpoint and the wire libraries are fakes here; the real provider is exercised by
the local integration run."""

from __future__ import annotations

import json
import sys
import threading
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
import yaml

BENCH = Path(__file__).resolve().parents[3] / "demo" / "named" / "perf" / "bench"
sys.path.insert(0, str(BENCH))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import credential as cr  # noqa: E402
import deployment_fixture as fx  # noqa: E402
import optimistic_load as ol  # noqa: E402
import request_render  # noqa: E402
import setup_contract as sc  # noqa: E402
from contract_model import SetupError  # noqa: E402

PERF_CONTRACT = BENCH / "setups" / "perf-stack.yaml"
ENV = {"PROVISA_HTTP_BASE_URL": "http://localhost:8001", "BENCH_SECRET": "pw-1"}
TRANSPORTS = list(ol.TRANSPORTS)


def _creds(**decl: Any) -> Any:
    def mutate(raw: dict[str, Any]) -> None:
        raw["deployment"]["credentials"] = decl

    return mutate


def _setup(mutate: Any = None, environ: dict[str, str] | None = None) -> sc.Setup:
    raw = yaml.safe_load(PERF_CONTRACT.read_text())
    if mutate:
        mutate(raw)
    return sc.setup_from_dict(
        raw, environ=environ or ENV, known_transports=TRANSPORTS, deployment=fx.build()
    )


PASSWORD = _creds(mode="env", kind="password", user="alice", env="BENCH_SECRET")
TOKEN = _creds(mode="env", kind="token", env="BENCH_SECRET")


# ------------------------------------------------------------------ the contract


def test_password_credentials_name_the_user_and_the_variable() -> None:
    c = _setup(PASSWORD).credentials
    assert (c.mode, c.kind, c.user, c.env) == ("env", "password", "alice", "BENCH_SECRET")


def test_a_token_needs_no_user() -> None:
    c = _setup(TOKEN).credentials
    assert (c.kind, c.user) == ("token", None)


def test_none_mode_carries_nothing() -> None:
    c = _setup().credentials
    assert (c.mode, c.kind, c.user, c.env) == ("none", None, None, None)


@pytest.mark.parametrize(
    "decl,msg",
    [
        ({"mode": "env", "env": "BENCH_SECRET"}, "deployment.credentials.kind: required"),
        (
            {"mode": "env", "kind": "cookie", "env": "BENCH_SECRET"},
            "kind: must be one of password, token",
        ),
        (
            {"mode": "env", "kind": "password", "env": "BENCH_SECRET"},
            "deployment.credentials.user: required",
        ),
        (
            {
                "mode": "env",
                "kind": "password",
                "user": "a",
                "env": "BENCH_SECRET",
                "password": "x",
            },
            "unknown key 'password'",
        ),
        ({"mode": "none", "user": "a"}, "unknown key 'user'"),
        (
            {"mode": "env", "kind": "token", "env": "NOT_SET"},
            "environment variable NOT_SET is not set",
        ),
    ],
)
def test_credentials_values(decl: dict, msg: str) -> None:
    with pytest.raises(SetupError, match=msg):
        _setup(_creds(**decl))


# ------------------------------------------------------------------ the credential


class FakeLogin:
    def __init__(self, refuse: bool = False) -> None:
        self.calls: list[dict[str, Any]] = []
        self.refuse = refuse

    def __call__(self, url: str, json: dict[str, Any], timeout: float) -> httpx.Response:
        req = httpx.Request("POST", url)
        self.calls.append({"url": url, **json})
        if self.refuse:
            return httpx.Response(401, json={"detail": "Invalid credentials"}, request=req)
        return httpx.Response(
            200,
            json={"access_token": f"jwt-{len(self.calls)}", "token_type": "bearer"},
            request=req,
        )


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> dict[str, float]:
    t = {"now": 1000.0}
    monkeypatch.setattr(cr, "_now", lambda: t["now"])
    monkeypatch.setattr(cr, "_TOKENS", {})
    return t


def _password(base: str = "http://x") -> cr.Credential:
    return cr.Credential(kind="password", user="alice", secret="pw-1", base_url=base)


def test_a_password_logs_in_once_and_presents_the_token(
    monkeypatch: pytest.MonkeyPatch, clock: dict[str, float]
) -> None:
    login = FakeLogin()
    monkeypatch.setattr(httpx, "post", login)
    c = _password()
    assert c.bearer() == "jwt-1" and c.bearer() == "jwt-1"
    assert login.calls == [{"url": "http://x/auth/login", "username": "alice", "password": "pw-1"}]


def test_the_token_is_renewed_before_it_expires(
    monkeypatch: pytest.MonkeyPatch, clock: dict[str, float]
) -> None:
    login = FakeLogin()
    monkeypatch.setattr(httpx, "post", login)
    c = _password()
    first = c.bearer()
    clock["now"] += cr.REFRESH_AFTER_S - 1
    assert c.bearer() == first
    clock["now"] += 2
    assert c.bearer() == "jwt-2"


def test_concurrent_callers_log_in_once(
    monkeypatch: pytest.MonkeyPatch, clock: dict[str, float]
) -> None:
    login = FakeLogin()
    monkeypatch.setattr(httpx, "post", login)
    c, got = _password(), []
    threads = [threading.Thread(target=lambda: got.append(c.bearer())) for _ in range(16)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert set(got) == {"jwt-1"} and len(login.calls) == 1


def test_a_refused_login_is_named(monkeypatch: pytest.MonkeyPatch, clock: dict[str, float]) -> None:
    monkeypatch.setattr(httpx, "post", FakeLogin(refuse=True))
    with pytest.raises(
        cr.CredentialError, match="login as alice refused by http://x/auth/login: 401"
    ):
        _password().bearer()


def test_a_token_is_presented_as_it_is() -> None:
    c = cr.Credential(kind="token", user=None, secret="pat-1", base_url="http://x")
    assert c.bearer() == "pat-1"


def test_pgwire_and_bolt_take_the_user_and_the_secret() -> None:
    assert _password().basic("org_admin") == ("alice", "pw-1")
    token = cr.Credential(kind="token", user=None, secret="pat-1", base_url="http://x")
    assert token.basic("org_admin") == (
        "org_admin",
        "pat-1",
    )  # no principal named: the role is the user


# ------------------------------------------------------------------ the endpoints carry it


def test_endpoints_carry_a_credential_built_from_the_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for k, v in ENV.items():
        monkeypatch.setenv(k, v)
    ep = ol.endpoints_from_setup(_setup(PASSWORD))
    assert ep.credential == cr.Credential(
        kind="password", user="alice", secret="pw-1", base_url="http://localhost:8001"
    )
    assert ol.endpoints_from_setup(_setup()).credential is None


# ------------------------------------------------------------------ every transport presents it


@pytest.fixture
def wire(monkeypatch: pytest.MonkeyPatch, clock: dict[str, float]) -> dict[str, list[Any]]:
    log: dict[str, list[Any]] = {"http": [], "pg": [], "bolt": [], "flight": [], "grpc": []}
    monkeypatch.setattr(httpx, "post", FakeLogin())
    real = httpx.Client

    def handler(request: httpx.Request) -> httpx.Response:
        log["http"].append(request.headers.get("authorization"))
        p = request.url.path
        if p.endswith("/graphql"):
            return httpx.Response(200, json={"data": {"f": [{"a": 1}]}})
        if p.endswith("/sql"):
            return httpx.Response(200, json={"data": {"t": [[1]]}, "columns": ["a"]})
        if p.endswith("/cypher"):
            return httpx.Response(200, json={"rows": [[1]], "columns": ["a"]})
        if "jsonapi" in p:
            return httpx.Response(200, json={"data": [{"attributes": {"a": 1}}]})
        return httpx.Response(200, json={"data": [{"a": 1}]})

    monkeypatch.setattr(
        httpx, "Client", lambda **kw: real(transport=httpx.MockTransport(handler), **kw)
    )

    import neo4j
    import psycopg
    import pyarrow as pa
    import pyarrow.flight as fl

    class Conn:
        def execute(self, sql: str) -> Any:
            return SimpleNamespace(fetchall=lambda: [(1,)], description=[("a",)])

        def close(self) -> None:
            pass

    monkeypatch.setattr(
        psycopg, "connect", lambda **kw: log["pg"].append((kw["user"], kw["password"])) or Conn()
    )

    class Flight:
        def __init__(self, location: str) -> None:
            pass

        def do_get(self, ticket: Any, options: Any = None) -> Any:
            log["flight"].append(json.loads(ticket.ticket))
            return SimpleNamespace(read_all=lambda: pa.table({"a": [1]}))

        def close(self) -> None:
            pass

    monkeypatch.setattr(fl, "FlightClient", Flight)
    monkeypatch.setattr(fl, "Ticket", lambda raw: SimpleNamespace(ticket=raw))

    class Drv:
        def session(self) -> Any:
            return SimpleNamespace(
                run=lambda t: [SimpleNamespace(keys=lambda: ["a"])], close=lambda: None
            )

        def close(self) -> None:
            pass

    monkeypatch.setattr(
        neo4j.GraphDatabase, "driver", lambda uri, auth: log["bolt"].append(auth) or Drv()
    )
    monkeypatch.setattr(neo4j, "bearer_auth", lambda token: ("bearer", token))

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


def _client(transport: str, setup: sc.Setup, monkeypatch: pytest.MonkeyPatch) -> Any:
    for k, v in ENV.items():
        monkeypatch.setenv(k, v)
    return ol.CLIENTS[transport](ol.endpoints_from_setup(setup))


def _call(client: Any, setup: sc.Setup, transport: str) -> None:
    client.call(request_render.build_query(setup, transport, cached=False), "org_admin")


def test_http_presents_a_fresh_bearer_token_on_every_request(
    monkeypatch: pytest.MonkeyPatch, wire: dict[str, list[Any]], clock: dict[str, float]
) -> None:
    setup = _setup(PASSWORD)
    for transport in ("graphql", "data_sql", "cypher_http", "rest", "jsonapi"):
        wire["http"].clear()
        client = _client(transport, setup, monkeypatch)
        _call(client, setup, transport)
        clock["now"] += cr.REFRESH_AFTER_S + 1
        _call(client, setup, transport)
        assert wire["http"][0] != wire["http"][1] and all(
            h.startswith("Bearer jwt-") for h in wire["http"]
        ), transport


def test_grpc_presents_the_token_on_each_call_and_does_not_cache_it(
    monkeypatch: pytest.MonkeyPatch, wire: dict[str, list[Any]], clock: dict[str, float]
) -> None:
    setup = _setup(PASSWORD)
    client = _client("grpc", setup, monkeypatch)
    _call(client, setup, "grpc")
    clock["now"] += cr.REFRESH_AFTER_S + 1
    _call(client, setup, "grpc")
    first, second = ([v for k, v in m if k == "authorization"] for m in wire["grpc"])
    assert first != second and first[0].startswith("Bearer jwt-")


def test_flight_presents_the_token_in_the_ticket(
    monkeypatch: pytest.MonkeyPatch, wire: dict[str, list[Any]], clock: dict[str, float]
) -> None:
    setup = _setup(PASSWORD)
    client = _client("flight_sql", setup, monkeypatch)
    _call(client, setup, "flight_sql")
    # the server reads the credential from the ticket (flight/server.py do_get: request["token"])
    assert wire["flight"][0]["token"].startswith("jwt-")


def test_pgwire_connects_as_the_user_with_the_secret(
    monkeypatch: pytest.MonkeyPatch, wire: dict[str, list[Any]]
) -> None:
    setup = _setup(PASSWORD)
    _call(_client("pgwire", setup, monkeypatch), setup, "pgwire")
    assert wire["pg"] == [("alice", "pw-1")]


def test_bolt_authenticates_as_the_user_with_the_secret(
    monkeypatch: pytest.MonkeyPatch, wire: dict[str, list[Any]]
) -> None:
    setup = _setup(PASSWORD)
    _call(_client("bolt", setup, monkeypatch), setup, "bolt")
    assert wire["bolt"] == [("alice", "pw-1")]


def test_bolt_presents_a_token_as_a_bearer(
    monkeypatch: pytest.MonkeyPatch, wire: dict[str, list[Any]]
) -> None:
    setup = _setup(TOKEN)
    _call(_client("bolt", setup, monkeypatch), setup, "bolt")
    assert wire["bolt"] == [("bearer", "pw-1")]


def test_no_credentials_sends_none(
    monkeypatch: pytest.MonkeyPatch, wire: dict[str, list[Any]]
) -> None:
    setup = _setup()
    _call(_client("graphql", setup, monkeypatch), setup, "graphql")
    _call(_client("pgwire", setup, monkeypatch), setup, "pgwire")
    assert wire["http"] == [None] and wire["pg"] == [("org_admin", "unused")]
