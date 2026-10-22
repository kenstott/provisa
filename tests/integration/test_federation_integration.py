# Copyright (c) 2026 Kenneth Stott
# Canary: b07c8d9e-0f1a-2345-6789-012345678901
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: Provisa composed into a real Apollo Router supergraph as a subgraph (REQ-259).

A supergraph cannot pass its caller's identity, so the subgraph is an endpoint with a declared
role: the config's ``federation.role``. The router signs in as that role (here: the
``x-provisa-role`` header every subgraph request carries), and every entity it fetches from
Provisa -- ``_entities`` -- is governed as that role.

The supergraph is Provisa plus a second subgraph, ``reviews``, whose reviews each point at an
order by key and resolve none of its fields. Asking the router for a review's order makes it fetch
the order from Provisa's ``_entities``. The declared role ``partner`` has a row rule (no ``eu``
orders), a mask (``customer`` is ``***``) and a column it cannot see (``internal_note``); each is
checked to hold through the router.

Everything here is the ``test`` instance: a Postgres this module starts on a leased port (the
source, the engine and the control plane), an isolated Provisa server, the reviews subgraph in
this process, a composition run in a ``node`` container, and the router container -- all removed
at the end."""

# Requirements: REQ-259

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import threading
import time
from pathlib import Path

import httpx
import pytest
import yaml

pytestmark = [pytest.mark.integration]

_ROLE = "partner"
_ROUTER_IMAGE = "ghcr.io/apollographql/router:v1.57.1"
_NODE_IMAGE = "node:20.18.0-slim"
_COMPOSITION = ["@apollo/composition@2.9.3", "graphql@16.9.0"]

# id, region, amount, customer, internal_note
_ORDERS = [
    (1, "us", 100, "ada", "n1"),
    (2, "eu", 200, "bob", "n2"),
    (3, "us", 300, "cy", "n3"),
]
# review body -> the order it is about
_REVIEWS = [("fast", 1), ("late", 2), ("fine", 3)]

# On Linux (CI) the containers share the host's network, so they reach the servers this module
# binds on loopback directly; Docker Desktop reaches the host's loopback as host.docker.internal.
_HOST_NETWORK = sys.platform.startswith("linux")
_HOST_FROM_CONTAINER = "127.0.0.1" if _HOST_NETWORK else "host.docker.internal"


class _Postgres:
    def __init__(self) -> None:
        from tests.port_lease import lease_ports

        (self.port,) = lease_ports(1)
        self._name = f"provisa-itest-apollo-pg-{os.getpid()}"

    def url(self, driver: str = "") -> str:
        return f"postgresql{driver}://provisa:provisa@127.0.0.1:{self.port}/provisa"

    def start(self) -> None:
        import psycopg

        subprocess.run(
            ["docker", "run", "-d", "--rm", "--memory", "512m", "--name", self._name]
            + ["-e", "POSTGRES_USER=provisa", "-e", "POSTGRES_PASSWORD=provisa"]
            + ["-e", "POSTGRES_DB=provisa", "-p", f"127.0.0.1:{self.port}:5432", "postgres:16"],
            check=True,
            capture_output=True,
        )
        # The image restarts the server once after initdb: ready means it answers repeatedly.
        deadline, answered = time.monotonic() + 120, 0
        while answered < 3:
            try:
                psycopg.connect(self.url(), connect_timeout=2).close()
                answered += 1
            except psycopg.OperationalError:
                answered = 0
                if time.monotonic() > deadline:
                    raise
            time.sleep(1)
        with psycopg.connect(self.url(), autocommit=True) as conn:
            conn.execute(
                "CREATE TABLE public.orders (id integer PRIMARY KEY, region varchar,"
                " amount integer, customer varchar, internal_note varchar)"
            )
            with conn.cursor() as cur:
                cur.executemany("INSERT INTO public.orders VALUES (%s, %s, %s, %s, %s)", _ORDERS)

    def stop(self) -> None:
        subprocess.run(["docker", "rm", "-f", self._name], capture_output=True)


@pytest.fixture(scope="module")
def postgres():
    pg = _Postgres()
    try:
        pg.start()
        yield pg
    finally:
        pg.stop()


def _config(pg_port: int) -> dict:
    """The isolated server's config: the orders table, the partner role's row rule, mask and
    hidden column, and the subgraph declared as the partner."""
    both = ["org_admin", _ROLE]
    return {
        "federation_engine": "pg",
        "auth": {
            "provider": "none",
            "assignments_source": "provisa",
            "default_assignments": [{"domain_id": "*", "role_id": r} for r in both],
        },
        "naming": {"domain_prefix": False, "rules": []},
        "cache": {"enabled": False},
        "domains": [{"id": "sales", "description": "Apollo subgraph e2e"}],
        # org_admin is the reserved administrative role (REQ-1349): not declared.
        "roles": [
            {
                "id": _ROLE,
                "description": "The role the supergraph's router reads as.",
                "capabilities": ["query_development"],
                "domain_access": ["*"],
            }
        ],
        "sources": [
            {
                "id": "fed-pg",
                "type": "postgresql",
                "host": "127.0.0.1",
                "port": pg_port,
                "database": "provisa",
                "username": "provisa",
                "password": "${env:PG_PASSWORD}",
            }
        ],
        "tables": [
            {
                "source_id": "fed-pg",
                "domain_id": "sales",
                "schema": "public",
                "table": "orders",
                "columns": [
                    {
                        "name": "id",
                        "data_type": "integer",
                        "is_primary_key": True,
                        "visible_to": both,
                    },
                    {"name": "region", "data_type": "varchar", "visible_to": both},
                    {"name": "amount", "data_type": "integer", "visible_to": both},
                    {
                        "name": "customer",
                        "data_type": "varchar",
                        "visible_to": both,
                        "masks": [
                            {"to": ["org_admin"], "mask": None},
                            {"to": ["*"], "mask": {"type": "constant", "value": "***"}},
                        ],
                    },
                    {"name": "internal_note", "data_type": "varchar", "visible_to": ["org_admin"]},
                ],
            }
        ],
        "rls_rules": [
            {
                "description": "The partner reads no EU order.",
                "table_id": "orders",
                "role_id": _ROLE,
                "filter": "region <> 'eu'",
            }
        ],
        "federation": {"enabled": True, "role": _ROLE, "subgraph_name": "orders"},
    }


@pytest.fixture(scope="module")
def provisa(postgres, tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer

    cfg = _config(postgres.port)
    path = tmp_path_factory.mktemp("apollo_subgraph") / "config.yaml"
    path.write_text(yaml.safe_dump(cfg))
    control_plane = postgres.url("+psycopg")
    srv = IsolatedServer(
        "apollo_subgraph_e2e",
        engine="pg",
        config=str(path),
        control_plane="postgres",
        env={
            "PG_PASSWORD": "provisa",
            "PLATFORM_DATABASE_URL": control_plane,
            "TENANT_DATABASE_URL": control_plane,
            "PROVISA_MATERIALIZE_URL": postgres.url(),
            "PROVISA_REDIS_EMBEDDED": "1",
            "PROVISA_REDIRECT_ENABLED": "false",
        },
    )
    srv.start(timeout=240)
    try:
        yield srv
    finally:
        srv.stop_process()


def _graphql(provisa, role: str, query: str, variables: dict | None = None) -> httpx.Response:
    return httpx.post(
        f"{provisa.base_url}/data/graphql",
        json={"query": query, "variables": variables or {}},
        headers={"x-provisa-role": role},
        timeout=provisa.request_timeout + 30,
    )


@pytest.fixture(scope="module")
def provisa_sdl(provisa) -> str:
    resp = _graphql(provisa, _ROLE, "query { _service { sdl } }")
    assert resp.status_code == 200, resp.text
    return resp.json()["data"]["_service"]["sdl"]


@pytest.fixture(scope="module")
def order_type(provisa_sdl) -> tuple[str, str]:
    """The orders entity's type name and its key field's type, as the subgraph's SDL has them: the
    type the ``orders`` root field answers (the role reads other keyed tables too, the meta
    domain's among them)."""
    root = re.search(r"^  orders(?:\(.*?\))?: \[(\w+)!\]", provisa_sdl, re.M | re.S)
    assert root, provisa_sdl[-4000:]
    type_name = root.group(1)
    match = re.search(rf'type {type_name} @key\(fields: "id"\) \{{\n(.*?)\n\}}', provisa_sdl, re.S)
    assert match, f'{type_name} has no @key(fields: "id")'
    (id_type,) = re.findall(r"^\s+id: (\S+)$", match.group(1), re.M)
    return type_name, id_type


# -- the reviews subgraph: reviews that point at an order by key ------------------------------


def _reviews_sdl(type_name: str, id_type: str) -> str:
    return f"""
extend schema @link(url: "https://specs.apollo.dev/federation/v2.3", import: ["@key"])

type Query {{
  partnerReviews: [Review!]!
}}

type Review {{
  body: String!
  order: {type_name}
}}

type {type_name} @key(fields: "id", resolvable: false) {{
  id: {id_type}
}}
"""


def _reviews_app(type_name: str, id_type: str):
    from graphql import build_schema, graphql_sync
    from starlette.applications import Starlette
    from starlette.requests import Request
    from starlette.responses import JSONResponse
    from starlette.routing import Route

    schema = build_schema(
        f"type Query {{ partnerReviews: [Review!]! }}\n"
        f"type Review {{ body: String! order: {type_name} }}\n"
        f"type {type_name} {{ id: {id_type} }}"
    )
    root = {"partnerReviews": [{"body": b, "order": {"id": o}} for b, o in _REVIEWS]}

    async def endpoint(request: Request) -> JSONResponse:
        body = await request.json()
        result = graphql_sync(
            schema, body["query"], root_value=root, variable_values=body.get("variables")
        )
        payload: dict = {"data": result.data}
        if result.errors:
            payload["errors"] = [e.formatted for e in result.errors]
        return JSONResponse(payload)

    return Starlette(routes=[Route("/", endpoint, methods=["POST"])])


@pytest.fixture(scope="module")
def reviews(order_type):
    import uvicorn

    from tests.port_lease import lease_ports

    (port,) = lease_ports(1)
    server = uvicorn.Server(
        uvicorn.Config(_reviews_app(*order_type), host="127.0.0.1", port=port, log_level="warning")
    )
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 30
    while not server.started:
        assert time.monotonic() < deadline, "reviews subgraph did not start"
        time.sleep(0.1)
    try:
        yield port
    finally:
        server.should_exit = True
        thread.join(timeout=10)


# -- the supergraph: composed in a node container, served by the router container ---------------

_COMPOSE_JS = """
const fs = require("fs");
const { parse } = require("graphql");
const { composeServices } = require("@apollo/composition");
const subgraphs = JSON.parse(fs.readFileSync("subgraphs.json", "utf8"));
const parsed = subgraphs.map((s) => {
  try {
    return { name: s.name, url: s.url, typeDefs: parse(s.sdl) };
  } catch (e) {
    // Which subgraph's SDL graphql-js refused, where, and the lines around it.
    const lines = s.sdl.split("\\n");
    const at = (e.locations || [{ line: 1 }])[0].line;
    const near = lines.slice(Math.max(0, at - 8), at + 2).map((l, i) => `${Math.max(1, at - 7) + i}: ${l}`);
    console.error(`${s.name}: ${e.message} at ${JSON.stringify(e.locations)}\\n${near.join("\\n")}`);
    process.exit(1);
  }
});
const result = composeServices(parsed);
if (result.errors) {
  console.error(result.errors.map((e) => e.message).join("\\n"));
  process.exit(1);
}
fs.writeFileSync("supergraph.graphql", result.supergraphSdl);
"""


def _compose(workdir: Path, subgraphs: list[dict]) -> None:
    (workdir / "subgraphs.json").write_text(json.dumps(subgraphs))
    (workdir / "compose.js").write_text(_COMPOSE_JS)
    run = subprocess.run(
        ["docker", "run", "--rm", "-v", f"{workdir}:/w", "-w", "/w", "-e", "HOME=/tmp"]
        + ["--user", f"{os.getuid()}:{os.getgid()}", _NODE_IMAGE, "sh", "-c"]
        + [f"npm install --silent --no-save {' '.join(_COMPOSITION)} && node compose.js"],
        capture_output=True,
        text=True,
        timeout=600,
    )
    assert run.returncode == 0, f"composition failed:\n{run.stdout}\n{run.stderr}"


@pytest.fixture(scope="module")
def router(provisa, provisa_sdl, order_type, reviews, tmp_path_factory):
    from tests.port_lease import lease_ports

    workdir = tmp_path_factory.mktemp("supergraph")
    _compose(
        workdir,
        [
            {
                "name": "orders",
                "url": f"http://{_HOST_FROM_CONTAINER}:{provisa.http_port}/data/graphql",
                "sdl": provisa_sdl,
            },
            {
                "name": "reviews",
                "url": f"http://{_HOST_FROM_CONTAINER}:{reviews}/",
                "sdl": _reviews_sdl(*order_type),
            },
        ],
    )
    port, health = lease_ports(2)
    listen = f"127.0.0.1:{port}" if _HOST_NETWORK else "0.0.0.0:4000"
    health_listen = f"127.0.0.1:{health}" if _HOST_NETWORK else "0.0.0.0:8088"
    (workdir / "router.yaml").write_text(
        yaml.safe_dump(
            {
                "supergraph": {"listen": listen},
                "health_check": {"listen": health_listen},
                "include_subgraph_errors": {"all": True},
                # The router signs in as the subgraph's declared role, an ordinary client.
                "headers": {
                    "all": {"request": [{"insert": {"name": "x-provisa-role", "value": _ROLE}}]}
                },
            }
        )
    )
    name = f"provisa-itest-apollo-router-{os.getpid()}"
    network = ["--network", "host"] if _HOST_NETWORK else ["-p", f"127.0.0.1:{port}:4000"]
    subprocess.run(
        ["docker", "run", "-d", "--rm", "--name", name, *network]
        + ["-e", "APOLLO_TELEMETRY_DISABLED=true", "-v", f"{workdir}:/dist/config:ro"]
        + [_ROUTER_IMAGE, "--config", "/dist/config/router.yaml"]
        + ["--supergraph", "/dist/config/supergraph.graphql"],
        check=True,
        capture_output=True,
    )
    url = f"http://127.0.0.1:{port}/"
    try:
        deadline = time.monotonic() + 120
        while True:
            try:
                if httpx.post(url, json={"query": "{ __typename }"}, timeout=5).status_code == 200:
                    break
            except httpx.TransportError:
                pass
            if time.monotonic() > deadline:
                logs = subprocess.run(["docker", "logs", name], capture_output=True, text=True)
                pytest.fail(f"router did not start:\n{logs.stdout}\n{logs.stderr}")
            time.sleep(1)
        yield url
    finally:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True)


def _supergraph_reviews(router: str) -> dict:
    resp = httpx.post(
        router,
        json={"query": "{ partnerReviews { body order { id region amount customer } } }"},
        timeout=120,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert "data" in body, body
    return {r["body"]: r["order"] for r in body["data"]["partnerReviews"]}


# -- the subgraph as the declared role ---------------------------------------------------------


def test_sdl_keys_the_entity_by_its_designated_primary_key(provisa_sdl, order_type):
    type_name, _ = order_type
    assert f'type {type_name} @key(fields: "id")' in provisa_sdl
    assert "extend schema @link" in provisa_sdl


def test_sdl_is_the_declared_roles_visibility(provisa_sdl):
    assert "internal_note" not in provisa_sdl


def test_only_the_declared_role_is_served_as_the_subgraph(provisa):
    resp = _graphql(provisa, "org_admin", "query { _service { sdl } }")
    assert resp.status_code == 400, resp.text


def test_entities_hold_the_declared_roles_row_rule_and_mask(provisa, order_type):
    type_name, _ = order_type
    query = (
        "query ($r: [_Any!]!) { _entities(representations: $r) {"
        f" ... on {type_name} {{ id region customer }} }} }}"
    )
    reps = [{"__typename": type_name, "id": i} for i in (1, 2, 3)]
    resp = _graphql(provisa, _ROLE, query, {"r": reps})
    assert resp.status_code == 200, resp.text
    entities = resp.json()["data"]["_entities"]
    assert entities[0] == {"__typename": type_name, "id": 1, "region": "us", "customer": "***"}
    assert entities[1] is None  # the row rule: no EU order
    assert entities[2]["customer"] == "***"


# -- through the router ------------------------------------------------------------------------


def test_router_fetches_entities_as_the_declared_role(router):
    orders = _supergraph_reviews(router)
    assert orders["fast"] == {"id": 1, "region": "us", "amount": 100, "customer": "***"}
    assert orders["fine"] == {"id": 3, "region": "us", "amount": 300, "customer": "***"}


def test_router_answers_no_row_the_declared_role_cannot_read(router):
    late = _supergraph_reviews(router)["late"]
    # The EU order is no row of the partner's: nothing of it but the key the review holds.
    assert late is None or (late["region"] is None and late["amount"] is None), late


def test_router_never_carries_an_unmasked_value(router):
    text = json.dumps(_supergraph_reviews(router))
    for _, _, _, customer, _ in _ORDERS:
        assert customer not in text
    assert "eu" not in text and "200" not in text
