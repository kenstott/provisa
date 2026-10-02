# Copyright (c) 2026 Kenneth Stott
# Canary: 6b1f8d34-2a7e-4c95-8d03-e4a9c7f1b256
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A role's row filter decides the ROWS it gets on every transport.

One real server, one table of four orders in two regions, three roles:

* ``org_admin`` — no row filter: every order;
* ``east_only`` — ``region = 'east'``: exactly org_admin's east orders;
* ``analyst`` — ``region = current_setting('provisa.user_region')``: the session variable is
  bound per request (REQ-1682); bound nowhere it resolves to NULL and the filter matches no row.

Each transport is read as each role and the assertion is on the rows that came back, never on a
status code: a filtered role must not see the unfiltered table on any way in, and a read as the
unbound analyst right after a read that returned rows must still be empty (no kept plan and no
cached result carries one role's rows to another)."""

# Requirements: REQ-1682, REQ-041

from __future__ import annotations

import asyncio
import json
import os
import re
import urllib.error
import urllib.request

import pytest
import sqlalchemy as sa

from tests.integration.worker_boot_harness import WorkerBoot, _config

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = int(os.environ.get("PG_PORT", "5432"))
_ROLES = ["org_admin", "analyst", "east_only"]
_EAST = {1, 3}
_ALL = {1, 2, 3, 4}


def _http(boot, role: str, method: str, path: str, body: dict | None = None, headers=None):
    req = urllib.request.Request(
        f"http://127.0.0.1:{boot.ports['http']}{path}",
        data=None if body is None else json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "x-provisa-role": role, **(headers or {})},
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode()


def _ok(status: int, body: str) -> dict:
    assert status == 200, f"{status} {body}"
    return json.loads(body)


def _graphql(boot, role: str, headers=None) -> set[int]:
    out = _ok(
        *_http(boot, role, "POST", "/data/graphql", {"query": "{ s__orders { id } }"}, headers)
    )
    return {r["id"] for r in out["data"]["s__orders"]}


def _sql_http(boot, role: str, headers=None) -> set[int]:
    out = _ok(
        *_http(boot, role, "POST", "/data/sql", {"sql": "SELECT id FROM sales.orders"}, headers)
    )
    return {r["id"] for r in out["data"]["sql"]}


def _rest(boot, role: str, headers=None) -> set[int]:
    out = _ok(*_http(boot, role, "GET", "/data/rest/sales/orders", None, headers))
    return {r["id"] for r in out["data"]}


def _jsonapi(boot, role: str, headers=None) -> set[int]:
    out = _ok(*_http(boot, role, "GET", "/data/jsonapi/sales/orders", None, headers))
    return {int(r["id"]) for r in out["data"]}  # a resource's id is its own member


def _cypher_http(boot, role: str, headers=None) -> set[int]:
    query = "MATCH (n:Orders) RETURN n.id AS id"
    out = _ok(*_http(boot, role, "POST", "/data/cypher", {"query": query}, headers))
    return {r["id"] for r in out["rows"]}


def _pgwire(boot, role: str) -> set[int]:
    import psycopg

    with psycopg.connect(
        host="127.0.0.1",
        port=boot.ports["pgwire"],
        user=role,
        password="provisa",
        dbname="provisa",
        autocommit=True,
        connect_timeout=30,
    ) as conn:
        return {r[0] for r in conn.execute("SELECT id FROM sales.orders").fetchall()}


def _flight(boot, role: str) -> set[int]:
    import pyarrow.flight as fl

    client = fl.connect(f"grpc://127.0.0.1:{boot.ports['flight']}")
    try:
        ticket = fl.Ticket(
            json.dumps({"query": "SELECT id FROM sales.orders", "role": role}).encode()
        )
        return set(client.do_get(ticket).read_all().column("id").to_pylist())
    finally:
        client.close()


def _bolt(boot, role: str) -> set[int]:
    from neo4j import GraphDatabase

    driver = GraphDatabase.driver(f"bolt://127.0.0.1:{boot.ports['bolt']}", auth=(role, ""))
    try:
        with driver.session() as sess:
            return {rec["id"] for rec in sess.run("MATCH (n:Orders) RETURN n.id AS id")}
    finally:
        driver.close()


def _grpc(boot, role: str) -> set[int]:
    import grpc
    from google.protobuf.message_factory import GetMessageClass

    from tests.grpc_proto_client import role_descriptor_pool

    _pool, svc = role_descriptor_pool(f"http://127.0.0.1:{boot.ports['http']}", role)
    method = next(
        m
        for m in svc.methods
        if m.name.startswith("Query")
        and "orders" in m.name.lower()
        and not m.name.endswith(("Aggregate", "GroupBy", "Batch"))
    )
    req_cls = GetMessageClass(method.input_type)
    resp_cls = GetMessageClass(method.output_type)
    channel = grpc.insecure_channel(f"127.0.0.1:{boot.ports['grpc']}")
    try:
        rpc = channel.unary_stream(
            f"/{svc.full_name}/{method.name}",
            request_serializer=req_cls.SerializeToString,
            response_deserializer=resp_cls.FromString,
        )
        return {m.id for m in rpc(req_cls(), metadata=(("x-provisa-role", role),), timeout=120)}
    finally:
        channel.close()


def _mcp(boot, role: str) -> set[int]:
    from mcp import ClientSession
    from mcp.client.streamable_http import streamablehttp_client

    async def _call() -> set[int]:
        url = f"http://127.0.0.1:{boot.ports['mcp']}/mcp"
        async with streamablehttp_client(url) as (read, write, _):
            async with ClientSession(read, write) as session:
                await session.initialize()
                result = await session.call_tool(
                    "run_sql", {"sql": "SELECT id FROM sales.orders", "role": role, "limit": 50}
                )
                assert not result.isError, result.content
                text = "".join(getattr(c, "text", "") for c in result.content)
                return {int(n) for n in re.findall(r'"id":\s*(\d+)', text)}

    return asyncio.run(_call())


_TRANSPORTS = {
    "graphql": _graphql,
    "sql_http": _sql_http,
    "rest": _rest,
    "jsonapi": _jsonapi,
    "cypher_http": _cypher_http,
    "pgwire": _pgwire,
    "flight": _flight,
    "bolt": _bolt,
    "grpc": _grpc,
    "mcp": _mcp,
}
# Where an unsecured deployment binds a session variable for one request: a header.
_HEADER_BOUND = ("graphql", "sql_http", "rest", "jsonapi", "cypher_http")


@pytest.fixture(scope="module")
def server():
    base = _config(_PG_HOST, _PG_PORT, "unused")
    orders = base["tables"][0]
    for column in orders["columns"]:
        column["visible_to"] = _ROLES
    boot = WorkerBoot(
        1,
        pg_host=_PG_HOST,
        pg_port=_PG_PORT,
        extra_config={
            "tables": [orders],
            "roles": base["roles"]
            + [{"id": "east_only", "capabilities": ["query_development"], "domain_access": ["*"]}],
            "rls_rules": [
                {
                    "table_id": "orders",
                    "role_id": "analyst",
                    "filter": "region = current_setting('provisa.user_region')",
                },
                {"table_id": "orders", "role_id": "east_only", "filter": "region = 'east'"},
            ],
        },
        env={"PROVISA_REDIRECT_ENABLED": "false"},
    )
    boot.create_database()
    try:
        own = sa.create_engine(boot.url, isolation_level="AUTOCOMMIT")
        with own.connect() as conn:
            conn.execute(sa.text("INSERT INTO public.orders VALUES (3, 'east'), (4, 'west')"))
        own.dispose()
        boot.start()
        boot.wait_all_ready(timeout=300)
        yield boot
    finally:
        boot.cleanup()


@pytest.mark.parametrize("transport", list(_TRANSPORTS))
def test_each_role_gets_exactly_its_filtered_rows(server, transport):
    read = _TRANSPORTS[transport]
    assert read(server, "org_admin") == _ALL
    assert read(server, "east_only") == _EAST  # exactly org_admin's east orders
    assert read(server, "analyst") == set()  # the variable is bound nowhere: no row


@pytest.mark.parametrize("transport", list(_TRANSPORTS))
def test_an_unbound_analyst_stays_empty_after_reads_that_returned_rows(server, transport):
    read = _TRANSPORTS[transport]
    assert read(server, "org_admin") == _ALL
    assert read(server, "analyst") == set()
    assert read(server, "east_only") == _EAST
    assert read(server, "analyst") == set()


@pytest.mark.parametrize("transport", _HEADER_BOUND)
def test_a_region_bound_for_one_request_filters_that_request_only(server, transport):
    read = _TRANSPORTS[transport]
    bound = {"x-provisa-session-user_region": "east"}
    assert read(server, "analyst", bound) == _EAST
    assert read(server, "analyst") == set()
    assert read(server, "analyst", {"x-provisa-session-user_region": "west"}) == _ALL - _EAST
