# Copyright (c) 2026 Kenneth Stott
# Canary: 1e8b4c6d-93a7-4f25-b0d1-7c2a9e5f3b48
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every transport ends a request at its own request timeout and names it (REQ-1905).

One server, one table whose scan the test can make slow (a view over ``pg_sleep``), and a 2 s
request timeout. On each of the ten transports the scan ends at about 2 s with an error that
names the transport and the operator setting the timeout comes from, and the next request on
that transport is served."""

# Requirements: REQ-1905

from __future__ import annotations

import asyncio
import json
import os
import time
import urllib.error
import urllib.request

import pytest
import sqlalchemy as sa

from tests.integration.worker_boot_harness import WorkerBoot, _config

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = int(os.environ.get("PG_PORT", "5432"))
_ROLE = "org_admin"
_TIMEOUT_S = 2
# Sooner than this the request was not held to its timeout at all; later, it outran it.
_BOUNDS = (1.5, 8.0)
_DEFAULT = "limits.request_timeout"


def _http(boot, method: str, path: str, body: dict | None = None) -> tuple[int, str]:
    req = urllib.request.Request(
        f"http://127.0.0.1:{boot.ports['http']}{path}",
        data=None if body is None else json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "x-provisa-role": _ROLE},
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode()


def _graphql(boot, table: str) -> tuple[bool, str]:
    status, body = _http(boot, "POST", "/data/graphql", {"query": f"{{ s__{table} {{ id }} }}"})
    return status == 200, f"{status} {body}"


def _sql_http(boot, table: str) -> tuple[bool, str]:
    status, body = _http(boot, "POST", "/data/sql", {"sql": f"SELECT id FROM sales.{table}"})
    return status == 200, f"{status} {body}"


def _rest(boot, table: str) -> tuple[bool, str]:
    status, body = _http(boot, "GET", f"/data/rest/sales/{table}")
    return status == 200, f"{status} {body}"


def _jsonapi(boot, table: str) -> tuple[bool, str]:
    status, body = _http(boot, "GET", f"/data/jsonapi/sales/{table}")
    return status == 200, f"{status} {body}"


def _cypher_http(boot, table: str) -> tuple[bool, str]:
    query = f"MATCH (n:{table.capitalize()}) RETURN n.id AS id"
    status, body = _http(boot, "POST", "/data/cypher", {"query": query})
    return status == 200, f"{status} {body}"


def _pgwire(boot, table: str) -> tuple[bool, str]:
    import psycopg

    try:
        with psycopg.connect(
            host="127.0.0.1",
            port=boot.ports["pgwire"],
            user=_ROLE,
            password="provisa",
            dbname="provisa",
            autocommit=True,
            connect_timeout=30,
        ) as conn:
            rows = conn.execute(f"SELECT id FROM sales.{table}").fetchall()  # noqa: S608
            # The connection is still usable after a statement that timed out.
            return True, f"{len(rows)} rows"
    except psycopg.Error as exc:
        return False, str(exc)


def _flight(boot, table: str) -> tuple[bool, str]:
    import pyarrow.flight as fl

    client = fl.connect(f"grpc://127.0.0.1:{boot.ports['flight']}")
    try:
        ticket = fl.Ticket(
            json.dumps({"query": f"SELECT id FROM sales.{table}", "role": _ROLE}).encode()
        )
        return True, f"{client.do_get(ticket).read_all().num_rows} rows"
    except fl.FlightError as exc:
        return False, str(exc)
    finally:
        client.close()


def _bolt(boot, table: str) -> tuple[bool, str]:
    from neo4j import GraphDatabase
    from neo4j.exceptions import Neo4jError

    driver = GraphDatabase.driver(f"bolt://127.0.0.1:{boot.ports['bolt']}", auth=(_ROLE, ""))
    try:
        with driver.session() as sess:
            query = f"MATCH (n:{table.capitalize()}) RETURN n.id AS id"
            return True, f"{len(list(sess.run(query)))} rows"
    except Neo4jError as exc:
        return False, f"{exc.code} {exc.message}"
    finally:
        driver.close()


def _grpc(boot, table: str) -> tuple[bool, str]:
    import grpc
    from google.protobuf.message_factory import GetMessageClass

    from tests.grpc_proto_client import role_descriptor_pool

    _pool, svc = role_descriptor_pool(f"http://127.0.0.1:{boot.ports['http']}", _ROLE)
    method = next(
        m
        for m in svc.methods
        if m.name.startswith("Query")
        and table in m.name.lower()
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
        rows = list(rpc(req_cls(), metadata=(("x-provisa-role", _ROLE),), timeout=120))
        return True, f"{len(rows)} rows"
    except grpc.RpcError as exc:
        return False, f"{exc.code().name} {exc.details()}"  # type: ignore[attr-defined]
    finally:
        channel.close()


def _mcp(boot, table: str) -> tuple[bool, str]:
    from mcp import ClientSession
    from mcp.client.streamable_http import streamablehttp_client

    async def _call() -> tuple[bool, str]:
        url = f"http://127.0.0.1:{boot.ports['mcp']}/mcp"
        async with streamablehttp_client(url) as (read, write, _):
            async with ClientSession(read, write) as session:
                await session.initialize()
                result = await session.call_tool(
                    "run_sql", {"sql": f"SELECT id FROM sales.{table}", "role": _ROLE, "limit": 5}
                )
                return not result.isError, str(result.content)

    return asyncio.run(_call())


# transport -> (its client, the setting its timeout comes from in this test)
_TRANSPORTS = {
    "graphql": (_graphql, _DEFAULT),
    "sql_http": (_sql_http, _DEFAULT),
    "rest": (_rest, _DEFAULT),
    "jsonapi": (_jsonapi, _DEFAULT),
    "cypher_http": (_cypher_http, _DEFAULT),
    "pgwire": (_pgwire, "limits.request_timeouts.pgwire"),
    "flight": (_flight, "limits.request_timeouts.flight"),
    "bolt": (_bolt, _DEFAULT),
    "grpc": (_grpc, _DEFAULT),
    "mcp": (_mcp, _DEFAULT),
}


@pytest.fixture(scope="module")
def server():
    orders = _config(_PG_HOST, _PG_PORT, "unused")["tables"][0]
    boot = WorkerBoot(
        1,
        pg_host=_PG_HOST,
        pg_port=_PG_PORT,
        extra_config={"tables": [orders, {**orders, "table": "slow"}]},
        env={"PROVISA_REDIRECT_ENABLED": "false"},
    )
    boot.create_database()
    try:
        own = sa.create_engine(boot.url, isolation_level="AUTOCOMMIT")
        with own.connect() as conn:
            # A scan of ``slow`` takes 30 s on the source once the switch is on; until then it
            # is three rows at once, so the server's own start-up reads of it are not slowed.
            conn.execute(sa.text("CREATE TABLE public.slow_switch (seconds integer)"))
            conn.execute(sa.text("INSERT INTO public.slow_switch VALUES (0)"))
            conn.execute(
                sa.text(
                    "CREATE VIEW public.slow AS SELECT g AS id, 'r'::text AS region "
                    "FROM generate_series(1, 3) g, "
                    "(SELECT pg_sleep((SELECT seconds FROM public.slow_switch))) s"
                )
            )
        boot.start()
        boot.wait_all_ready(timeout=300)
        status, body = _http(
            boot,
            "PUT",
            "/admin/settings",
            {
                "limits": {
                    "request_timeout": _TIMEOUT_S,
                    "request_timeouts": {"pgwire": _TIMEOUT_S, "flight": _TIMEOUT_S},
                }
            },
        )
        assert status == 200, body
        with own.connect() as conn:
            conn.execute(sa.text("UPDATE public.slow_switch SET seconds = 30"))
        own.dispose()
        yield boot
    finally:
        boot.cleanup()


@pytest.mark.parametrize("transport", list(_TRANSPORTS))
def test_a_request_ends_at_its_transports_timeout_naming_it(server, transport):
    client, setting = _TRANSPORTS[transport]

    started = time.monotonic()
    served, answer = client(server, "slow")
    elapsed = time.monotonic() - started

    assert not served, f"{transport}: the 30 s scan was served under a 2 s timeout: {answer}"
    assert _BOUNDS[0] < elapsed < _BOUNDS[1], f"{transport}: ended after {elapsed:.1f}s: {answer}"
    assert transport in answer and setting in answer, f"{transport}: {answer}"
    assert f"{_TIMEOUT_S}s" in answer, f"{transport}: {answer}"

    # The transport keeps serving: the next request on it is answered.
    served, answer = client(server, "orders")
    assert served, f"{transport}: the request after the timeout failed: {answer}"
