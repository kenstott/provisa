# Copyright (c) 2026 Kenneth Stott
# Canary: 1b9e4c70-8d25-4a63-9f07-c3e6a2d5b814
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The query complexity guard holds on every surface -- on a real server (REQ-1174).

One server, one table, two roles. ``analyst`` carries ``max_query_complexity``; ``org_admin``
carries none. The same over-limit statement is sent through each surface a client can send a
statement through -- SQL over HTTP, GraphQL, pgwire, Arrow Flight and MCP -- and each answers
the one refusal, with the score and the limit, in its own error shape. A statement within the
limit runs on every surface, and the role with no limit runs the over-limit statement."""

from __future__ import annotations

import asyncio
import json
import os
import urllib.error
import urllib.request

import psycopg
import pytest

from tests.integration.worker_boot_harness import WorkerBoot, _config

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = int(os.environ.get("PG_PORT", "5432"))
_LIMITED, _UNLIMITED = "analyst", "org_admin"
_LIMIT = 4

# 1 relation + 2 columns = 3: within the limit.
_NARROW = "SELECT id, region FROM sales.orders"
# 2 relations + 3 columns + 1 nested query = 6: over it.
_WIDE = "SELECT id, region FROM sales.orders WHERE id IN (SELECT id FROM sales.orders)"
_REFUSAL = f"query complexity 6 exceeds the role limit of {_LIMIT}"


@pytest.fixture(scope="module")
def server():
    roles = _config(_PG_HOST, _PG_PORT, "unused")["roles"]
    for role in roles:
        if role["id"] == _LIMITED:
            role["rate_limit"] = {"max_query_complexity": _LIMIT}
    boot = WorkerBoot(
        1,
        pg_host=_PG_HOST,
        pg_port=_PG_PORT,
        extra_config={"roles": roles},
        env={"PROVISA_REDIRECT_ENABLED": "false"},
    )
    boot.create_database()
    try:
        boot.start()
        boot.wait_all_ready(timeout=300)
        yield boot
    finally:
        boot.cleanup()


# --- clients ------------------------------------------------------------------------------------


def _http(boot, path: str, body: dict, role: str) -> tuple[int, dict]:
    req = urllib.request.Request(
        f"http://127.0.0.1:{boot.ports['http']}{path}",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "x-provisa-role": role},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())


def _pgwire(boot, role: str):
    return psycopg.connect(
        host="127.0.0.1",
        port=boot.ports["pgwire"],
        user=role,
        password="provisa",
        dbname="provisa",
        autocommit=True,
        connect_timeout=30,
    )


def _flight(boot, query: str, role: str):
    import pyarrow.flight as fl

    client = fl.connect(f"grpc://127.0.0.1:{boot.ports['flight']}")
    try:
        ticket = fl.Ticket(json.dumps({"query": query, "role": role}).encode())
        return client.do_get(ticket).read_all()
    finally:
        client.close()


def _mcp_run_sql(boot, sql: str, role: str) -> tuple[bool, str]:
    from mcp import ClientSession
    from mcp.client.streamable_http import streamablehttp_client

    async def _call() -> tuple[bool, str]:
        url = f"http://127.0.0.1:{boot.ports['mcp']}/mcp"
        async with streamablehttp_client(url) as (read, write, _):
            async with ClientSession(read, write) as session:
                await session.initialize()
                result = await session.call_tool("run_sql", {"sql": sql, "role": role, "limit": 50})
                return result.isError, "".join(getattr(c, "text", "") for c in result.content)

    return asyncio.run(_call())


def _orders_field(boot) -> str:
    """The GraphQL root field the orders table is exposed as, read from the schema."""
    status, body = _http(
        boot,
        "/data/graphql",
        {"query": "{ __schema { queryType { fields { name } } } }"},
        _UNLIMITED,
    )
    assert status == 200, body
    names = [f["name"] for f in body["data"]["__schema"]["queryType"]["fields"]]
    matches = [n for n in names if n.lower().endswith("orders")]
    assert len(matches) == 1, names
    return matches[0]


# --- SQL over HTTP ------------------------------------------------------------------------------


def test_sql_over_http_refuses_an_over_limit_statement_with_413(server):
    status, body = _http(server, "/data/sql", {"sql": _WIDE}, _LIMITED)
    assert status == 413, body
    assert _REFUSAL in body["detail"]
    assert "2 relations (0 remote), 0 joins, 3 columns, 1 nested queries" in body["detail"]
    assert body["code"] == "data.query_too_complex"
    assert body["params"] == {"score": 6, "limit": _LIMIT, "limit_of": "role"}


def test_sql_over_http_runs_a_statement_within_the_limit(server):
    status, body = _http(server, "/data/sql", {"sql": _NARROW}, _LIMITED)
    assert status == 200, body
    assert len(body["data"]["sql"]) >= 2


def test_a_role_with_no_limit_runs_the_same_statement(server):
    status, body = _http(server, "/data/sql", {"sql": _WIDE}, _UNLIMITED)
    assert status == 200, body


# --- GraphQL ------------------------------------------------------------------------------------


def test_graphql_refuses_an_over_limit_query_with_413(server):
    field = _orders_field(server)
    # One relation and two columns is within the limit; the same field asked for three times
    # over is three relations and six columns.
    within = f"{{ {field} {{ id region }} }}"
    status, body = _http(server, "/data/graphql", {"query": within}, _LIMITED)
    assert status == 200, body
    assert "errors" not in body, body

    selections = " ".join(f"a{i}: {field} {{ id region }}" for i in range(3))
    status, body = _http(server, "/data/graphql", {"query": f"{{ {selections} }}"}, _UNLIMITED)
    assert status == 200 and "errors" not in body, body


def test_graphql_one_field_over_the_limit_is_refused(server):
    field = _orders_field(server)
    # The limited role may not ask one field for more than the limit prices: its statement is
    # the relation plus every column selected, here pushed over by repeating the columns under
    # aliases.
    columns = " ".join(f"c{i}: id" for i in range(_LIMIT + 2))
    status, body = _http(
        server, "/data/graphql", {"query": f"{{ {field} {{ {columns} }} }}"}, _LIMITED
    )
    assert status == 413, body
    assert "query complexity" in body["detail"] and f"role limit of {_LIMIT}" in body["detail"]


# --- pgwire -------------------------------------------------------------------------------------


def test_pgwire_refuses_an_over_limit_statement_and_stays_usable(server):
    with _pgwire(server, _LIMITED) as conn:
        with pytest.raises(psycopg.Error) as raised:
            conn.execute(_WIDE)
        assert _REFUSAL in str(raised.value)
        assert len(conn.execute(_NARROW).fetchall()) >= 2


def test_pgwire_refuses_it_on_the_extended_protocol(server):
    with _pgwire(server, _LIMITED) as conn:
        with pytest.raises(psycopg.Error) as raised:
            conn.execute(_WIDE, prepare=True)
        assert _REFUSAL in str(raised.value)


# --- Arrow Flight -------------------------------------------------------------------------------


def test_flight_refuses_an_over_limit_statement(server):
    import pyarrow.flight as fl

    with pytest.raises(fl.FlightError) as raised:
        _flight(server, _WIDE, _LIMITED)
    assert _REFUSAL in str(raised.value)
    assert _flight(server, _NARROW, _LIMITED).num_rows >= 2
    assert _flight(server, _WIDE, _UNLIMITED).num_rows >= 2


# --- MCP ----------------------------------------------------------------------------------------


def test_mcp_run_sql_refuses_an_over_limit_statement(server):
    is_error, text = _mcp_run_sql(server, _WIDE, _LIMITED)
    assert is_error
    assert _REFUSAL in text
    is_error, text = _mcp_run_sql(server, _NARROW, _LIMITED)
    assert not is_error, text
