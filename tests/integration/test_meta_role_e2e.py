# Copyright (c) 2026 Kenneth Stott
# Canary: 2f7c0a91-6b38-4d15-9e42-1a8d5c3b7f06
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A request naming several held roles acts as their meta-role, on a real server (REQ-1620).

One server over PostgreSQL: ``orders(id, region, email)`` with ``email`` masked except to
``org_admin``. ``analyst`` sees east rows with the mask; ``east_reader`` and ``west_reader`` each
see their region's rows; ``org_admin`` sees everything. The meta-role is a child of every role it
names: its rows are the OR of its members' filters, and it sees a value unmasked when any member
does — so acting as (analyst, org_admin) is exactly acting as org_admin."""

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
_ROLES = ["org_admin", "analyst", "east_reader", "west_reader"]
_ROWS = [(1, "east"), (2, "west"), (3, "north")]
_ALL = [{"id": i, "region": r, "email": f"u{i}@x"} for i, r in _ROWS]


@pytest.fixture(scope="module")
def server():
    base = _config(_PG_HOST, _PG_PORT, "unused")
    orders = base["tables"][0]
    orders["columns"] = [
        {"name": "id", "data_type": "integer", "visible_to": _ROLES, "is_primary_key": True},
        {"name": "region", "data_type": "varchar", "visible_to": _ROLES},
        {
            "name": "email",
            "data_type": "varchar",
            "visible_to": _ROLES,
            "mask_type": "constant",
            "mask_value": "***",
            "unmasked_to": ["org_admin"],
        },
    ]
    reads = ["query_development", "full_results"]
    boot = WorkerBoot(
        1,
        pg_host=_PG_HOST,
        pg_port=_PG_PORT,
        extra_config={
            "tables": [orders],
            "roles": [{"id": r, "capabilities": reads, "domain_access": ["*"]} for r in _ROLES],
            "rls_rules": [
                {"table_id": "orders", "role_id": "analyst", "filter": "region = 'east'"},
                {"table_id": "orders", "role_id": "east_reader", "filter": "region = 'east'"},
                {"table_id": "orders", "role_id": "west_reader", "filter": "region = 'west'"},
            ],
        },
        env={"PROVISA_REDIRECT_ENABLED": "false"},
    )
    boot.create_database()
    try:
        engine = sa.create_engine(boot.url, isolation_level="AUTOCOMMIT")
        with engine.connect() as conn:
            conn.execute(sa.text("ALTER TABLE public.orders ADD COLUMN email varchar"))
            conn.execute(sa.text("DELETE FROM public.orders"))
            for order_id, region in _ROWS:
                conn.execute(
                    sa.text("INSERT INTO public.orders (id, region, email) VALUES (:i, :r, :e)"),
                    {"i": order_id, "r": region, "e": f"u{order_id}@x"},
                )
        engine.dispose()
        boot.start()
        boot.wait_all_ready(timeout=300)
        yield boot
    finally:
        boot.cleanup()


def _post(boot, roles: str, path: str, body: dict) -> tuple[int, dict]:
    req = urllib.request.Request(
        f"http://127.0.0.1:{boot.ports['http']}{path}",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "x-provisa-role": roles},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, {"error": exc.read().decode()}


def _sql(boot, roles: str) -> list[dict]:
    status, body = _post(
        boot, roles, "/data/sql", {"sql": "SELECT id, region, email FROM sales.orders ORDER BY id"}
    )
    assert status == 200, body
    return body["data"]["sql"]


def _graphql(boot, roles: str) -> list[dict]:
    status, body = _post(
        boot,
        roles,
        "/data/graphql",
        {"query": "{ s__orders(order_by: [{id: asc}]) { id region email } }"},
    )
    assert status == 200 and "errors" not in body, body
    return body["data"]["s__orders"]


def test_each_role_alone_reads_its_own_rows(server):
    assert _sql(server, "org_admin") == _ALL
    assert _sql(server, "analyst") == [{"id": 1, "region": "east", "email": "***"}]


@pytest.mark.parametrize("read", [_sql, _graphql])
def test_a_filtered_masked_role_beside_org_admin_reads_what_org_admin_reads(server, read):
    assert read(server, "analyst,org_admin") == read(server, "org_admin") == _ALL


@pytest.mark.parametrize("read", [_sql, _graphql])
def test_two_filtered_roles_read_the_or_of_their_filters(server, read):
    rows = read(server, "east_reader,west_reader")
    assert [(r["id"], r["region"]) for r in rows] == [(1, "east"), (2, "west")]
    assert {r["email"] for r in rows} == {"***"}  # masked for both: masked for the set


def test_the_order_of_the_set_does_not_matter(server):
    assert _sql(server, "west_reader,east_reader") == _sql(server, "east_reader,west_reader")


def test_a_meta_role_named_directly_is_refused(server):
    status, body = _post(
        server, "meta:analyst+org_admin", "/data/sql", {"sql": "SELECT id FROM sales.orders"}
    )
    assert status == 403 and "is not a role" in str(body), body


# --- the same sets over Bolt, pgwire and MCP ------------------------------------------------------


def _pgwire(boot, roles: str) -> list[dict]:
    import psycopg

    with psycopg.connect(
        host="127.0.0.1",
        port=boot.ports["pgwire"],
        user=roles,
        password="provisa",
        dbname="provisa",
        autocommit=True,
        connect_timeout=30,
    ) as conn:
        rows = conn.execute("SELECT id, region, email FROM sales.orders ORDER BY id").fetchall()
    return [{"id": i, "region": r, "email": e} for i, r, e in rows]


def _bolt(boot, roles: str) -> list[dict]:
    from neo4j import GraphDatabase

    driver = GraphDatabase.driver(f"bolt://127.0.0.1:{boot.ports['bolt']}", auth=("anyone", ""))
    try:
        with driver.session(database=f"provisa_{roles}") as sess:
            rows = sess.run(
                "MATCH (n:Orders) RETURN n.id AS id, n.region AS region, n.email AS email "
                "ORDER BY id"
            )
            return [dict(rec) for rec in rows]
    finally:
        driver.close()


def _mcp(boot, roles: str) -> list[dict]:
    from mcp import ClientSession
    from mcp.client.streamable_http import streamablehttp_client

    async def _call() -> tuple[bool, str]:
        url = f"http://127.0.0.1:{boot.ports['mcp']}/mcp"
        async with streamablehttp_client(url) as (read, write, _):
            async with ClientSession(read, write) as session:
                await session.initialize()
                result = await session.call_tool(
                    "run_sql",
                    {
                        "sql": "SELECT id, region, email FROM sales.orders ORDER BY id",
                        "role": roles,
                        "limit": 50,
                    },
                )
                return bool(result.isError), "".join(getattr(c, "text", "") for c in result.content)

    # Asserted out here: inside the client's task group a failure arrives wrapped in a group.
    is_error, text = asyncio.run(_call())
    assert not is_error, text
    found = re.findall(r'"id":\s*(\d+),\s*"region":\s*"(\w+)",\s*"email":\s*"([^"]*)"', text)
    assert found, text
    return [{"id": int(i), "region": r, "email": e} for i, r, e in found]


@pytest.mark.parametrize("read", [_pgwire, _bolt, _mcp])
def test_every_transport_acts_as_the_set(server, read):
    assert read(server, "analyst,org_admin") == _ALL
    rows = read(server, "west_reader,east_reader")
    assert [(r["id"], r["region"], r["email"]) for r in rows] == [
        (1, "east", "***"),
        (2, "west", "***"),
    ]


@pytest.mark.parametrize("read", [_pgwire, _bolt, _mcp])
def test_every_transport_refuses_a_meta_role_named_directly(server, read):
    with pytest.raises(Exception, match="is not a role|does not exist or is not accessible"):
        read(server, "meta:analyst+org_admin")
