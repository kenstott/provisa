# Copyright (c) 2026 Kenneth Stott
# Canary: 8f3a6d21-4b97-4c05-a1e8-2d7c9b5e3f16
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A command is called only by a role it is assigned to, on every surface that calls commands.

One real server over a PostgreSQL source holding ``orders(id, region)`` and a SQL function
registered as a command assigned to some roles. A role it is not assigned to finds no such
command: the call answers exactly as a name never registered does, on every surface.

Roles: ``org_admin`` and ``reader`` are assigned the command; ``outsider`` is not.
"""

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
_SEED = [(1, "east"), (2, "west"), (3, "east")]
_ROLES = ["org_admin", "reader", "outsider"]
_ASSIGNED = ["org_admin", "reader"]

_FUNCTIONS_SQL = [
    """CREATE FUNCTION public.region_count(r text) RETURNS TABLE(region text, n bigint)
       LANGUAGE sql AS $$ SELECT region, count(*) FROM public.orders WHERE region = r
       GROUP BY region $$""",
]


def _command(name: str, kind: str, arguments: list[dict], **extra) -> dict:
    return {
        "name": name,
        "source_id": "sales-pg",
        "schema": "public",
        "function_name": name,
        "returns": "",
        "kind": kind,
        "domain_id": "sales",
        "visible_to": _ASSIGNED,
        "arguments": arguments,
        **extra,
    }


@pytest.fixture(scope="module")
def server():
    base = _config(_PG_HOST, _PG_PORT, "unused")
    orders = base["tables"][0]
    for column in orders["columns"]:
        column["visible_to"] = _ROLES
        column["writable_by"] = ["org_admin"]
        if column["name"] == "id":
            column["is_primary_key"] = True
    reads = ["query_development", "full_results"]
    boot = WorkerBoot(
        1,
        pg_host=_PG_HOST,
        pg_port=_PG_PORT,
        extra_config={
            "tables": [orders],
            "roles": [
                {"id": "org_admin", "capabilities": [*reads, "write"], "domain_access": ["*"]},
                {"id": "reader", "capabilities": reads, "domain_access": ["*"]},
                {"id": "outsider", "capabilities": [*reads, "write"], "domain_access": ["*"]},
            ],
            "functions": [
                _command("region_count", "query", [{"name": "r", "type": "String"}]),
            ],
        },
        env={"PROVISA_REDIRECT_ENABLED": "false"},
    )
    boot.create_database()
    engine = sa.create_engine(boot.url, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        for ddl in _FUNCTIONS_SQL:
            conn.execute(sa.text(ddl))
    engine.dispose()
    try:
        boot.start()
        boot.wait_all_ready(timeout=300)
        yield boot
    finally:
        boot.cleanup()


@pytest.fixture
def source(server):
    engine = sa.create_engine(server.url, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        conn.execute(sa.text("DELETE FROM public.orders"))
        for order_id, region in _SEED:
            conn.execute(
                sa.text("INSERT INTO public.orders (id, region) VALUES (:i, :r)"),
                {"i": order_id, "r": region},
            )

    def rows() -> list[tuple[int, str]]:
        with engine.connect() as conn:
            return [
                tuple(r)
                for r in conn.execute(sa.text("SELECT id, region FROM public.orders ORDER BY id"))
            ]

    yield rows
    engine.dispose()


# --- surfaces: each answers (ok, text) --------------------------------------------------------


def _http(boot, role: str, path: str, body: dict) -> tuple[int, str]:
    req = urllib.request.Request(
        f"http://127.0.0.1:{boot.ports['http']}{path}",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "x-provisa-role": role},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode()


def _sql_http(boot, role: str, sql: str) -> tuple[bool, str]:
    status, body = _http(boot, role, "/data/sql", {"sql": sql})
    return status == 200, body


def _pgwire(boot, role: str, sql: str) -> tuple[bool, str]:
    import psycopg

    try:
        with psycopg.connect(
            host="127.0.0.1",
            port=boot.ports["pgwire"],
            user=role,
            password="provisa",
            dbname="provisa",
            connect_timeout=30,
            autocommit=True,
        ) as conn:
            rows = conn.execute(sql).fetchall()
        return True, json.dumps(rows, default=str)
    except psycopg.Error as exc:
        return False, str(exc)


def _mcp(boot, role: str, sql: str) -> tuple[bool, str]:
    from mcp import ClientSession
    from mcp.client.streamable_http import streamablehttp_client

    async def _call() -> tuple[bool, str]:
        url = f"http://127.0.0.1:{boot.ports['mcp']}/mcp"
        async with streamablehttp_client(url) as (read, write, _):
            async with ClientSession(read, write) as session:
                await session.initialize()
                result = await session.call_tool("run_sql", {"sql": sql, "role": role, "limit": 50})
                return not result.isError, "".join(getattr(c, "text", "") for c in result.content)

    return asyncio.run(_call())


def _cypher_http(boot, role: str, query: str, params: dict | None = None) -> tuple[bool, str]:
    status, body = _http(boot, role, "/data/cypher", {"query": query, "params": params or {}})
    return status == 200, body


def _bolt(boot, role: str, query: str, params: dict | None = None) -> tuple[bool, str]:
    from neo4j import GraphDatabase
    from neo4j.exceptions import Neo4jError

    driver = GraphDatabase.driver(f"bolt://127.0.0.1:{boot.ports['bolt']}", auth=(role, ""))
    try:
        with driver.session() as session:
            records = [r.data() for r in session.run(query, params or {})]
            return True, json.dumps(records, default=str)
    except Neo4jError as exc:
        return False, str(exc)
    finally:
        driver.close()


def _grpc_generic(boot, role: str, name: str, args: dict) -> tuple[bool, str]:
    import grpc

    from tests.grpc_proto_client import role_descriptor_pool

    from google.protobuf.message_factory import GetMessageClass

    _pool, svc = role_descriptor_pool(f"http://127.0.0.1:{boot.ports['http']}", role)
    method = svc.FindMethodByName("CallCommand")
    req_cls = GetMessageClass(method.input_type)
    resp_cls = GetMessageClass(method.output_type)
    with grpc.insecure_channel(f"127.0.0.1:{boot.ports['grpc']}") as channel:
        call = channel.unary_unary(
            f"/{svc.full_name}/CallCommand",
            request_serializer=req_cls.SerializeToString,
            response_deserializer=resp_cls.FromString,
        )
        try:
            resp = call(
                req_cls(name=name, args_json=json.dumps(args)),
                metadata=(("x-provisa-role", role),),
                timeout=120,
            )
            return True, resp.rows_json
        except grpc.RpcError as exc:
            return False, f"{exc.code().name} {exc.details()}"


def _grpc_typed(boot, role: str, rpc: str) -> tuple[bool, str]:
    """``Call{rpc}`` sent with an empty request — what a client without the role's proto sends."""
    import grpc

    with grpc.insecure_channel(f"127.0.0.1:{boot.ports['grpc']}") as channel:
        call = channel.unary_unary(
            f"/provisa.ProvisaService/Call{rpc}",
            request_serializer=lambda b: b,
            response_deserializer=lambda b: b,
        )
        try:
            call(b"", metadata=(("x-provisa-role", role),), timeout=120)
            return True, "ok"
        except grpc.RpcError as exc:
            return False, f"{exc.code().name} {exc.details()}"


def _graphql(boot, role: str, op: str, field: str) -> tuple[bool, str]:
    status, body = _http(boot, role, "/data/graphql", {"query": f"{op} {{ {field} }}"})
    return status == 200 and '"errors"' not in body, f"{status} {body}"


def _same_answer(a: tuple[bool, str], b: tuple[bool, str], name_a: str, name_b: str) -> None:
    """Two refusals that differ only in the name asked for."""
    assert a[0] is False and b[0] is False, (a, b)
    norm = [
        re.sub(r"\b" + re.escape(n) + r"\b", "<name>", t, flags=re.IGNORECASE)
        for t, n in ((a[1], name_a), (b[1], name_b))
    ]
    assert norm[0] == norm[1], norm


# --- a command the role may not use is a name never registered --------------------------------


_SQL_SURFACES = {"sql_http": _sql_http, "pgwire": _pgwire, "mcp": _mcp}


@pytest.mark.parametrize("surface", sorted(_SQL_SURFACES))
def test_sql_an_unassigned_command_reads_as_an_unknown_name(server, source, surface):
    send = _SQL_SURFACES[surface]
    assert send(server, "reader", "SELECT * FROM region_count('east')")[0]
    hidden = send(server, "outsider", "SELECT * FROM region_count('east')")
    unknown = send(server, "outsider", "SELECT * FROM nosuch_cmd('east')")
    _same_answer(hidden, unknown, "region_count", "nosuch_cmd")


@pytest.mark.parametrize("surface", ["cypher_http", "bolt"])
def test_cypher_an_unassigned_command_reads_as_an_unknown_name(server, source, surface):
    send = {"cypher_http": _cypher_http, "bolt": _bolt}[surface]
    hidden = send(server, "outsider", "CALL region_count('east')")
    unknown = send(server, "outsider", "CALL nosuch_cmd('east')")
    _same_answer(hidden, unknown, "region_count", "nosuch_cmd")
    # A role assigned the command calls it.
    assert send(server, "reader", "CALL region_count('east')")[0]


def test_grpc_an_unassigned_command_reads_as_an_unknown_name(server, source):
    _same_answer(
        _grpc_generic(server, "outsider", "region_count", {"r": "east"}),
        _grpc_generic(server, "outsider", "nosuch_cmd", {"r": "east"}),
        "region_count",
        "nosuch_cmd",
    )
    _same_answer(
        _grpc_typed(server, "outsider", "RegionCount"),
        _grpc_typed(server, "outsider", "NosuchCmd"),
        "RegionCount",
        "NosuchCmd",
    )


def test_graphql_an_unassigned_command_is_not_in_the_schema(server, source):
    # Shown to a role assigned it (the naming convention makes it camelCase), and to no other.
    assert _graphql(server, "reader", "query", 's__regionCount(r: "east")')[0]
    hidden = _graphql(server, "outsider", "query", 's__regionCount(r: "east")')
    unknown = _graphql(server, "outsider", "query", 's__nosuchCmd(r: "east")')
    _same_answer(hidden, unknown, "s__regionCount", "s__nosuchCmd")
