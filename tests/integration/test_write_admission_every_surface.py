# Copyright (c) 2026 Kenneth Stott
# Canary: 2c7e4b19-8f63-4d05-a9b2-6e1d3f8c5a70
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""One admission for every data write, on every surface that takes writes.

One real server over a PostgreSQL source, one table ``orders(id, region)``. A write is admitted
by the role's ``write`` right (REQ-868), the ``writable_by`` of every column it writes (REQ-663),
and the role's row filter — on the rows it touches and on the rows it leaves behind. Every
assertion is on the rows AT THE SOURCE after the call, read directly from PostgreSQL, never on
what the surface answered alone.

Roles:

* ``org_admin``   — holds ``write``; named in both columns' ``writable_by``; no row filter.
* ``east_writer`` — the same rights, with a row filter ``region = 'east'``.
* ``region_only`` — holds ``write``; named in ``region``'s ``writable_by`` only.
* ``east_reader`` — reads everything in ``east``; no ``write`` right; in no ``writable_by``.
"""

from __future__ import annotations

import asyncio
import json
import os
import urllib.error
import urllib.request

import pytest
import sqlalchemy as sa

from tests.integration.worker_boot_harness import WorkerBoot, _config

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = int(os.environ.get("PG_PORT", "5432"))
_SEED = [(1, "east"), (2, "west"), (3, "east"), (4, "west")]
_ROLES = ["org_admin", "east_writer", "region_only", "east_reader"]


@pytest.fixture(scope="module")
def server():
    base = _config(_PG_HOST, _PG_PORT, "unused")
    orders = base["tables"][0]
    for column in orders["columns"]:
        column["visible_to"] = _ROLES
        column["writable_by"] = (
            ["org_admin", "east_writer", "region_only"]
            if column["name"] == "region"
            else ["org_admin", "east_writer"]
        )
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
                {"id": "east_writer", "capabilities": [*reads, "write"], "domain_access": ["*"]},
                {"id": "region_only", "capabilities": [*reads, "write"], "domain_access": ["*"]},
                {"id": "east_reader", "capabilities": reads, "domain_access": ["*"]},
            ],
            "rls_rules": [
                {"table_id": "orders", "role_id": "east_writer", "filter": "region = 'east'"},
                {"table_id": "orders", "role_id": "east_reader", "filter": "region = 'east'"},
            ],
        },
        env={"PROVISA_REDIRECT_ENABLED": "false"},
    )
    boot.create_database()
    try:
        boot.start()
        boot.wait_all_ready(timeout=300)
        yield boot
    finally:
        boot.cleanup()


@pytest.fixture
def source(server):
    """The source table reset to its seed, and a reader of its rows as they are AT the source."""
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


# --- surfaces -----------------------------------------------------------------------------------
# A surface is how it spells five writes; each call returns (accepted, what the surface answered).


def _http(boot, role: str, method: str, path: str, body: dict | None = None) -> tuple[int, str]:
    req = urllib.request.Request(
        f"http://127.0.0.1:{boot.ports['http']}{path}",
        data=None if body is None else json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "x-provisa-role": role},
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode()


def _sql_http(boot, role: str, sql: str) -> tuple[bool, str]:
    status, body = _http(boot, role, "POST", "/data/sql", {"sql": sql})
    return status == 200, f"{status} {body}"


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


def _cypher_http(boot, role: str, query: str) -> tuple[bool, str]:
    status, body = _http(boot, role, "POST", "/data/cypher", {"query": query})
    return status == 200, f"{status} {body}"


def _bolt(boot, role: str, query: str) -> tuple[bool, str]:
    from neo4j import GraphDatabase
    from neo4j.exceptions import Neo4jError

    driver = GraphDatabase.driver(f"bolt://127.0.0.1:{boot.ports['bolt']}", auth=(role, ""))
    try:
        with driver.session() as session:
            list(session.run(query))
            return True, "ok"
    except Neo4jError as exc:
        return False, str(exc)
    finally:
        driver.close()


def _graphql(boot, role: str, mutation: str) -> tuple[bool, str]:
    status, body = _http(
        boot, role, "POST", "/data/graphql", {"query": f"mutation {{ {mutation} }}"}
    )
    return status == 200 and '"errors"' not in body, f"{status} {body}"


class _Sql:
    def __init__(self, send) -> None:
        self._send = send

    def insert(self, boot, role, order_id, region):
        return self._send(
            boot, role, f"INSERT INTO sales.orders (id, region) VALUES ({order_id}, '{region}')"
        )

    def set_region(self, boot, role, order_id, region):
        return self._send(
            boot, role, f"UPDATE sales.orders SET region = '{region}' WHERE id = {order_id}"
        )

    def set_id(self, boot, role, order_id, new_id):
        return self._send(
            boot, role, f"UPDATE sales.orders SET id = {new_id} WHERE id = {order_id}"
        )

    def delete(self, boot, role, order_id):
        return self._send(boot, role, f"DELETE FROM sales.orders WHERE id = {order_id}")

    def delete_region(self, boot, role, region):
        return self._send(boot, role, f"DELETE FROM sales.orders WHERE region = '{region}'")


class _Cypher:
    def __init__(self, send) -> None:
        self._send = send

    def insert(self, boot, role, order_id, region):
        return self._send(boot, role, f"CREATE (n:Orders {{id: {order_id}, region: '{region}'}})")

    def set_region(self, boot, role, order_id, region):
        return self._send(
            boot, role, f"MATCH (n:Orders) WHERE n.id = {order_id} SET n.region = '{region}'"
        )

    def set_id(self, boot, role, order_id, new_id):
        return self._send(
            boot, role, f"MATCH (n:Orders) WHERE n.id = {order_id} SET n.id = {new_id}"
        )

    def delete(self, boot, role, order_id):
        return self._send(boot, role, f"MATCH (n:Orders) WHERE n.id = {order_id} DELETE n")

    def delete_region(self, boot, role, region):
        return self._send(boot, role, f"MATCH (n:Orders) WHERE n.region = '{region}' DELETE n")


class _GraphQL:
    def insert(self, boot, role, order_id, region):
        return _graphql(
            boot,
            role,
            f's__insertOrders(input: {{id: {order_id}, region: "{region}"}}) {{ affected_rows }}',
        )

    def set_region(self, boot, role, order_id, region):
        return _graphql(
            boot,
            role,
            f's__updateOrders(set: {{region: "{region}"}}, where: {{id: {{eq: {order_id}}}}}) '
            "{ affected_rows }",
        )

    def set_id(self, boot, role, order_id, new_id):
        return _graphql(
            boot,
            role,
            f"s__updateOrders(set: {{id: {new_id}}}, where: {{id: {{eq: {order_id}}}}}) "
            "{ affected_rows }",
        )

    def delete(self, boot, role, order_id):
        return _graphql(
            boot, role, f"s__deleteOrders(where: {{id: {{eq: {order_id}}}}}) {{ affected_rows }}"
        )

    def delete_region(self, boot, role, region):
        return _graphql(
            boot,
            role,
            f's__deleteOrders(where: {{region: {{eq: "{region}"}}}}) {{ affected_rows }}',
        )


_SURFACES = {
    "sql_http": _Sql(_sql_http),
    "mcp": _Sql(_mcp),
    "cypher_http": _Cypher(_cypher_http),
    "bolt": _Cypher(_bolt),
    "graphql": _GraphQL(),
}
_ALL = sorted(_SURFACES)


# --- the write right ------------------------------------------------------------------------------


@pytest.mark.parametrize("surface", _ALL)
def test_a_role_without_the_write_right_writes_nothing(server, source, surface):
    s = _SURFACES[surface]
    for write in (
        lambda: s.insert(server, "east_reader", 10, "east"),
        lambda: s.set_region(server, "east_reader", 1, "changed"),
        lambda: s.delete(server, "east_reader", 1),
        lambda: s.delete_region(server, "east_reader", "east"),
    ):
        accepted, answer = write()
        assert not accepted, f"accepted: {answer}"
        assert "'write' right" in answer, answer
        assert source() == _SEED


@pytest.mark.parametrize("surface", _ALL)
def test_a_role_holding_the_rights_writes(server, source, surface):
    """The control: the same writes, as a role that holds the right and is named on the columns,
    are carried out."""
    s = _SURFACES[surface]
    accepted, answer = s.insert(server, "org_admin", 10, "north")
    assert accepted, answer
    assert (10, "north") in source()
    accepted, answer = s.set_region(server, "org_admin", 10, "south")
    assert accepted, answer
    assert (10, "south") in source()
    accepted, answer = s.delete(server, "org_admin", 10)
    assert accepted, answer
    assert source() == _SEED


# --- the columns ----------------------------------------------------------------------------------


@pytest.mark.parametrize("surface", _ALL)
def test_a_column_that_does_not_name_the_role_is_not_written(server, source, surface):
    s = _SURFACES[surface]
    # ``region_only`` holds the write right and is named on ``region`` alone.
    accepted, answer = s.set_region(server, "region_only", 2, "moved")
    assert accepted, answer
    assert (2, "moved") in source()

    for write in (
        lambda: s.set_id(server, "region_only", 2, 20),  # sets a column it is not named on
        lambda: s.insert(server, "region_only", 21, "west"),  # an INSERT writes id too
        lambda: s.delete(server, "region_only", 2),  # a DELETE removes whole rows
    ):
        before = source()
        accepted, answer = write()
        assert not accepted, f"accepted: {answer}"
        assert "write access to column 'id'" in answer, answer
        assert source() == before


# --- the row filter -------------------------------------------------------------------------------


@pytest.mark.parametrize("surface", _ALL)
def test_a_filtered_role_changes_only_rows_it_can_read(server, source, surface):
    s = _SURFACES[surface]
    # Writes that name a west row touch nothing.
    s.set_id(server, "east_writer", 2, 20)
    assert source() == _SEED
    s.delete(server, "east_writer", 2)
    assert source() == _SEED
    s.delete_region(server, "east_writer", "west")
    assert source() == _SEED
    # Its own rows it may change and remove.
    accepted, answer = s.set_id(server, "east_writer", 1, 11)
    assert accepted, answer
    assert source() == [(2, "west"), (3, "east"), (4, "west"), (11, "east")]
    accepted, answer = s.delete_region(server, "east_writer", "east")
    assert accepted, answer
    assert source() == [(2, "west"), (4, "west")]


@pytest.mark.parametrize("surface", _ALL)
def test_a_filtered_role_may_not_write_a_row_it_could_not_then_read(server, source, surface):
    s = _SURFACES[surface]
    accepted, answer = s.insert(server, "east_writer", 30, "west")
    assert not accepted, answer
    assert "outside role 'east_writer'" in answer, answer
    accepted, answer = s.set_region(server, "east_writer", 1, "west")
    assert not accepted, answer
    assert "outside role 'east_writer'" in answer, answer
    assert source() == _SEED

    # A row inside its filter is written.
    accepted, answer = s.insert(server, "east_writer", 31, "east")
    assert accepted, answer
    assert (31, "east") in source()


@pytest.mark.parametrize("surface", ["sql_http", "mcp"])
def test_an_unqualified_delete_removes_only_the_rows_the_role_can_read(server, source, surface):
    send = _sql_http if surface == "sql_http" else _mcp
    accepted, answer = send(server, "east_writer", "DELETE FROM sales.orders")
    assert accepted, answer
    assert source() == [(2, "west"), (4, "west")]


# --- TRUNCATE -------------------------------------------------------------------------------------


@pytest.mark.parametrize("surface", ["sql_http", "mcp"])
@pytest.mark.parametrize("role", ["org_admin", "east_writer", "east_reader"])
def test_truncate_is_refused_for_every_role(server, source, surface, role):
    send = _sql_http if surface == "sql_http" else _mcp
    accepted, answer = send(server, role, "TRUNCATE TABLE sales.orders")
    assert not accepted, answer
    assert "TRUNCATE is not available here: use DELETE, which is governed." in answer
    assert source() == _SEED


# --- pgwire ---------------------------------------------------------------------------------------


def _pgwire(boot, role: str):
    import psycopg

    return psycopg.connect(
        host="127.0.0.1",
        port=boot.ports["pgwire"],
        user=role,
        password="provisa",
        dbname="provisa",
        autocommit=True,
        connect_timeout=30,
    )


@pytest.mark.parametrize(
    "statement,kind",
    [
        ("INSERT INTO sales.orders (id, region) VALUES (40, 'east')", "INSERT"),
        ("UPDATE sales.orders SET region = 'x' WHERE id = 1", "UPDATE"),
        ("DELETE FROM sales.orders WHERE id = 1", "DELETE"),
        ("/* bulk */ delete from sales.orders", "DELETE"),
    ],
)
def test_pgwire_takes_no_data_write_statement(server, source, statement, kind):
    """REQ-615. Refused for the role that holds every right, before anything is governed or
    sent to a source, with SQLSTATE 0A000 naming where a write is taken."""
    import psycopg

    with _pgwire(server, "org_admin") as conn:
        with pytest.raises(psycopg.Error) as raised:
            conn.execute(statement)
        assert raised.value.sqlstate == "0A000"
        assert f"{kind} is not available over pgwire" in str(raised.value)
        assert "POST /data/sql" in str(raised.value)
        with pytest.raises(psycopg.Error) as prepared:
            conn.execute(statement, prepare=True)
        assert prepared.value.sqlstate == "0A000"
        assert conn.execute("SELECT count(*) FROM sales.orders").fetchone()[0] == len(_SEED)
    assert source() == _SEED


@pytest.mark.parametrize("role", ["org_admin", "east_writer", "east_reader"])
def test_pgwire_takes_no_bulk_load(server, source, role):
    """REQ-615: ``COPY … FROM STDIN`` is a write. Refused for every role with the refusal an
    INSERT gets, before the client is asked for any data."""
    import psycopg

    with _pgwire(server, role) as conn, conn.cursor() as cur:
        with pytest.raises(psycopg.Error) as raised:
            with cur.copy("COPY sales.orders (id, region) FROM STDIN") as copy:
                copy.write("50\teast\n")
        assert raised.value.sqlstate == "0A000"
        assert "COPY ... FROM STDIN is not available over pgwire" in str(raised.value)
    assert source() == _SEED


def test_a_copy_out_over_pgwire_is_a_governed_read(server):
    """``COPY … TO STDOUT`` is a read and keeps working: it is planned by the pipeline every
    pgwire read is, so the role's row filter is in what it returns."""

    def _rows(role: str) -> list[tuple[int, str]]:
        with _pgwire(server, role) as conn, conn.cursor() as cur:
            with cur.copy("COPY (SELECT id, region FROM sales.orders) TO STDOUT") as copy:
                text = b"".join(bytes(chunk) for chunk in copy).decode()
        return sorted(
            (int(i), r) for i, r in (line.split("\t") for line in text.splitlines() if line)
        )

    assert _rows("org_admin") == _SEED
    assert _rows("east_reader") == [(1, "east"), (3, "east")]
