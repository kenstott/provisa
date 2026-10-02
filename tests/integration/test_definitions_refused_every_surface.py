# Copyright (c) 2026 Kenneth Stott
# Canary: 4d8a1f63-9c25-4e70-b3a6-0e7f5c2d9b14
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Nothing is defined through a query protocol — on a real server, on every surface.

One server, one table. A statement that creates, alters or drops a relation is sent through
each surface a client can send a statement through: pgwire (simple and extended protocol), SQL
over HTTP, Arrow Flight, MCP, Cypher over HTTP, Bolt and the Airport protocol's DDL actions.
Each answers the one refusal (``<statement kind> is not available here: create it in the model
…``) in its own error shape; afterwards the source holds no new relation and the table its
columns, the model holds no new row, and nothing was sent to the engine or a source as DDL.

Reads on every surface, and data writes on the surfaces that take them, still work: a data
write is not a definition."""

from __future__ import annotations

import asyncio
import json
import os
import urllib.error
import urllib.request

import psycopg
import pytest
import sqlalchemy as sa

from tests.integration.worker_boot_harness import WorkerBoot, _config

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = int(os.environ.get("PG_PORT", "5432"))
_ROLE = "org_admin"
_HOW = (
    "is not available here: create it in the model (admin pages, admin API or config), or "
    "create it in the data source and admit it into the model."
)
_SQL_DEFINITIONS = [
    ("CREATE TABLE made AS SELECT * FROM sales.orders", "CREATE TABLE"),
    ("CREATE TABLE made (id INT)", "CREATE TABLE"),
    ("CREATE VIEW made AS SELECT * FROM sales.orders", "CREATE VIEW"),
    ("CREATE OR REPLACE VIEW made AS SELECT * FROM sales_pg.public.orders", "CREATE VIEW"),
    ("ALTER TABLE sales.orders ADD COLUMN made INT", "ALTER TABLE"),
    ("DROP TABLE sales.orders", "DROP TABLE"),
    ("CREATE INDEX made ON sales.orders (id)", "CREATE INDEX"),
]
_CYPHER_DEFINITIONS = [
    ("CREATE INDEX made FOR (n:Orders) ON (n.region)", "CREATE INDEX"),
    ("DROP CONSTRAINT made", "DROP CONSTRAINT"),
]


@pytest.fixture(scope="module")
def server():
    base = _config(_PG_HOST, _PG_PORT, "unused")
    orders = base["tables"][0]
    for column in orders["columns"]:
        column["writable_by"] = [_ROLE]
        if column["name"] == "id":
            column["is_primary_key"] = True
    boot = WorkerBoot(
        1,
        pg_host=_PG_HOST,
        pg_port=_PG_PORT,
        extra_config={"tables": [orders]},
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


def _http(boot, method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
    req = urllib.request.Request(
        f"http://127.0.0.1:{boot.ports['http']}{path}",
        data=None if body is None else json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "x-provisa-role": _ROLE},
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())


def _pgwire(boot):
    return psycopg.connect(
        host="127.0.0.1",
        port=boot.ports["pgwire"],
        user=_ROLE,
        password="provisa",
        dbname="provisa",
        autocommit=True,
        connect_timeout=30,
    )


def _flight(boot, query: str):
    import pyarrow.flight as fl

    client = fl.connect(f"grpc://127.0.0.1:{boot.ports['flight']}")
    try:
        ticket = fl.Ticket(json.dumps({"query": query, "role": _ROLE}).encode())
        return client.do_get(ticket).read_all()
    finally:
        client.close()


def _bolt(boot, query: str) -> list[dict]:
    from neo4j import GraphDatabase

    driver = GraphDatabase.driver(f"bolt://127.0.0.1:{boot.ports['bolt']}", auth=(_ROLE, ""))
    try:
        with driver.session() as session:
            return [dict(record) for record in session.run(query)]
    finally:
        driver.close()


def _mcp_run_sql(boot, sql: str) -> tuple[bool, str]:
    from mcp import ClientSession
    from mcp.client.streamable_http import streamablehttp_client

    async def _call() -> tuple[bool, str]:
        url = f"http://127.0.0.1:{boot.ports['mcp']}/mcp"
        async with streamablehttp_client(url) as (read, write, _):
            async with ClientSession(read, write) as session:
                await session.initialize()
                result = await session.call_tool(
                    "run_sql", {"sql": sql, "role": _ROLE, "limit": 50}
                )
                return result.isError, "".join(getattr(c, "text", "") for c in result.content)

    return asyncio.run(_call())


def _airport_action(boot, action: str, body: dict) -> None:
    import msgpack
    import pyarrow.flight as fl

    client = fl.connect(f"grpc://127.0.0.1:{boot.ports['airport']}")
    try:
        options = fl.FlightCallOptions(headers=[(b"authorization", f"Bearer {_ROLE}".encode())])
        list(client.do_action(fl.Action(action, msgpack.packb(body)), options))
    finally:
        client.close()


def _order_ids(boot) -> set[int]:
    status, body = _http(boot, "POST", "/data/sql", {"sql": "SELECT id FROM sales.orders"})
    assert status == 200, body
    return {row["id"] for row in body["data"]["sql"]}


# --- a definition is refused on every surface ---------------------------------------------------


@pytest.mark.parametrize("statement,kind", _SQL_DEFINITIONS)
def test_pgwire_refuses_a_definition(server, statement, kind):
    with _pgwire(server) as conn:
        with pytest.raises(psycopg.Error) as raised:
            conn.execute(statement)
        assert raised.value.sqlstate == "0A000"
        assert f"{kind} {_HOW}" in str(raised.value)
        assert conn.execute("SELECT count(*) FROM sales.orders").fetchone()[0] >= 2  # still usable


def test_pgwire_refuses_a_definition_on_the_extended_protocol(server):
    with _pgwire(server) as conn:
        with pytest.raises(psycopg.Error) as raised:
            conn.execute("CREATE TABLE made AS SELECT * FROM sales.orders", prepare=True)
        assert raised.value.sqlstate == "0A000"
        assert f"CREATE TABLE {_HOW}" in str(raised.value)


@pytest.mark.parametrize("statement,kind", _SQL_DEFINITIONS)
def test_sql_over_http_refuses_a_definition(server, statement, kind):
    status, body = _http(server, "POST", "/data/sql", {"sql": statement})
    assert status == 400, body
    assert body["detail"] == f"{kind} {_HOW}"


@pytest.mark.parametrize("statement,kind", _SQL_DEFINITIONS + _CYPHER_DEFINITIONS)
def test_flight_refuses_a_definition(server, statement, kind):
    import pyarrow.flight as fl

    with pytest.raises(fl.FlightError) as raised:
        _flight(server, statement)
    assert f"{kind} {_HOW}" in str(raised.value)
    assert "Cypher parse error" not in str(raised.value)  # the one message, not a language's own


@pytest.mark.parametrize("statement,kind", _SQL_DEFINITIONS)
def test_mcp_run_sql_refuses_a_definition(server, statement, kind):
    is_error, text = _mcp_run_sql(server, statement)
    assert is_error
    assert f"{kind} {_HOW}" in text


@pytest.mark.parametrize("statement,kind", _CYPHER_DEFINITIONS)
def test_cypher_over_http_refuses_a_definition(server, statement, kind):
    status, body = _http(server, "POST", "/data/cypher", {"query": statement})
    assert status == 400, body
    assert body["detail"] == f"{kind} {_HOW}"
    assert body["code"] == "data.definition_not_available"
    assert body["params"] == {"statement": kind}


@pytest.mark.parametrize("statement,kind", _CYPHER_DEFINITIONS)
def test_bolt_refuses_a_definition(server, statement, kind):
    from neo4j.exceptions import Neo4jError

    with pytest.raises(Neo4jError) as raised:
        _bolt(server, statement)
    assert f"{kind} {_HOW}" in str(raised.value)


@pytest.mark.parametrize(
    "action,body,kind",
    [
        (
            "create_table",
            {"catalog_name": "", "schema_name": "sales", "table_name": "made"},
            "CREATE TABLE",
        ),
        ("create_schema", {"catalog_name": "", "schema": "made"}, "CREATE SCHEMA"),
        ("drop_schema", {"name": "sales"}, "DROP SCHEMA"),
        (
            "drop_table",
            {"catalog_name": "", "schema_name": "sales", "table_name": "orders"},
            "DROP TABLE",
        ),
        ("add_column", {"catalog": "", "schema": "sales", "name": "orders"}, "ALTER TABLE"),
    ],
)
def test_airport_refuses_a_definition_action(server, action, body, kind):
    import pyarrow.flight as fl

    with pytest.raises(fl.FlightError) as raised:
        _airport_action(server, action, body)
    assert f"{kind} {_HOW}" in str(raised.value)


# --- and none of it reached the engine, a source or the model -----------------------------------


def test_the_refused_definitions_left_nothing_behind(server):
    with _pgwire(server) as conn:
        for statement, _kind in _SQL_DEFINITIONS:
            with pytest.raises(psycopg.Error):
                conn.execute(statement)
    for statement, _kind in _SQL_DEFINITIONS:
        _http(server, "POST", "/data/sql", {"sql": statement})
        _mcp_run_sql(server, statement)

    own = sa.create_engine(server.url)
    with own.connect() as conn:
        made = conn.execute(
            sa.text("SELECT relname FROM pg_class WHERE relname = 'made'")
        ).fetchall()
        assert made == []  # no table, view or index at the source
        columns = conn.execute(
            sa.text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = 'orders' ORDER BY 1"
            )
        ).fetchall()
        assert [c[0] for c in columns] == ["id", "region"]  # not altered, not dropped
        schemas = conn.execute(
            sa.text(
                "SELECT table_schema FROM information_schema.tables "
                "WHERE table_name = 'registered_tables'"
            )
        ).fetchall()
        assert schemas
        for (schema,) in schemas:
            held = conn.execute(
                sa.text(
                    f"SELECT count(*) FROM \"{schema}\".registered_tables WHERE table_name = 'made'"
                )
            ).scalar()
            assert held == 0, schema  # no row in the model
    own.dispose()

    log = server.log_text()
    assert "DDL(" not in log  # nothing was run as DDL on the engine or a source connection
    for surface_read in ("made", "sales.made"):
        status, body = _http(server, "POST", "/data/sql", {"sql": f"SELECT * FROM {surface_read}"})
        assert status == 403 and "not a registered table" in body["detail"], body


# --- reads and data writes are untouched --------------------------------------------------------


def test_reads_still_work_on_every_surface(server):
    assert {1, 2} <= _order_ids(server)
    with _pgwire(server) as conn:
        assert {r[0] for r in conn.execute("SELECT id FROM sales.orders").fetchall()} >= {1, 2}
    assert _flight(server, "SELECT id FROM sales.orders").num_rows >= 2
    assert not _mcp_run_sql(server, "SELECT id FROM sales.orders")[0]
    status, body = _http(server, "POST", "/data/graphql", {"query": "{ s__orders { id } }"})
    assert status == 200 and len(body["data"]["s__orders"]) >= 2, body
    status, body = _http(server, "GET", "/data/rest/sales/orders")
    assert status == 200 and len(body["data"]) >= 2, body
    status, body = _http(
        server, "POST", "/data/cypher", {"query": "MATCH (n:Orders) RETURN n.id AS id"}
    )
    assert status == 200 and len(body["rows"]) >= 2, body
    assert len(_bolt(server, "MATCH (n:Orders) RETURN n.id AS id")) >= 2


def test_a_data_write_over_sql_http_still_works(server):
    status, body = _http(
        server,
        "POST",
        "/data/sql",
        {"sql": "INSERT INTO sales.orders (id, region) VALUES (31, 'north')"},
    )
    assert status == 200, body
    assert 31 in _order_ids(server)
    status, body = _http(
        server,
        "POST",
        "/data/sql",
        {"sql": "UPDATE sales.orders SET region = 'south' WHERE id = 31"},
    )
    assert status == 200, body
    status, body = _http(
        server, "POST", "/data/sql", {"sql": "SELECT region FROM sales.orders WHERE id = 31"}
    )
    assert body["data"]["sql"] == [{"region": "south"}]
    status, body = _http(
        server, "POST", "/data/sql", {"sql": "DELETE FROM sales.orders WHERE id = 31"}
    )
    assert status == 200, body
    assert 31 not in _order_ids(server)


def test_a_data_write_through_mcp_still_works(server):
    is_error, text = _mcp_run_sql(
        server, "INSERT INTO sales.orders (id, region) VALUES (32, 'north')"
    )
    assert not is_error, text
    assert 32 in _order_ids(server)


def test_a_cypher_data_write_over_bolt_still_works(server):
    """``CREATE (n:Orders {…})`` writes a row — a pattern follows the verb — and is not taken for
    a definition by the same recogniser that refuses ``CREATE INDEX``."""
    _bolt(server, "CREATE (n:Orders {id: 33, region: 'north'})")
    assert 33 in _order_ids(server)


def test_a_graphql_mutation_still_works(server):
    """A GraphQL mutation lowers to an INSERT, which the semantic layer's guard lets by."""
    status, body = _http(
        server,
        "POST",
        "/data/graphql",
        {
            "query": 'mutation { s__insertOrders(input: {id: 34, region: "north"}) { affected_rows } }'
        },
    )
    assert status == 200 and body["data"]["s__insertOrders"]["affected_rows"] == 1, body
    assert 34 in _order_ids(server)


def test_the_explain_endpoint_refuses_a_definition_too(server):
    status, body = _http(
        server,
        "POST",
        "/data/sql/explain",
        {"sql": "CREATE TABLE made AS SELECT * FROM sales.orders"},
    )
    assert status == 400, body
    assert body["detail"] == f"CREATE TABLE {_HOW}"
