# Copyright (c) 2026 Kenneth Stott
# Canary: b79887a8-6326-4648-babe-f96413098f46
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: relationships govern joins on every surface (REQ-264, REQ-603, REQ-693).

A role without the ``ignore_relationships`` right may relate two registered tables only along a
registered relationship, however the statement is written and whichever surface carries it: SQL
over HTTP, Cypher over HTTP, Bolt and Flight. A role holding the right may relate them freely. A
relationship pattern over a registered relationship passes for both.
"""

# Requirements: REQ-264, REQ-603, REQ-693

from __future__ import annotations

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
_ROLES = ["org_admin", "bound", "free"]


def _table(name: str, *columns: tuple[str, str]) -> dict:
    return {
        "source_id": "sales-pg",
        "domain_id": "sales",
        "schema": "public",
        "table": name,
        "columns": [
            {"name": c, "data_type": t, "visible_to": _ROLES, "is_primary_key": c == "id"}
            for c, t in columns
        ],
    }


@pytest.fixture(scope="module")
def server():
    base = _config(_PG_HOST, _PG_PORT, "unused")
    reads = ["query_development", "full_results"]
    boot = WorkerBoot(
        1,
        pg_host=_PG_HOST,
        pg_port=_PG_PORT,
        extra_config={
            "tables": [
                _table("orders", ("id", "integer"), ("region", "varchar")),
                _table("customers", ("id", "integer"), ("name", "varchar"), ("region", "varchar")),
                _table("visits", ("id", "integer"), ("customer_id", "integer")),
            ],
            # The one registered relationship: a customer's visits. Nothing relates orders.
            "relationships": [
                {
                    "id": "customer-visits",
                    "source_table_id": "customers",
                    "source_column": "id",
                    "target_table_id": "visits",
                    "target_column": "customer_id",
                    "cardinality": "one-to-many",
                    "graphql_alias": "visits",
                }
            ],
            "roles": [
                {"id": "bound", "capabilities": reads, "domain_access": ["*"]},
                {
                    "id": "free",
                    "capabilities": [*reads, "ignore_relationships"],
                    "domain_access": ["*"],
                },
            ],
        },
        env={"PROVISA_REDIRECT_ENABLED": "false"},
    )
    del base
    boot.create_database()
    try:
        engine = sa.create_engine(boot.url, isolation_level="AUTOCOMMIT")
        with engine.connect() as conn:
            conn.execute(
                sa.text(
                    "CREATE TABLE public.customers (id integer PRIMARY KEY, name text, region text)"
                )
            )
            conn.execute(
                sa.text("CREATE TABLE public.visits (id integer PRIMARY KEY, customer_id integer)")
            )
            conn.execute(
                sa.text("INSERT INTO public.customers VALUES (1, 'ann', 'east'), (2, 'bo', 'west')")
            )
            conn.execute(sa.text("INSERT INTO public.visits VALUES (10, 1), (11, 1), (12, 2)"))
        engine.dispose()
        boot.start()
        boot.wait_all_ready(timeout=300)
        yield boot
    finally:
        boot.cleanup()


def _http(boot, role: str, path: str, body: dict) -> tuple[bool, str]:
    req = urllib.request.Request(
        f"http://127.0.0.1:{boot.ports['http']}{path}",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "x-provisa-role": role},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            return resp.status == 200, resp.read().decode()
    except urllib.error.HTTPError as exc:
        return False, f"{exc.code} {exc.read().decode()}"


def _sql_http(boot, role: str, statement: str) -> tuple[bool, str]:
    return _http(boot, role, "/data/sql", {"sql": statement})


def _cypher_http(boot, role: str, statement: str) -> tuple[bool, str]:
    return _http(boot, role, "/data/cypher", {"query": statement})


def _bolt(boot, role: str, statement: str) -> tuple[bool, str]:
    from neo4j import GraphDatabase
    from neo4j.exceptions import Neo4jError

    driver = GraphDatabase.driver(f"bolt://127.0.0.1:{boot.ports['bolt']}", auth=(role, ""))
    try:
        with driver.session() as session:
            return True, str([dict(r) for r in session.run(statement)])
    except Neo4jError as exc:
        return False, str(exc)
    finally:
        driver.close()


def _flight(boot, role: str, statement: str) -> tuple[bool, str]:
    import pyarrow.flight as fl

    client = fl.connect(f"grpc://127.0.0.1:{boot.ports['flight']}")
    try:
        ticket = fl.Ticket(json.dumps({"query": statement, "role": role}).encode())
        return True, str(client.do_get(ticket).read_all().to_pylist())
    except fl.FlightError as exc:
        return False, str(exc)
    finally:
        client.close()


_CYPHER_SURFACES = {"cypher_http": _cypher_http, "bolt": _bolt, "flight": _flight}

# Two labels related by a predicate, with no relationship pattern between them.
_CYPHER_OUTSIDE = {
    "two_match": (
        "MATCH (o:Orders) MATCH (c:Customers) WHERE o.region = c.region RETURN o.id, c.name"
    ),
    "optional_match": (
        "MATCH (o:Orders) OPTIONAL MATCH (c:Customers) WHERE o.region = c.region "
        "RETURN o.id, c.name"
    ),
    "with_then_match": (
        "MATCH (o:Orders) WITH o MATCH (c:Customers) WHERE o.region = c.region RETURN o.id, c.name"
    ),
}
_CYPHER_ALONG = "MATCH (c:Customers)-[r]->(v:Visits) RETURN c.name, v.id"

_SQL_OUTSIDE = {
    "explicit_on": (
        "SELECT o.id, c.name FROM sales.orders o JOIN sales.customers c ON o.region = c.region"
    ),
    "comma_where": (
        "SELECT o.id, c.name FROM sales.orders o, sales.customers c WHERE o.region = c.region"
    ),
    "cross_join_where": (
        "SELECT o.id, c.name FROM sales.orders o CROSS JOIN sales.customers c "
        "WHERE o.region = c.region"
    ),
    "on_true_where": (
        "SELECT o.id, c.name FROM sales.orders o JOIN sales.customers c ON TRUE "
        "WHERE o.region = c.region"
    ),
    "on_range": (
        "SELECT o.id, c.name FROM sales.orders o JOIN sales.customers c "
        "ON o.region >= c.region AND o.region <= c.region"
    ),
    "cte": (
        "WITH w AS (SELECT * FROM sales.orders) "
        "SELECT w.id, c.name FROM w JOIN sales.customers c ON w.region = c.region"
    ),
    "derived_table": (
        "SELECT w.id, c.name FROM (SELECT * FROM sales.orders) w "
        "JOIN sales.customers c ON w.region = c.region"
    ),
    "in_subquery": (
        "SELECT o.id FROM sales.orders o WHERE o.region IN (SELECT region FROM sales.customers)"
    ),
    "exists": (
        "SELECT o.id FROM sales.orders o WHERE EXISTS "
        "(SELECT 1 FROM sales.customers c WHERE c.region = o.region)"
    ),
    "scalar_subquery": (
        "SELECT o.id, (SELECT MAX(c.name) FROM sales.customers c WHERE c.region = o.region) AS n "
        "FROM sales.orders o"
    ),
    "using": "SELECT o.id FROM sales.orders o JOIN sales.customers c USING (region)",
    "natural": "SELECT o.id FROM sales.orders o NATURAL JOIN sales.customers c",
}
# Along the registered relationship, in any spelling: judged by the columns it pairs.
_SQL_ALONG = {
    "join_on": (
        "SELECT c.name, v.id FROM sales.customers c JOIN sales.visits v ON c.id = v.customer_id"
    ),
    "comma_where": (
        "SELECT c.name, v.id FROM sales.customers c, sales.visits v WHERE c.id = v.customer_id"
    ),
    "in_subquery": (
        "SELECT c.name FROM sales.customers c WHERE c.id IN (SELECT customer_id FROM sales.visits)"
    ),
    "not_exists": (
        "SELECT c.name FROM sales.customers c WHERE NOT EXISTS "
        "(SELECT 1 FROM sales.visits v WHERE v.customer_id = c.id)"
    ),
    "cte": (
        "WITH w AS (SELECT id, name FROM sales.customers) "
        "SELECT w.name, v.id FROM w JOIN sales.visits v ON w.id = v.customer_id"
    ),
}
# Along it, and also matched by something that is no registered relationship.
_SQL_ALONG_AND_MORE = (
    "SELECT c.name FROM sales.customers c JOIN sales.visits v "
    "ON c.id = v.customer_id AND v.id > c.id"
)


@pytest.mark.parametrize("surface", list(_CYPHER_SURFACES))
@pytest.mark.parametrize("shape", list(_CYPHER_OUTSIDE))
def test_cypher_relating_two_labels_outside_a_relationship_is_refused(server, surface, shape):
    run = _CYPHER_SURFACES[surface]
    accepted, said = run(server, "bound", _CYPHER_OUTSIDE[shape])
    assert not accepted, said
    assert "relationship" in said.lower(), said
    accepted, said = run(server, "free", _CYPHER_OUTSIDE[shape])
    assert accepted, said


@pytest.mark.parametrize("surface", list(_CYPHER_SURFACES))
def test_a_cypher_relationship_pattern_passes_for_every_role(server, surface):
    run = _CYPHER_SURFACES[surface]
    for role in ("bound", "free"):
        accepted, said = run(server, role, _CYPHER_ALONG)
        assert accepted, (role, said)
        assert "ann" in said and "bo" in said, said


@pytest.mark.parametrize("surface", list(_CYPHER_SURFACES))
@pytest.mark.parametrize("hops", ["[:HAS_VISITS*1..2]", "[*1..2]"], ids=["typed", "untyped"])
def test_a_variable_length_pattern_reads_the_same_rows_pointing_either_way(server, surface, hops):
    """(v)<-[:R*1..2]-(c) is (c)-[:R*1..2]->(v) (#151), and with no type given the pattern
    walks only the relationships joining its ends (#157): along the registered relationship, so
    it passes for a role the relationships bind, and both return the same rows."""
    run = _CYPHER_SURFACES[surface]
    returning = "RETURN c.name AS name, v.id AS id ORDER BY id"
    right, rows = run(server, "bound", f"MATCH (c:Customers)-{hops}->(v:Visits) {returning}")
    assert right, rows
    left, same = run(server, "bound", f"MATCH (v:Visits)<-{hops}-(c:Customers) {returning}")
    assert left, same
    assert same == rows, (rows, same)
    assert all(said in rows for said in ("ann", "bo", "10", "11", "12")), rows


_UNREGISTERED_TYPE = {
    "match": "MATCH (o:Orders)-[:NO_SUCH_REL]->(c:Customers) RETURN o.id",
    "exists": (
        "MATCH (o:Orders) WHERE EXISTS { MATCH (o)-[:NO_SUCH_REL]->(c:Customers) } RETURN o.id"
    ),
    "count": (
        "MATCH (o:Orders) WHERE COUNT { MATCH (o)-[:NO_SUCH_REL]->(c:Customers) } > 0 RETURN o.id"
    ),
    "comprehension": "MATCH (o:Orders) RETURN o.id, [(o)-[:NO_SUCH_REL]->(c:Customers) | c.id]",
    "call": (
        "MATCH (o:Orders) CALL { WITH o MATCH (o)-[:NO_SUCH_REL]->(c:Customers) "
        "RETURN c.id AS cid } RETURN o.id, cid"
    ),
}


@pytest.mark.parametrize("surface", list(_CYPHER_SURFACES))
@pytest.mark.parametrize("shape", list(_UNREGISTERED_TYPE))
def test_an_unregistered_cypher_relationship_type_is_refused(server, surface, shape):
    """Wherever the statement names it -- its own MATCH, a statement inside it, a pattern
    comprehension -- and for a role exempt from the relationships too: the type does not exist."""
    for role in ("bound", "free"):
        accepted, said = _CYPHER_SURFACES[surface](server, role, _UNREGISTERED_TYPE[shape])
        assert not accepted, (role, said)
        assert "NO_SUCH_REL" in said, (role, said)


@pytest.mark.parametrize("shape", list(_SQL_OUTSIDE))
def test_sql_relating_two_tables_outside_a_relationship_is_refused(server, shape):
    accepted, said = _sql_http(server, "bound", _SQL_OUTSIDE[shape])
    assert not accepted, said
    assert "V002" in said, said
    accepted, said = _sql_http(server, "free", _SQL_OUTSIDE[shape])
    assert accepted, said


@pytest.mark.parametrize("shape", list(_SQL_ALONG))
def test_a_sql_join_along_a_relationship_passes_for_every_role(server, shape):
    for role in ("bound", "free"):
        accepted, said = _sql_http(server, role, _SQL_ALONG[shape])
        assert accepted, (role, said)


def test_a_second_matching_beside_the_relationship_is_refused(server):
    accepted, said = _sql_http(server, "bound", _SQL_ALONG_AND_MORE)
    assert not accepted and "V002" in said, said
    accepted, said = _sql_http(server, "free", _SQL_ALONG_AND_MORE)
    assert accepted, said
