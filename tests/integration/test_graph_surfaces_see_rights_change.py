# Copyright (c) 2026 Kenneth Stott
# Canary: 29cea1d8-94e7-48ae-b387-f0ea1a5d7177
# Canary: PENDING
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: every surface reads a change of rights on the next request (REQ-1911).

Each repeats a statement until its role's governance is kept, then changes the role or the model
and asks again at once: a kept governance context never answers with the rights it was built from.
"""

# Requirements: REQ-1911

from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request

import pytest
import sqlalchemy as sa

from tests.integration.worker_boot_harness import WorkerBoot

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = int(os.environ.get("PG_PORT", "5432"))
_WEST = "west-only-value"
_READS = '["query_development", "full_results"]'


def _columns(region_to: list[str]) -> list[dict]:
    return [
        {
            "name": "id",
            "data_type": "integer",
            "visible_to": ["org_admin", "reader"],
            "is_primary_key": True,
        },
        {"name": "region", "data_type": "varchar", "visible_to": region_to},
    ]


@pytest.fixture(scope="module")
def server():
    boot = WorkerBoot(
        1,
        pg_host=_PG_HOST,
        pg_port=_PG_PORT,
        extra_config={
            "domains": [
                {"id": "sales", "description": "boot harness"},
                {"id": "hr", "description": "A domain the reader can be moved to."},
            ],
            "tables": [
                {
                    "source_id": "sales-pg",
                    "domain_id": "sales",
                    "schema": "public",
                    "table": "ledger",
                    "columns": _columns(["org_admin", "reader"]),
                }
            ],
            "roles": [
                {
                    "description": "Test role.",
                    "id": "reader",
                    "capabilities": ["query_development", "full_results"],
                    "domain_access": ["sales"],
                }
            ],
        },
        env={"PROVISA_REDIRECT_ENABLED": "false", "PROVISA_PUBLIC_URL": "http://rights.test"},
    )
    boot.create_database()
    try:
        engine = sa.create_engine(boot.url, isolation_level="AUTOCOMMIT")
        with engine.connect() as conn:
            conn.execute(
                sa.text("CREATE TABLE public.ledger (id integer PRIMARY KEY, region text)")
            )
            conn.execute(
                sa.text("INSERT INTO public.ledger VALUES (1, 'east'), (2, :west)"),
                {"west": _WEST},
            )
        engine.dispose()
        boot.start()
        boot.wait_all_ready(timeout=300)
        yield boot
    finally:
        boot.cleanup()


def _call(boot, path: str, *, role: str, data: bytes | None, content_type: str) -> str:
    req = urllib.request.Request(
        f"http://127.0.0.1:{boot.ports['http']}{path}",
        data=data,
        headers={
            "Content-Type": content_type,
            "x-provisa-role": role,
            "Accept": "application/sparql-results+json, application/json",
        },
        method="POST" if data is not None else "GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            return resp.read().decode()
    except urllib.error.HTTPError as exc:
        return f"{exc.code} {exc.read().decode()}"


def _admin(boot, mutation: str) -> None:
    body = json.loads(
        _call(
            boot,
            "/admin/graphql",
            role="org_admin",
            data=json.dumps({"query": mutation}).encode(),
            content_type="application/json",
        )
    )
    assert "errors" not in body, body
    assert all(v["success"] for v in body["data"].values()), body


def _cypher(boot, role: str = "reader") -> str:
    query = "MATCH (l:Sales:Ledger) RETURN l.id AS id, l.region AS region ORDER BY id"
    return _call(
        boot,
        "/data/cypher",
        role=role,
        data=json.dumps({"query": query}).encode(),
        content_type="application/json",
    )


def _sparql(boot, role: str = "reader") -> str:
    vocabulary = json.loads(
        _call(
            boot,
            "/data/sparql/vocabulary",
            role="org_admin",
            data=None,
            content_type="application/json",
        )
    )
    predicate = next(
        p["iri"] for p in vocabulary["predicates"] if p["iri"].endswith("ledger#region")
    )
    table = predicate.rpartition("#")[0]
    query = f"SELECT ?region WHERE {{ ?l a <{table}> . ?l <{predicate}> ?region }}"
    return _call(
        boot,
        "/data/sparql",
        role=role,
        data=urllib.parse.urlencode({"query": query}).encode(),
        content_type="application/x-www-form-urlencoded",
    )


_SQL = "SELECT id, region FROM sales.ledger ORDER BY id"


def _sql_http(boot, role: str = "reader") -> str:
    return _call(
        boot,
        "/data/sql",
        role=role,
        data=json.dumps({"sql": _SQL}).encode(),
        content_type="application/json",
    )


def _bolt(boot, role: str = "reader") -> str:
    from neo4j import GraphDatabase
    from neo4j.exceptions import Neo4jError

    statement = "MATCH (l:Sales:Ledger) RETURN l.id AS id, l.region AS region ORDER BY id"
    driver = GraphDatabase.driver(f"bolt://127.0.0.1:{boot.ports['bolt']}", auth=(role, ""))
    try:
        with driver.session() as session:
            return str([dict(r) for r in session.run(statement)])
    except Neo4jError as exc:
        return str(exc)
    finally:
        driver.close()


def _flight(boot, role: str = "reader") -> str:
    import pyarrow.flight as fl

    client = fl.connect(f"grpc://127.0.0.1:{boot.ports['flight']}")
    try:
        ticket = fl.Ticket(json.dumps({"query": _SQL, "role": role}).encode())
        return str(client.do_get(ticket).read_all().to_pylist())
    except fl.FlightError as exc:
        return str(exc)
    finally:
        client.close()


def _pgwire(boot, role: str = "reader") -> str:
    import psycopg2

    conn = psycopg2.connect(
        host="127.0.0.1",
        port=boot.ports["pgwire"],
        user=role,
        password="x",
        dbname="provisa",
        sslmode="disable",
    )
    try:
        with conn.cursor() as cursor:
            cursor.execute(_SQL)
            return str(cursor.fetchall())
    except psycopg2.Error as exc:
        return str(exc)
    finally:
        conn.close()


_SURFACES = {
    "cypher_http": _cypher,
    "sparql_http": _sparql,
    "sql_http": _sql_http,
    "bolt": _bolt,
    "flight": _flight,
    "pgwire": _pgwire,
}


def _warm(boot, ask) -> None:
    for _ in range(3):
        answer = ask(boot)
        assert _WEST in answer, answer


def _set_grant(boot, region_to: list[str]) -> None:
    grants = json.dumps(region_to)
    _admin(
        boot,
        'mutation { updateTable(input: {sourceId: "sales-pg", domainId: "sales", '
        'schemaName: "public", tableName: "ledger", columns: ['
        '{name: "id", visibleTo: ["org_admin", "reader"], dataType: "integer"}, '
        f'{{name: "region", visibleTo: {grants}, dataType: "varchar"}}]}}) '
        "{ success message } }",
    )


def _set_domains(boot, domains: list[str]) -> None:
    _admin(
        boot,
        'mutation { updateRole(input: {id: "reader", description: "Test role.", '
        f"capabilities: {_READS}, domainAccess: {json.dumps(domains)}}}) "
        "{ success message } }",
    )


@pytest.mark.parametrize("surface", list(_SURFACES))
def test_a_column_grant_taken_away_is_seen_on_the_next_request(server, surface):
    ask = _SURFACES[surface]
    _warm(server, ask)
    _set_grant(server, ["org_admin"])
    try:
        answer = ask(server)
        assert _WEST not in answer, answer
    finally:
        _set_grant(server, ["org_admin", "reader"])
    assert _WEST in ask(server)


@pytest.mark.parametrize("surface", list(_SURFACES))
def test_a_row_filter_added_is_seen_on_the_next_request(server, surface):
    ask = _SURFACES[surface]
    _warm(server, ask)
    _admin(
        server,
        'mutation { upsertRlsRule(input: {description: "Test rule.", tableId: "ledger", '
        'roleId: "reader", filterExpr: "id = 1"}) { success message } }',
    )
    try:
        answer = ask(server)
        assert _WEST not in answer and "east" in answer, answer
    finally:
        _admin(
            server,
            'mutation { upsertRlsRule(input: {description: "Test rule.", tableId: "ledger", '
            'roleId: "reader", filterExpr: "id > 0"}) { success message } }',
        )
    assert _WEST in ask(server)


@pytest.mark.parametrize("surface", list(_SURFACES))
def test_a_role_moved_out_of_a_domain_is_seen_on_the_next_request(server, surface):
    ask = _SURFACES[surface]
    _warm(server, ask)
    _set_domains(server, ["hr"])
    try:
        answer = ask(server)
        assert _WEST not in answer, answer
    finally:
        _set_domains(server, ["sales"])
    assert _WEST in ask(server)


@pytest.mark.parametrize("surface", list(_SURFACES))
def test_a_right_given_is_seen_on_the_next_request(server, surface):
    ask = _SURFACES[surface]
    _set_grant(server, ["org_admin"])
    try:
        for _ in range(3):
            assert _WEST not in ask(server)
    finally:
        _set_grant(server, ["org_admin", "reader"])
    assert _WEST in ask(server)
