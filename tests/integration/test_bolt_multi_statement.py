# Copyright (c) 2026 Kenneth Stott
# Canary: b2a773f5-b7c0-4beb-a857-177b2f89e324
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: several statements over Bolt, as Neo4j Browser's editor runs them (REQ-802).

Browser splits the editor's text on ``;`` and sends each statement as its own RUN, one after
another. Through the official driver, against one real server:

* two statements as two RUNs in one session (each auto-commit);
* the same in one explicit transaction (BEGIN, RUN, RUN, COMMIT -- a no-op by design), read one
  after the other and also held at once, each result pulled by its qid in small batches;
* one RUN whose text holds both, which some clients send: refused whole, never only one run.

Each statement's records equal that statement run alone, and governance applies to each: the
clerk sees only east orders, and every email and customer name masked."""

# Requirements: REQ-802

from __future__ import annotations

import os
from typing import LiteralString

import pytest
import sqlalchemy as sa

from tests.integration.worker_boot_harness import WorkerBoot

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = int(os.environ.get("PG_PORT", "5432"))
_ROLES = ["org_admin", "clerk"]
_MASKED = [
    {"to": ["org_admin"], "mask": None},
    {"to": ["*"], "mask": {"type": "constant", "value": "***"}},
]

_ORDERS = "MATCH (o:Orders) RETURN o.id AS id, o.email AS email ORDER BY id"
_CUSTOMERS = "MATCH (c:Customers) RETURN c.id AS id, c.name AS name ORDER BY id"

_EXPECTED = {
    ("org_admin", _ORDERS): [
        {"id": 1, "email": "a@x.io"},
        {"id": 2, "email": "b@x.io"},
        {"id": 3, "email": "c@x.io"},
    ],
    ("clerk", _ORDERS): [{"id": 1, "email": "***"}, {"id": 3, "email": "***"}],
    ("org_admin", _CUSTOMERS): [{"id": 1, "name": "ann"}, {"id": 2, "name": "bo"}],
    ("clerk", _CUSTOMERS): [{"id": 1, "name": "***"}, {"id": 2, "name": "***"}],
}


def _column(name: str, data_type: str, **extra) -> dict:
    return {
        "name": name,
        "data_type": data_type,
        "visible_to": _ROLES,
        "is_primary_key": name == "id",
        **extra,
    }


def _table(name: str, *columns: dict) -> dict:
    return {
        "source_id": "sales-pg",
        "domain_id": "sales",
        "schema": "public",
        "table": name,
        "columns": list(columns),
    }


@pytest.fixture(scope="module")
def server():
    boot = WorkerBoot(
        1,
        pg_host=_PG_HOST,
        pg_port=_PG_PORT,
        extra_config={
            "tables": [
                _table(
                    "orders",
                    _column("id", "integer"),
                    _column("region", "varchar"),
                    _column("email", "varchar", masks=_MASKED),
                ),
                _table(
                    "customers", _column("id", "integer"), _column("name", "varchar", masks=_MASKED)
                ),
            ],
            "roles": [
                {
                    "description": "Test role.",
                    "id": "clerk",
                    "capabilities": ["query_development", "full_results"],
                    "domain_access": ["*"],
                }
            ],
            "rls_rules": [
                {
                    "description": "Test rule.",
                    "table_id": "orders",
                    "role_id": "clerk",
                    "filter": "region = 'east'",
                }
            ],
        },
        env={"PROVISA_REDIRECT_ENABLED": "false"},
    )
    boot.create_database()
    try:
        engine = sa.create_engine(boot.url, isolation_level="AUTOCOMMIT")
        with engine.connect() as conn:
            conn.execute(sa.text("ALTER TABLE public.orders ADD COLUMN email text"))
            conn.execute(sa.text("DELETE FROM public.orders"))
            conn.execute(
                sa.text(
                    "INSERT INTO public.orders VALUES "
                    "(1, 'east', 'a@x.io'), (2, 'west', 'b@x.io'), (3, 'east', 'c@x.io')"
                )
            )
            conn.execute(
                sa.text("CREATE TABLE public.customers (id integer PRIMARY KEY, name text)")
            )
            conn.execute(sa.text("INSERT INTO public.customers VALUES (1, 'ann'), (2, 'bo')"))
        engine.dispose()
        boot.start()
        boot.wait_all_ready(timeout=300)
        yield boot
    finally:
        boot.cleanup()


def _driver(boot, role: str):
    from neo4j import GraphDatabase

    return GraphDatabase.driver(f"bolt://127.0.0.1:{boot.ports['bolt']}", auth=(role, ""))


def _alone(boot, role: str, statement: LiteralString) -> list[dict]:
    driver = _driver(boot, role)
    try:
        with driver.session() as session:
            return [dict(r) for r in session.run(statement)]
    finally:
        driver.close()


@pytest.mark.parametrize("role", _ROLES)
def test_each_statement_alone_is_governed(server, role):
    for statement in (_ORDERS, _CUSTOMERS):
        assert _alone(server, role, statement) == _EXPECTED[(role, statement)]


@pytest.mark.parametrize("role", _ROLES)
def test_two_runs_in_one_session_each_answer_as_run_alone(server, role):
    driver = _driver(server, role)
    try:
        with driver.session() as session:
            orders = [dict(r) for r in session.run(_ORDERS)]
            customers = [dict(r) for r in session.run(_CUSTOMERS)]
    finally:
        driver.close()
    assert orders == _alone(server, role, _ORDERS) == _EXPECTED[(role, _ORDERS)]
    assert customers == _alone(server, role, _CUSTOMERS) == _EXPECTED[(role, _CUSTOMERS)]


@pytest.mark.parametrize("role", _ROLES)
def test_two_runs_in_one_transaction_each_answer_as_run_alone(server, role):
    driver = _driver(server, role)
    try:
        with driver.session() as session:
            tx = session.begin_transaction()
            orders = [dict(r) for r in tx.run(_ORDERS)]
            customers = [dict(r) for r in tx.run(_CUSTOMERS)]
            tx.commit()
    finally:
        driver.close()
    assert orders == _EXPECTED[(role, _ORDERS)]
    assert customers == _EXPECTED[(role, _CUSTOMERS)]


@pytest.mark.parametrize("role", _ROLES)
def test_two_results_held_at_once_in_a_transaction_each_get_their_own_records(server, role):
    """One record per PULL: the first result is still open when the second RUN is sent, and the
    driver then pulls each by its qid."""
    driver = _driver(server, role)
    try:
        with driver.session(fetch_size=1) as session:
            tx = session.begin_transaction()
            first = tx.run(_ORDERS)
            second = tx.run(_CUSTOMERS)
            orders = [dict(r) for r in first]
            customers = [dict(r) for r in second]
            tx.commit()
    finally:
        driver.close()
    assert orders == _EXPECTED[(role, _ORDERS)]
    assert customers == _EXPECTED[(role, _CUSTOMERS)]


@pytest.mark.parametrize("role", _ROLES)
def test_one_run_holding_two_statements_is_refused_whole(server, role):
    from neo4j.exceptions import CypherSyntaxError

    driver = _driver(server, role)
    try:
        with driver.session() as session:
            with pytest.raises(CypherSyntaxError) as refused:
                session.run(f"{_ORDERS}; {_CUSTOMERS}").data()
            assert "exactly one statement per query but got: 2" in refused.value.message
            # The session goes on: the next statement answers as it would alone.
            assert [dict(r) for r in session.run(_CUSTOMERS)] == _EXPECTED[(role, _CUSTOMERS)]
    finally:
        driver.close()


def test_a_statement_ending_in_a_semicolon_answers_as_without_it(server):
    assert _alone(server, "clerk", f"{_ORDERS};") == _EXPECTED[("clerk", _ORDERS)]
