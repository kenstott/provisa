# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: a Bolt connection's ephemeral graph (REQ-2176), through the official driver
against one real server.

* A CREATE of a free label makes session nodes the same connection reads back; another
  connection never sees them, and they end with their connection.
* A value a CREATE takes from model data passes that data's governance: the clerk's session
  nodes hold only east orders' emails, masked.
* A session relationship ends on a model node, and MATCH reads it from the model row.
* SET n:L tags a model node, which MATCH (n:L) then reads; REMOVE n:L untags it.

Each case runs in one explicit transaction, so its statements share one connection."""

# Requirements: REQ-2176, REQ-615

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
    {"mask": {"type": "constant", "value": "***"}},
]


def _column(name: str, data_type: str, **extra) -> dict:
    return {
        "name": name,
        "data_type": data_type,
        "visible_to": _ROLES,
        "is_primary_key": name == "id",
        **extra,
    }


@pytest.fixture(scope="module")
def server():
    boot = WorkerBoot(
        1,
        pg_host=_PG_HOST,
        pg_port=_PG_PORT,
        extra_config={
            "tables": [
                {
                    "source_id": "sales-pg",
                    "domain_id": "sales",
                    "schema": "public",
                    "table": "orders",
                    "columns": [
                        _column("id", "integer"),
                        _column("region", "varchar"),
                        _column("email", "varchar", masks=_MASKED),
                    ],
                }
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
        engine.dispose()
        boot.start()
        boot.wait_all_ready(timeout=300)
        yield boot
    finally:
        boot.cleanup()


def _driver(boot, role: str):
    from neo4j import GraphDatabase

    return GraphDatabase.driver(f"bolt://127.0.0.1:{boot.ports['bolt']}", auth=(role, ""))


def _in_one_connection(boot, role: str, *statements: LiteralString) -> list[list[dict]]:
    """Each statement's records, the statements run in order in one transaction."""
    driver = _driver(boot, role)
    try:
        with driver.session() as session:
            tx = session.begin_transaction()
            answers = [[dict(r) for r in tx.run(statement)] for statement in statements]
            tx.commit()
    finally:
        driver.close()
    return answers


def test_session_nodes_are_read_back_in_their_connection_alone(server):
    from neo4j.exceptions import ClientError

    *_, notes = _in_one_connection(
        server,
        "org_admin",
        "CREATE (:Note {text: 'x'}), (:Note {text: 'y'})",
        "MATCH (n:Note) RETURN n.text AS text ORDER BY text",
    )
    assert notes == [{"text": "x"}, {"text": "y"}]
    # Another connection holds no session nodes: the label is none it can read.
    with pytest.raises(ClientError):
        _in_one_connection(server, "org_admin", "MATCH (n:Note) RETURN n.text AS text")


def test_a_value_from_model_data_passes_its_governance(server):
    *_, seen = _in_one_connection(
        server,
        "clerk",
        "MATCH (o:Orders) CREATE (:Seen {id: o.id, email: o.email})",
        "MATCH (s:Seen) RETURN s.id AS id, s.email AS email ORDER BY id",
    )
    assert seen == [{"id": 1, "email": "***"}, {"id": 3, "email": "***"}]


def test_a_session_relationship_ends_on_a_model_node(server):
    *_, flagged = _in_one_connection(
        server,
        "org_admin",
        "MATCH (o:Orders) WHERE o.id = 1 CREATE (o)-[:FLAGGED]->(:Flag {why: 'late'})",
        "MATCH (o:Orders)-[:FLAGGED]->(f:Flag) RETURN o.id AS id, o.email AS email, f.why AS why",
    )
    assert flagged == [{"id": 1, "email": "a@x.io", "why": "late"}]


def test_set_tags_a_model_node_and_remove_untags_it(server):
    _, tagged, _, untagged = _in_one_connection(
        server,
        "org_admin",
        "MATCH (o:Orders) WHERE o.id = 3 SET o:Vip",
        "MATCH (n:Vip) RETURN n.id AS id, n.email AS email",
        "MATCH (n:Vip) REMOVE n:Vip",
        "MATCH (n:Vip) RETURN n.id AS id",
    )
    assert tagged == [{"id": 3, "email": "c@x.io"}]
    assert untagged == []
