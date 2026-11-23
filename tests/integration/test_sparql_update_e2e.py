# Copyright (c) 2026 Kenneth Stott
# Canary: 8d4f1b62-3a7e-4c95-b0d8-2e6a9c1f7b34
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""SPARQL Update through /data/sparql against a real PostgreSQL source (REQ-2135, amended
2026-11-22): INSERT DATA, DELETE WHERE and DELETE/INSERT ... WHERE are written as the governed
table writes a Cypher write makes, sent as the SPARQL 1.1 Protocol sends an update (an
application/sparql-update body, or a form's update field).

One real server over a PostgreSQL source, one table ``orders(id, region)`` in domain ``sales``.
Every assertion is on the rows AT THE SOURCE after the call, read directly from PostgreSQL.

Roles:

* ``org_admin``   — holds ``write``; insert, update and delete grants over both columns.
* ``east_reader`` — reads everything in ``east``; no ``write`` right; no grant.
"""

# Requirements: REQ-2135, REQ-2074

from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request

import pytest
import sqlalchemy as sa

from tests.helpers import config_write_grants
from tests.integration.worker_boot_harness import WorkerBoot, _config

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = int(os.environ.get("PG_PORT", "5432"))
_SEED = [(1, "east"), (2, "west"), (3, "east")]
_ROLES = ["org_admin", "east_reader"]
BASE = "http://sparql-update.test"
ORDER = f"{BASE}/id/sales/orders"
REGION = f"{BASE}/vocab/sales/orders#region"
ORDERS = f"{BASE}/vocab/sales/orders"


@pytest.fixture(scope="module")
def server():
    base = _config(_PG_HOST, _PG_PORT, "unused")
    orders = base["tables"][0]
    for column in orders["columns"]:
        column["visible_to"] = _ROLES
        if column["name"] == "id":
            column["is_primary_key"] = True
    orders["write_grants"] = config_write_grants(["org_admin"], ["id", "region"])
    boot = WorkerBoot(
        1,
        pg_host=_PG_HOST,
        pg_port=_PG_PORT,
        extra_config={
            "tables": [orders],
            "roles": [
                {
                    "id": "east_reader",
                    "description": "Test role.",
                    "capabilities": ["query_development", "full_results"],
                    "domain_access": ["*"],
                },
            ],
            "rls_rules": [
                {
                    "description": "Test rule.",
                    "table_id": "orders",
                    "role_id": "east_reader",
                    "filter": "region = 'east'",
                },
            ],
        },
        # A subject's IRI starts with the deployment's public address (REQ-2135).
        env={"PROVISA_REDIRECT_ENABLED": "false", "PROVISA_PUBLIC_URL": BASE},
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

    def rows() -> list[tuple[int, str | None]]:
        with engine.connect() as conn:
            return [
                tuple(r)
                for r in conn.execute(sa.text("SELECT id, region FROM public.orders ORDER BY id"))
            ]

    yield rows
    engine.dispose()


def _post(boot, role: str, body: bytes, content_type: str) -> tuple[int, dict]:
    req = urllib.request.Request(
        f"http://127.0.0.1:{boot.ports['http']}/data/sparql",
        data=body,
        headers={"Content-Type": content_type, "x-provisa-role": role},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())


def _update(boot, role: str, text: str) -> tuple[int, dict]:
    return _post(boot, role, text.encode(), "application/sparql-update")


def test_insert_data_writes_a_row_at_the_source(server, source):
    status, doc = _update(
        server, "org_admin", f'INSERT DATA {{ <{ORDER}/9> a <{ORDERS}> ; <{REGION}> "north" }}'
    )
    assert status == 200, doc
    assert (9, "north") in source()


def test_insert_data_of_a_held_row_sets_its_property(server, source):
    status, doc = _update(server, "org_admin", f'INSERT DATA {{ <{ORDER}/2> <{REGION}> "south" }}')
    assert status == 200, doc
    assert source() == [(1, "east"), (2, "south"), (3, "east")]


def test_delete_where_deletes_the_rows_it_matches(server, source):
    status, doc = _update(
        server, "org_admin", f'DELETE WHERE {{ ?o a <{ORDERS}> ; <{REGION}> "east" }}'
    )
    assert status == 200, doc
    assert source() == [(2, "west")]


def test_modify_rewrites_each_solution(server, source):
    form = urllib.parse.urlencode(
        {
            "update": f'DELETE {{ ?o <{REGION}> "east" }} INSERT {{ ?o <{REGION}> "central" }} '
            f'WHERE {{ ?o <{REGION}> "east" }}'
        }
    ).encode()
    status, doc = _post(server, "org_admin", form, "application/x-www-form-urlencoded")
    assert status == 200, doc
    assert source() == [(1, "central"), (2, "west"), (3, "central")]


def test_a_role_without_the_write_right_writes_nothing(server, source):
    status, doc = _update(server, "east_reader", f'INSERT DATA {{ <{ORDER}/1> <{REGION}> "x" }}')
    assert status == 403, doc
    assert doc["code"] == "sparql.permission_denied"
    assert source() == _SEED


def test_graph_management_of_the_models_own_graph_is_refused_by_name(server, source):
    # REQ-2135 (amended 2026-11-23): DEFAULT and ALL hold the model's own graph.
    status, doc = _update(server, "org_admin", "DROP ALL")
    assert status == 400, doc
    assert doc["code"] == "sparql.refused"
    assert "names the model's own graph" in doc["detail"]
    assert source() == _SEED


def test_a_session_object_is_written_to_the_graph_session_and_read_by_cypher(server, source):
    """REQ-2135 (amended 2026-11-23): a blank node is a session node in the principal's graph
    session, in the tables Cypher's session objects use; the source is untouched."""
    status, doc = _update(
        server,
        "org_admin",
        'INSERT DATA { _:b a <http://ex.org/Ticket> ; <http://ex.org/title> "One" }',
    )
    assert status == 200, doc
    assert doc.get("session_changed") is True, doc
    assert doc["counters"]["nodes_created"] == 1, doc
    assert source() == _SEED
    request = urllib.request.Request(
        f"http://127.0.0.1:{server.ports['http']}/data/cypher",
        data=json.dumps({"query": "MATCH (n:Ticket) RETURN n.title AS t"}).encode(),
        headers={"Content-Type": "application/json", "x-provisa-role": "org_admin"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=120) as answered:
        rows = json.loads(answered.read())["rows"]
    assert rows in ([{"t": "One"}], [["One"]]), rows
    status, doc = _update(server, "org_admin", "DROP SILENT GRAPH <http://ex.org/g>")
    assert status == 200, doc
