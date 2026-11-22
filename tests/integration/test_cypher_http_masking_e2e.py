# Copyright (c) 2026 Kenneth Stott
# Canary: 5c2e9a47-6d18-4b3f-8e75-1a9d4c7b2f03
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Column masking over Cypher on HTTP (REQ-038, REQ-263, REQ-2059).

One real server over a PostgreSQL ``orders(id, region, email)``. ``email`` carries a mask list:
``org_admin`` reads it clear, every other reader reads the constant ``***``. A restricted role's
Cypher over ``POST /data/cypher`` reads the masked value the role's rule states, the same role
over SQL reads the same value, and the unrestricted role reads the clear value on both."""

from __future__ import annotations

import json
import os
import urllib.request

import pytest
import sqlalchemy as sa

from tests.integration.worker_boot_harness import WorkerBoot, _config

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = int(os.environ.get("PG_PORT", "5432"))
_ROWS = [(1, "east", "u1@x"), (2, "west", "u2@x"), (3, "east", "u3@x")]
_ROLES = ["org_admin", "masked_reader"]
_MASK = "***"


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
            "masks": [
                {"to": ["org_admin"], "mask": None},
                {"mask": {"type": "constant", "value": _MASK}},
            ],
        },
    ]
    boot = WorkerBoot(
        1,
        pg_host=_PG_HOST,
        pg_port=_PG_PORT,
        extra_config={
            "tables": [orders],
            "roles": [
                {
                    "id": "masked_reader",
                    "description": "Test role.",
                    "capabilities": ["query_development", "full_results"],
                    "domain_access": ["*"],
                },
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
            for order_id, region, email in _ROWS:
                conn.execute(
                    sa.text("INSERT INTO public.orders (id, region, email) VALUES (:i, :r, :e)"),
                    {"i": order_id, "r": region, "e": email},
                )
        engine.dispose()
        boot.start()
        boot.wait_all_ready(timeout=300)
        yield boot
    finally:
        boot.cleanup()


def _request(boot, role: str, path: str, body: dict | None) -> dict:
    req = urllib.request.Request(
        f"http://127.0.0.1:{boot.ports['http']}{path}",
        data=None if body is None else json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "x-provisa-role": role},
        method="GET" if body is None else "POST",
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        assert resp.status == 200
        return json.loads(resp.read().decode())


def _cypher_emails(boot, role: str) -> list[tuple[int, str]]:
    schema = _request(boot, role, "/data/graph-schema", None)
    label = next(n["label"] for n in schema["node_labels"] if n["domain_id"] == "sales")
    body = _request(
        boot,
        role,
        "/data/cypher",
        {"query": f"MATCH (n:{label}) RETURN n.id AS id, n.email AS email ORDER BY id"},
    )
    return [(r["id"], r["email"]) for r in body["rows"]]


def _sql_emails(boot, role: str) -> list[tuple[int, str]]:
    body = _request(
        boot,
        role,
        "/data/sql",
        {"sql": "SELECT id, email FROM sales.orders ORDER BY id"},
    )
    return [(r["id"], r["email"]) for r in body["data"]["sql"]]


def test_a_restricted_role_reads_the_masked_value_over_cypher_http(server):
    assert _cypher_emails(server, "masked_reader") == [(i, _MASK) for i, _, _ in _ROWS]


def test_a_restricted_role_reads_the_same_masked_value_over_sql_and_cypher(server):
    assert _sql_emails(server, "masked_reader") == _cypher_emails(server, "masked_reader")


def test_the_unrestricted_role_reads_the_clear_value_over_cypher_http(server):
    assert _cypher_emails(server, "org_admin") == [(i, e) for i, _, e in _ROWS]
    assert _sql_emails(server, "org_admin") == _cypher_emails(server, "org_admin")
