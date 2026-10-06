# Copyright (c) 2026 Kenneth Stott
# Canary: 6f3b0d28-9a41-4c7e-b5d2-e8a1c7f40936
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A table registered with an underscore before a digit is published to SQL under that name.

The GraphQL field name was camel-cased with the boundary before the digit dropped, and the SQL name
was derived back from it, so ``orders_2024`` was published as ``orders2024`` (GraphQL
``s__orders2024``). The SQL tests here pin that a table and an ingest table with such a name are
read by their registered names; the GraphQL test is the one that failed before the fix.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from typing import Any

import pytest
import sqlalchemy as sa

from tests.integration.worker_boot_harness import WorkerBoot, _config

pytestmark = [pytest.mark.integration]

_ROLES = ["org_admin", "analyst"]


@pytest.fixture(scope="module")
def server():
    pg_host = os.environ.get("PG_HOST", "localhost")
    pg_port = int(os.environ.get("PG_PORT", "5432"))
    boot = WorkerBoot(
        1, pg_host=pg_host, pg_port=pg_port, env={"PROVISA_REDIRECT_ENABLED": "false"}
    )
    base = _config(pg_host, pg_port, boot.database)
    boot._extra_config = {
        "sources": base["sources"] + [{"id": "pushed", "type": "ingest"}],
        "tables": [
            {
                "source_id": "sales-pg",
                "domain_id": "sales",
                "schema": "public",
                "table": "orders_2024",
                "columns": [
                    {"name": "id", "data_type": "integer", "visible_to": _ROLES},
                    {"name": "region", "data_type": "varchar", "visible_to": _ROLES},
                ],
            },
            {
                "source_id": "pushed",
                "domain_id": "sales",
                # The org's control-plane schema, where ingest writes (REQ-1771).
                "schema": "org_default",
                "table": "events_2024",
                "columns": [
                    {"name": "ext_id", "data_type": "text", "visible_to": _ROLES},
                    {"name": "value", "data_type": "text", "visible_to": _ROLES},
                ],
            },
        ],
    }
    boot.create_database()
    try:
        engine = sa.create_engine(boot.url, isolation_level="AUTOCOMMIT")
        with engine.connect() as conn:
            conn.execute(sa.text("CREATE TABLE public.orders_2024 (id integer, region text)"))
            conn.execute(sa.text("INSERT INTO public.orders_2024 VALUES (1, 'east'), (2, 'west')"))
        engine.dispose()
        boot.start()
        boot.wait_all_ready(timeout=300)
        yield boot
    finally:
        boot.cleanup()


def _post(boot, path: str, body: Any) -> tuple[int, Any]:
    req = urllib.request.Request(
        f"http://127.0.0.1:{boot.ports['http']}{path}",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "x-provisa-role": "org_admin"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, {"error": exc.read().decode()}


def test_a_registered_table_is_queried_by_its_registered_name(server):
    status, body = _post(
        server, "/data/sql", {"sql": "SELECT id FROM sales.orders_2024 ORDER BY id"}
    )
    assert status == 200, body
    assert [r["id"] for r in body["data"]["sql"]] == [1, 2]


def test_an_ingest_tables_rows_are_read_by_its_registered_name(server):
    status, body = _post(server, "/data/ingest/pushed/events_2024", {"ext_id": "a", "value": "x"})
    assert status == 202 and body["inserted"] == "1", body
    status, body = _post(
        server, "/data/sql", {"sql": "SELECT ext_id, value FROM sales.events_2024"}
    )
    assert status == 200, body
    assert [(r["ext_id"], r["value"]) for r in body["data"]["sql"]] == [("a", "x")]


def test_the_published_graphql_field_keeps_the_registered_name(server):
    """The GraphQL field the SQL name is derived from: s__orders_2024, never s__orders2024."""
    status, body = _post(server, "/data/graphql", {"query": "{ s__orders_2024 { id } }"})
    assert status == 200 and "errors" not in body, body
    assert sorted(r["id"] for r in body["data"]["s__orders_2024"]) == [1, 2]
