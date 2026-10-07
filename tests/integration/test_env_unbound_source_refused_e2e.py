# Copyright (c) 2026 Kenneth Stott
# Canary: b6b73c47-7534-4249-9a10-ddc8d2ba25f4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A source an environment holds unbound is read through no connection, on any route and any
surface (REQ-1491, REQ-1942), on a real server.

The org's environments share one engine, and that engine holds the attach prod made of the same
source. A read of an unbound source that reached the engine therefore returned prod's rows: a
single-source read once the pipeline handed a source with no pool to the engine, and a join from
the start, since a statement over two sources is the engine's. Both are refused before routing,
naming the source and the environment."""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from typing import Any

import pytest
import sqlalchemy as sa

from tests.integration.worker_boot_harness import WorkerBoot

pytestmark = [pytest.mark.integration]

_VISIBLE = {"visible_to": ["org_admin", "analyst"]}
_JOIN = "SELECT COUNT(*) AS n FROM sales.orders o JOIN sales.regions r ON o.region = r.name"


@pytest.fixture(scope="module")
def boot():
    host = os.environ.get("PG_HOST", "localhost")
    port = int(os.environ.get("PG_PORT", "5432"))
    b = WorkerBoot(1, pg_host=host, pg_port=port, env={"PROVISA_REDIRECT_ENABLED": "false"})
    connection = {
        "type": "postgresql",
        "host": host,
        "port": port,
        "database": b.database,
        "username": "provisa",
        "password": "${env:PG_PASSWORD}",
    }
    # A second source over the same database, so a join of the two is a two-source statement.
    b._extra_config = {
        "sources": [{"id": "sales-pg", **connection}, {"id": "ref-pg", **connection}],
        "tables": [
            {
                "source_id": "sales-pg",
                "domain_id": "sales",
                "schema": "public",
                "table": "orders",
                "columns": [
                    {"name": "id", "data_type": "integer", **_VISIBLE},
                    {"name": "region", "data_type": "varchar", **_VISIBLE},
                ],
            },
            {
                "source_id": "ref-pg",
                "domain_id": "sales",
                "schema": "public",
                "table": "regions",
                "columns": [{"name": "name", "data_type": "varchar", **_VISIBLE}],
            },
        ],
        # The join below is governed like any other: it needs its relationship approved.
        "relationships": [
            {
                "id": "order-region",
                "source_table_id": "orders",
                "target_table_id": "regions",
                "source_column": "region",
                "target_column": "name",
                "cardinality": "many-to-one",
            }
        ],
    }
    b.create_database()
    own = sa.create_engine(b.url, isolation_level="AUTOCOMMIT")
    with own.connect() as conn:
        conn.execute(sa.text("CREATE TABLE public.regions (name text PRIMARY KEY)"))
        conn.execute(sa.text("INSERT INTO public.regions VALUES ('east'), ('west')"))
    own.dispose()
    try:
        b.start()
        b.wait_all_ready(timeout=300)
        yield b
    finally:
        b.cleanup()


def _call(b, method: str, path: str, body: Any = None, env: str | None = None):
    headers = {"Content-Type": "application/json", "x-provisa-role": "org_admin"}
    if env is not None:
        headers["x-provisa-env"] = env
    req = urllib.request.Request(
        f"http://127.0.0.1:{b.ports['http']}{path}",
        data=None if body is None else json.dumps(body).encode(),
        headers=headers,
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=300) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, {"error": exc.read().decode()}


def _environment(b, name: str, data_mode: str) -> None:
    status, body = _call(
        b, "POST", f"/admin/orgs/{b.org_id}/environments", {"name": name, "data_mode": data_mode}
    )
    assert status == 200, body


def _sql(b, sql: str, env: str | None = None):
    return _call(b, "POST", "/data/sql", {"sql": sql}, env)


def _refused(status: int, body: dict, source: str, env: str) -> None:
    text = json.dumps(body)
    assert status == 409, (status, body)
    assert "data.source_unbound" in text, body
    assert f"source '{source}' has no connection in environment '{env}'" in text, body


def test_prod_reads_each_source_and_their_join(boot):
    status, body = _sql(boot, _JOIN)
    assert status == 200 and body["data"]["sql"] == [{"n": 2}], body


def test_a_read_of_one_unbound_source_is_refused_over_sql(boot):
    _environment(boot, "bare", "unbound")
    status, body = _sql(boot, "SELECT COUNT(*) AS n FROM sales.orders", "bare")
    _refused(status, body, "sales-pg", "bare")


def test_a_read_of_one_unbound_source_is_refused_over_graphql(boot):
    _environment(boot, "bareql", "unbound")
    status, body = _call(boot, "POST", "/data/graphql", {"query": "{ s__orders { id } }"}, "bareql")
    text = json.dumps(body)
    assert "source 'sales-pg' has no connection in environment 'bareql'" in text, (status, body)
    assert not (body.get("data") or {}).get("s__orders"), body


def test_a_join_naming_one_unbound_source_is_refused_and_the_bound_one_still_reads(boot):
    """Only sales-pg is cleared; ref-pg keeps the connection copied from prod. The join is a
    two-source statement, the engine's from the start, and is refused for the source it names."""
    _environment(boot, "half", "inherit")
    status, body = _sql(boot, _JOIN, "half")
    assert status == 200 and body["data"]["sql"] == [{"n": 2}], body

    path = f"/admin/orgs/{boot.org_id}/environments/half/sources/sales-pg/binding"
    status, body = _call(boot, "PUT", path, {"binding": "unbound"})
    assert status == 200, body

    status, body = _sql(boot, _JOIN, "half")
    _refused(status, body, "sales-pg", "half")
    status, body = _sql(boot, "SELECT COUNT(*) AS n FROM sales.regions", "half")
    assert status == 200 and body["data"]["sql"] == [{"n": 2}], body
