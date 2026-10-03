# Copyright (c) 2026 Kenneth Stott
# Canary: 2f9c6a84-1e37-4b52-9d0a-7c5e3b8f1a26
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A view's SQL is lowered against the whole model, on a real server.

Its meaning must not depend on which role sorts first: the table's grants here name only two
roles, and the seeded roles that sort before them see none of its columns. Before, a view over it
was lowered under the first such role's context and its refresh failed "schema does not exist"."""

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
_ROWS = [(1, "east"), (2, "west"), (3, "east")]
_SQL = "SELECT id, region, email FROM sales.orders"
# The table's grants name only these two roles: the seeded roles that sort before them (analyst,
# developer, …) see none of its columns, which is what made a view's lowering depend on order.
_ROLES = ["org_admin", "east_reader"]


def _columns(**extra: dict) -> list[dict]:
    cols = [
        {"name": "id", "data_type": "integer", "visible_to": _ROLES},
        {"name": "region", "data_type": "varchar", "visible_to": _ROLES},
        {"name": "email", "data_type": "varchar", "visible_to": _ROLES},
    ]
    for c in cols:
        c.update(extra.get(c["name"], {}))
    return cols


@pytest.fixture(scope="module")
def server():
    base = _config(_PG_HOST, _PG_PORT, "unused")
    orders = base["tables"][0]
    orders["columns"] = _columns(
        id={"is_primary_key": True},
        email={"mask_type": "constant", "mask_value": "***", "unmasked_to": ["org_admin"]},
    )

    def _view(table: str, **kw) -> dict:
        return {
            "source_id": "__derived__",
            "domain_id": "sales",
            "schema": "views",
            "table": table,
            "view_sql": _SQL,
            "columns": _columns(),
            **kw,
        }

    boot = WorkerBoot(
        1,
        pg_host=_PG_HOST,
        pg_port=_PG_PORT,
        extra_config={
            "tables": [orders, _view("orders_inline"), _view("orders_mat", materialize=True)],
            "roles": [
                {
                    "id": "org_admin",
                    "capabilities": ["query_development", "full_results", "observability"],
                    "domain_access": ["*"],
                },
                {
                    "id": "east_reader",
                    "capabilities": ["query_development", "full_results"],
                    "domain_access": ["*"],
                },
            ],
            "rls_rules": [
                {"table_id": "orders", "role_id": "east_reader", "filter": "region = 'east'"}
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
            for order_id, region in _ROWS:
                conn.execute(
                    sa.text("INSERT INTO public.orders (id, region, email) VALUES (:i, :r, :e)"),
                    {"i": order_id, "r": region, "e": f"u{order_id}@x"},
                )
        engine.dispose()
        boot.start()
        boot.wait_all_ready(timeout=300)
        yield boot
    finally:
        boot.cleanup()


def _post(boot, role: str, path: str, body: dict) -> tuple[int, dict]:
    req = urllib.request.Request(
        f"http://127.0.0.1:{boot.ports['http']}{path}",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "x-provisa-role": role},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, {"error": exc.read().decode()}


def _sql_rows(boot, role: str, table: str) -> list[tuple]:
    status, body = _post(
        boot, role, "/data/sql", {"sql": f"SELECT id, region, email FROM sales.{table} ORDER BY id"}
    )
    assert status == 200, body
    return [(r["id"], r["region"], r["email"]) for r in body["data"]["sql"]]


def _graphql_rows(boot, role: str, field: str) -> list[tuple]:
    status, body = _post(
        boot, role, "/data/graphql", {"query": f"{{ {field} {{ id region email }} }}"}
    )
    assert status == 200 and "errors" not in body, body
    return sorted((r["id"], r["region"], r["email"]) for r in body["data"][field])


_ALL = [(1, "east", "u1@x"), (2, "west", "u2@x"), (3, "east", "u3@x")]


@pytest.fixture(scope="module")
def refreshed(server):
    status, body = _post(
        server,
        "org_admin",
        "/admin/graphql",
        {"query": 'mutation { refreshMv(mvId: "view-orders_mat") { success message } }'},
    )
    assert status == 200 and body["data"]["refreshMv"]["success"], body
    return server


def test_a_materialized_view_is_built_whole_and_lowered_whatever_role_sorts_first(refreshed):
    """The materialized view is built from the whole table (it belongs to no reader), and its
    refresh lowers its SQL against the model, not a role's context."""
    assert _sql_rows(refreshed, "org_admin", "orders_mat") == _ALL
    assert _graphql_rows(refreshed, "org_admin", "s__ordersMat") == _ALL


def test_an_inline_view_is_lowered_whatever_role_sorts_first(server):
    assert _sql_rows(server, "org_admin", "orders_inline") == _ALL
    assert _graphql_rows(server, "org_admin", "s__ordersInline") == _ALL
