# Copyright (c) 2026 Kenneth Stott
# Canary: d19fcdb4-a723-4ac9-b98b-d11edd5762b8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A role computes only over what it can see, on a real server.

``east_reader`` has a row filter on ``orders`` (east rows) and a mask on ``email``; ``org_admin``
has neither. Through the table, an inline view over it and a materialized view over it — on SQL
over HTTP, pgwire and GraphQL — east_reader gets its rows, masked; its own filters and
expressions on the masked column see the mask; and a nested SELECT carries its row filter.
org_admin, the control, reads everything, and reads a materialized view from its stored rows."""

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
# This tree lowers a view's SQL under the first role by id (``analyst`` among the seeded roles),
# so that role is granted the columns too; the readers under test are the other two.
_ROLES = ["analyst", "org_admin", "east_reader"]


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


_EAST_MASKED = [(1, "east", "***"), (3, "east", "***")]


def _pgwire_rows(boot, role: str, sql: str) -> list[tuple]:
    import psycopg

    with psycopg.connect(
        host="127.0.0.1",
        port=boot.ports["pgwire"],
        user=role,
        password="provisa",
        dbname="provisa",
        autocommit=True,
        connect_timeout=30,
    ) as conn:
        return conn.execute(sql).fetchall()


# --- the table ------------------------------------------------------------------------------------


def test_the_table_is_governed_for_the_reader(server):  # noqa: F811
    assert _sql_rows(server, "org_admin", "orders") == _ALL
    assert _sql_rows(server, "east_reader", "orders") == _EAST_MASKED
    assert _graphql_rows(server, "east_reader", "s__orders") == _EAST_MASKED


# --- through a view -------------------------------------------------------------------------------


def test_an_inline_view_carries_the_readers_rules_on_the_table_it_reads(server):  # noqa: F811
    assert _sql_rows(server, "east_reader", "orders_inline") == _EAST_MASKED
    assert _graphql_rows(server, "east_reader", "s__ordersInline") == _EAST_MASKED
    assert (
        _pgwire_rows(
            server, "east_reader", "SELECT id, region, email FROM sales.orders_inline ORDER BY id"
        )
        == _EAST_MASKED
    )
    assert _sql_rows(server, "org_admin", "orders_inline") == _ALL


def test_a_materialized_view_gives_a_narrowed_reader_only_what_its_inputs_allow(refreshed):  # noqa: F811
    assert _sql_rows(refreshed, "east_reader", "orders_mat") == _EAST_MASKED
    assert _graphql_rows(refreshed, "east_reader", "s__ordersMat") == _EAST_MASKED


def test_a_reader_with_no_narrower_rule_reads_the_stored_rows(refreshed):  # noqa: F811
    """Proof that org_admin reads the materialized view's stored rows and east_reader its
    governed expansion: a row changed at the source behind the server (the view stays fresh)
    shows to org_admin through the inline view and to east_reader's expansion, but not through
    the materialized view org_admin reads."""
    engine = sa.create_engine(refreshed.url, isolation_level="AUTOCOMMIT")
    try:
        with engine.connect() as conn:
            conn.execute(sa.text("UPDATE public.orders SET region = 'west' WHERE id = 3"))
        try:
            assert _sql_rows(refreshed, "org_admin", "orders_inline")[2] == (3, "west", "u3@x")
            assert _sql_rows(refreshed, "org_admin", "orders_mat") == _ALL  # as built
            assert _graphql_rows(refreshed, "org_admin", "s__ordersMat") == _ALL
            assert _sql_rows(refreshed, "east_reader", "orders_mat") == [(1, "east", "***")]
        finally:
            with engine.connect() as conn:
                conn.execute(sa.text("UPDATE public.orders SET region = 'east' WHERE id = 3"))
    finally:
        engine.dispose()


# --- the reader's own filters and expressions -----------------------------------------------------


def _sql(boot, role: str, sql: str) -> list[dict]:
    status, body = _post(boot, role, "/data/sql", {"sql": sql})
    assert status == 200, body
    return body["data"]["sql"]


def test_a_filter_on_a_masked_column_is_refused_for_the_masked_reader(server):  # noqa: F811
    """REQ-531 (V005): a role may not filter on a column it sees masked — the filter would test
    the real value. Refused on every surface; a role that sees the column unmasked filters on it."""
    import psycopg

    by_email = "SELECT id FROM sales.orders WHERE email = 'u1@x'"
    assert _sql(server, "org_admin", by_email) == [{"id": 1}]
    status, body = _post(server, "east_reader", "/data/sql", {"sql": by_email})
    assert status == 403 and "V005" in str(body), body
    with pytest.raises(psycopg.Error, match="V005"):
        _pgwire_rows(server, "east_reader", by_email)
    where = {"query": '{ s__orders(where: {email: {eq: "u1@x"}}) { id } }'}
    status, body = _post(server, "east_reader", "/data/graphql", where)
    assert status == 403 and "V005" in str(body), body
    status, body = _post(server, "org_admin", "/data/graphql", where)
    assert status == 200 and body["data"]["s__orders"] == [{"id": 1}], body


def test_ordering_and_grouping_by_a_masked_column_use_the_mask(server):  # noqa: F811
    """Ordering by the real value would reveal its order; grouping by it its distinct count."""
    ordered = "SELECT id FROM sales.orders ORDER BY email DESC, id"
    assert [r["id"] for r in _sql(server, "org_admin", ordered)] == [3, 2, 1]
    assert [r["id"] for r in _sql(server, "east_reader", ordered)] == [1, 3]
    grouped = "SELECT count(*) AS n FROM sales.orders GROUP BY email"
    assert sorted(r["n"] for r in _sql(server, "org_admin", grouped)) == [1, 1, 1]
    assert [r["n"] for r in _sql(server, "east_reader", grouped)] == [2]


def test_an_expression_over_a_masked_column_sees_the_mask(server):  # noqa: F811
    upper = "SELECT id, upper(email) AS e FROM sales.orders ORDER BY id"
    assert _sql(server, "east_reader", upper) == [{"id": 1, "e": "***"}, {"id": 3, "e": "***"}]
    assert _sql(server, "org_admin", upper)[0] == {"id": 1, "e": "U1@X"}


def test_a_nested_select_carries_the_row_filter(server):  # noqa: F811
    """Row 2 is west. A reader filtered to east must not learn it exists through a CTE or an
    EXISTS beside another read of the table."""
    exists = "SELECT a.id FROM sales.orders a WHERE EXISTS (SELECT 1 FROM sales.orders b WHERE b.id = 2) ORDER BY a.id"
    assert [r["id"] for r in _sql(server, "org_admin", exists)] == [1, 2, 3]
    assert _sql(server, "east_reader", exists) == []
    cte = (
        "WITH o AS (SELECT id, region FROM sales.orders) "
        "SELECT o.id FROM sales.orders a JOIN o ON o.id = a.id + 1 ORDER BY o.id"
    )
    assert [r["id"] for r in _sql(server, "org_admin", cte)] == [2, 3]
    assert _sql(server, "east_reader", cte) == []  # a.id + 1 is 2 or 4: neither is an east row
