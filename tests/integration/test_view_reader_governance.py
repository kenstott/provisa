# Copyright (c) 2026 Kenneth Stott
# Canary: 2f9c6a84-1e37-4b52-9d0a-7c5e3b8f1a26
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

import pytest
import sqlalchemy as sa

from tests.integration.test_view_lowering_e2e import (  # noqa: F401
    _ALL,
    _graphql_rows,
    _post,
    _sql_rows,
    refreshed,
    server,
)

pytestmark = [pytest.mark.integration]

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
