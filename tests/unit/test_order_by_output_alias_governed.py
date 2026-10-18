# Copyright (c) 2026 Kenneth Stott
# Canary: 239c32e0-0a0c-475b-abec-cd27646c740c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An ORDER BY naming one of the statement's own output columns orders by it, under any role
(REQ-263, REQ-040).

An output alias (``SUM(x) AS revenue ... ORDER BY revenue``) is the statement's own name, not a
column of any table, so column governance has nothing to decide about it: it is neither hidden
nor masked. Governed as if it were a table column that no table makes visible, it was replaced
by NULL, and an ordering by a constant is dropped -- the role's rows came back unordered."""

# Requirements: REQ-263, REQ-040

from __future__ import annotations

import pytest

from provisa.compiler.stage2 import GovernanceContext, apply_governance


def _gov() -> GovernanceContext:
    gov = GovernanceContext()
    gov.table_map = {"store.orders": 1, "orders": 1, "store.customers": 2, "customers": 2}
    gov.all_columns = {
        1: [
            ("id", "integer"),
            ("customer_id", "integer"),
            ("status", "varchar"),
            ("total_cents", "integer"),
        ],
        2: [("id", "integer"), ("region", "varchar"), ("email", "varchar")],
    }
    # A role that sees some columns of each table, and a row rule on one.
    gov.visible_columns = {
        1: frozenset({"id", "customer_id", "status", "total_cents"}),
        2: frozenset({"id", "region"}),
    }
    gov.rls_rules = {1: "status != 'cancelled'"}
    return gov


@pytest.mark.parametrize(
    "order_by",
    ["ORDER BY revenue DESC", "ORDER BY revenue DESC, region", "ORDER BY 2 DESC"],
)
def test_an_ordering_by_an_output_column_is_kept(order_by):
    governed = apply_governance(
        "SELECT c.region AS region, SUM(o.total_cents) / 100.0 AS revenue "
        'FROM "store"."orders" AS o JOIN "store"."customers" AS c ON c.id = o.customer_id '
        f"GROUP BY c.region {order_by}",
        _gov(),
    )
    assert order_by in governed, governed


def test_an_ordering_by_a_hidden_column_is_still_refused_or_dropped():
    """A column the role does not see is no output of the statement: governed as before."""
    governed = apply_governance(
        'SELECT c.region FROM "store"."customers" AS c ORDER BY c.email', _gov()
    )
    assert "email" not in governed


def test_an_output_named_as_a_hidden_column_orders_by_the_output():
    """ORDER BY reads an output name before a table's column of that name: the ordering is by
    the governed output, which names nothing hidden."""
    governed = apply_governance(
        'SELECT c.region AS email FROM "store"."customers" AS c ORDER BY email', _gov()
    )
    assert governed.endswith("ORDER BY email"), governed
    assert "c.email" not in governed and '"c"."email"' not in governed
