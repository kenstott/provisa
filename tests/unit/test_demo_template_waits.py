# Copyright (c) 2026 Kenneth Stott
# Canary: 4c1e8a73-2b9d-4f05-a6e2-9d7b3f0c5e18
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A demo template that waits on its replicas says what it waits on, table by table (REQ-2006).
retail's regions seeding ran 900s with every eu replica recorded as built and the template never
counting one as landed, and no line saying why."""

# Requirements: REQ-2006

from __future__ import annotations

from types import SimpleNamespace

from provisa.api.demo_replicas import LANDED, landing
from provisa.api.demo_templates import waiting_on


def _row(table: str, *, store: str = "s1", rows: int = 3) -> dict:
    return {
        "source_id": "eu-shop",
        "schema_name": "public",
        "table_name": table,
        "built_store": store,
        "completed_rows": rows,
    }


def test_a_replica_is_landed_when_built_for_this_store_and_holding_its_rows():
    assert landing([_row("customers")], "s1", {"eu-shop__public__customers": 3}) == {
        "eu-shop__public__customers": LANDED
    }


def test_each_replica_not_landed_says_why():
    rows = [_row("customers", store="s0"), _row("orders"), _row("staff", rows=5)]
    held = {"eu-shop__public__customers": 3, "eu-shop__public__staff": 2}
    assert landing(rows, "s1", held) == {
        "eu-shop__public__customers": "recorded as built for store s0, not this one (s1)",
        "eu-shop__public__orders": "recorded as built, and not in the store",
        "eu-shop__public__staff": "holds 2 rows of the 5 its build recorded",
    }


def test_a_template_waits_on_each_declared_table_not_landed_and_on_none_landed():
    tables = [
        SimpleNamespace(source_id="eu-shop", schema_name="public", table_name=t)
        for t in ("customers", "orders", "staff")
    ]
    states = {
        "eu-shop__public__customers": LANDED,
        "eu-shop__public__staff": "holds 2 rows of the 5 its build recorded",
    }
    assert waiting_on(tables, states) == (
        "eu-shop.public.orders: no build recorded; "
        "eu-shop.public.staff: holds 2 rows of the 5 its build recorded"
    )
