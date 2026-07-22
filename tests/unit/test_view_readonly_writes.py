# Copyright (c) 2026 Kenneth Stott
# Canary: 7e1a4b93-2c6d-48f5-a0b1-9d3e5c7f2148
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1157: a view_sql / MV-backed relation is query-only — raw-SQL surfaces MUST reject writes.

Because every raw-SQL surface (pgwire, REST /data/sql, Flight SQL, MCP, Bolt/Cypher, gRPC) funnels
through the one governed pipeline, the write-rejection lives in ONE place (_reject_view_writes, called
from _govern_and_route and _govern_and_route_compiled) and therefore covers all of them.
"""

from __future__ import annotations

import sqlglot
import pytest

from provisa.pgwire import _pipeline
from provisa.pgwire._pipeline import _reject_view_writes


class _State:
    # daily_totals is a view/MV-backed relation; orders is a base table.
    view_sql_map = {"daily_totals": "SELECT customer_id AS id, count(*) FROM orders GROUP BY 1"}


_WRITES_TO_VIEW = [
    "INSERT INTO daily_totals (id, n) VALUES (1, 2)",
    "INSERT INTO daily_totals SELECT customer_id, count(*) FROM orders GROUP BY 1",
    "INSERT INTO daily_totals VALUES (1, 2) ON CONFLICT (id) DO UPDATE SET n = 3",  # upsert
    "UPDATE daily_totals SET n = 5 WHERE id = 1",
    "DELETE FROM daily_totals WHERE id = 1",
    'UPDATE "shelter"."daily_totals" SET n = 5',  # schema-qualified target still matched by leaf
]

_ALLOWED = [
    # base-table writes are never affected
    "INSERT INTO orders (id) VALUES (1)",
    "UPDATE orders SET region = 'x' WHERE id = 1",
    "DELETE FROM orders WHERE id = 1",
    # reading a view in the SOURCE of a base-table write is fine — only the TARGET is guarded
    "INSERT INTO orders SELECT id FROM daily_totals",
    # plain reads of the view are fine
    "SELECT * FROM daily_totals",
    "SELECT o.id FROM orders o JOIN daily_totals d ON o.id = d.id",
]


@pytest.mark.parametrize("sql", _WRITES_TO_VIEW)
def test_write_to_view_rejected(sql):
    with pytest.raises(PermissionError, match="query-only"):
        _reject_view_writes(sqlglot.parse_one(sql, read="postgres"), _State())


@pytest.mark.parametrize("sql", _ALLOWED)
def test_allowed_statements_pass(sql):
    _reject_view_writes(sqlglot.parse_one(sql, read="postgres"), _State())  # must not raise


def test_no_view_map_is_noop():
    class _Empty:
        view_sql_map = {}

    _reject_view_writes(sqlglot.parse_one("INSERT INTO daily_totals VALUES (1)", read="postgres"), _Empty())


async def test_compiled_pipeline_entrypoint_rejects_view_write():
    """A surface entrypoint (the compiled Bolt/Cypher/GQL path) rejects a view write end-to-end,
    proving the guard is wired into the pipeline, not just the helper."""

    class _FakeState:
        contexts = {"admin": object()}
        rls_contexts: dict = {}
        view_sql_map = {"daily_totals": "SELECT 1"}

    with pytest.raises(PermissionError, match="query-only"):
        await _pipeline._govern_and_route_compiled(
            "UPDATE daily_totals SET n = 1", "admin", state=_FakeState()
        )
