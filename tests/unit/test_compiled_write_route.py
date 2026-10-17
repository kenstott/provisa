# Copyright (c) 2026 Kenneth Stott
# Canary: 3d8f2a61-9c47-4e15-b0a8-6e2f1c7d9a34
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A write on a compiled surface (a Cypher write over HTTP or Bolt, a GraphQL mutation) is routed
as a write (REQ-031, REQ-670): to its source's own driver, never the engine. The engine reaches a
source it attaches (sqlite, a DuckDB file) through a view, which takes no INSERT, UPDATE or DELETE;
routed as a read, a Cypher CREATE on such a table failed with the engine's "is not a table"."""

# Requirements: REQ-031, REQ-670

from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from provisa.compiler.directives import NO_CACHE_HINT
from tests.unit.test_buffered_auto_threshold import _decision
from tests.unit.test_view_route import _state
from tests.write_governance import grants_over

pytestmark = pytest.mark.asyncio


@pytest.fixture(autouse=True)
def _org(bind_org):
    bind_org("org-a")


async def _routed_as(monkeypatch, sql: str) -> Mapping[str, Any]:
    """What routing was told about the statement."""
    import provisa.api.app as app_mod
    from provisa.pgwire import _pipeline, governed_plan

    monkeypatch.setattr(governed_plan, "_rebuild_in_progress", lambda: False)
    state = _state(kept=False)
    # The analyst holds the write right, the orders table's source takes every write, and the
    # analyst may write its column.
    state.roles["analyst"]["capabilities"] = ["write"]
    state.tables = [
        {
            **t,
            "write_ops": ["delete", "insert", "update"],
            "write_returns_rows": False,
            "write_refused_forms": [],
            # REQ-2074: the analyst's write grants over every column.
            "write_grants": list(
                grants_over("analyst", [c["column_name"] for c in t["columns"]]).values()
            ),
        }
        if t["table_name"] == "orders"
        else t
        for t in state.tables
    ]
    monkeypatch.setattr(app_mod, "state", state, raising=False)
    route = AsyncMock(side_effect=_decision("DIRECT"))
    with patch.object(_pipeline, "_optimize_and_route", new=route):
        await _pipeline._govern_and_route_compiled(
            sql, "analyst", state=state, cache_hint=NO_CACHE_HINT, sdl_joins=False
        )
    return route.call_args.kwargs


@pytest.mark.parametrize(
    "sql",
    [
        'INSERT INTO "sales"."orders" ("id") VALUES (99981)',
        'UPDATE "sales"."orders" SET "id" = 99982 WHERE "id" = 99981',
        'DELETE FROM "sales"."orders" WHERE "id" = 99981',
    ],
)
async def test_a_compiled_write_is_routed_as_a_write(monkeypatch, sql):
    assert (await _routed_as(monkeypatch, sql))["is_mutation"] is True


async def test_a_compiled_read_is_routed_as_a_read(monkeypatch):
    routed = await _routed_as(monkeypatch, 'SELECT "id" FROM "sales"."orders"')
    assert routed["is_mutation"] is False
