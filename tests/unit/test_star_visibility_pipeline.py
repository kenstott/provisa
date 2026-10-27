# Copyright (c) 2026 Kenneth Stott
# Canary: 65b72cde-c023-43cc-ad32-14bdc21fb316
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A star reads only what the role is served, on every surface (REQ-040, REQ-263).

The raw statement path (pgwire, Flight SQL, the Airport server, /data/sql, MCP) and the compiled
path (GraphQL, REST, JSON:API, gRPC, Cypher, Flight's compiled commands) each govern a statement
through ``stage2``; a view's body, a star in it included, is put into the statement before it is
governed. Here each path is driven with a star: a table the role is served no column of is
refused, and a table it is served some of is read as those, masked.
"""

# Requirements: REQ-040, REQ-263

from __future__ import annotations

from unittest.mock import patch

import pytest

from tests.unit.test_raw_sql_column_governance import (
    SOURCE_ID,
    TABLE_ID,
    _ORDERS_TABLE_DICT,
    _fake_pipeline_state,
)

#: orders with no column the analyst is served.
_HIDDEN = {
    **_ORDERS_TABLE_DICT,
    "columns": [{**c, "visible_to": ["admin"]} for c in _ORDERS_TABLE_DICT["columns"]],
}


async def _raw(monkeypatch, sql: str, *, tables: list[dict], views: dict | None = None) -> str:
    """The raw statement path every SQL surface takes: the governed SQL it plans."""
    import provisa.api.app as app_mod
    from provisa.pgwire import _pipeline
    from provisa.transpiler.router import Route, RouteDecision

    state = _fake_pipeline_state(tables=tables)
    if views:
        state.view_sql_map = views
        state.role_build_inputs = {"draft_tables": []}  # every input of the view in service
    monkeypatch.setattr(app_mod, "state", state, raising=False)
    decision = RouteDecision(
        route=Route.DIRECT, source_id=SOURCE_ID, dialect="postgres", reason="t"
    )

    async def _route(exec_sql, governed_sql, gov_ctx, ctx, state, **kwargs):
        return exec_sql, decision, False, {SOURCE_ID}, ()

    with patch.object(_pipeline, "_optimize_and_route", side_effect=_route):
        return (await _pipeline._govern_and_route(sql, "analyst")).sql


async def _compiled(monkeypatch, sql: str, *, tables: list[dict]) -> str:
    """The compiled path's governance (GraphQL, REST, JSON:API, gRPC, Cypher, Flight compiled)."""
    import provisa.api.app as app_mod
    from provisa.pgwire import _pipeline

    state = _fake_pipeline_state(tables=tables)
    monkeypatch.setattr(app_mod, "state", state, raising=False)
    governed = await _pipeline._govern_compiled(sql, "analyst", state, {}, sdl_joins=True)
    return governed.governed_sql


@pytest.mark.parametrize("sql", ["SELECT * FROM orders", "SELECT o.* FROM orders AS o"])
async def test_the_raw_path_refuses_a_star_over_a_table_the_role_is_served_nothing_of(
    monkeypatch, sql
):
    with pytest.raises(PermissionError, match="(?i)no column of"):
        await _raw(monkeypatch, sql, tables=[_HIDDEN])


async def test_the_raw_path_reads_a_qualified_star_as_the_served_columns_masked(monkeypatch):
    governed = await _raw(monkeypatch, "SELECT o.* FROM orders AS o", tables=[_ORDERS_TABLE_DICT])
    assert "amount" not in governed and ".*" not in governed
    assert "'***'" in governed  # the analyst's mask on region


async def test_a_view_whose_body_is_a_star_reads_only_the_served_columns(monkeypatch):
    views = {"everything": "SELECT * FROM orders"}
    governed = await _raw(
        monkeypatch, "SELECT * FROM everything", tables=[_ORDERS_TABLE_DICT], views=views
    )
    assert "amount" not in governed and "'***'" in governed


async def test_a_view_whose_body_is_a_star_over_a_hidden_table_is_refused(monkeypatch):
    views = {"everything": "SELECT * FROM orders"}
    # Refused before its star is reached: a view over a table the reader may read nothing of is
    # not read through (view_read.require_inputs_reached).
    with pytest.raises(PermissionError, match="not accessible"):
        await _raw(monkeypatch, "SELECT * FROM everything", tables=[_HIDDEN], views=views)


@pytest.mark.parametrize("sql", ["SELECT * FROM orders", "SELECT o.* FROM orders AS o"])
async def test_the_compiled_path_refuses_a_star_over_a_table_the_role_is_served_nothing_of(
    monkeypatch, sql
):
    with pytest.raises(PermissionError, match="(?i)no column of"):
        await _compiled(monkeypatch, sql, tables=[_HIDDEN])


async def test_the_compiled_path_reads_a_star_as_the_served_columns_masked(monkeypatch):
    governed = await _compiled(monkeypatch, "SELECT * FROM orders", tables=[_ORDERS_TABLE_DICT])
    assert "amount" not in governed and "*" not in governed.replace("'***'", "")
    assert "'***'" in governed


def test_the_table_ids_are_the_fixtures():
    assert TABLE_ID == _ORDERS_TABLE_DICT["id"]
