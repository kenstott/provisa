# Copyright (c) 2026 Kenneth Stott
# Canary: 3f9b1d47-8a20-4c6e-b5d1-7e2c0a9f4863
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Multi-statement batches: every entry point can send several statements, and each MUST be governed
+ executed in order — never silently reduced to the first (the parse_one trap). The split is
statement-aware so a ';' inside a literal/comment/dollar-quote does not mis-split (parser differential).
"""

from __future__ import annotations

import pytest

from provisa.compiler.sql_rewrite import split_sql_statements
from provisa.executor.result import QueryResult
from provisa.pgwire import _pipeline
from provisa.transpiler.router import Route


# --------------------------------------------------------------------------- #
# Statement-aware split
# --------------------------------------------------------------------------- #
def test_split_ignores_semicolons_in_literals_comments_dollar_quotes():
    assert split_sql_statements("SELECT 'a;b'; SELECT 1 /* c;d */; SELECT 2") == [
        "SELECT 'a;b'",
        "SELECT 1 /* c;d */",
        "SELECT 2",
    ]
    assert split_sql_statements("SELECT $$ e;f $$") == ["SELECT $$ e;f $$"]
    assert split_sql_statements("SELECT 1 -- x;y\n; SELECT 2") == ["SELECT 1 -- x;y", "SELECT 2"]


def test_split_edge_cases():
    assert split_sql_statements("SELECT 1;") == ["SELECT 1"]  # trailing ; dropped
    assert split_sql_statements("   ") == []
    assert split_sql_statements("SELECT 1") == ["SELECT 1"]  # single, no ;
    # naive str.split(';') would produce 6 fragments for this; statement-aware yields 3
    assert len(split_sql_statements("SELECT 'a;b'; SELECT 1 /* c;d */; SELECT 2")) == 3


# --------------------------------------------------------------------------- #
# execute_sql_batch — govern+execute EVERY statement, return the last result
# --------------------------------------------------------------------------- #
async def test_execute_sql_batch_runs_all_in_order_returns_last(monkeypatch):
    order: list[str] = []

    async def _no_cmd(stmt, role, state):
        return None

    async def _gar(stmt, role_id, **kw):
        return _pipeline._Plan(
            route=Route.ENGINE,
            sql=stmt,
            source_id="pg",
            dialect="trino",
            physical_sql=stmt,
            stamp=_pipeline._mint_stamp(),
        )

    async def _exec(plan, state=None):
        order.append(plan.sql)
        return QueryResult(rows=[(plan.sql,)], column_names=["s"])

    monkeypatch.setattr("provisa.pgwire.function_call.maybe_invoke_registered_function", _no_cmd)
    monkeypatch.setattr(_pipeline, "_govern_and_route", _gar)
    monkeypatch.setattr(_pipeline, "_execute_plan", _exec)

    result = await _pipeline.execute_sql_batch("SELECT 1; SELECT 2; SELECT 3", "admin", object())

    assert order == ["SELECT 1", "SELECT 2", "SELECT 3"]  # ALL executed, in order
    assert result.rows == [("SELECT 3",)]  # last statement's result returned


async def test_execute_sql_batch_single_statement(monkeypatch):
    async def _no_cmd(stmt, role, state):
        return None

    async def _gar(stmt, role_id, **kw):
        return _pipeline._Plan(
            route=Route.ENGINE,
            sql=stmt,
            source_id="pg",
            dialect="trino",
            physical_sql=stmt,
            stamp=_pipeline._mint_stamp(),
        )

    async def _exec(plan, state=None):
        return QueryResult(rows=[(1,)], column_names=["n"])

    monkeypatch.setattr("provisa.pgwire.function_call.maybe_invoke_registered_function", _no_cmd)
    monkeypatch.setattr(_pipeline, "_govern_and_route", _gar)
    monkeypatch.setattr(_pipeline, "_execute_plan", _exec)

    result = await _pipeline.execute_sql_batch("SELECT 1", "admin", object())
    assert result.rows == [(1,)]


# --------------------------------------------------------------------------- #
# govern_batch_final_plan — leading statements run (governed); final plan returned
# --------------------------------------------------------------------------- #
async def test_govern_batch_final_plan_runs_leading_returns_final(monkeypatch):
    executed: list[str] = []

    async def _gar(stmt, role_id, **kw):
        return _pipeline._Plan(
            route=Route.ENGINE,
            sql=stmt,
            source_id="pg",
            dialect="trino",
            physical_sql=stmt,
            stamp=_pipeline._mint_stamp(),
        )

    async def _exec(plan, state=None):
        executed.append(plan.sql)
        return QueryResult(rows=[], column_names=[])

    monkeypatch.setattr(_pipeline, "_govern_and_route", _gar)
    monkeypatch.setattr(_pipeline, "_execute_plan", _exec)

    final = await _pipeline.govern_batch_final_plan(
        "SELECT 1; SELECT 2; SELECT 3", "admin", object(), deliver=None
    )

    assert executed == ["SELECT 1", "SELECT 2"]  # leading statements executed (governed)
    assert final.sql == "SELECT 3"  # final statement returned as a plan, not executed here
    assert _pipeline.stamp_is_valid(final.stamp)  # and it carries a valid governed-provenance stamp


async def test_govern_batch_final_plan_empty_batch_raises(monkeypatch):
    with pytest.raises(ValueError, match="empty SQL batch"):
        await _pipeline.govern_batch_final_plan("   ", "admin", object(), deliver=None)


# --------------------------------------------------------------------------- #
# execute_sql_batch_each — the same loop, one outcome per statement
# --------------------------------------------------------------------------- #
def _stub_pipeline(monkeypatch, order: list[str], fails: str | None = None):
    async def _no_cmd(stmt, role, state):
        return None

    async def _gar(stmt, role_id, **kw):
        if stmt == fails:
            raise PermissionError(f"refused: {stmt}")
        return _pipeline._Plan(
            route=Route.ENGINE,
            sql=stmt,
            source_id="pg",
            dialect="trino",
            physical_sql=stmt,
            stamp=_pipeline._mint_stamp(),
        )

    async def _exec(plan, state=None):
        order.append(plan.sql)
        return QueryResult(rows=[(plan.sql,)], column_names=["s"])

    monkeypatch.setattr("provisa.pgwire.function_call.maybe_invoke_registered_function", _no_cmd)
    monkeypatch.setattr(_pipeline, "_govern_and_route", _gar)
    monkeypatch.setattr(_pipeline, "_execute_plan", _exec)


async def test_execute_sql_batch_each_answers_every_statement_in_order(monkeypatch):
    order: list[str] = []
    _stub_pipeline(monkeypatch, order)

    outcomes = await _pipeline.execute_sql_batch_each("SELECT 'a;b'; SELECT 2", "admin", object())

    assert order == ["SELECT 'a;b'", "SELECT 2"]
    assert [o.statement for o in outcomes] == ["SELECT 'a;b'", "SELECT 2"]
    assert [o.result.rows for o in outcomes if o.result] == [[("SELECT 'a;b'",)], [("SELECT 2",)]]
    assert all(o.error is None for o in outcomes)


async def test_execute_sql_batch_each_keeps_running_past_a_failed_statement(monkeypatch):
    order: list[str] = []
    _stub_pipeline(monkeypatch, order, fails="SELECT 2")

    outcomes = await _pipeline.execute_sql_batch_each(
        "SELECT 1; SELECT 2; SELECT 3", "admin", object()
    )

    assert order == ["SELECT 1", "SELECT 3"]
    assert outcomes[0].result is not None and outcomes[0].result.rows == [("SELECT 1",)]
    assert isinstance(outcomes[1].error, PermissionError) and outcomes[1].result is None
    assert outcomes[2].result is not None and outcomes[2].result.rows == [("SELECT 3",)]


async def test_execute_sql_batch_still_raises_a_failed_statement(monkeypatch):
    _stub_pipeline(monkeypatch, [], fails="SELECT 2")
    with pytest.raises(PermissionError):
        await _pipeline.execute_sql_batch("SELECT 1; SELECT 2", "admin", object())
