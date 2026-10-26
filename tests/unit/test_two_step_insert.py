# Copyright (c) 2026 Kenneth Stott
# Canary: f5feebc5-a04b-47ad-9eba-d4ac83532cb5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An INSERT ... SELECT whose SELECT reads another source, or embeds the text of its rows, is
written in two steps (REQ-2001, GitHub issue 253).

Sent whole, it went DIRECT to whichever of its sources came first. Now its governed SELECT is read
as any read, and its rows are written through the target's write route as INSERT ... VALUES under
the statement's own target, columns and ON CONFLICT clause, its values bound.
"""

# Requirements: REQ-2001, REQ-2002

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from provisa.executor.result import QueryResult
from provisa.pgwire import _pipeline
from provisa.mv.time_travel import Travel
from provisa.vector.embed_fn import Folded, RowEmbed

_UPSERT = (
    'INSERT INTO "kb"."docs" ("id", "title") SELECT "t"."ticket_id", "t"."subject" '
    'FROM "support"."tickets" AS "t" WHERE "t"."updated_at" > $1 '
    'ON CONFLICT ("id") DO UPDATE SET "title" = EXCLUDED."title"'
)


def _governed(sql: str = _UPSERT, table_ids=(1, 2), embed=None):
    ctx = SimpleNamespace(
        tables={
            "docs": SimpleNamespace(table_id=1, source_id="qdrant"),
            "tickets": SimpleNamespace(table_id=2, source_id="pg"),
            "notes": SimpleNamespace(table_id=3, source_id="qdrant"),
        }
    )
    import sqlglot

    return _pipeline._Governed(
        sql=sql,
        role_id="steward",
        role=None,
        ctx=ctx,
        gov_ctx=SimpleNamespace(),
        comment_params=None,
        parsed=sqlglot.parse_one(sql, read="postgres"),
        metric_semantic_sql=None,
        table_ids=table_ids,
        governed_semantic=sql,
        schema_generation=(None, None),
        stamp="governed",
        travel=Travel(),
        memo={"embed": embed} if embed is not None else {},
    )


def test_an_insert_select_reading_another_source_is_written_in_two_steps():
    two = _pipeline._two_step_insert(_governed(), 1, None)
    assert two is not None and two.target_source == "qdrant"


def test_one_reading_only_its_targets_source_is_sent_whole():
    sql = 'INSERT INTO "kb"."docs" ("id") SELECT "n"."id" FROM "kb"."notes" AS "n"'
    assert _pipeline._two_step_insert(_governed(sql, table_ids=(1, 3)), 1, None) is None


def test_one_that_embeds_the_text_of_its_rows_is_written_in_two_steps():
    sql = 'INSERT INTO "kb"."docs" ("id", "title") SELECT "n"."id", "n"."title" FROM "kb"."notes" AS "n"'
    embed = Folded(sql, rows=(RowEmbed(1, "title", "mini"),))
    assert _pipeline._two_step_insert(_governed(sql, table_ids=(1, 3), embed=embed), 1, None)


def test_an_insert_of_values_is_sent_whole():
    sql = 'INSERT INTO "kb"."docs" ("id") VALUES ($1)'
    assert _pipeline._two_step_insert(_governed(sql, table_ids=(1,)), 1, None) is None


@pytest.mark.asyncio
async def test_the_rows_read_are_written_in_batches_keeping_the_on_conflict(monkeypatch):
    two = _pipeline._two_step_insert(_governed(), 1, None)
    assert two is not None
    two.batch = 2
    two.read = SimpleNamespace()
    monkeypatch.setattr(
        _pipeline,
        "_execute_plan_bound",
        AsyncMock(
            return_value=QueryResult(
                rows=[(1, "a"), (2, "b"), (3, "c")], column_names=["ticket_id", "subject"]
            )
        ),
    )
    writes: list[tuple[str, list | None]] = []

    async def route(governed, *, params=None, **_kw):
        writes.append((governed.governed_semantic, params))
        return SimpleNamespace(statement=governed.governed_semantic)

    monkeypatch.setattr(_pipeline, "route_governed", route)
    monkeypatch.setattr(
        _pipeline,
        "_run_plan_terminal",
        AsyncMock(
            side_effect=lambda w, _s: QueryResult(
                rows=[], column_names=[], rowcount=2 if "$3" in w.statement else 1
            )
        ),
    )
    plan = SimpleNamespace(two_step=two)
    result = await _pipeline._run_two_step(plan, object())  # type: ignore[arg-type]
    assert result.rowcount == 3
    first, second = writes
    assert first[0] == (
        'INSERT INTO "kb"."docs" ("id", "title") VALUES ($1, $2), ($3, $4) '
        'ON CONFLICT("id") DO UPDATE SET "title" = EXCLUDED."title"'
    )
    assert first[1] == [1, "a", 2, "b"]
    assert second[1] == [3, "c"] and "VALUES ($1, $2) ON CONFLICT" in second[0]


@pytest.mark.asyncio
async def test_no_rows_read_is_no_write(monkeypatch):
    two = _pipeline._two_step_insert(_governed(), 1, None)
    assert two is not None
    two.read = SimpleNamespace()
    monkeypatch.setattr(
        _pipeline,
        "_execute_plan_bound",
        AsyncMock(return_value=QueryResult(rows=[], column_names=["ticket_id", "subject"])),
    )
    routed = AsyncMock()
    monkeypatch.setattr(_pipeline, "route_governed", routed)
    result = await _pipeline._run_two_step(SimpleNamespace(two_step=two), object())  # type: ignore[arg-type]
    assert result.rowcount == 0
    routed.assert_not_awaited()


@pytest.mark.asyncio
async def test_the_plan_reads_its_select_and_carries_the_statements_one_record(monkeypatch):
    from provisa.transpiler.router import Route

    embed = Folded(_UPSERT, rows=(RowEmbed(1, "title", "mini"),), audit=({"id": "mini"},))
    governed = _governed(embed=embed)
    two = _pipeline._two_step_insert(governed, 1, None)
    assert two is not None
    read_plan = SimpleNamespace(sources=frozenset({"pg"}), audit="read-audit", embedded=None)
    captured: dict = {}

    async def route(gov, *, params=None, as_of=None):
        captured.update(sql=gov.governed_semantic, params=params, memo=gov.memo)
        return read_plan

    monkeypatch.setattr(_pipeline, "route_governed", route)
    monkeypatch.setattr("provisa.compiler.write_admission.written_table_id", lambda *_a, **_k: 1)
    state = SimpleNamespace(source_dialects={"qdrant": "qdrant"})
    plan = await _pipeline._two_step_plan(
        governed, two, params=["2026-10-11"], as_of=None, audit="the-audit", state=state
    )
    assert captured["sql"].startswith('SELECT "t"."ticket_id", "t"."subject" FROM')
    assert captured["params"] == ["2026-10-11"] and captured["memo"] == {}
    assert read_plan.audit is None and read_plan.embedded is embed
    assert (plan.route, plan.source_id, plan.writes_tables, plan.audit) == (
        Route.DIRECT,
        "qdrant",
        True,
        "the-audit",
    )
    assert plan.sources == frozenset({"pg", "qdrant"})
    assert not _pipeline.streams_from_terminal(plan)
