# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A MERGE whose USING query reads a source other than its target's is written in two steps, as
an INSERT ... SELECT is (REQ-2001, amended 2026-11-14).

Sent whole, it went DIRECT to one of its sources, which could not answer the other's tables. Now
its governed USING query is read as any read, and the MERGE runs once on the target's source,
its USING the rows read, bound as typed values under the USING's alias.
"""

# Requirements: REQ-2001

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
import sqlglot

from provisa.executor.result import QueryResult
from provisa.mv.time_travel import Travel
from provisa.pgwire import _pipeline

# As governance leaves it: the USING a governed relation of the code table, under its alias.
_MERGE = (
    'MERGE INTO "er"."bridge" AS "t" USING (SELECT "m"."id" AS "id", "m"."org" AS "org" '
    'FROM "code"."matches" AS "m" WHERE "m"."org" <> \'hidden\') AS "m" ON "t"."id" = "m"."id" '
    'WHEN MATCHED THEN UPDATE SET "org" = "m"."org" '
    'WHEN NOT MATCHED THEN INSERT ("id", "org") VALUES ("m"."id", "m"."org") '
    "WHEN NOT MATCHED BY SOURCE THEN DELETE"
)
_SAME_SOURCE = (
    'MERGE INTO "er"."bridge" AS "t" USING (SELECT "s"."id" AS "id", "s"."org" AS "org" '
    'FROM "er"."staged" AS "s") AS "s" ON "t"."id" = "s"."id" '
    'WHEN MATCHED THEN UPDATE SET "org" = "s"."org"'
)


def _governed(sql: str = _MERGE, table_ids=(1, 2)):
    ctx = SimpleNamespace(
        tables={
            "bridge": SimpleNamespace(table_id=1, source_id="er-pg"),
            "matches": SimpleNamespace(table_id=2, source_id="er-code"),
            "staged": SimpleNamespace(table_id=3, source_id="er-pg"),
        }
    )
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
        memo={},
    )


def _shape(*columns: tuple[str, str]):
    async def shape():
        return list(columns)

    return shape


def test_a_merge_whose_using_reads_another_source_is_written_in_two_steps():
    two = _pipeline._two_step_write(_governed(), 1, None)
    assert two is not None and two.merge and two.target_source == "er-pg"


def test_a_merge_reading_only_its_targets_source_is_sent_whole():
    assert _pipeline._two_step_write(_governed(_SAME_SOURCE, table_ids=(1, 3)), 1, None) is None


def test_a_merge_whose_using_is_rows_already_is_sent_whole():
    # The second step's own statement: its USING the rows read, no table.
    sql = (
        'MERGE INTO "er"."bridge" AS "t" USING (VALUES (CAST($1 AS BIGINT), CAST($2 AS TEXT))) '
        'AS "m"("id", "org") ON "t"."id" = "m"."id" WHEN MATCHED THEN UPDATE SET "org" = "m"."org"'
    )
    assert _pipeline._two_step_write(_governed(sql), 1, None) is None


@pytest.mark.asyncio
async def test_the_plan_reads_the_using_query_first_and_carries_the_statements_one_record(
    monkeypatch,
):
    from provisa.transpiler.router import Route

    governed = _governed()
    two = _pipeline._two_step_write(governed, 1, None)
    assert two is not None
    read_plan = SimpleNamespace(sources=frozenset({"er-code"}), audit="read-audit")
    captured: dict = {}

    async def route(gov, *, params=None, as_of=None):
        captured.update(sql=gov.governed_semantic, parsed=gov.parsed, memo=gov.memo)
        return read_plan

    monkeypatch.setattr(_pipeline, "route_governed", route)
    monkeypatch.setattr("provisa.compiler.write_admission.written_table_id", lambda *_a, **_k: 1)
    state = SimpleNamespace(source_dialects={"er-pg": "postgres"})
    plan = await _pipeline._two_step_plan(
        governed, two, params=None, as_of=None, audit="the-audit", state=state
    )
    # The read is the governed USING query: the code table's row rule in it.
    assert captured["sql"] == (
        'SELECT "m"."id" AS "id", "m"."org" AS "org" FROM "code"."matches" AS "m" '
        'WHERE "m"."org" <> \'hidden\''
    )
    assert isinstance(captured["parsed"], sqlglot.exp.Select) and captured["memo"] == {}
    assert read_plan.audit is None
    assert (plan.route, plan.source_id, plan.writes_tables, plan.audit) == (
        Route.DIRECT,
        "er-pg",
        True,
        "the-audit",
    )
    assert plan.sources == frozenset({"er-code", "er-pg"})
    assert "MERGE" in (plan.route_reason or "")


def _run_with(monkeypatch, rows, shape):
    two = _pipeline._two_step_write(_governed(), 1, None)
    assert two is not None
    two.read = SimpleNamespace(result_shape=shape)
    monkeypatch.setattr(
        _pipeline,
        "_execute_plan_bound",
        AsyncMock(return_value=QueryResult(rows=rows, column_names=["id", "org"])),
    )
    writes: list[tuple[str, list | None]] = []

    async def route(governed, *, params=None, **_kw):
        writes.append((governed.governed_semantic, params))
        return SimpleNamespace(statement=governed.governed_semantic)

    monkeypatch.setattr(_pipeline, "route_governed", route)
    monkeypatch.setattr(
        _pipeline,
        "_run_plan_terminal",
        AsyncMock(return_value=QueryResult(rows=[], column_names=[], rowcount=5)),
    )
    return two, writes


@pytest.mark.asyncio
async def test_the_rows_read_are_merged_on_the_target_in_one_statement(monkeypatch):
    rows = [(1, "acme"), (2, "globex"), (3, "initech")]
    two, writes = _run_with(monkeypatch, rows, _shape(("id", "bigint"), ("org", "text")))
    result = await _pipeline._run_two_step(SimpleNamespace(two_step=two), object())  # type: ignore[arg-type]
    assert result.rowcount == 5
    ((statement, params),) = writes
    assert statement == (
        'MERGE INTO "er"."bridge" AS "t" USING (VALUES (CAST($1 AS BIGINT), CAST($2 AS TEXT)), '
        "(CAST($3 AS BIGINT), CAST($4 AS TEXT)), (CAST($5 AS BIGINT), CAST($6 AS TEXT))) "
        'AS "m"("id", "org") ON "t"."id" = "m"."id" '
        'WHEN MATCHED THEN UPDATE SET "org" = "m"."org" '
        'WHEN NOT MATCHED THEN INSERT ("id", "org") VALUES ("m"."id", "m"."org") '
        "WHEN NOT MATCHED BY SOURCE THEN DELETE"
    )
    assert params == [1, "acme", 2, "globex", 3, "initech"]


@pytest.mark.asyncio
async def test_no_rows_read_still_merges_so_the_rows_not_in_the_source_go(monkeypatch):
    two, writes = _run_with(monkeypatch, [], _shape(("id", "bigint"), ("org", "text")))
    await _pipeline._run_two_step(SimpleNamespace(two_step=two), object())  # type: ignore[arg-type]
    ((statement, params),) = writes
    assert (
        'USING (SELECT CAST(NULL AS BIGINT) AS "id", CAST(NULL AS TEXT) AS "org" WHERE FALSE) '
        'AS "m" ON'
    ) in statement
    assert "WHEN NOT MATCHED BY SOURCE THEN DELETE" in statement and params == []


@pytest.mark.asyncio
async def test_a_using_column_whose_type_cannot_be_derived_is_refused_by_name(monkeypatch):
    from provisa.compiler.result_shape import Underived

    two, writes = _run_with(
        monkeypatch, [(1, "acme")], _shape(("id", "bigint"), ("org", Underived("no type")))
    )
    with pytest.raises(ValueError, match="'org' cannot be derived; CAST it"):
        await _pipeline._run_two_step(SimpleNamespace(two_step=two), object())  # type: ignore[arg-type]
    assert writes == []
