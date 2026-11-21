# Copyright (c) 2026 Kenneth Stott
# Canary: 6eb144af-f324-48ab-bcbf-0cbb9059eef7
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
    # One MERGE on the target: the rows read FULL JOINed to it, so the WHEN NOT MATCHED BY
    # SOURCE is carried as a WHEN MATCHED over the target rows no row read matches.
    assert statement == (
        'MERGE INTO "er"."bridge" AS "t" USING (SELECT "m".*, "provisa_target".ctid AS '
        '"provisa_row" FROM (VALUES (CAST($1 AS BIGINT), CAST($2 AS TEXT), TRUE), '
        "(CAST($3 AS BIGINT), CAST($4 AS TEXT), TRUE), (CAST($5 AS BIGINT), CAST($6 AS TEXT), "
        'TRUE)) AS "m"("id", "org", "provisa_read") FULL JOIN "er"."bridge" AS "provisa_target" '
        'ON "provisa_target"."id" = "m"."id") AS "m" ON "t".ctid = "m"."provisa_row" '
        'WHEN MATCHED AND "m"."provisa_read" IS NOT NULL THEN UPDATE SET "org" = "m"."org" '
        'WHEN NOT MATCHED THEN INSERT ("id", "org") VALUES ("m"."id", "m"."org") '
        'WHEN MATCHED AND "m"."provisa_read" IS NULL THEN DELETE'
    )
    assert params == [1, "acme", 2, "globex", 3, "initech"]


@pytest.mark.asyncio
async def test_no_rows_read_still_merges_so_the_rows_not_in_the_source_go(monkeypatch):
    two, writes = _run_with(monkeypatch, [], _shape(("id", "bigint"), ("org", "text")))
    await _pipeline._run_two_step(SimpleNamespace(two_step=two), object())  # type: ignore[arg-type]
    ((statement, params),) = writes
    assert (
        '(SELECT CAST(NULL AS BIGINT) AS "id", CAST(NULL AS TEXT) AS "org", TRUE AS '
        '"provisa_read" WHERE FALSE) AS "m" FULL JOIN "er"."bridge"'
    ) in statement
    assert statement.endswith('WHEN MATCHED AND "m"."provisa_read" IS NULL THEN DELETE')
    assert params == []


@pytest.mark.asyncio
async def test_a_using_column_whose_type_cannot_be_derived_is_refused_by_name(monkeypatch):
    from provisa.compiler.result_shape import Underived

    two, writes = _run_with(
        monkeypatch, [(1, "acme")], _shape(("id", "bigint"), ("org", Underived("no type")))
    )
    with pytest.raises(ValueError, match="'org' cannot be derived; CAST it"):
        await _pipeline._run_two_step(SimpleNamespace(two_step=two), object())  # type: ignore[arg-type]
    assert writes == []


def test_a_merge_without_a_by_source_clause_merges_the_rows_read_alone():
    sql = _MERGE.replace(" WHEN NOT MATCHED BY SOURCE THEN DELETE", "")
    two = _pipeline._two_step_write(_governed(sql), 1, None)
    assert two is not None
    statement, params = _pipeline._merged_rows(two, ["id", "org"], _TYPES, [(1, "acme")])
    assert statement.sql(dialect="postgres").startswith(
        'MERGE INTO "er"."bridge" AS "t" USING (VALUES (CAST($1 AS BIGINT), CAST($2 AS TEXT))) '
        'AS "m"("id", "org") ON "t"."id" = "m"."id"'
    )
    assert params == [1, "acme"]


@pytest.mark.parametrize(
    ("clauses", "refused"),
    [
        (' WHEN NOT MATCHED BY SOURCE THEN UPDATE SET "org" = NULL', "not THEN UPDATE"),
        (
            ' WHEN NOT MATCHED BY SOURCE AND "t"."id" > 5 THEN DELETE'
            " WHEN NOT MATCHED BY SOURCE THEN DELETE",
            "it holds 2",
        ),
    ],
)
def test_a_by_source_clause_it_cannot_carry_is_refused_by_name_before_anything_is_read(
    clauses, refused
):
    sql = _MERGE.replace(" WHEN NOT MATCHED BY SOURCE THEN DELETE", clauses)
    with pytest.raises(ValueError, match=refused):
        _pipeline._two_step_write(_governed(sql), 1, None)


# --- on PostgreSQL 16, which has no WHEN NOT MATCHED BY SOURCE ---------------------------------

_TYPES = [sqlglot.exp.DataType.build("bigint"), sqlglot.exp.DataType.build("text")]
_TABLES = {
    "er.bridge": [
        {"id": 1, "org": "old"},
        {"id": 2, "org": "globex"},
        {"id": 9, "org": "stale"},
    ],
    "code.matches": [
        {"id": 1, "org": "acme", "secret": "s1"},
        {"id": 2, "org": "globex", "secret": "s2"},
        {"id": 3, "org": "initech", "secret": "s3"},
        {"id": 4, "org": "hidden", "secret": "s4"},
    ],
}
_RAW = (
    "MERGE INTO er.bridge AS t USING code.matches AS m ON t.id = m.id "
    "WHEN MATCHED THEN UPDATE SET org = m.org "
    "WHEN NOT MATCHED THEN INSERT (id, org) VALUES (m.id, m.org) "
    "WHEN NOT MATCHED BY SOURCE THEN DELETE"
)


@pytest.fixture(scope="module")
def pg(embedded_postgres):
    from tests.governed_pg import governed_database

    return governed_database(embedded_postgres, "test_two_step_merge")


def _gov():
    from provisa.compiler.stage2 import GovernanceContext
    from tests.write_governance import grants_over

    gov = GovernanceContext()
    gov.role_id = "steward"
    gov.can_write = True
    gov.table_map = {"er.bridge": 1, "code.matches": 2}
    gov.all_columns = {
        1: [("id", "bigint"), ("org", "text")],
        2: [("id", "bigint"), ("org", "text"), ("secret", "text")],
    }
    gov.write_grants = {1: grants_over("steward", ("id", "org"))}
    gov.rls_rules = {2: "org <> 'hidden'"}  # the code table's row rule
    gov.visible_columns = {2: frozenset({"id", "org"})}  # its secret is not served
    gov.write_ops = {1: frozenset({"insert", "update", "delete"})}
    gov.write_returns_rows = {1: True}
    gov.write_refused_forms = {1: frozenset()}
    return gov


def _replace(pg, raw: str, tables=_TABLES):
    """``raw`` governed, its USING read on PostgreSQL, then its second step run there."""
    from provisa.compiler.stage2 import apply_governance

    governed = apply_governance(raw, _gov())
    two = _pipeline._TwoStepWrite(
        governed=None,
        template=sqlglot.parse_one(governed, read="postgres"),
        target_source="er-pg",
    )
    read = two.read_query(two.template).sql(dialect="postgres")
    rows = pg.answer(read, tables)
    names = list(pg.names)
    statement, values = _pipeline._merged_rows(two, names, _TYPES, rows)
    return names, pg.written(statement.sql(dialect="postgres"), tables, params=values)


def test_on_postgresql_16_the_merge_updates_inserts_and_deletes_in_one_statement(pg):
    names, written = _replace(pg, _RAW)
    # The code table read under the role: its row rule keeps 'hidden' out, its secret unserved.
    assert names == ["id", "org"]
    assert written.after["er.bridge"] == [(1, "acme"), (2, "globex"), (3, "initech")]
    # Its count is the MERGE's: two updated, one inserted, one deleted.
    assert written.count == 4


def test_on_postgresql_16_no_rows_read_empties_the_target(pg):
    tables = {**_TABLES, "code.matches": [{"id": 4, "org": "hidden", "secret": "s4"}]}
    _names, written = _replace(pg, _RAW, tables)
    assert written.after["er.bridge"] == [] and written.count == 3


def test_a_by_source_condition_is_carried_to_the_delete(pg):
    # A WHEN may read its source and constants (governance), so a BY SOURCE condition is a
    # constant one; one that admits no row deletes none.
    raw = _RAW.replace("BY SOURCE THEN DELETE", "BY SOURCE AND 1 = 0 THEN DELETE")
    _names, written = _replace(pg, raw)
    assert written.after["er.bridge"] == [(1, "acme"), (2, "globex"), (3, "initech"), (9, "stale")]


def test_a_column_the_role_is_not_served_never_reaches_the_merge(pg):
    from tests.governed_pg import Refused

    # The USING read is the governed relation, which holds no secret: a WHEN naming it names a
    # column the rows read do not have, and the target refuses the statement.
    with pytest.raises(Refused, match="secret"):
        _replace(pg, _RAW.replace("SET org = m.org", "SET org = m.secret"))


# --- a MERGE sent whole carries its BY SOURCE DELETE too (REQ-2001, amended 2026-11-14) ---------

_STAGED = {
    **_TABLES,
    "er.staged": [
        {"id": 1, "org": "acme"},
        {"id": 2, "org": "globex"},
        {"id": 3, "org": "initech"},
    ],
}
_WHENS = (
    "WHEN MATCHED THEN UPDATE SET org = s.org "
    "WHEN NOT MATCHED THEN INSERT (id, org) VALUES (s.id, s.org) "
    "WHEN NOT MATCHED BY SOURCE THEN DELETE"
)
_PG = SimpleNamespace(source_dialects={"er-pg": "postgres"})


def _whole_gov():
    gov = _gov()
    gov.table_map = {**gov.table_map, "er.staged": 3}
    gov.all_columns = {**gov.all_columns, 3: [("id", "bigint"), ("org", "text")]}
    return gov


def _sent(raw: str, state=_PG):
    """``raw`` governed, then as route_governed sends it whole to its target's source."""
    from provisa.compiler.stage2 import apply_governance

    governed = _governed(apply_governance(raw, _whole_gov()), table_ids=(1, 3))
    return governed, _pipeline._carried_merge(governed, 1, state)


def test_on_postgresql_16_a_same_source_merge_updates_inserts_and_deletes_in_one_statement(pg):
    _governed_merge, sent = _sent(
        f"MERGE INTO er.bridge AS t USING er.staged AS s ON t.id = s.id {_WHENS}"
    )
    assert "NOT MATCHED BY SOURCE" not in sent.governed_semantic
    written = pg.written(sent.governed_semantic, _STAGED)
    assert written.after["er.bridge"] == [(1, "acme"), (2, "globex"), (3, "initech")]
    assert written.count == 4  # two updated, one inserted, one deleted


def test_on_postgresql_16_a_merge_over_rows_a_command_answered_deletes_in_one_statement(pg):
    # A code command in the USING is run first and stands there as the VALUES list of its rows.
    raw = (
        "MERGE INTO er.bridge AS t USING (SELECT * FROM (VALUES (1, 'acme'), (3, 'initech')) "
        f"AS v(id, org)) AS s ON t.id = s.id {_WHENS}"
    )
    governed, sent = _sent(raw)
    assert _pipeline._two_step_write(governed, 1, None) is None  # its USING is rows already
    written = pg.written(sent.governed_semantic, _STAGED)
    assert written.after["er.bridge"] == [(1, "acme"), (3, "initech")]
    assert written.count == 4  # one updated, one inserted, two deleted


def test_a_by_source_condition_is_carried_when_the_merge_is_sent_whole(pg):
    raw = f"MERGE INTO er.bridge AS t USING er.staged AS s ON t.id = s.id {_WHENS}".replace(
        "BY SOURCE THEN", "BY SOURCE AND 1 = 0 THEN"
    )
    _governed_merge, sent = _sent(raw)
    written = pg.written(sent.governed_semantic, _STAGED)
    assert written.after["er.bridge"] == [(1, "acme"), (2, "globex"), (3, "initech"), (9, "stale")]


def test_a_merge_without_a_by_source_clause_is_sent_as_before():
    raw = f"MERGE INTO er.bridge AS t USING er.staged AS s ON t.id = s.id {_WHENS}".replace(
        " WHEN NOT MATCHED BY SOURCE THEN DELETE", ""
    )
    governed, sent = _sent(raw)
    assert sent is governed
    assert sent.governed_semantic == governed.governed_semantic


def test_a_merge_into_a_target_that_is_not_postgresql_keeps_its_by_source_clause():
    governed, sent = _sent(
        f"MERGE INTO er.bridge AS t USING er.staged AS s ON t.id = s.id {_WHENS}",
        SimpleNamespace(source_dialects={"er-pg": "tsql"}),
    )
    assert sent is governed and "NOT MATCHED BY SOURCE" in sent.governed_semantic


@pytest.mark.parametrize(
    ("clauses", "refused"),
    [
        (" WHEN NOT MATCHED BY SOURCE THEN UPDATE SET org = NULL", "not THEN UPDATE"),
        (
            " WHEN NOT MATCHED BY SOURCE AND 1 = 0 THEN DELETE"
            " WHEN NOT MATCHED BY SOURCE THEN DELETE",
            "it holds 2",
        ),
    ],
)
def test_a_by_source_clause_it_cannot_carry_is_refused_by_name_when_sent_whole(clauses, refused):
    raw = f"MERGE INTO er.bridge AS t USING er.staged AS s ON t.id = s.id {_WHENS}".replace(
        " WHEN NOT MATCHED BY SOURCE THEN DELETE", clauses
    )
    with pytest.raises(ValueError, match=refused):
        _sent(raw)
