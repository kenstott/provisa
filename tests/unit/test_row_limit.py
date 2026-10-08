# Copyright (c) 2026 Kenneth Stott
# Canary: ebc9d018-bb55-4de2-81ab-415e59f0aaa2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A read that a row limit cut short says so (REQ-1949): which limit bounds a governed read,
the statement that asks for the row after it, and the check where a buffered answer is made."""

# Requirements: REQ-1949

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import duckdb
import pytest

from provisa.compiler.row_limit import ROLE, TABLE, RowLimit, binds, cut_warning, next_row_sql
from provisa.compiler.stage2 import GovernanceContext, apply_governance
from provisa.core.statement_warnings import collecting
from provisa.executor.result import QueryResult
from provisa.pgwire import _pipeline
from provisa.transpiler.router import Route


@pytest.mark.parametrize(
    ("sql", "bound"),
    [
        ("SELECT id FROM t", True),
        ("SELECT id FROM t LIMIT 11", True),
        ("SELECT id FROM t LIMIT $1", True),
        ("SELECT id FROM t LIMIT 10", False),  # the statement asked for exactly that many
        ("SELECT id FROM t LIMIT 3", False),
        ("SELECT id FROM a UNION ALL SELECT id FROM b", True),
    ],
)
def test_a_ceiling_bounds_a_statement_unless_its_own_limit_is_within_it(sql, bound):
    assert binds(sql, 10) is bound


def _governed(sql: str, role: int | None, table: int | None = None) -> tuple[str, RowLimit | None]:
    gov = GovernanceContext()
    gov.table_map = {"t": 1}
    gov.all_columns = {1: [("id", "integer")]}
    gov.limit_ceiling = role
    if table is not None:
        gov.table_ceilings = {1: table}
    return apply_governance(sql, gov), gov.row_limit


def test_governance_records_the_limit_that_bounds_the_read_and_whose_it_is():
    sql, limit = _governed("SELECT id FROM t", 10)
    assert sql.endswith("LIMIT 10") and limit == RowLimit(10, ROLE)
    # A table's smaller ceiling is the one that bounds, and is named as the table's.
    sql, limit = _governed("SELECT id FROM t", 10, table=4)
    assert sql.endswith("LIMIT 4") and limit == RowLimit(4, TABLE)
    sql, limit = _governed("SELECT id FROM t", 4, table=10)
    assert sql.endswith("LIMIT 4") and limit == RowLimit(4, ROLE)
    # The statement's own LIMIT within the ceiling: the statement bounds itself, nothing is cut.
    sql, limit = _governed("SELECT id FROM t LIMIT 3", 10)
    assert sql.endswith("LIMIT 3") and limit is None
    # No ceiling at all (full results): none.
    assert _governed("SELECT id FROM t", None)[1] is None


def test_the_statement_never_asks_for_more_than_the_limit():
    """The governed statement is bounded AT the limit, as before this requirement: whatever
    reads it -- a buffered answer, a stream, a landed result -- gets no row past it."""
    for own in ("", " LIMIT 500", " LIMIT $1"):
        assert "LIMIT 10" in _governed(f"SELECT id FROM t{own}", 10)[0]


@pytest.fixture
def engine():
    con = duckdb.connect()
    con.execute("CREATE TABLE t AS SELECT range::INTEGER AS id FROM range(11)")
    con.execute("CREATE TABLE exact AS SELECT range::INTEGER AS id FROM range(10)")
    return con


def test_the_next_row_exists_exactly_when_rows_were_left_out(engine):
    """One row past the limit is a cut read; a table of exactly the limit is not."""
    past = next_row_sql("SELECT id FROM t ORDER BY id LIMIT 10", 10, "duckdb")
    whole = next_row_sql("SELECT id FROM exact ORDER BY id LIMIT 10", 10, "duckdb")
    assert past is not None and whole is not None
    assert engine.execute(past).fetchall() == [(10,)]
    assert engine.execute(whole).fetchall() == []


def test_the_next_row_respects_the_statements_own_offset_and_shape(engine):
    skipped = next_row_sql("SELECT id FROM t ORDER BY id LIMIT 10 OFFSET 1", 10, "duckdb")
    assert skipped is not None and engine.execute(skipped).fetchall() == []
    capped = next_row_sql(
        "SELECT * FROM (SELECT id FROM t ORDER BY id LIMIT 500) AS _govern_capped LIMIT 10",
        10,
        "duckdb",
    )
    assert capped is not None and len(engine.execute(capped).fetchall()) == 1
    union = next_row_sql("SELECT id FROM t UNION ALL SELECT id FROM exact LIMIT 10", 10, "duckdb")
    assert union is not None and len(engine.execute(union).fetchall()) == 1


@pytest.mark.parametrize(
    "sql",
    ["SELECT id FROM t", "SELECT id FROM t LIMIT 7", "SELECT id FROM t LIMIT $1"],
)
def test_a_statement_not_bounded_at_the_limit_has_no_next_row_statement(sql):
    assert next_row_sql(sql, 10, "postgres") is None


def test_the_warning_names_the_limit_and_whose_it_is():
    role, table = cut_warning(RowLimit(10, ROLE)), cut_warning(RowLimit(4, TABLE))
    assert (role.code, role.params) == ("statement.rows_cut", {"limit": 10, "kind": "role"})
    assert (table.code, table.params) == ("statement.rows_cut", {"limit": 4, "kind": "table"})
    assert "cut at 10 rows" in role.message and "the role" in role.message
    assert "cut at 4 rows" in table.message and "a table it reads" in table.message


def _plan(route=Route.ENGINE, limit: RowLimit | None = RowLimit(3, ROLE)) -> _pipeline._Plan:
    plan = _pipeline._Plan(
        route=route,
        sql="SELECT id FROM t LIMIT 3",
        source_id="pg",
        dialect="postgres",
        physical_sql='SELECT id FROM "pg"."public"."t" LIMIT 3',
    )
    plan.row_limit = limit
    return plan


_STATE = SimpleNamespace(federation_engine=SimpleNamespace(dialect="duckdb"))


def _answer(rows: int, **kw) -> QueryResult:
    return QueryResult(rows=[(i,) for i in range(rows)], column_names=["id"], **kw)


@pytest.mark.parametrize(
    ("rows", "more", "warned"),
    [(3, True, True), (3, False, False), (2, True, False)],
    ids=["filled_and_more", "filled_exactly", "within_the_limit"],
)
def test_an_answer_says_it_was_cut_only_when_rows_were_left_out(monkeypatch, rows, more, warned):
    asked = []

    async def were_cut(plan, state):
        asked.append(plan)
        return more

    monkeypatch.setattr(_pipeline, "rows_were_cut", were_cut)
    plan = _plan()
    with collecting() as found:
        asyncio.run(_pipeline._warn_if_cut(plan, _answer(rows), _STATE))
    expected = [cut_warning(RowLimit(3, ROLE))] if warned else []
    # On the plan (the surfaces that answer from it) and in the request's collector (HTTP, Bolt).
    assert plan.warnings == expected and found == expected
    # An answer within the limit is known whole: nothing more is read for it.
    assert len(asked) == (1 if rows == 3 else 0)


def test_a_read_no_limit_bounds_and_a_landed_result_are_not_checked_here(monkeypatch):
    async def never(plan, state):
        raise AssertionError("not asked")

    monkeypatch.setattr(_pipeline, "rows_were_cut", never)
    asyncio.run(_pipeline._warn_if_cut(_plan(limit=None), _answer(3), _STATE))
    landed = QueryResult(rows=[], column_names=[], redirect={"row_count": 3})
    asyncio.run(_pipeline._warn_if_cut(_plan(), landed, _STATE))


def test_a_warning_found_with_no_collector_open_is_kept_on_the_plan(monkeypatch):
    """A surface that answers from the plan (pgwire, Flight, gRPC) opens no collector while the
    rows are read: the warning is not an error there, it is on the plan."""

    async def were_cut(plan, state):
        return True

    monkeypatch.setattr(_pipeline, "rows_were_cut", were_cut)
    plan = _plan()
    asyncio.run(_pipeline._warn_if_cut(plan, _answer(3), _STATE))
    assert plan.warnings == [cut_warning(RowLimit(3, ROLE))]


@pytest.mark.parametrize("route", [Route.ENGINE, Route.DIRECT])
def test_the_next_row_is_asked_through_the_terminal_the_answer_came_from(monkeypatch, route):
    ran = []

    async def terminal(plan, state):
        ran.append(plan)
        return _answer(1)

    monkeypatch.setattr(_pipeline, "_run_plan_terminal", terminal)
    plan = _plan(route)
    assert asyncio.run(_pipeline.rows_were_cut(plan, _STATE)) is True
    (asking,) = ran
    read = asking.physical_sql if route == Route.ENGINE else asking.sql
    assert "LIMIT 1" in read and "OFFSET 3" in read
    # Nothing is landed, audited or checked again for it, and the plan itself is untouched.
    assert asking.materialize is None and asking.audit is None and asking.row_limit is None
    assert plan.sql.endswith("LIMIT 3") and plan.physical_sql.endswith("LIMIT 3")

    async def none_left(plan, state):
        return _answer(0)

    monkeypatch.setattr(_pipeline, "_run_plan_terminal", none_left)
    assert asyncio.run(_pipeline.rows_were_cut(_plan(route), _STATE)) is False


def test_a_check_that_fails_is_said_in_the_answer_never_passed_over(monkeypatch):
    """The answer is read and stands; that its row limit could not be checked is one of its
    warnings, with why -- not an error, and not silence."""

    async def broken(plan, state):
        raise ConnectionError("source went away")

    monkeypatch.setattr(_pipeline, "_run_plan_terminal", broken)
    plan = _plan()
    with collecting() as found:
        asyncio.run(_pipeline._warn_if_cut(plan, _answer(3), _STATE))
    (warning,) = found
    assert plan.warnings == [warning] and warning.code == "statement.rows_cut_unchecked"
    assert warning.params == {"limit": 3, "kind": "role", "reason": "source went away"}

    # A statement an optimization left without the governed bound outermost: said the same way.
    unbounded = _plan()
    unbounded.physical_sql = "SELECT id FROM t"
    asyncio.run(_pipeline._warn_if_cut(unbounded, _answer(3), _STATE))
    assert [w.code for w in unbounded.warnings] == ["statement.rows_cut_unchecked"]
    assert "outermost LIMIT 3" in unbounded.warnings[0].params["reason"]


def test_the_requests_deadline_ends_the_request_as_it_does_the_read(monkeypatch):
    async def too_late(plan, state):
        raise TimeoutError("request timed out")

    monkeypatch.setattr(_pipeline, "_run_plan_terminal", too_late)
    with pytest.raises(TimeoutError):
        asyncio.run(_pipeline._warn_if_cut(_plan(), _answer(3), _STATE))


def test_a_temporary_table_filled_from_a_cut_read_carries_the_same_warning(monkeypatch):
    """The read that fills a temporary table is the plan's own governed SELECT: it is checked
    like any read, before the rows are landed (REQ-615) -- one warning, not one of its own."""
    order = []

    async def read(plan, state):
        return _answer(3)

    async def were_cut(plan, state):
        order.append("checked")
        return True

    async def land(action, result, state, role_id):
        order.append("landed")
        return QueryResult(rows=[], column_names=[], rowcount=len(result.rows))

    monkeypatch.setattr(_pipeline, "_execute_plan_in_org", read)
    monkeypatch.setattr(_pipeline, "_reads_an_org_secret", lambda plan, state: False)
    monkeypatch.setattr(_pipeline, "rows_were_cut", were_cut)
    monkeypatch.setattr("provisa.pgwire.temp_exec.apply", land)
    plan = _plan()
    plan.temp = SimpleNamespace(kind="create", name="t")
    plan.role_id = "analyst"
    result = asyncio.run(_pipeline._execute_plan_bound(plan, _STATE))
    assert result.rowcount == 3 and order == ["checked", "landed"]
    assert [w.code for w in plan.warnings] == ["statement.rows_cut"]


def test_the_catalog_says_it_in_every_locale():
    import json
    import pathlib

    locales = pathlib.Path(__file__).parents[2] / "provisa-ui/src/i18n/locales"
    english = json.loads((locales / "en/serverErrors.json").read_text())["serverErrors"]
    text = english["statement"]["rows_cut"]
    assert "{{limit}}" in text and "{{kind}}" in text
    for catalog in sorted(locales.glob("*/serverErrors.json")):
        statement = json.loads(catalog.read_text())["serverErrors"]["statement"]
        said, unchecked = statement["rows_cut"], statement["rows_cut_unchecked"]
        assert "{{limit}}" in said and "{{kind}}" in said, catalog
        assert all(p in unchecked for p in ("{{limit}}", "{{kind}}", "{{reason}}")), catalog
        if catalog.parent.name != "en":
            assert said != text and unchecked != english["statement"]["rows_cut_unchecked"]
