# Copyright (c) 2026 Kenneth Stott
# Canary: 6d2b9f15-a3c7-4e80-b14d-79e0c5a2f8d3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2202: find terms in a registered table -- a domain, a table, its id column and its text
column, each row one passage. The rows are read through the governed pipeline as the steward's
acting role; a candidate's passages cite the table and the row's id; the scan is held to the
role's LLM budget, a scan the budget has no room for refused before it starts; it is a scan only,
not a reference library document."""

# Requirements: REQ-2202

from __future__ import annotations

import types
from typing import Any

import pytest

from provisa.api.admin import term_finding_table as table
from provisa.api.errors import ApiError
from provisa.security.llm_budget import Admitted, Budget, Limit, WindowUse
from provisa.term_finding import model, scan

SOURCE = {
    "domain": "d",
    "table": "notes",
    "id_column": "note_id",
    "text_column": "body",
}
REGISTERED = {
    "id": 7,
    "domain_id": "d",
    "table_name": "notes",
    "alias": None,
    "columns": [
        {"column_name": "note_id", "alias": None},
        {"column_name": "note_text", "alias": "body"},
    ],
}
ROWS = [
    ("n-1", '"Customer" means any party that holds an account.'),
    ("n-2", None),
    ("n-3", "   "),
    ("n-4", "A customer account is opened after KYC checks."),
]


# -- the rows as passages -----------------------------------------------------------------------


def test_each_row_with_text_is_one_passage_cited_by_its_id():
    found = table.passages(ROWS)
    assert [(p.ordinal, p.row, p.section, p.page) for p in found] == [
        (1, "n-1", None, None),
        (2, "n-4", None, None),
    ]
    assert found[1].text == "A customer account is opened after KYC checks."


def test_the_statement_is_written_from_the_registration_in_id_order():
    sql = table.statement(REGISTERED, table.source_of(SOURCE))
    assert sql == 'SELECT "note_id", "body" FROM "d"."notes" ORDER BY "note_id"'


def test_a_source_names_all_four_and_its_table_and_columns_must_be_registered():
    with pytest.raises(ApiError) as missing:
        table.source_of({**SOURCE, "text_column": ""})
    assert missing.value.code == "term_finding.table_source"
    state = types.SimpleNamespace(tables=[REGISTERED])
    with pytest.raises(ApiError) as unknown:
        table.registered(state, table.source_of({**SOURCE, "table": "memos"}))
    assert (unknown.value.status_code, unknown.value.code) == (404, "schema.table_not_found")
    with pytest.raises(ApiError) as column:
        table.statement(REGISTERED, table.source_of({**SOURCE, "text_column": "summary"}))
    assert column.value.code == "term_finding.unknown_column"


def test_a_table_prompt_says_its_passages_are_rows_and_cites_each_by_id():
    prompt = scan.extraction_prompt("d.notes", table.passages(ROWS), scan.TABLE)
    assert "passages of the table 'd.notes' (each passage is one row of the table)" in prompt
    assert "[P1] (row 'n-1')" in prompt
    assert "[P2] (row 'n-4')" in prompt


# -- read through the one pipeline, as the acting role ------------------------------------------


class _Pipeline:
    """The governed pipeline's two steps, recording what each was asked."""

    def __init__(self, rows: list[tuple], limit_outcome: str | None = None):
        self.rows = rows
        self.limit_outcome = limit_outcome
        self.governed: list[tuple[str, str]] = []
        self.executed: list[Any] = []

    async def govern(self, sql: str, role_id: str, **kwargs: Any) -> Any:
        self.governed.append((sql, role_id))
        return types.SimpleNamespace(
            limit_outcome=self.limit_outcome,
            row_limit=types.SimpleNamespace(limit=2, kind="role"),
        )

    async def execute(self, plan: Any, state: Any) -> Any:
        self.executed.append(plan)
        return types.SimpleNamespace(rows=self.rows, column_names=["note_id", "body"])


def _state() -> Any:
    return types.SimpleNamespace(tables=[REGISTERED], roles={"steward": {"id": "steward"}})


@pytest.fixture
def pipeline(monkeypatch):
    from provisa.pgwire import _pipeline

    def _make(rows: list[tuple], limit_outcome: str | None = None) -> _Pipeline:
        fake = _Pipeline(rows, limit_outcome)
        monkeypatch.setattr(_pipeline, "_govern_and_route", fake.govern)
        monkeypatch.setattr(_pipeline, "_execute_plan", fake.execute)
        return fake

    return _make


async def test_the_rows_are_read_through_the_governed_pipeline_as_the_role(pipeline):
    fake = pipeline(ROWS)
    found = await table.read(_state(), table.source_of(SOURCE), "steward")
    assert fake.governed == [
        ('SELECT "note_id", "body" FROM "d"."notes" ORDER BY "note_id"', "steward")
    ]
    assert len(fake.executed) == 1
    assert [p.row for p in found] == ["n-1", "n-4"]


@pytest.mark.parametrize("outcome", ["cut", "unchecked"])
async def test_a_read_its_row_limit_bounds_is_refused_by_name(pipeline, outcome):
    pipeline(ROWS[:2], limit_outcome=outcome)
    with pytest.raises(ApiError) as caught:
        await table.read(_state(), table.source_of(SOURCE), "steward")
    assert (caught.value.status_code, caught.value.code) == (409, "term_finding.rows_cut")
    assert "row limit of role 'steward', 2 rows" in str(caught.value.detail)


# -- held to the role's LLM budget before it starts ---------------------------------------------


class _Budgeted:
    """A guard admitting the call as ``steward`` with ``budget``, its windows as ``uses``."""

    def __init__(self, budget: Budget | None, uses: tuple[WindowUse, ...] = ()):
        self.admitted: Admitted | None = None
        self._admitted = Admitted("steward", budget, uses)

    async def admit(self, model_name: str) -> None:
        self.admitted = self._admitted


class _NoClient:
    vendor = "anthropic"
    model = "claude-test"


def _held(budget: Budget | None, uses: tuple[WindowUse, ...] = ()) -> model.Held:
    return model.Held(_NoClient(), _Budgeted(budget, uses), None)


DAY = Limit(1000, "tokens")


async def test_a_scan_the_budget_has_no_room_for_is_refused_naming_the_window():
    held = _held(Budget(windows={"day": DAY}), (WindowUse("day", DAY, 900, None),))
    with pytest.raises(model.OverBudget) as caught:
        await held.require_room(["x" * 2000], "d.notes")
    said = str(caught.value)
    assert "d.notes" in said
    assert "500 tokens" in said
    assert "role 'steward' has 100 tokens left of its LLM budget for the day" in said


async def test_a_scan_over_the_per_call_cap_is_refused_naming_it():
    held = _held(Budget(call_cap=Limit(100, "tokens")))
    with pytest.raises(model.OverBudget, match="caps one LLM call at 100 tokens"):
        await held.require_room(["x" * 40, "x" * 800], "d.notes")


async def test_a_scan_within_the_budget_or_with_none_starts():
    roomy = _held(Budget(windows={"day": DAY}), (WindowUse("day", DAY, 0, None),))
    await roomy.require_room(["x" * 2000], "d.notes")
    await _held(None).require_room(["x" * 10_000_000], "d.notes")
