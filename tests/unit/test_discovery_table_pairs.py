# Copyright (c) 2026 Kenneth Stott
# Canary: 8e4a1c7d-2f95-4b36-a0d8-6c3e9b1f5a27
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-413 (amended 2026-11-18): the semantic check scores table pairs that code lists, batched.

- every source x target table pair sharing a compared type group is listed in exactly one call;
- a call lists each of its tables once, then its numbered pairs;
- answers are read per number; a column pair of an answered table pair not named scores zero;
- a listed pair left unanswered is asked again, then named by the run, never taken as zero;
- tables and columns are named by alias where one is set, never by physical name, and the
  answer's names are mapped back;
- sample values are each column's distinct values, not its first rows.
"""

# Requirements: REQ-413

from __future__ import annotations

import json
from collections import Counter

import pytest

from provisa.discovery import gather
from provisa.discovery.columns import UNKNOWN, ColumnFacts, TableFacts, type_groups
from provisa.discovery.context import SAMPLE_VALUES
from provisa.discovery.direction import Proposal
from provisa.discovery.proposals import BELOW_THRESHOLD
from provisa.discovery.runner import SAME_NAME, Tally, execute
from provisa.discovery.windows import bound, plan, table_pairs
from provisa.llm.client import Completion
from provisa.llm.limits import DEFAULT_LIMITS
from tests.unit.discovery_answers import (
    answer,
    column_pair,
    listed_pairs,
    listed_tables,
)


def _col(name: str, data_type: str = "integer", *, alias: str | None = None, samples=()):
    return ColumnFacts(
        name=name,
        data_type=data_type,
        description=None,
        tags=(),
        classes=(UNKNOWN,),
        sensitive=False,
        samples=tuple(samples),
        is_primary_key=name == "id",
        groups=type_groups(data_type, subtype=None, array_depth=0),
        alias=alias,
    )


def _tbl(tid: int, name: str, *columns: ColumnFacts, alias: str | None = None) -> TableFacts:
    return TableFacts(
        table_id=tid,
        domain_id="d",
        schema_name="s",
        table_name=name,
        description=None,
        tags=(),
        role=None,
        columns=columns,
        alias=alias,
    )


ORDERS = _tbl(1, "orders", _col("id"), _col("customer_id"), _col("placed_at", "timestamp"))
CUSTOMERS = _tbl(2, "customers", _col("id"), _col("name", "varchar"))
NOTES = _tbl(3, "notes", _col("body", "varchar"))
EVENTS = _tbl(4, "events", _col("at", "timestamp"))


class _Watcher:
    async def stop_asked(self) -> bool:
        return False

    async def returned(self, tally: Tally, proposals: list[Proposal]) -> None:
        return None


async def _direct(fn, *args):
    return fn(*args)


async def _execute(the_plan, ask) -> tuple[Tally, list[Proposal]]:
    return await execute(
        the_plan,
        DEFAULT_LIMITS[1],
        ask,
        _Watcher(),
        threshold=0.7,
        key_values=None,
        to_thread=_direct,
    )


# -- the table pairs --------------------------------------------------------------------------------


def test_every_source_by_target_pair_sharing_a_type_group_is_listed_in_exactly_one_call():
    source = [ORDERS, NOTES, EVENTS]
    target = [CUSTOMERS, NOTES, EVENTS]
    the_plan = plan(source, target, 100_000, 150)  # one pair to a call
    assert len(the_plan.calls) == 2
    listed = Counter(
        (p.source.table_id, p.target.table_id) for call in the_plan.calls for p in call.pairs
    )
    # orders x customers share whole numbers, notes x customers text; orders x notes share none,
    # notes has one column, so notes x notes has no two; a timestamp is in no group, so events
    # pairs with nothing.
    assert listed == Counter({(1, 2): 1, (3, 2): 1})
    assert the_plan.table_pairs == 2


def test_one_set_pairs_each_two_tables_once_a_table_with_itself_included():
    pairs = {(p.source.table_id, p.target.table_id) for p in table_pairs([ORDERS, CUSTOMERS], None)}
    # customers' name is text alone, its id a whole number alone: customers x customers has no
    # two compared columns.
    assert pairs == {(2, 1), (1, 1)}


def test_the_count_before_run_bounds_the_calls_and_counts_calls():
    source, target = [ORDERS, NOTES], [CUSTOMERS, NOTES]
    k, calls, pairs = bound(source, target, 100_000, 150, SAMPLE_VALUES)
    the_plan = plan(source, target, 100_000, 150)
    assert calls == 2
    assert len(the_plan.calls) <= calls and pairs >= the_plan.table_pairs
    assert k == the_plan.k


def test_a_call_lists_each_table_once_then_its_numbered_pairs():
    (call,) = plan([ORDERS, CUSTOMERS, NOTES], None, *DEFAULT_LIMITS).calls
    prompt = call.prompt(0.7)
    names = [t["name"] for t in listed_tables(prompt)]
    assert sorted(names) == sorted(set(names)) == ["customers", "notes", "orders"]
    pairs = listed_pairs(prompt)
    assert [p["pair"] for p in pairs] == list(range(1, len(pairs) + 1))
    assert "batch of" in prompt and "separate table pairs" in prompt
    assert "Judge each pair on its own" in prompt and '"none"' in prompt
    assert "suggested at 0.7 or more" in prompt  # the run's threshold (J1463)


def test_a_call_lists_only_comparable_columns_and_never_a_date():
    (call,) = plan([ORDERS, CUSTOMERS], None, *DEFAULT_LIMITS).calls
    orders = next(t for t in listed_tables(call.prompt(0.7)) if t["name"] == "orders")
    assert [c["name"] for c in orders["comparable_columns"]] == ["id", "customer_id"]


# -- reading the answers ---------------------------------------------------------------------------


async def test_answers_are_read_per_number_and_an_unnamed_column_pair_scores_zero():
    the_plan = plan([ORDERS], [CUSTOMERS], *DEFAULT_LIMITS)
    named = column_pair(("orders", "customer_id"), ("customers", "id"), 0.95)

    def ask(prompt: str, max_tokens: int) -> Completion:
        return Completion(answer(prompt, [named]), "acme", "m", None, False)

    tally, kept = await _execute(the_plan, ask)
    assert [p.edge for p in kept] == [(1, "customer_id", 2, "id")]
    rows = {r.proposal.edge: (r.left_out, r.proposal) for r in tally.left_out if r.proposal}
    left_out, zero = rows[(1, "id", 2, "id")]
    assert left_out == BELOW_THRESHOLD
    # Its two columns share a name: it says so (REQ-413, amended 2026-11-20).
    assert (zero.confidence, zero.reasoning) == (0.0, SAME_NAME)
    assert tally.zero_scored == 1 and tally.unanswered == [] and tally.asked_again == 0


async def test_none_answers_a_pair_and_scores_each_of_its_column_pairs_zero():
    the_plan = plan([ORDERS], [CUSTOMERS], *DEFAULT_LIMITS)
    tally, kept = await _execute(
        the_plan, lambda p, m: Completion(answer(p), "acme", "m", None, False)
    )
    assert kept == []
    assert {r.proposal.edge for r in tally.left_out if r.proposal is not None} == {
        (1, "id", 2, "id"),
        (1, "customer_id", 2, "id"),
    }
    assert tally.zero_scored == 2 and tally.unanswered == []


async def test_an_unanswered_pair_is_asked_again_and_then_named_never_zero():
    the_plan = plan([ORDERS, NOTES], [CUSTOMERS], *DEFAULT_LIMITS)
    prompts: list[str] = []

    def ask(prompt: str, max_tokens: int) -> Completion:
        prompts.append(prompt)
        return Completion(answer(prompt, omit=[("notes", "customers")]), "acme", "m", None, False)

    tally, _ = await _execute(the_plan, ask)
    assert len(prompts) == 2
    assert [(p["source"], p["target"]) for p in listed_pairs(prompts[1])] == [
        ("notes", "customers")
    ]
    assert tally.asked_again == 1
    assert tally.unanswered == ["notes × customers"]
    assert tally.as_dict()["unanswered"] == ["notes × customers"]
    # Never taken as zero: no row of the unanswered pair.
    assert not any(r.proposal is not None and r.proposal.start[0] == 3 for r in tally.left_out)


async def test_an_unanswered_pair_answered_the_second_time_is_not_named():
    the_plan = plan([ORDERS], [CUSTOMERS], *DEFAULT_LIMITS)
    calls = 0

    def ask(prompt: str, max_tokens: int) -> Completion:
        nonlocal calls
        calls += 1
        text = "[]" if calls == 1 else answer(prompt)
        return Completion(text, "acme", "m", None, False)

    tally, _ = await _execute(the_plan, ask)
    assert calls == 2 and tally.asked_again == 1 and tally.unanswered == []
    assert tally.zero_scored == 2


async def test_a_failed_call_s_pairs_are_asked_again_then_named():
    the_plan = plan([ORDERS], [CUSTOMERS], *DEFAULT_LIMITS)

    def ask(prompt: str, max_tokens: int) -> Completion:
        raise ConnectionError("down")

    tally, _ = await _execute(the_plan, ask)
    assert tally.done == 2 and tally.unanswered == ["orders × customers"]
    assert tally.failed[0]["table_pairs"] == ["orders × customers"]


# -- names people use -------------------------------------------------------------------------------


ALIASED_ORDERS = _tbl(
    1,
    "ord_tbl_x",
    _col("id"),
    _col("cst_fk", alias="customer_id"),
    alias="orders",
)
ALIASED_CUSTOMERS = _tbl(2, "cust_tbl_x", _col("id", alias="customer_number"), alias="customers")


async def test_aliases_are_sent_and_mapped_back_physical_names_never_sent():
    the_plan = plan([ALIASED_ORDERS], [ALIASED_CUSTOMERS], *DEFAULT_LIMITS)
    prompts: list[str] = []

    def ask(prompt: str, max_tokens: int) -> Completion:
        prompts.append(prompt)
        named = column_pair(("orders", "customer_id"), ("customers", "customer_number"), 0.95)
        return Completion(answer(prompt, [named]), "acme", "m", None, False)

    tally, kept = await _execute(the_plan, ask)
    (prompt,) = prompts
    for physical in ("ord_tbl_x", "cust_tbl_x", "cst_fk"):
        assert physical not in prompt
    assert '"name":"customer_id"' in prompt and '"name":"customer_number"' in prompt
    assert [p.edge for p in kept] == [(1, "cst_fk", 2, "id")]


async def test_a_physical_name_in_the_answer_where_an_alias_is_set_is_unknown():
    the_plan = plan([ALIASED_ORDERS], [ALIASED_CUSTOMERS], *DEFAULT_LIMITS)

    def ask(prompt: str, max_tokens: int) -> Completion:
        named = column_pair(("orders", "cst_fk"), ("customers", "customer_number"), 0.95)
        return Completion(answer(prompt, [named]), "acme", "m", None, False)

    tally, kept = await _execute(the_plan, ask)
    assert kept == []
    assert "unknown_column" in [r.left_out for r in tally.left_out]


def test_two_tables_sharing_a_name_are_told_apart_by_domain():
    a = _tbl(1, "orders", _col("id"))
    b = TableFacts(**{**a.__dict__, "table_id": 2, "domain_id": "e"})
    (call,) = plan([a], [b], *DEFAULT_LIMITS).calls
    assert {t["name"] for t in listed_tables(call.prompt(0.7))} == {"d.orders", "e.orders"}


# -- sample values -----------------------------------------------------------------------------------


async def test_sample_values_are_each_column_s_distinct_values_not_its_first_rows(monkeypatch):
    from provisa.profiler import run as profiler

    statements: list[str] = []

    async def governed(sql: str):
        statements.append(sql)
        return ["c"], [("x",), ("y",)]

    monkeypatch.setattr(profiler, "_governed", governed)
    monkeypatch.setattr(
        gather, "_published", lambda state, tid, name: ('"d"."t"', {"a": "a", "b": "b"})
    )
    # The run's samples per column, set in the dialog (REQ-413, amended 2026-11-18).
    samples = await gather._samples(object(), 1, "t", ["a", "b"], 3)
    assert samples == {"a": ("x", "y"), "b": ("x", "y")}
    assert statements == [
        'SELECT DISTINCT "a" FROM "d"."t" WHERE "a" IS NOT NULL LIMIT 3',
        'SELECT DISTINCT "b" FROM "d"."t" WHERE "b" IS NOT NULL LIMIT 3',
    ]


@pytest.mark.parametrize("value", ["Riverside"])
def test_sample_values_sent_are_the_column_s_values(value):
    t = _tbl(1, "t", _col("n", "varchar", samples=(value, "Old Town")))
    (call,) = plan([t, NOTES], None, *DEFAULT_LIMITS).calls
    listed = {x["name"]: x for x in listed_tables(call.prompt(0.7))}
    assert listed["t"]["comparable_columns"][0]["sample_values"] == [value, "Old Town"]
    assert json.dumps(listed)  # what is sent is JSON
