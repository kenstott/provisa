# Copyright (c) 2026 Kenneth Stott
# Canary: 5c2e8a17-9d43-4b06-b7f1-3a6d0e2c8f95
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-413 (amended 2026-11-16): a run's checks go in order of cost -- declared foreign keys, the
model's semantic proposals, Jev's re-score, then the key-value measure -- and only the pairs whose
MODEL confidence lies between the measure cutoff and the threshold are measured. At or above the
threshold a pair is suggested on the model's confidence, unmeasured; below the cutoff it is left
out, unmeasured; a band pair whose key values match is admitted. Without Key value overlap a band
pair is left out as below the threshold, unmeasured. The sample size is the most distinct values
read of each column."""

# Requirements: REQ-413, REQ-1789

from __future__ import annotations

import json
from dataclasses import replace
from types import SimpleNamespace
from typing import Any

from provisa.discovery import gather, measure, proposals
from provisa.discovery.columns import UNKNOWN, ColumnFacts, TableFacts, type_groups
from provisa.discovery.context import CONTAINED_AT, KEY_SCORE, SHARED_CEILING, Scoring
from provisa.discovery.direction import Proposal, from_answer
from provisa.discovery.measure import Measurer
from provisa.discovery.proposals import BELOW_THRESHOLD, NOT_COMPARABLE
from provisa.discovery.runner import KeyValues, Tally, execute, judged
from provisa.discovery.windows import plan
from tests.unit.discovery_answers import from_ids
from tests.unit.discovery_engine_reader import EngineReader
from provisa.llm.client import Completion
from provisa.llm.limits import DEFAULT_LIMITS

CUTOFF = 0.2
#: The run's threshold: the tick threshold of the person who started it.
THRESHOLD = 0.7
#: The measured score's settings at their defaults.
DEFAULT_SCORING = Scoring(CONTAINED_AT, SHARED_CEILING)


def _semantic(src: tuple[int, str], tgt: tuple[int, str], confidence: float) -> Proposal:
    return from_answer("semantics", src, tgt, "many-to-one", confidence, "names match")


class _Measure:
    """A measure that records the pairs it was given; each scores as ``scores`` says (by its
    source column), a key relationship by default."""

    def __init__(self, events: list[tuple[str, list]], scores: dict[str, float] | None = None):
        self.events = events
        self.scores = scores or {}

    async def __call__(self, found: list[Proposal]) -> list[Proposal]:
        self.events.append(("measure", [p.start for p in found]))
        return [replace(p, score=self.scores.get(p.start[1], KEY_SCORE)) for p in found]


def _jev(events: list[tuple[str, list]], score: float | None = None):
    async def refine(found: list[Proposal]) -> list[float | None]:
        events.append(("jev", [p.start for p in found]))
        return [score for _ in found]

    return refine


HIGH = _semantic((1, "high"), (2, "id"), 0.9)
AT = _semantic((1, "at"), (2, "id"), THRESHOLD)
BAND = _semantic((1, "band"), (2, "id"), 0.45)
EDGE = _semantic((1, "edge"), (2, "id"), CUTOFF)
LOW = _semantic((1, "low"), (2, "id"), 0.1)
APART = _semantic((1, "apart"), (2, "id"), 0.5)


async def _judged(
    found,
    apart=(),
    *,
    key_values=True,
    refine=None,
    scores=None,
    threshold=THRESHOLD,
    cutoff=CUTOFF,
):
    events: list[tuple[str, list]] = []
    tally = Tally(total=1, threshold=threshold)
    kv = KeyValues(_Measure(events, scores), cutoff) if key_values else None
    out = await judged(list(found), list(apart), kv, refine(events) if refine else None, tally)
    return out, tally, events


def _left(tally: Tally, i: int = 0) -> Proposal:
    proposal = tally.left_out[i].proposal
    assert proposal is not None
    return proposal


def _reasons(tally: Tally) -> dict[str, str | None]:
    return {r.proposal.start[1]: r.left_out for r in tally.left_out if r.proposal is not None}


async def test_jev_rescores_every_semantic_proposal_before_the_measure_runs():
    _, _, events = await _judged([HIGH, BAND, LOW], refine=lambda ev: _jev(ev, 0.5))
    assert [name for name, _ in events] == ["jev", "measure"]
    assert events[0][1] == [(1, "high"), (1, "band"), (1, "low")]
    assert events[1][1] == [(1, "band")]


async def test_a_pair_at_or_above_the_threshold_is_suggested_on_the_models_confidence_unmeasured():
    out, tally, events = await _judged([HIGH, AT])
    assert [(p.start[1], p.measure, p.score, p.rank) for p in out] == [
        ("high", None, None, 0.9),
        ("at", None, None, THRESHOLD),
    ]
    assert events == []
    assert tally.measured == 0


async def test_a_pair_below_the_cutoff_is_left_out_unmeasured():
    out, tally, events = await _judged([LOW])
    assert out == []
    assert _reasons(tally) == {"low": BELOW_THRESHOLD}
    assert _left(tally).score is None
    assert events == []
    assert tally.below_threshold == 1 and tally.measured == 0


async def test_a_pair_at_the_cutoff_is_in_the_band_and_measured():
    out, tally, events = await _judged([EDGE])
    assert events == [("measure", [(1, "edge")])]
    assert [p.start[1] for p in out] == ["edge"]


async def test_a_band_pair_whose_key_values_match_is_admitted_with_its_measure_beside_the_models():
    out, tally, _ = await _judged([BAND])
    assert [(p.start[1], p.confidence, p.score) for p in out] == [("band", 0.45, KEY_SCORE)]
    assert (tally.measured, tally.admitted, tally.below_threshold) == (1, 1, 0)
    assert tally.as_dict()["measured"] == 1 and tally.as_dict()["admitted"] == 1


async def test_a_band_pair_whose_values_do_not_match_is_left_out_with_its_measure():
    out, tally, _ = await _judged([BAND], scores={"band": 0.3})
    assert out == []
    assert _reasons(tally) == {"band": BELOW_THRESHOLD}
    assert _left(tally).score == 0.3
    assert (tally.measured, tally.admitted, tally.below_threshold) == (1, 0, 1)


async def test_the_band_is_judged_on_the_models_confidence_never_jevs():
    # Jev rates the band pair high and the confident pair low: neither moves between bands.
    _, _, events = await _judged([HIGH, BAND, LOW], refine=lambda ev: _jev(ev, 0.95))
    assert events[-1] == ("measure", [(1, "band")])
    out, _, events = await _judged([HIGH, BAND], refine=lambda ev: _jev(ev, 0.05))
    assert events[-1] == ("measure", [(1, "band")])
    assert [(p.start[1], p.scored) for p in out] == [("high", 0.05), ("band", 0.05)]


async def test_without_key_value_overlap_a_band_pair_is_left_out_below_the_threshold_unmeasured():
    out, tally, events = await _judged([HIGH, BAND, LOW], key_values=False)
    assert [p.start[1] for p in out] == ["high"]
    assert _reasons(tally) == {"band": BELOW_THRESHOLD, "low": BELOW_THRESHOLD}
    assert all(_left(tally, i).measure is None for i in range(2))
    assert all(_left(tally, i).score is None for i in range(2))
    assert events == [] and tally.measured == 0


async def test_a_pair_whose_columns_are_not_compared_is_never_measured():
    out, tally, events = await _judged([], [APART])
    assert out == [] and _reasons(tally) == {"apart": NOT_COMPARABLE}
    assert events == []
    assert _left(tally).measure is None


# --- the run: semantic, Jev, then the measure ------------------------------------------------------


def _col(name: str, *, pk: bool = False) -> ColumnFacts:
    return ColumnFacts(
        name=name,
        data_type="integer",
        description=None,
        tags=(),
        classes=(UNKNOWN,),
        sensitive=False,
        samples=(),
        is_primary_key=pk,
        groups=type_groups("integer", subtype=None, array_depth=0),
    )


def _tbl(tid: int, name: str, *columns: ColumnFacts) -> TableFacts:
    return TableFacts(
        table_id=tid,
        domain_id="sales",
        schema_name="public",
        table_name=name,
        description=None,
        tags=(),
        role=None,
        columns=columns,
    )


class _Watcher:
    async def stop_asked(self) -> bool:
        return False

    async def returned(self, tally: Tally, proposals: list[Proposal]) -> None:
        return None


async def _direct(fn, *args):
    return fn(*args)


#: The tables of the runs below, by id.
_NAMES = {1: "orders", 2: "customers"}


def _item(src: str, confidence: float) -> dict:
    return {
        "source_table_id": 1,
        "source_column": src,
        "target_table_id": 2,
        "target_column": "id",
        "cardinality": "many-to-one",
        "confidence": confidence,
        "reasoning": "names match",
    }


async def test_a_run_asks_the_model_then_jev_then_measures_only_the_band():
    # orders' key is named apart from customers.id: a same-name pair is measured whatever the
    # model answers (REQ-413, amended 2026-11-20), and the band is what is under test.
    orders = _tbl(1, "orders", _col("order_id", pk=True), _col("customer_id"), _col("store_id"))
    customers = _tbl(2, "customers", _col("id", pk=True))
    planned = plan([orders], [customers], *DEFAULT_LIMITS)
    events: list[tuple[str, list]] = []

    def ask(prompt: str, max_tokens: int) -> Completion:
        events.append(("model", []))
        answer = [_item("customer_id", 0.9), _item("store_id", 0.4)]
        return Completion(from_ids(prompt, json.dumps(answer), _NAMES), "acme", "m", None, False)

    tally, found = await execute(
        planned,
        4_096,
        ask,
        _Watcher(),
        threshold=THRESHOLD,
        key_values=KeyValues(_Measure(events), CUTOFF),
        refine=_jev(events, 0.6),
        to_thread=_direct,
    )
    assert [name for name, _ in events] == ["model", "jev", "measure"]
    assert events[2][1] == [(1, "store_id")]
    assert {p.start[1]: (p.score, p.scored) for p in found} == {
        "customer_id": (None, 0.6),
        "store_id": (KEY_SCORE, 0.6),
    }
    assert (tally.measured, tally.admitted) == (1, 1)


# --- the band's top is the run's threshold, the starting person's tick threshold -----------------
# REQ-413 (amended 2026-11-18): not a fixed 0.7.


async def test_a_pair_at_or_above_a_threshold_of_half_is_kept_unmeasured():
    pair = _semantic((1, "at_55"), (2, "id"), 0.55)
    out, tally, events = await _judged([pair], threshold=0.5)
    assert [(p.start[1], p.score) for p in out] == [("at_55", None)]
    assert events == [] and tally.measured == 0


async def test_a_pair_between_the_cutoff_and_a_threshold_of_half_is_measured():
    pair = _semantic((1, "at_40"), (2, "id"), 0.4)
    out, tally, events = await _judged([pair], threshold=0.5, cutoff=0.2)
    assert events == [("measure", [(1, "at_40")])]
    assert tally.measured == 1


async def test_a_pair_under_a_threshold_of_0_8_is_measured_though_over_0_7():
    pair = _semantic((1, "at_60"), (2, "id"), 0.6)
    _, tally, events = await _judged([pair], threshold=0.8, cutoff=0.2)
    assert events == [("measure", [(1, "at_60")])]
    assert tally.measured == 1


async def test_a_pair_under_the_cutoff_is_not_measured_whatever_the_threshold():
    pair = _semantic((1, "at_10"), (2, "id"), 0.1)
    out, tally, events = await _judged([pair], threshold=0.5, cutoff=0.2)
    assert out == [] and events == []
    assert _reasons(tally) == {"at_10": BELOW_THRESHOLD}


async def test_a_measured_pair_is_admitted_at_the_runs_threshold_not_a_fixed_one():
    pair = _semantic((1, "band"), (2, "id"), 0.3)
    out, tally, _ = await _judged([pair], threshold=0.5, scores={"band": 0.55})
    assert [(p.start[1], p.score) for p in out] == [("band", 0.55)]
    assert tally.admitted == 1


async def test_the_runs_threshold_is_the_one_its_summary_reports():
    _, tally, _ = await _judged([], threshold=0.55)
    assert tally.as_dict()["threshold"] == 0.55


async def test_a_run_tells_the_model_its_threshold_and_judges_against_it():
    # orders' key is named apart from customers.id: a same-name pair is measured whatever the
    # model answers (REQ-413, amended 2026-11-20), and the band is what is under test.
    orders = _tbl(1, "orders", _col("order_id", pk=True), _col("customer_id"), _col("store_id"))
    customers = _tbl(2, "customers", _col("id", pk=True))
    planned = plan([orders], [customers], *DEFAULT_LIMITS)
    events: list[tuple[str, list]] = []
    prompts: list[str] = []

    def ask(prompt: str, max_tokens: int) -> Completion:
        prompts.append(prompt)
        answer = [_item("customer_id", 0.6), _item("store_id", 0.85)]
        return Completion(from_ids(prompt, json.dumps(answer), _NAMES), "acme", "m", None, False)

    tally, found = await execute(
        planned,
        4_096,
        ask,
        _Watcher(),
        threshold=0.8,
        key_values=KeyValues(_Measure(events, {"customer_id": 0.3}), CUTOFF),
        to_thread=_direct,
    )
    assert prompts and all("A relationship is suggested at 0.8 or more" in t for t in prompts)
    assert not any("suggested at 0.7" in t for t in prompts)
    # 0.6 is under 0.8, so it is measured; 0.85 is kept on the model's confidence.
    assert events == [("measure", [(1, "customer_id")])]
    assert [p.start[1] for p in found] == ["store_id"]
    assert tally.as_dict()["threshold"] == 0.8


# --- the sample size reaches the measurer ----------------------------------------------------------


async def test_the_sample_size_is_the_most_non_null_values_read_of_each_column():
    tables = {1: _tbl(1, "a", _col("x")), 2: _tbl(2, "b", _col("y"))}
    reader = EngineReader(
        {("a", "x"): list(range(100)), ("b", "y"): list(range(200))}, sql_type="BIGINT"
    )
    measurer = Measurer(reader, tables, decided=frozenset(), bound=40, scoring=DEFAULT_SCORING)
    got = await measurer.measured(_semantic((1, "x"), (2, "y"), 0.5))
    [(_, sql)] = reader.queries
    assert sql.count("IS NOT NULL LIMIT 40)") == 2
    assert got.measure is not None and got.measure.bound == 40
    assert got.measure.source.values == 40 and got.measure.source.more is True
    assert got.measure.target.values == 40 and got.measure.target.more is True


async def test_the_values_reader_reads_at_most_the_sample_size(monkeypatch):
    calls: list[str] = []

    def published_as(_state, role, table_id):
        return '"sales"."a"', {"x": "x"}

    async def governed_as(sql, role):
        calls.append(sql)
        return [(1, 1), (2, 1), (3, 1)]

    monkeypatch.setattr(gather, "_published_as", published_as)
    monkeypatch.setattr(gather, "_governed_as", governed_as)
    reader = gather.values_reader(SimpleNamespace(), "analyst")
    got = await reader.values(_tbl(1, "a", _col("x")), "x", 2)
    assert calls[0].endswith("LIMIT 3")
    assert isinstance(got, measure.Values)
    assert got.read == (1, 2) and got.more is True
    assert measure.DISTINCT_BOUND == 10_000


# --- an admitted row says why ----------------------------------------------------------------------


def _row(**over: Any) -> SimpleNamespace:
    row: dict[str, Any] = {
        "id": 1,
        "run_id": 1,
        "kind": "semantics",
        "source_table_id": 1,
        "source_column": "band",
        "target_table_id": 2,
        "target_column": "id",
        "cardinality": "many-to-one",
        "score": KEY_SCORE,
        "measure": json.dumps({"bound": 10_000}),
        "confidence": 0.45,
        "scored": None,
        "reasoning": "names match",
        "suggested_name": None,
        "left_out": None,
        "answer": None,
        # REQ-1586: a discovery_run_proposals row carries its junction columns, empty off one.
        "via_table_id": None,
        "via_source_column": None,
        "via_target_column": None,
    }
    row.update(over)
    return SimpleNamespace(**row)


def test_a_suggestion_admitted_on_its_measure_says_it_was_admitted_on_matching_key_values():
    assert proposals._view(_row(), set(), None)["admitted_on_key_values"] is True


def test_a_suggestion_on_the_models_confidence_or_a_measured_row_left_out_says_nothing_of_it():
    unmeasured = _row(score=None, measure=None, confidence=0.9)
    assert proposals._view(unmeasured, set(), None)["admitted_on_key_values"] is False
    low = _row(score=0.3, left_out=BELOW_THRESHOLD)
    assert proposals._view(low, set(), None)["admitted_on_key_values"] is False
