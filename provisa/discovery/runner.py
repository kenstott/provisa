# Copyright (c) 2026 Kenneth Stott
# Canary: 4d6c0a3e-7b21-4f95-8c1e-b5a9e2f07d13
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The semantic calls of a discovery run, asked one at a time (REQ-413, amended 2026-11-18).

Each call lists numbered table pairs (:class:`provisa.discovery.windows.Call`); its answer is read
per number (:func:`read_answers`), the column names it gives mapped back to the run's columns. A
column pair of an answered table pair that the answer does not name scores zero and is judged and
recorded like any other. A listed pair left without an answer -- omitted, cut off, in an
unreadable answer or a failed call -- is asked once more after the first pass
(:meth:`provisa.discovery.windows.Plan.again`); still unanswered, the run names it
(:attr:`Tally.unanswered`). It is never taken as a zero.

A call the vendor refuses as too long is split in half and asked again
(:meth:`provisa.discovery.windows.Call.halves`). One failed call is reported, as a catalogued
message (:func:`provisa.llm.client.failure`), and the rest continue. Between requests the run's control is asked whether to stop: a stop ends the run after
the request in flight and keeps what was returned.

Nothing an answer holds is dropped: an element that is not kept is recorded with why
(:attr:`Tally.left_out`, the reasons of :mod:`provisa.discovery.proposals`).
"""

# Requirements: REQ-413

from __future__ import annotations

import asyncio
import json
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field, replace
from typing import Any, Protocol

from provisa.discovery.analyzer import AnswerList
from provisa.discovery.columns import ColumnFacts, compared
from provisa.discovery.direction import Proposal, from_answer
from provisa.discovery.proposals import (
    BELOW_THRESHOLD,
    INVALID_SHAPE,
    NOT_COMPARABLE,
    UNKNOWN_COLUMN,
    Recorded,
)
from provisa.discovery.windows import Call, ColumnPair, Plan, TablePair
from provisa.llm.client import failure

# What a vendor says when it refuses a request as longer than the model reads: Anthropic's
# "prompt is too long", OpenAI's "context_length_exceeded" / "maximum context length", and the
# "too many tokens" other OpenAI-wire gateways answer with.
_TOO_LONG = re.compile(
    r"prompt is too long|context[_ ]length|maximum context length|too many tokens|input is too long",
    re.IGNORECASE,
)

_REQUIRED = (
    "source_table",
    "source_column",
    "target_table",
    "target_column",
    "cardinality",
    "confidence",
)

#: An answered table pair's column pair the answer does not name: it scores zero (REQ-413,
#: amended 2026-11-18).
NOT_NAMED = "not named by the model for its table pair"

#: A same-name column pair in one type group that no answer named: it is measured whatever the
#: model answered (REQ-413, amended 2026-11-20).
SAME_NAME = "same name in one type group, measured whatever the model answered"


def same_name(proposal: Proposal) -> bool:
    """Its two columns share a name (REQ-413, amended 2026-11-20): with Key value overlap chosen
    it is measured whatever its model confidence, and the model can only add pairs to it."""
    return proposal.start[1] == proposal.end[1]


def same_name_pairs(pair: TablePair) -> list[ColumnPair]:
    """The column pairs of ``pair`` compared in some type group whose two columns share a name."""
    return [(a, b) for a, b in pair.column_pairs() if a.name == b.name]


#: An answer no list could be read from, as a failed request's catalogued message.
UNREADABLE = {
    "code": "discovery.answer_unreadable",
    "params": {},
    "message": "The model's answer held no readable list.",
}


#: Where an answer's list of table pairs can open: a ``[`` before an object or its own end.
_LIST_OPENING = re.compile(r"\[(?=\s*[{\]])")


def _of_pairs(value: Any) -> bool:
    return isinstance(value, list) and all(isinstance(v, dict) and "pair" in v for v in value)


def read_pair_answers(text: str, cut_off: bool) -> AnswerList:
    """The answer's list of table pair answers, whatever text or code fence is around it: the
    first JSON list whose elements each name a ``pair``, so a pair's own list of column pairs is
    never taken for it. An answer that stopped at its token limit (``cut_off``) and whose list
    never closed gives the pair answers that were complete, marked ``cut_short``."""
    decoder = json.JSONDecoder()
    opens = [m.start() for m in _LIST_OPENING.finditer(text)]
    for i in opens:
        try:
            value, _ = decoder.raw_decode(text, i)
        except json.JSONDecodeError:
            continue
        if _of_pairs(value):
            return AnswerList(value)
    if cut_off and opens:
        items: list = []
        pos = opens[0] + 1
        while True:
            while pos < len(text) and text[pos] in " \t\r\n,":
                pos += 1
            try:
                item, pos = decoder.raw_decode(text, pos)
            except json.JSONDecodeError:
                break
            items.append(item)
        return AnswerList(items, cut_short=True)
    return AnswerList(None)


def too_long(exc: BaseException) -> bool:
    """Whether a model call's error is the vendor refusing the request as too long."""
    return _TOO_LONG.search(str(exc)) is not None


@dataclass
class Tally:
    """What the semantic requests have done so far, against the run's ``threshold`` (REQ-413,
    amended 2026-11-18): the tick threshold of the person who started it, taken when it started.
    A proposal at it or above is kept; the key value overlap band runs from the cutoff up to it."""

    total: int
    threshold: float
    #: The run's table pairs (each listed in exactly one call of the first pass).
    table_pairs: int = 0
    done: int = 0
    split: int = 0
    proposed: int = 0
    #: Elements that named no two compared columns of their table pair.
    invalid: int = 0
    #: Column pairs of answered table pairs the answers did not name: each scored zero.
    zero_scored: int = 0
    #: Table pairs asked a second time, left unanswered the first.
    asked_again: int = 0
    #: Table pairs still unanswered after the second asking, each by name (``source × target``).
    unanswered: list[str] = field(default_factory=list)
    #: Proposals whose two columns are not compared: their classes do not meet.
    not_comparable: int = 0
    #: Proposals left out under :attr:`threshold`: by their measure where they were measured,
    #: else by the model's confidence.
    below_threshold: int = 0
    #: Pairs in the band between the measure cutoff and :attr:`threshold` measured on their
    #: data, and those of them admitted on matching key values (REQ-413).
    measured: int = 0
    admitted: int = 0
    #: Proposals a second scorer was asked about, and those of them it scored under
    #: :attr:`threshold`. Its score removes none (REQ-1789).
    scored: int = 0
    scored_below_threshold: int = 0
    #: Requests whose proposals the second scorer could not score: they kept the model's.
    scoring_failed: int = 0
    cut_short: int = 0
    unreadable: int = 0
    #: Each failed call: its ``table_pairs`` by name and its catalogued message (``code``, ``params`` and the
    #: English ``message``). An unreadable answer is one of them.
    failed: list[dict[str, Any]] = field(default_factory=list)
    stopped: bool = False
    #: Every element read and not kept, each with why.
    left_out: list[Recorded] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "requests_total": self.total,
            "requests_done": self.done,
            "table_pairs": self.table_pairs,
            "zero_scored": self.zero_scored,
            "asked_again": self.asked_again,
            "unanswered": list(self.unanswered),
            "split": self.split,
            "proposed": self.proposed,
            "invalid": self.invalid,
            "not_comparable": self.not_comparable,
            "below_threshold": self.below_threshold,
            "measured": self.measured,
            "admitted": self.admitted,
            "scored": self.scored,
            "scored_below_threshold": self.scored_below_threshold,
            "scoring_failed": self.scoring_failed,
            "threshold": self.threshold,
            "cut_short": self.cut_short,
            "unreadable": self.unreadable,
            "failed": list(self.failed),
            "stopped": self.stopped,
        }


class Control(Protocol):
    """The run's link to whoever watches it."""

    async def stop_asked(self) -> bool: ...

    async def returned(self, tally: Tally, proposals: list[Proposal]) -> None:
        """A request returned: ``proposals`` are every semantic proposal so far."""
        ...


#: One model call: (prompt, max output tokens) -> the vendor's Completion. Blocking; run on a
#: thread.
Ask = Callable[[str, int], Any]


#: The characters kept of an element that made no proposal, as the model wrote it.
ANSWER_CHARS = 2000


def _written(raw: Any) -> str:
    return json.dumps(raw, ensure_ascii=False, default=str)[:ANSWER_CHARS]


def _columns(table: Any) -> dict[str, ColumnFacts]:
    """A table's columns by the name the model is given (:attr:`ColumnFacts.shown`)."""
    return {c.shown: c for c in table.columns}


def read_answers(
    call: Call, items: list, tally: Tally
) -> tuple[set[int], list[Proposal], list[Proposal], list[Proposal]]:
    """``(answered, proposals, apart, zeros)``: the numbers of the call's pairs that were
    answered, the proposals the answers make (two compared columns of their pair, mapped back
    from the names given), those naming two columns whose classes do not meet (each counted), and
    every column pair of an answered table pair that its answer does not name, as a proposal
    scored zero (counted in :attr:`Tally.zero_scored`). ``tally`` counts every element that makes no proposal
    and keeps it with why."""
    labels = call.labels
    answered: set[int] = set()
    out: list[Proposal] = []
    apart: list[Proposal] = []
    named: dict[int, set[frozenset]] = {}

    def no_proposal(raw: Any, reason: str) -> None:
        tally.invalid += 1
        tally.left_out.append(Recorded("semantics", reason, answer=_written(raw)))

    for item in items:
        number = item.get("pair") if isinstance(item, dict) else None
        pairs = item.get("column_pairs") if isinstance(item, dict) else None
        if (
            not isinstance(number, int)
            or isinstance(number, bool)
            or not 1 <= number <= len(call.pairs)
            or not (pairs == "none" or isinstance(pairs, list))
        ):
            no_proposal(item, INVALID_SHAPE)
            continue
        answered.add(number)
        pair = call.pairs[number - 1]
        sides = {
            labels[pair.source.table_id]: pair.source,
            labels[pair.target.table_id]: pair.target,
        }
        for raw in [] if pairs == "none" else pairs:
            tally.proposed += 1
            if (
                not isinstance(raw, dict)
                or any(k not in raw for k in _REQUIRED)
                or raw["cardinality"] not in ("many-to-one", "one-to-many")
            ):
                no_proposal(raw, INVALID_SHAPE)
                continue
            try:
                confidence = float(raw["confidence"])
            except (TypeError, ValueError):
                no_proposal(raw, INVALID_SHAPE)
                continue
            s_table = sides.get(str(raw["source_table"]))
            t_table = sides.get(str(raw["target_table"]))
            directed = not pair.either_direction
            if (
                s_table is None
                or t_table is None
                or (directed and (s_table is not pair.source or t_table is not pair.target))
                or (s_table is t_table and pair.source is not pair.target)
            ):
                no_proposal(raw, UNKNOWN_COLUMN)
                continue
            a = _columns(s_table).get(str(raw["source_column"]))
            b = _columns(t_table).get(str(raw["target_column"]))
            if a is None or b is None:
                no_proposal(raw, UNKNOWN_COLUMN)
                continue
            source = (s_table.table_id, a.name)
            target = (t_table.table_id, b.name)
            named.setdefault(number, set()).add(frozenset({source, target}))
            proposal = from_answer(
                "semantics",
                source,
                target,
                str(raw["cardinality"]),
                confidence,
                str(raw.get("reasoning", "")),
                str(raw.get("suggested_name", "")),
            )
            if not compared(a, b):
                tally.not_comparable += 1
                apart.append(proposal)
            else:
                out.append(proposal)
    zeros = [
        z
        for number in sorted(answered)
        for z in _zeros(call.pairs[number - 1], named.get(number, set()), tally)
    ]
    return answered, out, apart, zeros


def _zeros(pair: TablePair, named: set[frozenset], tally: Tally) -> list[Proposal]:
    """The column pairs of an answered ``pair`` its answer did not name, each scored zero."""
    out: list[Proposal] = []
    for a, b in pair.column_pairs():
        source = (pair.source.table_id, a.name)
        target = (pair.target.table_id, b.name)
        if frozenset({source, target}) in named:
            continue
        tally.zero_scored += 1
        reason = SAME_NAME if a.name == b.name else NOT_NAMED
        out.append(from_answer("semantics", source, target, "many-to-one", 0.0, reason))
    return out


def unanswered_same_name(
    plan: Plan, answered: set[tuple[int, int]], tally: Tally
) -> list[Proposal]:
    """Every same-name column pair of a table pair of ``plan`` no answer covered (unanswered,
    failed or unreadable), once each, scored zero (REQ-413, amended 2026-11-20)."""
    seen: set[frozenset] = set()
    out: list[Proposal] = []
    for call in plan.calls:
        for pair in call.pairs:
            if pair.key in answered:
                continue
            for a, b in same_name_pairs(pair):
                source = (pair.source.table_id, a.name)
                target = (pair.target.table_id, b.name)
                if frozenset({source, target}) in seen:
                    continue
                seen.add(frozenset({source, target}))
                tally.zero_scored += 1
                out.append(from_answer("semantics", source, target, "many-to-one", 0.0, SAME_NAME))
    return out


#: Measures proposals on their data (:meth:`provisa.discovery.measure.Measurer.all`): each with
#: its measure and the score it gives.
Measure = Callable[[list[Proposal]], Awaitable[list[Proposal]]]


@dataclass(frozen=True)
class KeyValues:
    """Key value overlap, chosen for a run (REQ-413, amended 2026-11-16): ``measure`` measures
    the semantic pairs whose MODEL confidence is at ``cutoff`` or more and under the run's
    threshold (:attr:`Tally.threshold`), and every same-name pair under the threshold whatever
    its model confidence (amended 2026-11-20); no other pair is measured."""

    measure: Measure
    cutoff: float


#: A second scorer (REQ-1789): for each proposal, in order, the confidence it gives, or None
#: where it gave none. None for all of them: it could not be asked.
Rescore = Callable[[list[Proposal]], Awaitable[list[float | None] | None]]


def rescored(
    found: list[Proposal], scores: list[float | None] | None, tally: Tally
) -> list[Proposal]:
    """``found``, each with the second scorer's confidence beside the model's (REQ-1789). The
    model's decides which are suggestions; those the scorer put under the run's threshold are
    counted, and none is removed."""
    if scores is None:
        tally.scoring_failed += 1
        return found
    out: list[Proposal] = []
    for proposal, score in zip(found, scores, strict=True):
        if score is not None:
            tally.scored += 1
            if score < tally.threshold:
                tally.scored_below_threshold += 1
        out.append(replace(proposal, scored=score))
    return out


async def judged(
    found: list[Proposal],
    apart: list[Proposal],
    key_values: KeyValues | None,
    refine: Rescore | None,
    tally: Tally,
    zeros: list[Proposal] | None = None,
) -> list[Proposal]:
    """An answer's proposals judged in order of cost (REQ-413, amended 2026-11-16): Jev's
    re-score (``refine``) kept beside the model's confidence; then the key-value measure of only
    the pairs whose model confidence lies in the band from the cutoff up to the run's threshold
    (:attr:`Tally.threshold`, REQ-413 amended 2026-11-18). At or above the threshold a proposal is
    kept on the model's confidence, unmeasured; below the band, or in it without ``key_values``,
    it is left out below the threshold, unmeasured. A band pair is kept when its measure scores
    it at the threshold or more (admitted on matching key values), and is left out with its
    measure otherwise. Proposals whose columns are not compared are left out unmeasured.
    ``zeros``: column pairs the model was asked about and did not name, scored zero (REQ-413,
    amended 2026-11-18); the second scorer is not asked of them, and they are judged with the
    rest, so the band and the list see them. A same-name pair (:func:`same_name`) under the
    threshold is in the band whatever its model confidence (REQ-413, amended 2026-11-20): the
    model may add pairs to the measure but never drop one."""
    threshold = tally.threshold
    tally.left_out.extend(Recorded("semantics", NOT_COMPARABLE, p) for p in apart)
    if refine is not None and found:
        found = rescored(found, await refine(found), tally)
    found = [*found, *(zeros or [])]
    out = [p for p in found if p.confidence >= threshold]
    band = (
        []
        if key_values is None
        else [
            p
            for p in found
            if p.confidence < threshold and (key_values.cutoff <= p.confidence or same_name(p))
        ]
    )
    in_band = {id(p) for p in band}
    below = [p for p in found if p.confidence < threshold and id(p) not in in_band]
    if band:
        assert key_values is not None
        measured = await key_values.measure(band)
        tally.measured += len(measured)
        for proposal in measured:
            if proposal.rank >= threshold:
                tally.admitted += 1
                out.append(proposal)
            else:
                below.append(proposal)
    for proposal in below:
        tally.below_threshold += 1
        tally.left_out.append(Recorded("semantics", BELOW_THRESHOLD, proposal))
    return out


async def execute(
    plan: Plan,
    max_tokens: int,
    ask: Ask,
    control: Control,
    *,
    threshold: float,
    key_values: KeyValues | None,
    refine: Rescore | None = None,
    to_thread: Callable[..., Awaitable[Any]] = asyncio.to_thread,
) -> tuple[Tally, list[Proposal]]:
    """Ask every call of ``plan`` in order, splitting one refused as too long; then ask once more,
    batched again, every table pair left unanswered, and name those still unanswered. Stop when
    the control asks. Each call states the run's ``threshold`` to the model, and the proposals are
    judged against it, in order of cost (:func:`judged`): ``refine`` re-scores them (REQ-1789,
    when a Jev key is configured), its confidence kept beside the model's; then, with Key value
    overlap chosen (``key_values``), the pairs in its band are measured on their data."""
    queue = list(plan.calls)
    tally = Tally(total=len(queue), threshold=threshold, table_pairs=plan.table_pairs)
    proposals: list[Proposal] = []
    unanswered: list[TablePair] = []
    covered: set[tuple[int, int]] = set()
    second = False

    def left(call: Call, pairs: list[TablePair]) -> None:
        if second:
            tally.unanswered.extend(call.named(p) for p in pairs)
        else:
            unanswered.extend(pairs)

    while queue or (unanswered and not second):
        if not queue:
            second = True
            queue = plan.again(unanswered)
            tally.asked_again = len(unanswered)
            tally.total += len(queue)
            unanswered = []
        if await control.stop_asked():
            tally.stopped = True
            break
        call = queue.pop(0)
        try:
            answer = await to_thread(ask, call.prompt(threshold), max_tokens)
        except Exception as exc:  # one failed call is reported, the rest go on (REQ-413)
            if too_long(exc) and call.splittable:
                halves = call.halves()
                queue[0:0] = halves
                tally.split += 1
                tally.total += len(halves) - 1
                continue
            tally.done += 1
            tally.failed.append(
                {"table_pairs": [call.named(p) for p in call.pairs], **failure(exc)}
            )
            left(call, list(call.pairs))
            await control.returned(tally, proposals)
            continue
        read = read_pair_answers(answer.text, answer.cut_off)
        tally.done += 1
        if read.items is None:
            tally.unreadable += 1
            tally.failed.append({"table_pairs": [call.named(p) for p in call.pairs], **UNREADABLE})
            left(call, list(call.pairs))
            await control.returned(tally, proposals)
            continue
        answered, found, apart, zeros = read_answers(call, read.items, tally)
        proposals.extend(await judged(found, apart, key_values, refine, tally, zeros))
        covered.update(call.pairs[n - 1].key for n in answered)
        left(call, [p for n, p in enumerate(call.pairs, start=1) if n not in answered])
        if answer.cut_off:
            tally.cut_short += 1
        await control.returned(tally, proposals)
    if not tally.stopped:
        rest = unanswered_same_name(plan, covered, tally)
        if rest:
            proposals.extend(await judged([], [], key_values, None, tally, rest))
            await control.returned(tally, proposals)
    return tally, proposals
