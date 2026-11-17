# Copyright (c) 2026 Kenneth Stott
# Canary: 4d6c0a3e-7b21-4f95-8c1e-b5a9e2f07d13
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The semantic requests of a discovery run, asked one at a time (REQ-413).

An answer that is cut off or full, or a request the vendor refuses as too long, is split in half
and asked again (:meth:`provisa.discovery.windows.Request.halves`); the complete suggestions of an
answer are kept either way, so nothing is dropped. One failed request is reported, as a
catalogued message (:func:`provisa.llm.client.failure`), and the rest continue. Between requests the run's control is asked whether to stop: a stop ends the run after
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

from provisa.discovery.analyzer import read_answer_list
from provisa.discovery.columns import comparable
from provisa.discovery.context import KEPT_AT
from provisa.discovery.direction import Proposal, from_answer
from provisa.discovery.proposals import (
    BELOW_THRESHOLD,
    INVALID_SHAPE,
    NOT_COMPARABLE,
    SCORED_BELOW_THRESHOLD,
    UNKNOWN_COLUMN,
    Recorded,
)
from provisa.discovery.windows import Request
from provisa.llm.client import failure

#: The confidence a semantic suggestion needs to be kept (as the discovery router's
#: MIN_CONFIDENCE), which the request's instructions state to the model.
MIN_CONFIDENCE = KEPT_AT

# What a vendor says when it refuses a request as longer than the model reads: Anthropic's
# "prompt is too long", OpenAI's "context_length_exceeded" / "maximum context length", and the
# "too many tokens" other OpenAI-wire gateways answer with.
_TOO_LONG = re.compile(
    r"prompt is too long|context[_ ]length|maximum context length|too many tokens|input is too long",
    re.IGNORECASE,
)

_REQUIRED = (
    "source_table_id",
    "source_column",
    "target_table_id",
    "target_column",
    "cardinality",
    "confidence",
)


#: An answer no list could be read from, as a failed request's catalogued message.
UNREADABLE = {
    "code": "discovery.answer_unreadable",
    "params": {},
    "message": "The model's answer held no readable list.",
}


def too_long(exc: BaseException) -> bool:
    """Whether a model call's error is the vendor refusing the request as too long."""
    return _TOO_LONG.search(str(exc)) is not None


@dataclass
class Tally:
    """What the semantic requests have done so far."""

    total: int
    done: int = 0
    split: int = 0
    proposed: int = 0
    #: Elements that named no two compared columns of their request.
    invalid: int = 0
    #: Proposals whose two columns are not compared: their classes do not meet.
    not_comparable: int = 0
    #: Proposals the model itself gave less than :data:`MIN_CONFIDENCE`.
    below_threshold: int = 0
    #: Proposals a second scorer was asked about, and those of them it scored under
    #: :data:`MIN_CONFIDENCE` though the model had not.
    scored: int = 0
    scored_below_threshold: int = 0
    #: Requests whose proposals the second scorer could not score: they kept the model's.
    scoring_failed: int = 0
    cut_short: int = 0
    unreadable: int = 0
    #: Each failed request: its ``group`` and its catalogued message (``code``, ``params`` and the
    #: English ``message``). An unreadable answer is one of them.
    failed: list[dict[str, Any]] = field(default_factory=list)
    stopped: bool = False
    #: Every element read and not kept, each with why.
    left_out: list[Recorded] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "requests_total": self.total,
            "requests_done": self.done,
            "split": self.split,
            "proposed": self.proposed,
            "invalid": self.invalid,
            "not_comparable": self.not_comparable,
            "below_threshold": self.below_threshold,
            "scored": self.scored,
            "scored_below_threshold": self.scored_below_threshold,
            "scoring_failed": self.scoring_failed,
            "threshold": MIN_CONFIDENCE,
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


def read_items(request: Request, items: list, tally: Tally) -> list[Proposal]:
    """The proposals an answer's elements make: each names two different compared columns of
    the request, of its group, whose classes meet, at :data:`MIN_CONFIDENCE` or more; ``tally``
    counts the rest and keeps each with why it is left out."""
    columns = {(t.table_id, c.name): c for t, c in request.source.columns()}
    if request.target is not None:
        columns.update({(t.table_id, c.name): c for t, c in request.target.columns()})
    out: list[Proposal] = []

    def no_proposal(raw: Any, reason: str) -> None:
        tally.invalid += 1
        tally.left_out.append(Recorded("semantics", reason, answer=_written(raw)))

    for raw in items:
        tally.proposed += 1
        if (
            not isinstance(raw, dict)
            or any(k not in raw for k in _REQUIRED)
            or raw["cardinality"] not in ("many-to-one", "one-to-many")
        ):
            no_proposal(raw, INVALID_SHAPE)
            continue
        try:
            source = (int(raw["source_table_id"]), str(raw["source_column"]))
            target = (int(raw["target_table_id"]), str(raw["target_column"]))
            confidence = float(raw["confidence"])
        except (TypeError, ValueError):
            no_proposal(raw, INVALID_SHAPE)
            continue
        a, b = columns.get(source), columns.get(target)
        if a is None or b is None:
            no_proposal(raw, UNKNOWN_COLUMN)
            continue
        proposal = from_answer(
            "semantics",
            source,
            target,
            str(raw["cardinality"]),
            confidence,
            str(raw.get("reasoning", "")),
            str(raw.get("suggested_name", "")),
        )
        if not comparable(a, b, request.group):
            tally.not_comparable += 1
            tally.left_out.append(Recorded("semantics", NOT_COMPARABLE, proposal))
        elif confidence < MIN_CONFIDENCE:
            tally.below_threshold += 1
            tally.left_out.append(Recorded("semantics", BELOW_THRESHOLD, proposal))
        else:
            out.append(proposal)
    return out


#: A second scorer (REQ-1789): for each proposal, in order, the confidence it gives, or None
#: where it gave none. None for all of them: it could not be asked.
Rescore = Callable[[list[Proposal]], Awaitable[list[float | None] | None]]


def rescored(found: list[Proposal], scores: list[float | None] | None, tally: Tally) -> list:
    """``found`` with the second scorer's confidence in place of the model's: a proposal it
    scored under :data:`MIN_CONFIDENCE` is left out, both confidences kept; one it gave no score
    keeps the model's."""
    if scores is None:
        tally.scoring_failed += 1
        return found
    kept: list[Proposal] = []
    for proposal, score in zip(found, scores, strict=True):
        if score is None:
            kept.append(proposal)
            continue
        tally.scored += 1
        if score < MIN_CONFIDENCE:
            tally.scored_below_threshold += 1
            tally.left_out.append(
                Recorded("semantics", SCORED_BELOW_THRESHOLD, proposal, scored=score)
            )
        else:
            kept.append(replace(proposal, confidence=score))
    return kept


async def execute(
    requests: list[Request],
    k: int,
    max_tokens: int,
    ask: Ask,
    control: Control,
    *,
    refine: Rescore | None = None,
    to_thread: Callable[..., Awaitable[Any]] = asyncio.to_thread,
) -> tuple[Tally, list[Proposal]]:
    """Ask every request in order, splitting as it must; stop when the control asks. ``refine``
    re-scores a request's proposals (REQ-1789, when a Jev key is configured): its confidence
    replaces the model's (:func:`rescored`)."""
    queue = list(requests)
    tally = Tally(total=len(queue))
    proposals: list[Proposal] = []
    while queue:
        if await control.stop_asked():
            tally.stopped = True
            break
        request = queue.pop(0)
        try:
            answer = await to_thread(ask, request.prompt(k), max_tokens)
        except Exception as exc:  # one failed request is reported, the rest go on (REQ-413)
            if too_long(exc) and request.splittable:
                halves = request.halves()
                queue[0:0] = halves
                tally.split += 1
                tally.total += len(halves) - 1
                continue
            tally.done += 1
            tally.failed.append({"group": request.group, **failure(exc)})
            await control.returned(tally, proposals)
            continue
        read = read_answer_list(answer.text, answer.cut_off)
        tally.done += 1
        if read.items is None:
            tally.unreadable += 1
            tally.failed.append({"group": request.group, **UNREADABLE})
            await control.returned(tally, proposals)
            continue
        found = read_items(request, read.items, tally)
        if refine is not None and found:
            found = rescored(found, await refine(found), tally)
        proposals.extend(found)
        full = len(read.items) >= k
        if (answer.cut_off or full) and request.splittable:
            halves = request.halves()
            queue[0:0] = halves
            tally.split += 1
            # The split request counted as done; its halves are new requests.
            tally.total += len(halves)
        elif answer.cut_off:
            tally.cut_short += 1
        await control.returned(tally, proposals)
    return tally, proposals
