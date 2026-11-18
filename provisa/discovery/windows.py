# Copyright (c) 2026 Kenneth Stott
# Canary: 6e8b2d14-0f7c-4a93-b5d1-c9a47e3f2b60
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The calls a semantic discovery run makes (REQ-413, amended 2026-11-18).

Code lists every table pair of the run's scope (:func:`table_pairs`): each source table with each
target table that shares a compared type group with it (dates and timestamps are in none); with
the target the same as the source, each two tables once, a table with itself included. The model
never chooses which tables to compare. The pairs are batched, in order, into calls sized from the
chosen model's input limit at :data:`provisa.llm.limits.CHARS_PER_TOKEN` characters per token
(REQ-464), and at most as many pairs to a call as the model's output limit answers at
:data:`TOKENS_PER_PAIR` tokens each. A call lists its tables once, then its pairs numbered from 1.

:func:`plan` is the run's work list. :func:`bound` is the count shown before Run: the most calls
:func:`plan` can give for the same tables, computed from their registration alone. A pair left
unanswered is asked once more in calls of their own (:meth:`Plan.again`), which the count does
not include.
"""

# Requirements: REQ-413, REQ-464

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, replace

from provisa.discovery import context
from provisa.discovery.columns import ColumnFacts, TableFacts, compared
from provisa.llm.limits import CHARS_PER_TOKEN

#: REQ-413: a call lists at most as many table pairs as the output limit answers at this many
#: tokens per pair.
TOKENS_PER_PAIR = 150


class NoRoomError(ValueError):
    """The model's limits hold no call: its output limit answers no pair, or its input limit
    holds nothing beside the instructions."""


ColumnPair = tuple[ColumnFacts, ColumnFacts]


@dataclass(frozen=True)
class TablePair:
    """One table pair of a run. ``either_direction``: source and target are one set of tables,
    so a relationship may start in either."""

    source: TableFacts
    target: TableFacts
    either_direction: bool

    def column_pairs(self) -> list[ColumnPair]:
        """Every two different columns of the pair compared in some type group, source first:
        unordered for a table with itself in one set, ordered where source and target differ."""
        out: list[ColumnPair] = []
        same = self.source.table_id == self.target.table_id
        for i, a in enumerate(self.source.columns):
            for j, b in enumerate(self.target.columns):
                if same and (j == i or (self.either_direction and j < i)):
                    continue
                if compared(a, b):
                    out.append((a, b))
        return out

    @property
    def key(self) -> tuple[int, int]:
        return (self.source.table_id, self.target.table_id)


@dataclass(frozen=True)
class Call:
    """One model call: its table pairs, numbered from 1 in order. ``labels``: each table's name
    in the run's calls, by id."""

    pairs: tuple[TablePair, ...]
    labels: dict[int, str]

    def tables(self) -> list[TableFacts]:
        """The call's tables, each once, in order of first appearance."""
        seen: dict[int, TableFacts] = {}
        for p in self.pairs:
            seen.setdefault(p.source.table_id, p.source)
            seen.setdefault(p.target.table_id, p.target)
        return list(seen.values())

    def named(self, pair: TablePair) -> str:
        """A pair as a run names it to people: ``source × target``."""
        return f"{self.labels[pair.source.table_id]} × {self.labels[pair.target.table_id]}"

    def prompt(self, threshold: float) -> str:
        """The call as sent, stating the run's ``threshold`` (REQ-413)."""
        return context.prompt(
            [context.table_context(t, self.labels[t.table_id]) for t in self.tables()],
            [
                context.pair_entry(
                    n, self.labels[p.source.table_id], self.labels[p.target.table_id]
                )
                for n, p in enumerate(self.pairs, start=1)
            ],
            threshold=threshold,
            either_direction=self.pairs[0].either_direction,
        )

    @property
    def splittable(self) -> bool:
        return len(self.pairs) > 1

    def halves(self) -> list["Call"]:
        """The call's pairs split in two, in order (a call the vendor refuses as too long)."""
        cut = len(self.pairs) // 2
        return [Call(self.pairs[:cut], self.labels), Call(self.pairs[cut:], self.labels)]


def per_call(output_tokens: int) -> int:
    """How many table pairs one call lists: what the output limit answers at
    :data:`TOKENS_PER_PAIR` tokens each."""
    k = output_tokens // TOKENS_PER_PAIR
    if k < 1:
        raise NoRoomError(
            f"the model's output limit of {output_tokens} tokens answers no table pair "
            f"({TOKENS_PER_PAIR} tokens each)"
        )
    return k


def labels(tables: Sequence[TableFacts]) -> dict[int, str]:
    """Each table's name in the run's calls, by id: its name as people see it
    (:attr:`TableFacts.shown`), with its domain before it where two of the run's tables share
    that name, and its id after where they share both."""
    by_id = {t.table_id: t for t in tables}
    shown: dict[str, list[int]] = {}
    for t in by_id.values():
        shown.setdefault(t.shown, []).append(t.table_id)
    out: dict[int, str] = {}
    for name, ids in shown.items():
        if len(ids) == 1:
            out[ids[0]] = name
            continue
        qualified = {i: f"{by_id[i].domain_id}.{name}" for i in ids}
        counts: dict[str, int] = {}
        for q in qualified.values():
            counts[q] = counts.get(q, 0) + 1
        for i, q in qualified.items():
            out[i] = q if counts[q] == 1 else f"{q}#{i}"
    return out


def _ordered(tables: Sequence[TableFacts]) -> list[TableFacts]:
    return sorted(tables, key=lambda t: (t.domain_id, t.table_name, t.table_id))


def table_pairs(
    source: Sequence[TableFacts], target: Sequence[TableFacts] | None
) -> list[TablePair]:
    """Every table pair of the scope, in order: each source table with each target table that
    has two different columns compared with each other. ``target`` None: the target is the same
    as the source, and each two tables are one pair, a table with itself included."""
    src = _ordered(source)
    out: list[TablePair] = []
    if target is None:
        for i, a in enumerate(src):
            for b in src[i:]:
                pair = TablePair(a, b, True)
                if pair.column_pairs():
                    out.append(pair)
        return out
    for a in src:
        for b in _ordered(target):
            pair = TablePair(a, b, False)
            if pair.column_pairs():
                out.append(pair)
    return out


@dataclass(frozen=True)
class Plan:
    """A run's calls, and how pairs are batched into calls (``room`` characters of tables and
    pairs, at most ``k`` pairs to a call)."""

    k: int
    room: int
    labels: dict[int, str]
    calls: list[Call]
    table_pairs: int

    def batch(self, pairs: Sequence[TablePair]) -> list[Call]:
        return batch(pairs, self.labels, self.room, self.k)

    def again(self, pairs: Sequence[TablePair]) -> list[Call]:
        """The calls asking ``pairs`` once more: each left unanswered."""
        return self.batch(pairs)


def _room(input_tokens: int, k: int, *, either_direction: bool, not_read: bool) -> int:
    """The characters a call's tables and pairs may hold: the input limit in characters less the
    instructions (weighed at the widest threshold they can state) and headers."""
    fixed = (
        context.instructions_chars(k, either_direction=either_direction, not_read=not_read)
        + context.HEADERS
    )
    room = input_tokens * CHARS_PER_TOKEN - fixed
    if room < 1:
        raise NoRoomError(
            f"the model's input limit of {input_tokens} tokens holds nothing beside the "
            "instructions"
        )
    return room


def batch(pairs: Sequence[TablePair], names: dict[int, str], room: int, k: int) -> list[Call]:
    """``pairs`` batched in order into calls of at most ``k`` pairs whose tables (each once) and
    numbered pairs take at most ``room`` characters. A pair heavier than ``room`` is a call
    alone."""
    table_w: dict[int, int] = {}

    def weight_of(t: TableFacts) -> int:
        if t.table_id not in table_w:
            table_w[t.table_id] = context.table_weight(t, names[t.table_id])
        return table_w[t.table_id]

    calls: list[Call] = []
    current: list[TablePair] = []
    tables: dict[int, int] = {}
    pair_ws: list[int] = []
    for p in pairs:
        added = {t.table_id: weight_of(t) for t in (p.source, p.target) if t.table_id not in tables}
        pw = context.pair_weight(k, names[p.source.table_id], names[p.target.table_id])
        trial = context.list_weight([*tables.values(), *added.values()]) + context.list_weight(
            [*pair_ws, pw]
        )
        if current and (len(current) >= k or trial > room):
            calls.append(Call(tuple(current), names))
            current, tables, pair_ws = [], {}, []
            added = {t.table_id: weight_of(t) for t in (p.source, p.target)}
        current.append(p)
        tables.update(added)
        pair_ws.append(pw)
    if current:
        calls.append(Call(tuple(current), names))
    return calls


def plan(
    source: Sequence[TableFacts],
    target: Sequence[TableFacts] | None,
    input_tokens: int,
    output_tokens: int,
) -> Plan:
    """The run's calls in order. ``target`` None: the target is the same as the source."""
    k = per_call(output_tokens)
    either = target is None
    every = [*source, *(target or [])]
    names = labels(every)
    room = _room(input_tokens, k, either_direction=either, not_read=any(t.reduced for t in every))
    pairs = table_pairs(source, target)
    return Plan(k, room, names, batch(pairs, names, room, k), len(pairs))


def _widest(tables: Sequence[TableFacts]) -> list[TableFacts]:
    return [replace(t, columns=tuple(context.widest(c) for c in t.columns)) for t in tables]


def bound(
    source: Sequence[TableFacts],
    target: Sequence[TableFacts] | None,
    input_tokens: int,
    output_tokens: int,
) -> tuple[int, int, int]:
    """``(k, calls, table pairs)``: the pairs each call lists at most, and upper bounds on the
    calls and the table pairs :func:`plan` gives for these tables whatever their values and
    classes turn out to be, from their registration alone (REQ-413: the count before Run reads
    no table).

    The tables are registered ones: every column of a read table has the class unknown and meets
    every other of its type group, so the pairs counted are every pair the run's classes can give.
    Each column is weighed at the most it can send (:func:`provisa.discovery.context.widest`), so
    each of the run's pairs weighs no more. Batching
    in order takes as many pairs into each call as fit, and a run's call holding fewer or lighter
    pairs still fits, so the run makes no more calls than counted."""
    k = per_call(output_tokens)
    either = target is None
    every = [*source, *(target or [])]
    names = labels(every)
    room = _room(input_tokens, k, either_direction=either, not_read=any(t.reduced for t in every))
    heaviest = {t.table_id: t for t in _widest(every)}
    pairs = [
        replace(p, source=heaviest[p.source.table_id], target=heaviest[p.target.table_id])
        for p in table_pairs(source, target)
    ]
    return k, len(batch(pairs, names, room, k)), len(pairs)
