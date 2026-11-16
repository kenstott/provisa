# Copyright (c) 2026 Kenneth Stott
# Canary: 0b7d4e92-1a6c-4f38-8e25-d3f6a9c1b748
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Which way a suggested relationship runs, and which suggestions are removed before anything is
shown (REQ-413).

A relationship starts at the referring column and ends at the one side, many-to-one. When both
directions are suggested one is kept, by the first rule that decides (:data:`RULES` in order): a
foreign key constraint; a primary key is the one side; roles (it starts at the fact-like table and
ends at the dimension-like table, a recorded role used where there is one, else inferred per pair);
the model's statement of many-to-one; the higher confidence; only then a bounded sample of each
column read for repeated values, the side with none being the one side, reported as unique in a
sample of N rows; last, the table whose domain and name sort first.

Removed, each kind counted (:class:`Removed`): a column to that same column (two different columns
of one table are kept); primary key to primary key; both sides repeating in that sample; a match
for an existing relationship in either direction; one pending or rejected; a semantic suggestion
matching a foreign key of the same run, the foreign key being kept.
"""

# Requirements: REQ-413

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field, replace
from typing import Literal

from provisa.discovery.columns import TableFacts

Kind = Literal["foreign_keys", "semantics"]
ColumnRef = tuple[int, str]  # (table id, column name)
Edge = tuple[int, str, int, str]  # (start table, start column, end table, end column)

#: The rules that choose a direction, in the order they are tried.
RULES: tuple[str, ...] = (
    "foreign_key",
    "primary_key",
    "roles",
    "model_statement",
    "confidence",
    "uniqueness_sample",
    "fixed_order",
)


@dataclass(frozen=True)
class Proposal:
    """A relationship as proposed: ``start`` the referring column as the proposal states it
    (a one-to-many answer is read the other way round), ``written`` the direction it was written
    in."""

    kind: Kind
    start: ColumnRef
    end: ColumnRef
    written: tuple[ColumnRef, ColumnRef]
    confidence: float
    reasoning: str
    suggested_name: str = ""

    @property
    def edge(self) -> Edge:
        return (self.start[0], self.start[1], self.end[0], self.end[1])

    @property
    def pair(self) -> frozenset[ColumnRef]:
        return frozenset({self.start, self.end})


def from_answer(
    kind: Kind,
    source: ColumnRef,
    target: ColumnRef,
    cardinality: str,
    confidence: float,
    reasoning: str,
    suggested_name: str = "",
) -> Proposal:
    """A proposal from an element of a model's answer (or a foreign key: many-to-one)."""
    start, end = (source, target) if cardinality == "many-to-one" else (target, source)
    return Proposal(kind, start, end, (source, target), confidence, reasoning, suggested_name)


@dataclass
class Removed:
    """How many suggestions each removal took out."""

    same_column: int = 0
    primary_key_to_primary_key: int = 0
    both_repeating: int = 0
    existing: int = 0
    pending: int = 0
    rejected: int = 0
    matches_foreign_key: int = 0
    out_of_scope: int = 0

    def as_dict(self) -> dict[str, int]:
        return dict(self.__dict__)


@dataclass(frozen=True)
class Sampled:
    """A column's bounded sample: how many rows were read and whether a value repeated."""

    rows: int
    repeats: bool


SampleReader = Callable[[TableFacts, str], Awaitable[Sampled]]


@dataclass
class Known:
    """What a run's suggestions are checked against."""

    existing: set[Edge] = field(default_factory=set)
    pending: set[Edge] = field(default_factory=set)
    rejected: set[Edge] = field(default_factory=set)
    #: Keys of the tables of the run, and relationships already ending at each table: what a
    #: role is inferred from.
    relationships_ending: set[int] = field(default_factory=set)


def _is_pk(tables: dict[int, TableFacts], ref: ColumnRef) -> bool:
    col = tables[ref[0]].column(ref[1])
    return col is not None and col.is_primary_key


def inferred_role(table: TableFacts, others: Sequence[TableFacts], known: Known) -> str | None:
    """A table's role inferred per pair (REQ-413): fact-like holds two or more columns matching
    other tables' keys; dimension-like has a single-column key whose name recurs elsewhere or
    relationships already ending there. Neither, or both: None."""
    elsewhere = [t for t in others if t.table_id != table.table_id]
    other_keys = {k for t in elsewhere for k in t.primary_key}
    fact = sum(1 for c in table.columns if c.name in other_keys) >= 2
    key = table.primary_key
    dimension = len(key) == 1 and (
        any(t.column(key[0]) is not None for t in elsewhere)
        or table.table_id in known.relationships_ending
    )
    if fact == dimension:
        return None
    return "fact" if fact else "dimension"


def role_of(table: TableFacts, others: Sequence[TableFacts], known: Known) -> str | None:
    """The recorded role where there is one, else the inferred one."""
    if table.role in ("fact", "dimension"):
        return table.role
    return inferred_role(table, others, known)


@dataclass(frozen=True)
class Decision:
    start: ColumnRef
    rule: str
    evidence: str = ""


async def decide(
    proposals: Sequence[Proposal],
    tables: dict[int, TableFacts],
    known: Known,
    read_sample: SampleReader,
) -> Decision | None:
    """The direction of one pair of columns proposed both ways, by the first rule that decides
    (:data:`RULES`). None: both columns repeat in their samples, and the pair is removed."""
    a, b = sorted(proposals[0].pair)
    starts = {p.start for p in proposals}

    fk_starts = {p.start for p in proposals if p.kind == "foreign_keys"}
    if len(fk_starts) == 1:
        return Decision(next(iter(fk_starts)), "foreign_key")

    a_pk, b_pk = _is_pk(tables, a), _is_pk(tables, b)
    if a_pk != b_pk:
        return Decision(b if a_pk else a, "primary_key")

    if a[0] != b[0]:
        universe = list(tables.values())
        ra = role_of(tables[a[0]], universe, known)
        rb = role_of(tables[b[0]], universe, known)
        if {ra, rb} == {"fact", "dimension"}:
            return Decision(a if ra == "fact" else b, "roles")

    if len(starts) == 1:
        return Decision(next(iter(starts)), "model_statement")

    best: dict[ColumnRef, float] = {}
    for p in proposals:
        best[p.start] = max(best.get(p.start, 0.0), p.confidence)
    if best.get(a, 0.0) != best.get(b, 0.0):
        return Decision(a if best.get(a, 0.0) > best.get(b, 0.0) else b, "confidence")

    sa = await read_sample(tables[a[0]], a[1])
    sb = await read_sample(tables[b[0]], b[1])
    if sa.repeats and sb.repeats:
        return None
    if sa.repeats != sb.repeats:
        one, sample = (b, sb) if sa.repeats else (a, sa)
        start = a if one == b else b
        evidence = f"{one[1]} unique in a sample of {sample.rows} rows"
        return Decision(start, "uniqueness_sample", evidence)

    ta, tb = tables[a[0]], tables[b[0]]
    first = a if (ta.domain_id, ta.table_name, a[1]) <= (tb.domain_id, tb.table_name, b[1]) else b
    return Decision(first, "fixed_order")


def _kept(proposals: Sequence[Proposal], decision: Decision) -> Proposal:
    """The proposal kept for a decided pair: the most confident one in the decided direction, or
    the most confident one turned that way."""
    ranked = sorted(proposals, key=lambda p: (p.kind != "foreign_keys", -p.confidence))
    same = [p for p in ranked if p.start == decision.start]
    chosen = same[0] if same else replace(ranked[0], start=ranked[0].end, end=ranked[0].start)
    if decision.evidence:
        reasoning = (
            f"{chosen.reasoning}; {decision.evidence}" if chosen.reasoning else (decision.evidence)
        )
        chosen = replace(chosen, reasoning=reasoning)
    return chosen


def _needs_decision(proposals: Sequence[Proposal]) -> bool:
    """Both directions suggested: written both ways, or read both ways."""
    return len({p.written for p in proposals}) > 1 or len({p.start for p in proposals}) > 1


@dataclass
class Outcome:
    kept: list[Proposal]
    removed: Removed
    #: How many pairs each rule decided.
    decided_by: dict[str, int]


async def settle(
    proposals: Sequence[Proposal],
    tables: dict[int, TableFacts],
    source_ids: set[int],
    target_ids: set[int],
    known: Known,
    read_sample: SampleReader,
) -> Outcome:
    """The suggestions to show, from every proposal of a run: removals applied, one direction
    kept per pair of columns, only relationships starting in the source set and ending in the
    target set."""
    removed = Removed()
    decided_by: dict[str, int] = {}
    fk_pairs = {p.pair for p in proposals if p.kind == "foreign_keys"}
    pending: list[Proposal] = []
    for p in proposals:
        if p.start == p.end:
            removed.same_column += 1
        elif _is_pk(tables, p.start) and _is_pk(tables, p.end):
            removed.primary_key_to_primary_key += 1
        elif p.kind == "semantics" and p.pair in fk_pairs:
            removed.matches_foreign_key += 1
        else:
            pending.append(p)

    by_pair: dict[frozenset[ColumnRef], list[Proposal]] = {}
    for p in pending:
        by_pair.setdefault(p.pair, []).append(p)

    kept: list[Proposal] = []
    for group in by_pair.values():
        if not _needs_decision(group):
            kept.append(max(group, key=lambda p: p.confidence))
            continue
        decision = await decide(group, tables, known, read_sample)
        if decision is None:
            removed.both_repeating += 1
            continue
        decided_by[decision.rule] = decided_by.get(decision.rule, 0) + 1
        kept.append(_kept(group, decision))

    shown: list[Proposal] = []
    for p in kept:
        reverse = (p.end[0], p.end[1], p.start[0], p.start[1])
        if p.start[0] not in source_ids or p.end[0] not in target_ids:
            removed.out_of_scope += 1
        elif p.edge in known.existing or reverse in known.existing:
            removed.existing += 1
        elif p.edge in known.pending:
            removed.pending += 1
        elif p.edge in known.rejected:
            removed.rejected += 1
        else:
            shown.append(p)
    return Outcome(shown, removed, decided_by)
