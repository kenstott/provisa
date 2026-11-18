# Copyright (c) 2026 Kenneth Stott
# Canary: 3e8a1c57-6b24-4d90-a7f3-9c2e5b81d406
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every pair a discovery run proposes, measured on its data (REQ-413).

For each pair a run measures (the semantic pairs in the band between the measure cutoff and the
run's threshold, with Key value overlap chosen: :func:`provisa.discovery.runner.judged`), one query
counts the overlap in the engine (:func:`join_count_sql`), through the one governed pipeline as the
person who started the run (:func:`governed_reader`). Each column's non-null values are read up to
the run's sample size (set in the discover dialog; :data:`DISTINCT_BOUND` prefills it), as a
limit the engine stops at, and joined to the other column's non-null values: how many of the source's distinct
values read find a match in the target, and the reverse. Whether a column is distinct (a key) is
judged on the rows read: no value read repeats. No distinct count over a whole column is taken and
no column's values leave the engine. A column holding more non-null values than the sample size is
counted on the rows read, and the measure says so (``more``). A null never counts toward the
overlap, toward a column being a key, or toward the sample size.

The measure decides the pair's cardinality (:meth:`Measure.cardinality`): many-to-one when the
target is distinct, one-to-one when both are, many-to-many when neither is. A semantic pair whose
source alone is distinct is turned round, so it starts at the referring column; a foreign key is
declared and keeps its direction. Its score is given by the measure
(:func:`provisa.discovery.context.measured_score`); the model's own confidence is kept beside it.

A table that requires a parameter is never read (REQ-1986): the pair is measured on what its other
column gives, and says which side was not read. A column the person cannot read is not measured
either, and the pair says so. Such a pair's score is the model's: no values were compared.
"""

# Requirements: REQ-413, REQ-1986

from __future__ import annotations

from collections.abc import Collection, Sequence
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any, Literal, Protocol

from provisa.discovery.columns import TableFacts
from provisa.discovery.context import Scoring, measured_score

if TYPE_CHECKING:
    from provisa.discovery.direction import ColumnRef, Edge, Proposal

#: The sample size's default (REQ-413, amended 2026-11-18): it only prefills the discover dialog,
#: where a run's own is set: the most non-null values read of one column for a pair's measure.
DISTINCT_BOUND = 10_000

#: Why a column was not measured: its table requires a parameter and is never read
#: (REQ-1986); or the person who started the run cannot read it.
NOT_READ = "not_read"
NOT_READABLE = "not_readable"
Unmeasured = Literal["not_read", "not_readable"]

Cardinality = Literal["many-to-one", "one-to-one", "many-to-many", "one-to-many"]


@dataclass(frozen=True)
class Values:
    """A column's distinct non-null values as read: at most the run's sample size, most repeated
    first. ``more``: the column holds more distinct values than were read. ``distinct``: no value
    occurs twice in the whole column."""

    read: tuple[Any, ...]
    more: bool
    distinct: bool

    @property
    def keys(self) -> frozenset[Any]:
        return frozenset(comparable_value(v) for v in self.read)


def comparable_value(value: Any) -> Any:
    """``value`` as two columns' values are compared: a uuid as its text, an integral decimal as
    the integer it is, so the same value read from two sources compares equal."""
    from decimal import Decimal
    from uuid import UUID

    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Decimal) and value == value.to_integral_value():
        return int(value)
    return value


@dataclass(frozen=True)
class Column:
    """A column as the run's role reads it: its quoted relation and its quoted column name."""

    relation: str
    name: str


class Reader(Protocol):
    """How a run reaches columns for the measure."""

    def column(self, table: TableFacts, column: str) -> Column | Unmeasured:
        """The column as the run's role reads it, or why it is not measured."""
        ...

    async def rows(self, tables: Sequence[TableFacts], sql: str) -> list[tuple]:
        """The rows of one read of ``tables`` through the governed pipeline as the run's role."""
        ...


def _sample(column: Column, bound: int) -> str:
    """Up to ``bound`` of the column's non-null values, as rows read: a limit the engine stops at."""
    return f"SELECT {column.name} AS v FROM {column.relation} WHERE {column.name} IS NOT NULL LIMIT {bound}"


def _counted(alias: str, column: Column, bound: int) -> list[str]:
    """The rows read of ``alias`` (a column's sample), its distinct values among them, and the
    rows found up to one past ``bound`` (more than ``bound``: the column holds more)."""
    return [
        f"(SELECT COUNT(*) FROM {alias})",
        f"(SELECT COUNT(DISTINCT v) FROM {alias})",
        f"(SELECT COUNT(*) FROM ({_sample(column, bound + 1)}) AS {alias}_more)",
    ]


def _matched(alias: str, other: Column) -> str:
    """How many distinct values of ``alias`` (a column's sample) find a match in ``other``."""
    return (
        f"(SELECT COUNT(DISTINCT {alias}.v) FROM {alias} "
        f"JOIN {other.relation} AS {alias}_in ON {alias}.v = {alias}_in.{other.name})"
    )


def join_count_sql(source: Column | None, target: Column | None, bound: int) -> str:
    """The one query that measures a pair in the engine. Per side read (None: not measured), in
    order source then target: its rows read, its distinct values read, its rows up to
    ``bound + 1``; then, both read, the source's distinct values read found in the target and the
    target's found in the source."""
    sides = [(a, c) for a, c in (("s", source), ("t", target)) if c is not None]
    assert sides, "a pair with neither column read is not queried"
    ctes = ", ".join(f"{a} AS ({_sample(c, bound)})" for a, c in sides)
    out = [item for a, c in sides for item in _counted(a, c, bound)]
    if source is not None and target is not None:
        out += [_matched("s", target), _matched("t", source)]
    return f"WITH {ctes} SELECT {', '.join(out)}"


def governed_reader(state: Any, role: str) -> Reader:
    """The reader of a run's measure: each pair's query through the governed pipeline as ``role``,
    the role of the person who started the run. A table that requires a parameter is never read
    (REQ-1986); a table or column ``role`` cannot read is not measured. Either is said on the
    pair."""
    from provisa.discovery import gather

    class _Reader:
        def column(self, table: TableFacts, column: str) -> Column | Unmeasured:
            if table.reduced:
                return NOT_READ
            published = gather._published_as(state, role, table.table_id)
            if published is None or column not in published[1]:
                return NOT_READABLE
            relation, exposed = published
            return Column(relation, gather._quote(exposed[column]))

        async def rows(self, tables: Sequence[TableFacts], sql: str) -> list[tuple]:
            names = ", ".join(dict.fromkeys(t.table_name for t in tables))
            return await gather._reading(names, gather._governed_as(sql, role))

    return _Reader()


@dataclass(frozen=True)
class Side:
    """One column of a measured pair: how many distinct values were read (``more``: it holds
    more), whether its values are distinct, and why it was not measured (None: it was)."""

    values: int | None
    more: bool
    distinct: bool | None
    unmeasured: Unmeasured | None

    def as_dict(self) -> dict[str, Any]:
        return {
            "values": self.values,
            "more": self.more,
            "distinct": self.distinct,
            "unmeasured": self.unmeasured,
        }


def _unmeasured(why: Unmeasured) -> Side:
    return Side(None, False, None, why)


def _read(counts: Sequence[Any], bound: int) -> Side:
    """A side read, from its rows read, distinct values read and rows up to ``bound + 1``."""
    rows, distinct, upto = (int(c) for c in counts)
    return Side(distinct, upto > bound, rows == distinct, None)


@dataclass(frozen=True)
class Measure:
    """A proposed pair measured on its data. ``source_in_target``: how many of the source's
    distinct values read occur in the target; ``target_in_source`` the reverse. Both None when a
    side was not measured."""

    source: Side
    target: Side
    source_in_target: int | None
    target_in_source: int | None
    bound: int
    #: The run's measured-score settings (REQ-413, amended 2026-11-18).
    scoring: Scoring

    @property
    def compared(self) -> bool:
        """Whether values of both columns were compared: both read, the source holding some."""
        return self.source_in_target is not None and bool(self.source.values)

    @property
    def contained(self) -> float | None:
        """The share of the source's distinct values read that occur in the target."""
        if not self.compared:
            return None
        assert self.source.values is not None and self.source_in_target is not None
        return self.source_in_target / self.source.values

    @property
    def back(self) -> float | None:
        """The share of the target's distinct values read that occur in the source."""
        if self.target_in_source is None or not self.target.values:
            return None
        return self.target_in_source / self.target.values

    def cardinality(self) -> Cardinality | None:
        """Many-to-one when the target is distinct, one-to-one when both are, many-to-many when
        neither is, one-to-many when the source alone is. None: the target was not measured, or
        it repeats and the source was not measured, so the data does not say."""
        target, source = self.target.distinct, self.source.distinct
        if target is None:
            return None
        if target:
            return "one-to-one" if source else "many-to-one"
        if source is None:
            return None
        return "one-to-many" if source else "many-to-many"

    def score(self) -> float | None:
        """The pair's score by the measure (:func:`provisa.discovery.context.measured_score`);
        None when no values were compared and the measure gives none."""
        contained = self.contained
        if contained is None:
            return None
        assert self.target.distinct is not None
        return measured_score(contained, self.back or 0.0, self.target.distinct, self.scoring)

    def turned(self) -> Measure:
        """The measure of the same pair read the other way round."""
        return Measure(
            self.target,
            self.source,
            self.target_in_source,
            self.source_in_target,
            self.bound,
            self.scoring,
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "bound": self.bound,
            "source": self.source.as_dict(),
            "target": self.target.as_dict(),
            "source_in_target": self.source_in_target,
            "target_in_source": self.target_in_source,
        }


class Measurer:
    """Measures a run's proposed pairs, one engine query per pair. ``decided`` holds
    the pairs already registered (a junction's legs among them) or decided by a reviewer, in the
    direction stored: REQ-413 (amended 2026-11-16) never proposes one again, so none is measured
    as a new pair and each is left out for what it already is."""

    def __init__(
        self,
        reader: Reader,
        tables: dict[int, TableFacts],
        *,
        decided: Collection[Edge],
        bound: int,
        scoring: Scoring,
    ) -> None:
        self._reader = reader
        #: The run's sample size: the most non-null values read of each column.
        self._bound = bound
        #: The run's measured-score settings.
        self._scoring = scoring
        self._decided = decided
        self._tables = tables
        self._pairs: dict[tuple[ColumnRef, ColumnRef], Measure] = {}

    async def _count(self, source: ColumnRef, target: ColumnRef) -> Measure:
        """The pair measured by one query in the engine (:func:`join_count_sql`)."""
        tables = (self._tables[source[0]], self._tables[target[0]])
        s = self._reader.column(tables[0], source[1])
        t = self._reader.column(tables[1], target[1])
        if isinstance(s, str) and isinstance(t, str):
            return Measure(_unmeasured(s), _unmeasured(t), None, None, self._bound, self._scoring)
        sql = join_count_sql(
            None if isinstance(s, str) else s, None if isinstance(t, str) else t, self._bound
        )
        read = [tables[i] for i, c in enumerate((s, t)) if not isinstance(c, str)]
        row = (await self._reader.rows(read, sql))[0]
        if isinstance(s, str):
            read = _read(row[0:3], self._bound)
            return Measure(_unmeasured(s), read, None, None, self._bound, self._scoring)
        if isinstance(t, str):
            read = _read(row[0:3], self._bound)
            return Measure(read, _unmeasured(t), None, None, self._bound, self._scoring)
        return Measure(
            _read(row[0:3], self._bound),
            _read(row[3:6], self._bound),
            int(row[6]),
            int(row[7]),
            self._bound,
            self._scoring,
        )

    async def pair(self, source: ColumnRef, target: ColumnRef) -> Measure:
        """The measure of ``source`` -> ``target``."""
        if (target, source) in self._pairs:
            return self._pairs[(target, source)].turned()
        if (source, target) not in self._pairs:
            self._pairs[(source, target)] = await self._count(source, target)
        return self._pairs[(source, target)]

    async def measured(self, proposal: Proposal) -> Proposal:
        """``proposal`` with its measure and the score it gives. A semantic pair whose source
        alone is distinct is turned round to start at the referring column. A pair already
        registered or decided is returned as proposed, unmeasured."""
        from provisa.discovery.direction import turned

        start, end = proposal.start, proposal.end
        if proposal.edge in self._decided or (*end, *start) in self._decided:
            return proposal
        measure = await self.pair(proposal.start, proposal.end)
        measured = replace(proposal, measure=measure, score=measure.score())
        if proposal.kind == "semantics" and measure.cardinality() == "one-to-many":
            return turned(measured)
        return measured

    async def all(self, proposals: Sequence[Proposal]) -> list[Proposal]:
        return [await self.measured(p) for p in proposals]
