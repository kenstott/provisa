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

For each proposed pair, foreign key or semantic, each column's distinct values are read, at most
:data:`DISTINCT_BOUND` of them, through the one governed pipeline as the person who started the
run (:func:`provisa.discovery.gather.values_reader`). The measure is how many of the source
column's distinct values occur in the target column and the reverse, and whether each column's
values are distinct (a key). Whether a column is distinct is exact whatever the bound: its values
are read most repeated first, so the first one read says whether any repeats. A column holding
more distinct values than the bound is counted on the first :data:`DISTINCT_BOUND` read, and the
measure says so; where the other column holds more than the bound, the values read are looked up
in it rather than compared with its first values.

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
from provisa.discovery.context import measured_score

if TYPE_CHECKING:
    from provisa.discovery.direction import ColumnRef, Edge, Proposal

#: The most distinct values read of one column for a pair's measure.
DISTINCT_BOUND = 10_000

#: Why a column was not measured: its table requires a parameter and is never read
#: (REQ-1986); or the person who started the run cannot read it.
NOT_READ = "not_read"
NOT_READABLE = "not_readable"
Unmeasured = Literal["not_read", "not_readable"]

Cardinality = Literal["many-to-one", "one-to-one", "many-to-many", "one-to-many"]


@dataclass(frozen=True)
class Values:
    """A column's distinct non-null values as read: at most :data:`DISTINCT_BOUND`, most repeated
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


class Reader(Protocol):
    """How a run reads columns' values for the measure."""

    async def values(self, table: TableFacts, column: str) -> Values | Unmeasured:
        """The column's distinct values, or why it is not measured."""
        ...

    async def found(self, table: TableFacts, column: str, among: Values) -> frozenset[Any]:
        """Those of ``among``'s values that occur in the column, compared as
        :func:`comparable_value` gives them."""
        ...


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


def _side(read: Values | Unmeasured) -> Side:
    if isinstance(read, str):
        return Side(None, False, None, read)
    return Side(len(read.read), read.more, read.distinct, None)


@dataclass(frozen=True)
class Measure:
    """A proposed pair measured on its data. ``source_in_target``: how many of the source's
    distinct values read occur in the target; ``target_in_source`` the reverse. Both None when a
    side was not measured."""

    source: Side
    target: Side
    source_in_target: int | None
    target_in_source: int | None
    bound: int = DISTINCT_BOUND

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
        return measured_score(contained, self.back or 0.0, self.target.distinct)

    def turned(self) -> Measure:
        """The measure of the same pair read the other way round."""
        return Measure(
            self.target, self.source, self.target_in_source, self.source_in_target, self.bound
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
    """Measures a run's proposed pairs, reading each column once for the run. ``decided`` holds
    the pairs already registered (a junction's legs among them) or decided by a reviewer, in the
    direction stored: REQ-413 (amended 2026-11-16) never proposes one again, so none is measured
    as a new pair and each is left out for what it already is."""

    def __init__(
        self, reader: Reader, tables: dict[int, TableFacts], *, decided: Collection[Edge]
    ) -> None:
        self._reader = reader
        self._decided = decided
        self._tables = tables
        self._values: dict[ColumnRef, Values | Unmeasured] = {}
        self._pairs: dict[tuple[ColumnRef, ColumnRef], Measure] = {}

    async def _read(self, ref: ColumnRef) -> Values | Unmeasured:
        if ref not in self._values:
            self._values[ref] = await self._reader.values(self._tables[ref[0]], ref[1])
        return self._values[ref]

    async def _found(self, of: Values, in_ref: ColumnRef, there: Values) -> int:
        """How many of ``of``'s values occur in the column ``in_ref``, whose values read are
        ``there``: compared with them when they are all of its values, else looked up in it."""
        if not there.more:
            return len(of.keys & there.keys)
        return len(await self._reader.found(self._tables[in_ref[0]], in_ref[1], of))

    async def pair(self, source: ColumnRef, target: ColumnRef) -> Measure:
        """The measure of ``source`` -> ``target``."""
        if (target, source) in self._pairs:
            return self._pairs[(target, source)].turned()
        if (source, target) not in self._pairs:
            s, t = await self._read(source), await self._read(target)
            if isinstance(s, str) or isinstance(t, str):
                measure = Measure(_side(s), _side(t), None, None)
            else:
                measure = Measure(
                    _side(s),
                    _side(t),
                    await self._found(s, target, t),
                    await self._found(t, source, s),
                )
            self._pairs[(source, target)] = measure
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
