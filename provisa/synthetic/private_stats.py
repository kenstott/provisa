# Copyright (c) 2026 Kenneth Stott
# Canary: 593fb9ac-147e-4e8f-8875-f0cb158b2e7c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The statistics of a private synthetic dataset, measured under ε (REQ-1939, DIFFERENTIAL
PRIVACY; maintainer rulings X1 and the P9a audit).

A private dataset reads nothing from a profile run's recorded values: those are computed without
noise, and anything derived from them would carry what they reveal. Every statistic generation
draws from is measured here from the table itself, as the organisation's administrator through the
governed pipeline, by a mechanism whose sensitivity to one row is bounded, and noised with Laplace
noise of scale sensitivity/ε_statistic (:mod:`provisa.synthetic.privacy` holds the budget):

* a count -- the table's rows, a column's NULLs, its distinct values -- one row changing it by 1;
* a distribution (a number, a date, a difference between two columns, a parent's children): its
  bounds are the operator's public ones where the column's rule or fake declares them, else the
  1st and 99th percentiles read off a histogram over a PUBLIC grid of the type's domain (every bin
  of the grid noised, empty ones included); the values are clipped to the bounds and a histogram
  of 100 equal-width bins between them is noised, from which the quantile sketch is derived;
* the shares of a column's values with no fake, and a relationship's hot parents: the column's
  counts sorted from most to least, the first K kept and zero-padded to K (K public), never a
  value -- one row moving the sorted vector by 1;
* the shares of a declared value list, or of true and false: the counts of those public values.

A per-parent child count moves a parent between two bins when one child row changes, so the
fan-out histograms are noised at sensitivity 2.
"""

# Requirements: REQ-1939

from __future__ import annotations

import math
import random
from dataclasses import dataclass
from typing import Any

from provisa.synthetic.privacy import DROP_FACTOR, Budget, laplace

#: A number's public grid: sign and tenth of a decade of |x|, from 1e-6 to 1e15.
_LOG_LOW, _LOG_HIGH, _PER_DECADE = -6, 15, 10
#: A date's public grid: whole years from 1900 to 2100, in seconds since the epoch.
_EPOCH_1900, _YEAR_S, _YEARS = -2208988800.0, 31557600.0, 200
#: A count's public grid (children per parent): twentieths of a decade of 1 + n, up to 10^7.
_COUNT_DECADES, _COUNT_PER_DECADE = 7, 20
FINE_BINS = 100
BOUND_QUANTILES = (0.01, 0.99)


def _number_edges() -> list[float]:
    pos = [
        10 ** (_LOG_LOW + i / _PER_DECADE) for i in range((_LOG_HIGH - _LOG_LOW) * _PER_DECADE + 1)
    ]
    return [-x for x in reversed(pos)] + [0.0] + pos


def _date_edges() -> list[float]:
    return [_EPOCH_1900 + i * _YEAR_S for i in range(_YEARS + 1)]


def _count_edges() -> list[float]:
    return [
        10 ** (i / _COUNT_PER_DECADE) - 1 for i in range(_COUNT_DECADES * _COUNT_PER_DECADE + 1)
    ]


def grid(kind: str) -> list[float]:
    """The public bin edges of ``kind`` (number | date | count): no edge depends on the data."""
    return {"number": _number_edges, "date": _date_edges, "count": _count_edges}[kind]()


def bin_sql(expr: str, edges: list[float]) -> str:
    """The index of the bin ``expr`` falls in among ``edges`` (0 below the first, len(edges)
    above the last): a CASE over the public edges."""
    whens = " ".join(f"WHEN {expr} < {e!r} THEN {i}" for i, e in enumerate(edges))
    return f"(CASE {whens} ELSE {len(edges)} END)"


def noised_histogram(
    rng: random.Random, observed: dict[int, int], bins: int, eps: float, sensitivity: float
) -> list[float]:
    """Every bin of the public set, observed or empty, with Laplace noise; floored at zero."""
    scale = sensitivity / eps
    return [max(0.0, observed.get(i, 0) + laplace(rng, scale)) for i in range(bins)]


def quantile_from_bins(edges: list[float], counts: list[float], q: float) -> float:
    """The value at quantile ``q`` of a histogram over ``edges`` (bins 1..len(edges)-1 between
    consecutive edges; bin 0 and the last bin, outside the edges, at the outer edges)."""
    total = sum(counts)
    if total <= 0:
        return edges[0] if q < 0.5 else edges[-1]
    target, acc = q * total, 0.0
    for i, c in enumerate(counts):
        if c > 0 and acc + c >= target:
            if i == 0:
                return edges[0]
            if i >= len(edges):
                return edges[-1]
            lo, hi = edges[i - 1], edges[i]
            return lo + (hi - lo) * (target - acc) / c
        acc += c
    return edges[-1]


def sketch_from_bins(lo: float, hi: float, counts: list[float]) -> tuple[float, ...]:
    """101 quantile points of a histogram of equal-width bins between ``lo`` and ``hi``."""
    edges = [lo + (hi - lo) * i / len(counts) for i in range(len(counts) + 1)]
    inner = [0.0, *counts, 0.0]
    return tuple(quantile_from_bins(edges, inner, i / 100) for i in range(101))


def fine_bin_sql(expr: str, lo: float, hi: float) -> str:
    """``expr`` clipped to [lo, hi] and binned into :data:`FINE_BINS` equal-width bins."""
    width = (hi - lo) / FINE_BINS if hi > lo else 1.0
    return (
        f"LEAST(GREATEST(CAST(FLOOR(({expr} - {lo!r}) / {width!r}) AS BIGINT), 0), {FINE_BINS - 1})"
    )


@dataclass
class Measurer:
    """Runs the governed statements of one private dataset and noises their answers under the
    budget. ``governed`` is provisa.profiler.run._governed (injected for tests)."""

    budget: Budget
    rng: random.Random
    governed: Any

    def _eps(self, family: str) -> float:
        """The statistic's ε, charged to the budget as it is measured."""
        eps = self.budget.per_statistic[family]
        self.budget.charged += eps
        return eps

    def _weigh(self, label: tuple[str, str | None], family: str, value: float, noise: float):
        """Record a statistic's noised value beside the scale of its noise, for the report."""
        self.budget.noise.append((label[0], label[1], family, value, noise))

    async def count(
        self,
        family: str,
        sql: str,
        label: tuple[str, str | None],
        against: float | None = None,
        weigh: bool = True,
    ) -> int:
        """A count (one row changes it by 1), noised. ``against`` is what the count is a share of
        (a column's NULLs of the table's rows), weighed against its noise in its place."""
        _n, rows = await self.governed(sql)
        scale = 1.0 / self._eps(family)
        value = max(0, round(int(rows[0][0]) + laplace(self.rng, scale)))
        if weigh:
            self._weigh(label, family, value if against is None else against, scale)
        return value

    def weigh(self, label: tuple[str, str | None], family: str, against: float) -> None:
        """Weigh a count measured earlier (``weigh`` false) against what it is a share of."""
        self._weigh(label, family, against, 1.0 / self.budget.per_statistic[family])

    async def sorted_counts(
        self,
        family: str,
        table: str,
        column: str,
        k: int,
        label: tuple[str, str],
    ) -> list[int]:
        """The column's value counts, most first, the first ``k`` zero-padded, noised; counts
        below the threshold the noise implies are left out. No value is read."""
        _n, rows = await self.governed(
            f"SELECT COUNT(*) AS n FROM {table} x WHERE x.{column} IS NOT NULL "
            f"GROUP BY x.{column} ORDER BY n DESC LIMIT {k}"
        )
        counts = [int(r[0]) for r in rows] + [0] * (k - len(rows))
        scale = 1.0 / self._eps(family)
        noised = sorted((c + laplace(self.rng, scale) for c in counts), reverse=True)
        kept = [round(c) for c in noised if c >= scale * DROP_FACTOR]
        self._weigh(label, family, sum(kept), scale * math.sqrt(2 * k))
        if len(kept) < len(noised):
            self.budget.dropped.append(
                (*label, family, f"{len(noised) - len(kept)} of its {k} counts below the noise")
            )
        return kept

    async def value_counts(
        self, family: str, table: str, column: str, values: list[str], label: tuple[str, str]
    ) -> dict[str, int]:
        """The counts of public values (a declared list, true and false), each noised."""
        cast = f"CAST(x.{column} AS TEXT)"
        _n, rows = await self.governed(
            f"SELECT {cast} AS v, COUNT(*) AS n FROM {table} x GROUP BY {cast}"
        )
        seen = {str(v).lower() if values == ["true", "false"] else v: int(n) for v, n in rows}
        scale = 1.0 / self._eps(family)
        out = {v: max(0, round(seen.get(v, 0) + laplace(self.rng, scale))) for v in values}
        self._weigh(label, family, sum(out.values()), scale * math.sqrt(2 * len(values)))
        return out

    async def contingency(
        self, family: str, table: str, a: str, b: str, ka: int, kb: int, label: tuple[str, str]
    ) -> list[list[float]]:
        """The counts of every pair of two columns' states (``a`` in 0..ka-1, ``b`` in 0..kb-1,
        both public domains), every cell noised, empty ones included; floored at zero."""
        _n, rows = await self.governed(
            f"SELECT {a} AS sa, {b} AS sb, COUNT(*) AS n FROM {table} x "
            f"WHERE {a} IS NOT NULL AND {b} IS NOT NULL GROUP BY 1, 2"
        )
        seen = {(int(x), int(y)): int(n) for x, y, n in rows}
        scale = 1.0 / self._eps(family)
        cells = [
            [max(0.0, seen.get((i, j), 0) + laplace(self.rng, scale)) for j in range(kb)]
            for i in range(ka)
        ]
        self._weigh(label, family, sum(map(sum, cells)), scale * math.sqrt(2 * ka * kb))
        return cells

    async def distribution(
        self,
        family: str,
        table: str,
        expr: str,
        kind: str,
        label: tuple[str, str | None],
        *,
        bounds: tuple[float, float] | None = None,
        where: str = "",
        sensitivity: float = 1.0,
    ) -> tuple[tuple[float, ...], tuple[float, float]] | None:
        """The quantile sketch of ``expr`` over the table's rows (``where`` narrowing them), and
        the bounds it was clipped to: public ``bounds`` where given, else private ones read off
        the public grid of ``kind``. Half the statistic's ε finds private bounds, half the
        histogram; with public bounds the histogram takes it all. None where the noise leaves no
        row."""
        eps = self._eps(family)
        cond = f" WHERE {expr} IS NOT NULL{where}"
        if bounds is None:
            edges = grid(kind)
            _n, rows = await self.governed(
                f"SELECT {bin_sql(expr, edges)} AS b, COUNT(*) AS n FROM {table} x{cond} "
                f"GROUP BY {bin_sql(expr, edges)}"
            )
            coarse = noised_histogram(
                self.rng,
                {int(b): int(n) for b, n in rows},
                len(edges) + 1,
                eps / 2,
                sensitivity,
            )
            # Post-processing, so still ε-private: a cell below the threshold the noise implies is
            # read as empty, so the grid's many empty bins do not carry the bounds outward. The
            # threshold grows with the grid, so fewer than one empty bin in ten is expected to
            # survive it (a Laplace draw exceeds s·t with probability e^-t / 2).
            floor = sensitivity / (eps / 2) * math.log(5 * (len(edges) + 1))
            coarse = [c if c >= floor else 0.0 for c in coarse]
            if sum(coarse) <= 0:
                return None
            bounds = (
                quantile_from_bins(edges, coarse, BOUND_QUANTILES[0]),
                quantile_from_bins(edges, coarse, BOUND_QUANTILES[1]),
            )
            eps = eps / 2
        lo, hi = bounds
        if hi <= lo:
            hi = lo + 1.0
        fine = fine_bin_sql(expr, lo, hi)
        _n, rows = await self.governed(
            f"SELECT {fine} AS b, COUNT(*) AS n FROM {table} x{cond} GROUP BY {fine}"
        )
        counts = noised_histogram(
            self.rng, {int(b): int(n) for b, n in rows}, FINE_BINS, eps, sensitivity
        )
        self._weigh(label, family, sum(counts), sensitivity / eps * math.sqrt(2 * FINE_BINS))
        if sum(counts) <= 0:
            return None
        return sketch_from_bins(lo, hi, counts), (lo, hi)


def epoch(expr: str, family: str) -> str:
    if family == "temporal":
        return f"EXTRACT(EPOCH FROM CAST({expr} AS TIMESTAMP))"
    return f"CAST({expr} AS DOUBLE PRECISION)"


def expected_relative_error(eps: float, rows: int) -> float:
    """A rough scale of a count's noise against the rows, for the report."""
    return math.sqrt(2) / eps / max(rows, 1)
