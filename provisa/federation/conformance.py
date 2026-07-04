# Copyright (c) 2026 Kenneth Stott
# Canary: 8c2d9a71-4b08-4e75-9f12-3c7a0d4f9c17
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Governance-parity conformance across federation engines (REQ-827).

Any engine admitted to the FederationEngine contract (REQ-825) MUST produce the same
GOVERNED results — RLS filtering, column masking, relationship-join enforcement, and
row/aggregate output — as the reference engine for the same query and identity. Governance
is defined in the semantic layer and enforced when the query compiles to IR, so only the
transpile→transport stages differ per engine. SQLGlot transpiles syntax, but semantic edges
still differ (NULL ordering, type coercion, decimal precision, regex/collation, timezone,
case sensitivity) — and these MUST NOT change which rows are visible, which cells are masked,
or which joins are enforced.

An engine is certified only by passing a shared conformance suite against a fixed golden
dataset + identity matrix, diffed against the reference engine; any divergence fails
certification. This is a HARD gate: an uncertified engine must not be selectable in production.

This module is the comparator + the certification gate. Comparison is order-INDEPENDENT
(NULL ordering is an allowed semantic edge) but visibility/masking-EXACT (a row present in one
but not the other, or a differently-masked cell, is a divergence).
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Divergence:  # REQ-827
    """A governed-output difference between the reference and a candidate engine."""

    kind: str  # "only_in_reference" | "only_in_candidate"
    row: tuple


@dataclass(frozen=True)
class ConformanceResult:  # REQ-827
    certified: bool
    divergences: list[Divergence] = field(default_factory=list)


def compare_governed_results(
    reference: list[tuple],
    candidate: list[tuple],
) -> ConformanceResult:
    """Diff a candidate engine's governed rows against the reference (REQ-827).

    Order-independent multiset comparison: any row visible in exactly one side (an RLS leak /
    over-filter) or a differently-masked cell (the row tuple differs) is a divergence. Certified
    only when there are none.
    """
    ref = Counter(reference)
    cand = Counter(candidate)
    divergences: list[Divergence] = []
    for row, n in (ref - cand).items():
        divergences.extend(Divergence("only_in_reference", row) for _ in range(n))
    for row, n in (cand - ref).items():
        divergences.extend(Divergence("only_in_candidate", row) for _ in range(n))
    return ConformanceResult(certified=not divergences, divergences=divergences)


class UncertifiedEngineError(Exception):  # REQ-827
    """Raised when an uncertified engine is selected — the hard production gate."""

    def __init__(self, engine: str) -> None:
        self.engine = engine
        super().__init__(
            f"engine {engine!r} is not conformance-certified and must not be selectable"
        )


class ConformanceRegistry:  # REQ-827
    """Which engines have passed the governance-parity suite. The reference is always certified."""

    def __init__(self, reference: str) -> None:
        self.reference = reference
        self._certified: set[str] = {reference}

    def certify(self, engine: str) -> None:
        self._certified.add(engine)

    def revoke(self, engine: str) -> None:
        if engine == self.reference:
            raise ValueError("cannot revoke the reference engine")
        self._certified.discard(engine)

    def is_certified(self, engine: str) -> bool:
        return engine in self._certified

    def require_certified(self, engine: str) -> None:
        """Hard gate: raise unless the engine is certified (REQ-827)."""
        if engine not in self._certified:
            raise UncertifiedEngineError(engine)
