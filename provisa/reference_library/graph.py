# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The glossary walk of graph retrieval (REQ-2185, GRAPH RETRIEVAL).

From the seed terms the walk follows the glossary's relationships, either way along each, to the
terms they reach. A reached term's priority is the product of the weights of the relationship
types on its path (:data:`WEIGHTS`), so it falls with each hop: a seed's is 1.0. A term is reached
by its highest-priority path, and the walk stops where the priority would fall below
:data:`CUTOFF`. TRANSLATION_OF and ANTONYM_OF conduct nothing: a translation is the same concept
in another language, already reached by its own name, and an antonym is its opposite.

Nothing here reads a database: the caller gives the terms and relationships the role may read.
"""

# Requirements: REQ-2185

from __future__ import annotations

import heapq
import itertools
import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass

#: Each relationship type's weight along a path; a type absent here conducts nothing.
WEIGHTS: Mapping[str, float] = {
    "SYNONYM_OF": 1.0,
    "PREFERRED_TERM_FOR": 1.0,
    "KIND_OF": 0.8,
    "PART_OF": 0.8,
    "VALID_VALUE_OF": 0.8,
    "DERIVED_FROM": 0.6,
    "REPLACES": 0.6,
    "RELATED_TO": 0.4,
}

#: The walk stops where a path's priority falls below this.
CUTOFF = 0.25

#: Floats are compared within this relative tolerance, never exactly.
_TOLERANCE = 1e-14


@dataclass(frozen=True)
class Step:
    """One hop of a path: the term reached and the relationship type crossed to it."""

    term_id: int
    rel_type: str


@dataclass(frozen=True)
class Reached:
    """A term the walk reached, its priority and its path: the seed it started from, then each
    hop to it (none for a seed)."""

    term_id: int
    priority: float
    seed: int
    steps: tuple[Step, ...]


def _conducts(priority: float) -> bool:
    return priority > CUTOFF or math.isclose(priority, CUTOFF, rel_tol=_TOLERANCE)


def _rank(priority: float) -> float:
    """A priority as the walk orders by it: two products of the same weights in another order
    differ in their last bits, and rank as equal."""
    return round(priority, 12)


def walk(
    seeds: Sequence[int],
    edges: Iterable[tuple[int, int, str]],
    terms: Iterable[int],
) -> list[Reached]:
    """The terms reached from ``seeds`` over ``edges`` (from term, to term, type), among
    ``terms`` alone (those the role may read and in service), highest priority first. A term is
    given once, by its best path; between paths of one priority, the one of fewer hops, then the
    one found first -- from the earlier seed of ``seeds``."""
    readable = set(terms)
    adjacent: dict[int, list[tuple[int, str, float]]] = {}
    for a, b, rel_type in edges:
        weight = WEIGHTS.get(rel_type, 0.0)
        if weight <= 0.0 or a == b or a not in readable or b not in readable:
            continue
        adjacent.setdefault(a, []).append((b, rel_type, weight))
        adjacent.setdefault(b, []).append((a, rel_type, weight))
    for out in adjacent.values():
        out.sort(key=lambda o: (-o[2], o[1], o[0]))

    order = itertools.count()
    frontier: list[tuple[float, int, int, Reached]] = []
    for seed in dict.fromkeys(seeds):
        if seed in readable:
            heapq.heappush(frontier, (-1.0, 0, next(order), Reached(seed, 1.0, seed, ())))
    reached: dict[int, Reached] = {}
    while frontier:
        _, hops, _, here = heapq.heappop(frontier)
        if here.term_id in reached:
            continue
        reached[here.term_id] = here
        for there, rel_type, weight in adjacent.get(here.term_id, ()):
            priority = here.priority * weight
            if there in reached or not _conducts(priority):
                continue
            heapq.heappush(
                frontier,
                (
                    -_rank(priority),
                    hops + 1,
                    next(order),
                    Reached(there, priority, here.seed, (*here.steps, Step(there, rel_type))),
                ),
            )
    return list(reached.values())


def relationship(rel_type: str) -> str:
    """How a path's label names a relationship type: ``PART_OF`` is "part of"."""
    return rel_type.lower().replace("_", " ")


def label(path: Reached, names: Mapping[int, str]) -> str:
    """A reached term's path as a candidate is labelled with it: "via <seed> -> <term> (part of)"."""
    hops = "".join(f" -> {names[s.term_id]} ({relationship(s.rel_type)})" for s in path.steps)
    return f"via {names[path.seed]}{hops}"
