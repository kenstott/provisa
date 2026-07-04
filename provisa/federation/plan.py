# Copyright (c) 2026 Kenneth Stott
# Canary: 5c9d2a40-6b18-4e75-8f02-1c7a0d4f9b97
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The PLAN stage output — the ordered execution plan (REQ-825).

Stage 4 of the execution pipeline is the one IMPURE stage: a planner that (a) chooses the
ROUTE — direct native-driver execution for a single reachable source, vs handing the query
to the federation engine — and (b) resolves RESIDENCY PREREQUISITES: for each source whose
federation strategy is MATERIALIZED and stale, a side-effecting prep step that loads/refreshes
it into the store before execute (REQ-826).

The output is not a SQL string but an ordered plan: a prep phase (0..n materializations, may
be empty) feeding a single terminal step. This module is the pure composition — given the
query's sources, the engine, and a staleness oracle, it produces that Plan; the effects
(running the prep loads, codegen, execute) are carried out downstream.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING

from provisa.federation.strategy import Strategy, federate, requires_residency

if TYPE_CHECKING:
    from provisa.core.models import Source
    from provisa.federation.engine import FederationEngine


class Route(str, Enum):  # REQ-825
    DIRECT = "direct"  # single reachable source → native driver
    ENGINE = "engine"  # hand the query to the federation engine


@dataclass(frozen=True)
class PrepStep:  # REQ-825 residency prerequisite
    """A side-effecting residency prep: load/refresh a MATERIALIZED source into the store."""

    source_id: str
    strategy: Strategy


@dataclass(frozen=True)
class Plan:  # REQ-825
    """An ordered execution plan: a (possibly empty) prep phase feeding a terminal route."""

    prep: list[PrepStep] = field(default_factory=list)
    route: Route = Route.ENGINE


def build_execution_plan(
    sources: list[Source],
    engine: FederationEngine,
    is_stale: Callable[[str], bool],
) -> Plan:
    """Compose the PLAN stage output for a query over ``sources`` (REQ-825).

    ``is_stale(source_id)`` reports whether a MATERIALIZED source needs a residency refresh
    (the REQ-855 freshness gate). A single VIRTUAL source routes DIRECT; anything else — more
    than one source, or a source that is scanned/materialized — routes to the engine.
    """
    strategies = {s.id: federate(s, engine) for s in sources}
    prep = [
        PrepStep(s.id, strategies[s.id])
        for s in sources
        if requires_residency(strategies[s.id]) and is_stale(s.id)
    ]
    route = (
        Route.DIRECT
        if len(sources) == 1 and strategies[sources[0].id] is Strategy.VIRTUAL
        else Route.ENGINE
    )
    return Plan(prep=prep, route=route)
