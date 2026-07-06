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
    from provisa.federation.cardinality import Estimate
    from provisa.federation.engine import FederationEngine
    from provisa.federation.promote import PushdownDemand


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
    *,
    demand_of: Callable[[str], PushdownDemand] | None = None,
    estimate_of: Callable[[str], Estimate] | None = None,
    prefer_materialized_of: Callable[[str], bool] | None = None,
    materialization_backend: str | None = None,
) -> Plan:
    """Compose the PLAN stage output for a query over ``sources`` (REQ-825).

    ``is_stale(source_id)`` reports whether a MATERIALIZED source needs a residency refresh
    (the REQ-855 freshness gate). A single VIRTUAL source routes DIRECT; anything else — more
    than one source, or a source that is scanned/materialized — routes to the engine.

    ``demand_of`` + ``estimate_of`` arm cost-based promotion (REQ-826 extension): per source
    they supply the query's pushdown demand and a cardinality estimate, so federate() may
    promote a reachable-but-weak-pushdown large scan to MATERIALIZED. Both must be given to
    arm it; a promoted source then joins the prep phase like any other MATERIALIZED source.

    ``prefer_materialized_of`` is the MANUAL counterpart to that automatic gate: a per-source
    policy override forcing MATERIALIZED for a source the engine could reach live but that, for
    this use case, serves better from the store (the connector is a poor fit). When it forces a
    source to the store, ``materialization_backend`` MUST name a backend the engine can read back
    — else the override resolves to MATERIALIZED with nowhere to land. The guard fails loud here
    at plan time rather than silently at execute (see validate_materialization_backend, REQ-846).
    """
    strategies: dict[str, Strategy] = {}
    for s in sources:
        prefer = prefer_materialized_of(s.id) if prefer_materialized_of is not None else False
        strategy = federate(
            s,
            engine,
            prefer_materialized=prefer,
            demand=demand_of(s.id) if demand_of is not None else None,
            estimate=estimate_of(s.id) if estimate_of is not None else None,
        )
        if prefer and strategy is Strategy.MATERIALIZED:
            _require_materialization_backend(engine, materialization_backend, s.id)
        strategies[s.id] = strategy
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


def _require_materialization_backend(
    engine: FederationEngine, backend: str | None, source_id: str
) -> None:
    """Guard a prefer_materialized override: the engine must have a store to land ``source_id`` into.

    A source forced to the store needs a backend the engine can READ BACK (REQ-846). None →
    the override has nowhere to land; a backend the engine has no ATTACH connector for is a
    land-into-land regress. Either way, fail loud at plan time (REQ-826 extension).
    """
    from provisa.federation.materialization import (
        InvalidMaterializationBackend,
        validate_materialization_backend,
    )

    if backend is None:
        raise InvalidMaterializationBackend(
            f"source {source_id!r} is set prefer_materialized on engine {engine.name!r}, but no "
            f"materialization backend is configured to land it into"
        )
    validate_materialization_backend(engine, backend)
