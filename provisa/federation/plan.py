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

import time
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
    from provisa.freshness.subject import FreshnessSubject


class Route(str, Enum):  # REQ-825
    DIRECT = "direct"  # single reachable source → native driver
    ENGINE = "engine"  # hand the query to the federation engine


class UnscannableSource(Exception):  # REQ-897
    """A source resolved to a SCAN (read a file/object in place) on an engine that does NOT declare
    the ``file_native`` capability trait. The DECLARED trait is the planner input; a SCAN without it
    is a declaration/connector inconsistency, never a silently-materialized fallback."""

    def __init__(self, engine: str, source_id: str) -> None:
        self.engine = engine
        self.source_id = source_id
        super().__init__(
            f"engine {engine!r} resolved source {source_id!r} to a SCAN but does not declare the "
            "file_native trait (REQ-897)"
        )


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
    load_protected_of: Callable[[str], bool] | None = None,
    resident_of: Callable[[str], bool] | None = None,
    materialization_backend: str | None = None,
    freshness_subject_of: Callable[[str], FreshnessSubject] | None = None,
    now: float | None = None,
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

    ``load_protected_of`` marks a source LOAD-PROTECTED (REQ-1141): like prefer_materialized it
    forces MATERIALIZED (removes the live route), but additionally the READ path must NOT pull the
    source — the scheduler is the sole writer. A load-protected source is therefore put in the prep
    phase ONLY when it is not yet resident (``resident_of(s.id)`` is False / unavailable); once a
    snapshot exists, staleness never triggers a query-path refresh (that is the scheduler's job).
    ``resident_of`` reports whether a snapshot already exists in the store; absent it, a
    load-protected source falls back to a single first-load (treated as not resident).

    ``freshness_subject_of`` arms the REQ-860 SOURCE-level freshness gate: for a source with
    ``freshness_gate=True`` it supplies that source's observed residency state as a
    FreshnessSubject, and the source's own ``change_signal`` + ``cache_ttl`` freshness predicate —
    not the generic ``is_stale`` oracle — decides whether it needs a residency refresh. A
    stale/failed verdict lands the source in the prep phase (the existing refresh/produce path);
    fresh skips it. ``now`` is the evaluation clock (defaulted to wall time when a gated source is
    present); the decision itself stays pure (predicate has no side effects, REQ-856).
    """
    strategies: dict[str, Strategy] = {}
    load_protected: set[str] = set()
    for s in sources:
        protected = load_protected_of(s.id) if load_protected_of is not None else False
        if protected:
            load_protected.add(s.id)
        prefer = (
            prefer_materialized_of(s.id) if prefer_materialized_of is not None else False
        ) or protected
        strategy = federate(
            s,
            engine,
            prefer_materialized=prefer,
            demand=demand_of(s.id) if demand_of is not None else None,
            estimate=estimate_of(s.id) if estimate_of is not None else None,
        )
        if prefer and strategy is Strategy.MATERIALIZED:
            _require_materialization_backend(engine, materialization_backend, s.id)
        if strategy is Strategy.SCAN and not engine.file_native:
            # REQ-897: a SCAN resolves a file/object source read IN PLACE — only a file_native engine
            # can. The DECLARED ``file_native`` trait is the authoritative planner INPUT here; a SCAN
            # on an engine that does not declare it is a trait/connector inconsistency, never a
            # silently-accepted plan. Fail loud (also raises UndeclaredTrait if the trait is unset).
            raise UnscannableSource(engine.name, s.id)
        strategies[s.id] = strategy
    prep = [
        PrepStep(s.id, strategies[s.id])
        for s in sources
        if requires_residency(strategies[s.id])
        and _needs_prep(s, is_stale, freshness_subject_of, now, s.id in load_protected, resident_of)
    ]
    route = (
        Route.DIRECT
        if len(sources) == 1 and strategies[sources[0].id] is Strategy.VIRTUAL
        else Route.ENGINE
    )
    return Plan(prep=prep, route=route)


def _needs_prep(
    source: Source,
    is_stale: Callable[[str], bool],
    freshness_subject_of: Callable[[str], FreshnessSubject] | None,
    now: float | None,
    load_protected: bool,
    resident_of: Callable[[str], bool] | None,
) -> bool:
    """Whether a MATERIALIZED source needs a residency prep before this query reads it.

    REQ-1141: a LOAD-PROTECTED source is refreshed only by the scheduler, never on the query path.
    A read therefore preps it ONLY when no snapshot exists yet (first load); once resident, a query
    always serves the last snapshot regardless of staleness (the scheduler owns freshness). Absent a
    ``resident_of`` oracle, first-load is assumed (treated as not resident) — never a query-path
    re-pull of an already-resident protected source.

    Otherwise (REQ-860): a source with ``freshness_gate=True`` is decided by its OWN freshness
    predicate (change_signal + cache_ttl) over its observed residency state — a stale/failed verdict
    triggers the refresh; every other source uses the generic ``is_stale`` oracle. When a gated
    source is present but no ``now`` was supplied, the wall clock is the evaluation time (the
    decision is pure; sampling the clock here is the caller's effect).
    """
    if load_protected:
        return not resident_of(source.id) if resident_of is not None else True
    if source.freshness_gate and freshness_subject_of is not None:
        from provisa.freshness.source_gate import gate_source

        eval_now = now if now is not None else time.time()
        return not gate_source(source, freshness_subject_of(source.id), eval_now).is_fresh
    return is_stale(source.id)


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
