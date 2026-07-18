# Copyright (c) 2026 Kenneth Stott
# Canary: 9a3c1f70-6b28-4e51-8d04-2c7b0d4f1e69
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Plain-English refresh-policy summary, derived per (source, table, engine) (REQ-1143).

The effective serving/refresh behaviour of a table is a decision tree over reachability (per
ENGINE), prefer_materialized, load_protected, off-peak window, cadence, and probe. No steward can
read the raw config and know the outcome. This module derives a one-line human summary — and
misconfiguration warnings — from the SAME resolution the planner uses: ``federate(source, engine)``
(strategy.py) for reachability and ``resolve_refresh_policy`` (scheduled_refresh.py) for the gates.

It is computed per (source, engine) because reachability is engine-specific: the same source may be
VIRTUAL on one engine and UnreachableSource on another (REQ-826). The summary therefore may read
differently across engines; the caller passes the engine in context.

Pure: no I/O. The server renders the ``PolicySummary`` (text + optional warning + a machine
``serving`` tag) onto the table-detail payload so the UI never re-derives the tree (REQ-1143).
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

from provisa.federation.scheduled_refresh import OffPeakWindow, resolve_refresh_policy
from provisa.federation.strategy import Strategy, federate

if TYPE_CHECKING:
    from provisa.core.models import Source, Table
    from provisa.federation.engine import FederationEngine


class Serving(str, Enum):  # REQ-1143 — machine tag for UI styling
    LIVE = "live"  # reached directly, always fresh
    SCHEDULED = "scheduled"  # load-protected snapshot, scheduler-refreshed, zero query-path load
    CACHE = "cache"  # REQ-826 lazy read-through cache (query-triggered on staleness)
    FROZEN = "frozen"  # loaded once, never refreshed (unreachable + no refresh policy)


@dataclass(frozen=True)
class PolicySummary:  # REQ-1143
    """A one-line effective-policy summary for a (source, table, engine). ``text`` is the headline;
    ``warning`` is a non-None misconfiguration note; ``serving`` is the machine tag."""

    text: str
    serving: Serving
    warning: str | None = None


def _fmt_window(w: OffPeakWindow) -> str:
    def hhmm(m: int) -> str:
        return f"{m // 60:02d}:{m % 60:02d}"

    return f"{hhmm(w.start_minute)}–{hhmm(w.end_minute)} {w.tz}"


def _fmt_cadence(seconds: int) -> str:
    if seconds % 3600 == 0:
        return f"{seconds // 3600}h"
    if seconds % 60 == 0:
        return f"{seconds // 60}m"
    return f"{seconds}s"


def _live_reachable(source: Source, engine: FederationEngine) -> bool:
    """Whether ``engine`` can serve ``source`` LIVE (VIRTUAL/SCAN) — reachability is engine-specific.

    Resolves the strategy WITHOUT prefer_materialized (the question is capability, not policy). An
    UnreachableSource means the engine has no connector at all for this source type."""
    from provisa.federation.engine import UnreachableSource

    try:
        strat = federate(source, engine, prefer_materialized=False)
    except UnreachableSource:
        return False
    return strat in (Strategy.VIRTUAL, Strategy.SCAN)


def _describe_scheduled(policy) -> str:
    """The prose for an armed load-protected scheduled snapshot (REQ-1141 gates)."""
    clauses: list[str] = []
    if policy.window is not None:
        clauses.append(f"during {_fmt_window(policy.window)}")
    if policy.cadence is not None:
        clauses.append(f"at most every {_fmt_cadence(policy.cadence)}")
    if policy.probe_capable:
        clauses.append("only when the source has changed")
    when = ", ".join(clauses) if clauses else "on schedule"
    return f"Scheduled snapshot — refreshed {when}; queries never touch the source."


def describe_refresh_policy(
    source: Source, table: Table, engine: FederationEngine
) -> PolicySummary:
    """Derive the plain-English refresh-policy summary for one (source, table, engine) (REQ-1143).

    Mirrors the planner's decision tree exactly; every branch below is the effective outcome, not a
    restatement of the raw knobs. The two no-refresh-policy warnings are the SAME condition
    resolving differently by engine-specific reachability (REQ-1141 boundary)."""
    policy = resolve_refresh_policy(source, table)
    live = _live_reachable(source, engine)

    # 1. Load-protected + armed → scheduler-only refresh, zero query-path load.
    if policy.load_protected and policy.armed:
        return PolicySummary(_describe_scheduled(policy), Serving.SCHEDULED)

    prefer = (
        source.prefer_materialized
        if table.prefer_materialized is None
        else table.prefer_materialized
    ) or policy.load_protected

    # 2. Materialized with a cadence but not load-protected → REQ-826 lazy read-through cache.
    if prefer and policy.cadence is not None:
        return PolicySummary(
            f"Cached — refreshed on access when older than {_fmt_cadence(policy.cadence)}.",
            Serving.CACHE,
        )

    # 3/4/5. No refresh policy. Split on engine-specific reachability (REQ-1141 boundary).
    if not prefer:
        if live:
            return PolicySummary("Live — reached directly, always fresh.", Serving.LIVE)
        # Unreachable live, not forced to materialize, no cadence: served from the store as loaded.
        return PolicySummary(
            "Snapshot — loaded on first access, no automatic refresh configured.",
            Serving.FROZEN,
        )

    # prefer_materialized set (or load_protected without a gate) but NO refresh policy:
    if live:
        return PolicySummary(
            "Live — reached directly, always fresh.",
            Serving.LIVE,
            warning=(
                "prefer_materialized has no effect on this engine: the source is reachable live "
                "and no refresh policy is set, so it is served live."
            ),
        )
    return PolicySummary(
        "Frozen snapshot — loaded once, never refreshes.",
        Serving.FROZEN,
        warning=(
            "This source cannot be served live on this engine and has no refresh policy, so it is "
            "loaded once and never refreshed — valid only for static reference data."
        ),
    )
