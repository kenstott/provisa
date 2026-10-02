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
ENGINE), replicate, load_protected, off-peak window, cadence, and probe. No steward can
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
    CACHE = "cache"  # REQ-826 served from its replica, refreshed on staleness
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

    Resolves the strategy WITHOUT the replicate setting (the question is capability, not policy).
    An UnreachableSource means the engine has no connector at all for this source type."""
    from provisa.federation.engine import UnreachableSource

    try:
        strat = federate(source, engine, replicated=False)
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
    source: Source,
    table: Table,
    engine: FederationEngine,
    default_ttl: int = 300,
    *,
    promoted: bool = False,
) -> PolicySummary:
    """Derive the plain-English refresh-policy summary for one (source, table, engine) (REQ-1143).

    Mirrors the planner's decision tree exactly; every branch below is the effective outcome, not a
    restatement of the raw knobs: the table's resolved ``replicate`` (REQ-826, ``core.replicate``)
    and load protection, and what THIS engine can read in place. Only Always (0) and load
    protection guarantee the replica; Never (-1) and the Hot values are best effort, and the text
    says what they come to on this engine.

    ``default_ttl`` is the global response-cache TTL (``state.response_cache_default_ttl``). It is the
    read-through refresh cadence a table gets when it sets no explicit ``cache_ttl`` — the SAME chain
    the Effective-TTL column resolves (table → source → global). ``promoted``: the table passed its
    threshold and is in the promoted set."""
    from provisa.core.replicate import NEVER, floor_of, resolved_replicate

    policy = resolve_refresh_policy(source, table)
    live = _live_reachable(source, engine)

    # The effective read-through TTL: explicit cache_ttl (table→source, == policy.cadence) else the
    # global default. <= 0 means caching is disabled (never re-fetched from the cache path).
    eff_ttl = policy.cadence if policy.cadence is not None else default_ttl

    # 1. Load-protected + armed → scheduler-only refresh, zero query-path load.
    if policy.load_protected and policy.armed:
        return PolicySummary(_describe_scheduled(policy), Serving.SCHEDULED)

    replicate = resolved_replicate(source, table)

    # 2. The operator's setting puts reads on the replica: Always, load protection, or a table
    # past its threshold. The source is not read by a query on any engine.
    if floor_of(replicate, policy.load_protected, promoted=promoted) is not None:
        if policy.cadence is not None:
            return PolicySummary(
                "Replicated — reads come from the replica, refreshed on access when older than "
                f"{_fmt_cadence(policy.cadence)}.",
                Serving.CACHE,
            )
        return PolicySummary(
            "Replicated — reads come from the replica. It is built once and refreshed only "
            "when the source reports a change or the scheduler runs.",
            Serving.CACHE,
        )

    # 3. Live on this engine: Never, Hot-N below its threshold, or Default.
    if live:
        if replicate == NEVER:
            return PolicySummary(
                "Live — read directly from the source; never replicated.", Serving.LIVE
            )
        if replicate is not None:
            return PolicySummary(
                f"Live — read directly from the source until it passes {replicate} governed "
                "statements per interval, then served from its replica (best effort: reads stay "
                "live while the replica is built).",
                Serving.LIVE,
            )
        return PolicySummary("Live — reached directly, always fresh.", Serving.LIVE)

    # 4. This engine cannot read the source in place: the replica is the only way to reach it,
    # whatever the setting. Never is best effort, and here it cannot be met.
    if replicate == NEVER:
        when = (
            f"re-fetched on access when older than {_fmt_cadence(eff_ttl)}"
            if eff_ttl > 0
            else "loaded on first access and never re-fetched (caching disabled)"
        )
        return PolicySummary(
            "Replicated — this engine cannot read the source in place, so Never cannot apply "
            f"here (it is best effort): reads come from the replica, {when}.",
            Serving.CACHE if eff_ttl > 0 else Serving.FROZEN,
        )
    if eff_ttl > 0:
        return PolicySummary(
            f"Cached — re-fetched on access when older than {_fmt_cadence(eff_ttl)}.",
            Serving.CACHE,
        )
    return PolicySummary(
        "Snapshot — loaded on first access, never re-fetched (caching disabled).",
        Serving.FROZEN,
    )
