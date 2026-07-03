# Copyright (c) 2026 Kenneth Stott
# Canary: 07eda9dd-c5e9-42dc-b995-cb4b7d5dcc71
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""FreshnessPredicate strategies (REQ-858).

Pluggable, composable strategies that unify REQ-855's three modes plus a boolean
content predicate:

- ``Ttl``        — time-based (exists today across MV/cache): fresh while the last
  refresh is within a TTL window.
- ``Probe``      — REQ-855 opaque-token change detection: fresh iff the current
  ``freshness_token`` matches the token captured at the last refresh. A ``None``
  token means the source cannot produce one; PROBE alone reports FAILED and the
  composed ``TtlThenProbe`` degrades to TTL (REQ-847).
- ``Transitive`` — fresh iff the wrapped predicate holds AND every upstream subject
  is transitively fresh (the one net-new capability beyond REQ-855's per-entry check).
- ``TtlThenProbe`` — composition: TTL floors probe frequency (probe only after the
  TTL window elapses).

Every strategy is a pure decision — ``evaluate`` returns a :class:`Decision` with no
side effects. Refresh/trigger semantics are the caller's responsibility (REQ-856).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from provisa.freshness.decision import Decision, Freshness, failed, fresh, stale
from provisa.freshness.subject import FreshnessSubject


class Strategy(Protocol):  # REQ-858
    def evaluate(self, subject: FreshnessSubject, now: float) -> Decision: ...


def _refresh_state(subject: FreshnessSubject) -> Decision | None:
    """Shared precondition: gate on the subject's last-refresh outcome (REQ-857).

    Returns a terminal Decision when freshness cannot proceed (never refreshed →
    STALE; last refresh failed → FAILED), or None when the strategy should run.
    """
    if not subject.last_refresh_ok():
        return failed("last refresh failed")
    if subject.last_refresh_at() is None:
        return stale("never refreshed")
    return None


@dataclass(frozen=True)
class Ttl:  # REQ-858
    ttl_seconds: float

    def evaluate(self, subject: FreshnessSubject, now: float) -> Decision:
        gate = _refresh_state(subject)
        if gate is not None:
            return gate
        ts = subject.last_refresh_at()
        assert ts is not None  # guaranteed by _refresh_state
        age = now - ts
        if age < self.ttl_seconds:
            return fresh(f"age {age:.0f}s < ttl {self.ttl_seconds:.0f}s")
        return stale(f"age {age:.0f}s >= ttl {self.ttl_seconds:.0f}s")


@dataclass(frozen=True)
class Probe:  # REQ-858 (REQ-855 token change detection)
    def evaluate(self, subject: FreshnessSubject, now: float) -> Decision:
        gate = _refresh_state(subject)
        if gate is not None:
            return gate
        # The subject owns its transport (DB query, file mtime, …) and returns None
        # when it cannot produce a token — including on transport failure (REQ-857);
        # the predicate stays pure and never catches.
        current = subject.freshness_token()
        if current is None:
            return failed("source cannot produce a freshness token")
        baseline = subject.refresh_token()
        if baseline is None:
            return stale("no baseline token from last refresh")
        if current == baseline:
            return fresh("freshness token unchanged")
        return stale("freshness token changed")


@dataclass(frozen=True)
class TtlThenProbe:  # REQ-858 composition (TTL floors probe frequency)
    ttl_seconds: float

    def evaluate(self, subject: FreshnessSubject, now: float) -> Decision:
        ttl = Ttl(self.ttl_seconds).evaluate(subject, now)
        if ttl.state is Freshness.FRESH:
            return fresh(f"within TTL, probe skipped ({ttl.reason})")
        if ttl.state is Freshness.FAILED:
            return ttl
        probe = Probe().evaluate(subject, now)
        if probe.state is Freshness.FAILED:
            # Token unsupported/unavailable → degrade to the TTL verdict (REQ-847).
            return stale(f"TTL elapsed, probe unavailable ({probe.reason})")
        return probe


@dataclass(frozen=True)
class Transitive:  # REQ-858 (net-new beyond REQ-855)
    base: Strategy

    def evaluate(self, subject: FreshnessSubject, now: float) -> Decision:
        return self._evaluate(subject, now, set())

    def _evaluate(self, subject: FreshnessSubject, now: float, seen: set[int]) -> Decision:
        sid = id(subject)
        if sid in seen:
            # Cycle in the upstream graph — treat as satisfied so recursion terminates.
            return fresh("cycle guard: already evaluated")
        seen.add(sid)
        own = self.base.evaluate(subject, now)
        if own.state is not Freshness.FRESH:
            return own
        for up in subject.upstream():
            d = self._evaluate(up, now, seen)
            if d.state is not Freshness.FRESH:
                return Decision(d.state, f"upstream stale: {d.reason}")
        return fresh("self and all upstream fresh")


def evaluate(subject: FreshnessSubject, strategy: Strategy, now: float) -> Decision:  # REQ-856
    """Evaluate a subject against a strategy. Pure — no side effects."""
    return strategy.evaluate(subject, now)
