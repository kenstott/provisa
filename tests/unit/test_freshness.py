# Copyright (c) 2026 Kenneth Stott
# Canary: c1a2b3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for the freshness module (REQ-856, REQ-857, REQ-858).

Pure logic — no I/O, no DB, no docker. Exercises TTL/PROBE/TRANSITIVE strategies,
the TTL+PROBE composition, and the fresh/stale/failed decision surface.
"""

from __future__ import annotations

from dataclasses import dataclass

from provisa.freshness import (
    Freshness,
    FreshnessSubject,
    Probe,
    Transitive,
    Ttl,
    TtlThenProbe,
    evaluate,
)


@dataclass
class FakeSubject:
    """Minimal FreshnessSubject for tests."""

    refreshed_at: float | None = 100.0
    ok: bool = True
    token: str | None = None
    baseline: str | None = None
    ups: tuple = ()

    def last_refresh_at(self) -> float | None:
        return self.refreshed_at

    def last_refresh_ok(self) -> bool:
        return self.ok

    def upstream(self):
        return self.ups

    def freshness_token(self) -> str | None:
        return self.token

    def refresh_token(self) -> str | None:
        return self.baseline


def test_fakesubject_conforms_to_protocol():
    assert isinstance(FakeSubject(), FreshnessSubject)


# --- shared refresh-outcome gate (REQ-857) -------------------------------------


def test_never_refreshed_is_stale():
    d = evaluate(FakeSubject(refreshed_at=None), Ttl(50), now=120)
    assert d.state is Freshness.STALE
    assert "never" in d.reason


def test_last_refresh_failed_is_failed():
    d = evaluate(FakeSubject(refreshed_at=100, ok=False), Ttl(50), now=120)
    assert d.state is Freshness.FAILED
    assert "failed" in d.reason


# --- TTL ------------------------------------------------------------------------


def test_ttl_within_window_is_fresh():
    assert evaluate(FakeSubject(refreshed_at=100), Ttl(50), now=120).state is Freshness.FRESH


def test_ttl_boundary_is_stale():
    # age == ttl is NOT fresh (strict <)
    assert evaluate(FakeSubject(refreshed_at=100), Ttl(50), now=150).state is Freshness.STALE


def test_ttl_elapsed_is_stale():
    assert evaluate(FakeSubject(refreshed_at=100), Ttl(50), now=200).state is Freshness.STALE


# --- PROBE (REQ-855 token change detection) ------------------------------------


def test_probe_unchanged_token_is_fresh():
    d = evaluate(FakeSubject(token="abc", baseline="abc"), Probe(), now=999)
    assert d.state is Freshness.FRESH


def test_probe_changed_token_is_stale():
    d = evaluate(FakeSubject(token="new", baseline="old"), Probe(), now=999)
    assert d.state is Freshness.STALE
    assert "changed" in d.reason


def test_probe_no_token_is_failed():
    # None token: source cannot produce one (degrades to TTL only under composition)
    d = evaluate(FakeSubject(token=None, baseline="old"), Probe(), now=999)
    assert d.state is Freshness.FAILED


def test_probe_no_baseline_is_stale():
    d = evaluate(FakeSubject(token="abc", baseline=None), Probe(), now=999)
    assert d.state is Freshness.STALE


def test_probe_transport_failure_reports_none_and_is_failed():
    # Contract: the subject returns None on transport failure (must not raise),
    # so PROBE reports FAILED — cannot produce a token (REQ-857).
    d = evaluate(FakeSubject(token=None, baseline="abc"), Probe(), now=999)
    assert d.state is Freshness.FAILED


# --- TTL+PROBE composition (TTL floors probe frequency) ------------------------


def test_ttl_then_probe_within_ttl_skips_probe():
    # Token changed, but still within TTL → fresh without probing.
    d = evaluate(
        FakeSubject(refreshed_at=100, token="new", baseline="old"), TtlThenProbe(50), now=120
    )
    assert d.state is Freshness.FRESH
    assert "probe skipped" in d.reason


def test_ttl_then_probe_after_ttl_probes():
    d = evaluate(
        FakeSubject(refreshed_at=100, token="new", baseline="old"), TtlThenProbe(50), now=200
    )
    assert d.state is Freshness.STALE


def test_ttl_then_probe_after_ttl_unchanged_token_is_fresh():
    d = evaluate(
        FakeSubject(refreshed_at=100, token="same", baseline="same"), TtlThenProbe(50), now=200
    )
    assert d.state is Freshness.FRESH


def test_ttl_then_probe_degrades_to_ttl_when_token_unsupported():
    # TTL elapsed and no token → degrade to TTL verdict (stale), not failed (REQ-847).
    d = evaluate(
        FakeSubject(refreshed_at=100, token=None, baseline="old"), TtlThenProbe(50), now=200
    )
    assert d.state is Freshness.STALE
    assert "probe unavailable" in d.reason


# --- TRANSITIVE (REQ-858 net-new) ----------------------------------------------


def test_transitive_self_and_upstream_fresh():
    up = FakeSubject(refreshed_at=100)
    d = evaluate(FakeSubject(refreshed_at=100, ups=(up,)), Transitive(Ttl(50)), now=120)
    assert d.state is Freshness.FRESH


def test_transitive_own_stale_short_circuits():
    up = FakeSubject(refreshed_at=100)
    d = evaluate(FakeSubject(refreshed_at=100, ups=(up,)), Transitive(Ttl(50)), now=300)
    assert d.state is Freshness.STALE


def test_transitive_upstream_stale_propagates():
    stale_up = FakeSubject(refreshed_at=10)  # far older
    d = evaluate(FakeSubject(refreshed_at=100, ups=(stale_up,)), Transitive(Ttl(50)), now=120)
    assert d.state is Freshness.STALE
    assert "upstream" in d.reason


def test_transitive_multi_level():
    leaf = FakeSubject(refreshed_at=100)
    mid = FakeSubject(refreshed_at=100, ups=(leaf,))
    root = FakeSubject(refreshed_at=100, ups=(mid,))
    assert evaluate(root, Transitive(Ttl(50)), now=120).state is Freshness.FRESH
    # A stale leaf two levels down makes the root stale.
    leaf.refreshed_at = 10
    assert evaluate(root, Transitive(Ttl(50)), now=120).state is Freshness.STALE


def test_transitive_cycle_guard_terminates():
    a = FakeSubject(refreshed_at=100)
    b = FakeSubject(refreshed_at=100)
    a.ups = (b,)
    b.ups = (a,)  # cycle
    # Must not recurse infinitely; both fresh → fresh.
    assert evaluate(a, Transitive(Ttl(50)), now=120).state is Freshness.FRESH


# --- decision surface -----------------------------------------------------------


def test_decision_is_fresh_helper():
    assert evaluate(FakeSubject(refreshed_at=100), Ttl(50), now=120).is_fresh is True
    assert evaluate(FakeSubject(refreshed_at=100), Ttl(50), now=999).is_fresh is False
