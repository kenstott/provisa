# Copyright (c) 2026 Kenneth Stott
# Canary: 5d7e9a1b-2c3f-4d6a-8b0e-1f2a3c4d5e6f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-859: MV and the API/pg cache conform to FreshnessSubject and share the one
FreshnessPredicate. Behaviour-preserving unification — no I/O, no DB, no docker.
"""

from __future__ import annotations

import time


from provisa.freshness import FreshnessSubject, Ttl, evaluate
from provisa.freshness.adapters import StateSubject


def test_state_subject_conforms_to_protocol():
    assert isinstance(StateSubject(refreshed_at=1.0), FreshnessSubject)


def test_state_subject_reports_its_fields():
    s = StateSubject(refreshed_at=100.0, ok=False, token="t", baseline="b", upstream_subjects=())
    assert s.last_refresh_at() == 100.0
    assert s.last_refresh_ok() is False
    assert s.freshness_token() == "t"
    assert s.refresh_token() == "b"
    assert s.upstream() == ()


def test_state_subject_evaluates_ttl():
    fresh = evaluate(StateSubject(refreshed_at=100.0), Ttl(50), now=120)
    stale = evaluate(StateSubject(refreshed_at=100.0), Ttl(50), now=200)
    assert fresh.is_fresh and not stale.is_fresh


# --- MV conforms (REQ-859) — behaviour-preserving vs the old TTL check ----------


def _mv(**kw):
    from provisa.mv.models import MVDefinition, MVStatus

    mv = MVDefinition(
        id="m1",
        source_tables=["t"],
        target_catalog="c",
        target_schema="s",
        refresh_interval=kw.get("interval", 100),
    )
    mv.status = kw.get("status", MVStatus.FRESH)
    mv.last_refresh_at = kw.get("refreshed_at", 1000.0)
    mv.last_error = kw.get("last_error")
    return mv


def test_mv_freshness_subject_reflects_state():
    mv = _mv(refreshed_at=1000.0, last_error=None)
    subj = mv.freshness_subject()
    assert subj.last_refresh_at() == 1000.0
    assert subj.last_refresh_ok() is True


def test_mv_fresh_within_ttl():
    assert _mv(refreshed_at=1000.0, interval=100).is_fresh_at(1050) is True


def test_mv_stale_past_ttl():
    assert _mv(refreshed_at=1000.0, interval=100).is_fresh_at(1200) is False


def test_mv_ttl_boundary_is_stale():
    # age == interval is not fresh (strict <), matching the prior semantics
    assert _mv(refreshed_at=1000.0, interval=100).is_fresh_at(1100) is False


def test_mv_non_fresh_status_short_circuits():
    from provisa.mv.models import MVStatus

    assert _mv(status=MVStatus.STALE, refreshed_at=1000.0).is_fresh_at(1050) is False
    assert _mv(status=MVStatus.REFRESHING, refreshed_at=1000.0).is_fresh_at(1050) is False


def test_mv_never_refreshed_is_not_fresh():
    assert _mv(refreshed_at=None).is_fresh_at(1050) is False


# --- API fills conform (REQ-859) — the wired stale_hashes path ----------------------


class _FillConn:
    """An engine session whose fill table exists and holds one fetch time per argument set."""

    dialect = "duckdb"

    def __init__(self, cached: dict[str, float]) -> None:
        self._cached = cached
        self._last = ""

    def execute(self, sql, params=None):
        self._last = sql
        return self

    def fetchall(self):
        if self._last.startswith("SELECT 1 "):
            return [(1,)]
        return list(self._cached.items())


def _fill_table():
    from provisa.api_source import fill_cache
    from provisa.api_source.engine_cache import CacheLocation

    fill_cache._mem_fresh.clear()
    return fill_cache.FillTable(CacheLocation("c", "sch", "relational"), "tbl", ())


def test_a_fill_fetched_recently_is_fresh():
    from provisa.api_source import fill_cache

    conn = _FillConn({"hash": time.time() - 10})
    assert fill_cache.stale_hashes(conn, _fill_table(), ["hash"], ttl=300) == []


def test_a_fill_fetched_before_its_ttl_is_stale():
    from provisa.api_source import fill_cache

    conn = _FillConn({"hash": time.time() - 600})
    assert fill_cache.stale_hashes(conn, _fill_table(), ["hash"], ttl=300) == ["hash"]


def test_an_argument_set_never_fetched_is_stale():
    from provisa.api_source import fill_cache

    assert fill_cache.stale_hashes(_FillConn({}), _fill_table(), ["hash"], ttl=300) == ["hash"]
