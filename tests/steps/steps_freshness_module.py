# Copyright (c) 2026 Kenneth Stott
# Canary: fc6c8d91-e860-44b3-94c5-697e12259275
#
# This source code is licensed under the Business Source License 1.1

"""BDD step implementations for REQ-859: Freshness Module — Source Connectors."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from pytest_bdd import given, scenarios, then, when

from provisa.freshness import FreshnessSubject, Ttl, evaluate
from provisa.freshness.adapters import StateSubject
from provisa.mv.models import MVDefinition, MVStatus
from provisa.api_source import fill_cache
from provisa.api_source.engine_cache import CacheLocation

scenarios("../features/REQ-859.feature")


@pytest.fixture
def shared_data():
    return {}


# ---------------------------------------------------------------------------
# Given
# ---------------------------------------------------------------------------


@given(
    "a materialized view and an API/pg cache entry, each with its own last-refresh state",
    target_fixture="shared_data",
)
def given_mv_and_pg_cache_with_refresh_state():
    """
    Create an MV and a pg-cache entry whose refresh timestamps are well-known
    so we can assert deterministic freshness outcomes.
    """
    # MV: last refreshed 50 s ago, TTL = 100 s → should be FRESH
    mv_refresh_epoch = 1_000.0
    mv = MVDefinition(
        id="bdd-mv-859",
        source_tables=["orders"],
        target_catalog="iceberg",
        target_schema="public",
        refresh_interval=100,
    )
    mv.status = MVStatus.FRESH
    mv.last_refresh_at = mv_refresh_epoch
    mv.last_error = None

    # pg-cache entry: cached 10 s ago, TTL = 300 s → should be fresh
    pg_cached_at = datetime.now(UTC) - timedelta(seconds=10)

    # Stale MV: last refreshed 200 s ago (beyond TTL of 100 s) → should be stale
    mv_stale = MVDefinition(
        id="bdd-mv-859-stale",
        source_tables=["orders"],
        target_catalog="iceberg",
        target_schema="public",
        refresh_interval=100,
    )
    mv_stale.status = MVStatus.FRESH
    mv_stale.last_refresh_at = mv_refresh_epoch
    mv_stale.last_error = None

    # Stale pg-cache entry: cached 600 s ago (beyond TTL of 300 s)
    pg_cached_at_stale = datetime.now(UTC) - timedelta(seconds=600)

    return {
        "mv": mv,
        "mv_refresh_epoch": mv_refresh_epoch,
        "mv_now_fresh": mv_refresh_epoch + 50,  # 50 s after refresh → fresh
        "mv_now_stale": mv_refresh_epoch + 200,  # 200 s after refresh → stale
        "pg_cached_at_fresh": pg_cached_at,
        "pg_cached_at_stale": pg_cached_at_stale,
        "pg_ttl": 300,
    }


# ---------------------------------------------------------------------------
# When
# ---------------------------------------------------------------------------


@when("their freshness is evaluated")
def when_freshness_is_evaluated(shared_data):
    """
    Evaluate freshness for both the MV and the pg-cache entry using the unified
    FreshnessPredicate (evaluate()).  Results are stored in shared_data so the
    Then step can assert them.
    """
    mv: MVDefinition = shared_data["mv"]

    # --- MV freshness subject ---
    mv_subject = mv.freshness_subject()
    shared_data["mv_subject"] = mv_subject

    ttl_mv = Ttl(mv.refresh_interval)
    result_mv_fresh = evaluate(mv_subject, ttl_mv, now=shared_data["mv_now_fresh"])
    result_mv_stale = evaluate(mv_subject, ttl_mv, now=shared_data["mv_now_stale"])
    shared_data["result_mv_fresh"] = result_mv_fresh
    shared_data["result_mv_stale"] = result_mv_stale

    # Also exercise the is_fresh_at lifecycle-gated path (FRESH status + TTL)
    shared_data["mv_is_fresh_at_true"] = mv.is_fresh_at(shared_data["mv_now_fresh"])
    shared_data["mv_is_fresh_at_false"] = mv.is_fresh_at(shared_data["mv_now_stale"])

    # --- pg-cache freshness subject (fresh case) ---
    pg_cached_epoch_fresh = shared_data["pg_cached_at_fresh"].replace(tzinfo=UTC).timestamp()
    pg_subject_fresh = StateSubject(refreshed_at=pg_cached_epoch_fresh)
    shared_data["pg_subject_fresh"] = pg_subject_fresh
    import time as _time

    now_epoch = _time.time()
    result_pg_fresh = evaluate(pg_subject_fresh, Ttl(shared_data["pg_ttl"]), now=now_epoch)
    shared_data["result_pg_fresh"] = result_pg_fresh

    # --- pg-cache freshness subject (stale case) ---
    pg_cached_epoch_stale = shared_data["pg_cached_at_stale"].replace(tzinfo=UTC).timestamp()
    pg_subject_stale = StateSubject(refreshed_at=pg_cached_epoch_stale)
    shared_data["pg_subject_stale"] = pg_subject_stale
    result_pg_stale = evaluate(pg_subject_stale, Ttl(shared_data["pg_ttl"]), now=now_epoch)
    shared_data["result_pg_stale"] = result_pg_stale


# ---------------------------------------------------------------------------
# Then
# ---------------------------------------------------------------------------


@then(
    "both expose that state as a FreshnessSubject and the TTL decision is produced by"
    " the one shared FreshnessPredicate, yielding the same fresh/stale result as the"
    " prior per-consumer checks."
)
def then_both_are_freshness_subjects_with_unified_predicate(shared_data):
    """
    Assert:
    1. MV and pg-cache both produce StateSubject instances that conform to the
       FreshnessSubject protocol.
    2. The unified evaluate() yields the expected fresh/stale decisions.
    3. MVDefinition.is_fresh_at() (lifecycle gate + TTL) agrees with evaluate().
    4. pg-cache's _is_fresh() internal path (via mock conn) delegates correctly.
    """
    # 1. Protocol conformance
    mv_subject: StateSubject = shared_data["mv_subject"]
    pg_subject_fresh: StateSubject = shared_data["pg_subject_fresh"]
    pg_subject_stale: StateSubject = shared_data["pg_subject_stale"]

    assert isinstance(mv_subject, FreshnessSubject), (
        "MV freshness_subject() must return a FreshnessSubject"
    )
    assert isinstance(pg_subject_fresh, FreshnessSubject), (
        "pg-cache StateSubject must conform to FreshnessSubject"
    )
    assert isinstance(pg_subject_stale, FreshnessSubject), (
        "pg-cache StateSubject (stale) must conform to FreshnessSubject"
    )

    # Verify the MV subject carries the correct refresh timestamp
    mv: MVDefinition = shared_data["mv"]
    assert mv_subject.last_refresh_at() == mv.last_refresh_at
    assert mv_subject.last_refresh_ok() is True  # last_error was None

    # 2. Unified predicate results
    assert shared_data["result_mv_fresh"].is_fresh is True, "MV evaluated within TTL must be fresh"
    assert shared_data["result_mv_stale"].is_fresh is False, "MV evaluated beyond TTL must be stale"
    assert shared_data["result_pg_fresh"].is_fresh is True, (
        "pg-cache entry cached 10 s ago (TTL=300) must be fresh"
    )
    assert shared_data["result_pg_stale"].is_fresh is False, (
        "pg-cache entry cached 600 s ago (TTL=300) must be stale"
    )

    # 3. MVDefinition.is_fresh_at lifecycle gate agrees with evaluate()
    assert shared_data["mv_is_fresh_at_true"] is True, (
        "is_fresh_at() within TTL should return True when status is FRESH"
    )
    assert shared_data["mv_is_fresh_at_false"] is False, (
        "is_fresh_at() beyond TTL should return False"
    )

    # Status gate: a non-FRESH status always yields False regardless of TTL
    mv_refreshing = MVDefinition(
        id="bdd-mv-859-refreshing",
        source_tables=["orders"],
        target_catalog="iceberg",
        target_schema="public",
        refresh_interval=100,
    )
    mv_refreshing.status = MVStatus.REFRESHING
    mv_refreshing.last_refresh_at = mv.last_refresh_at
    mv_refreshing.last_error = None
    assert mv_refreshing.is_fresh_at(shared_data["mv_now_fresh"]) is False, (
        "REFRESHING status must short-circuit to False"
    )

    # 4. The API cache's freshness check (its parameter-set fills, api_source.fill_cache)
    #    delegates the TTL decision to evaluate() — verified over a stand-in engine session.
    _verify_api_fills_delegate(shared_data)


class _FillConn:
    """An engine session whose fill table exists and holds one fetch time per argument set."""

    dialect = "duckdb"

    def __init__(self, cached_at) -> None:
        self._cached_at = cached_at
        self._last = ""

    def execute(self, sql, params=None):
        self._last = sql
        return self

    def fetchall(self):
        if self._last.startswith("SELECT 1 "):
            return [(1,)]
        return [("phash", self._cached_at.timestamp())]


def _verify_api_fills_delegate(shared_data):
    """``fill_cache.stale_hashes`` reads each argument set's fetch time and leaves the TTL
    decision to the shared evaluate()."""
    table = fill_cache.FillTable(CacheLocation("c", "sch", "relational"), "tbl", ())
    fill_cache._mem_fresh.clear()
    fresh = _FillConn(shared_data["pg_cached_at_fresh"])
    assert fill_cache.stale_hashes(fresh, table, ["phash"], ttl=300) == [], (
        "a recently fetched API fill must be fresh"
    )
    fill_cache._mem_fresh.clear()
    stale = _FillConn(shared_data["pg_cached_at_stale"])
    assert fill_cache.stale_hashes(stale, table, ["phash"], ttl=300) == ["phash"], (
        "an API fill fetched before its TTL must be stale"
    )
    fill_cache._mem_fresh.clear()
