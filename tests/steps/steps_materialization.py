# Copyright (c) 2026 Kenneth Stott
# Canary: 743b3b78-320c-4779-9e3d-41c2eb56f7dc
#
# This source code is licensed under the Business Source License 1.1

import time
import uuid
from typing import Any, Dict

import pytest
from pytest_bdd import given, scenarios, then, when

scenarios("../features/REQ-930.feature")


# ---------------------------------------------------------------------------
# Shared state fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def shared_data() -> Dict[str, Any]:
    return {}


# ---------------------------------------------------------------------------
# Helpers / in-process stubs for the federation engine
# ---------------------------------------------------------------------------


class _FederationEngine:
    """Minimal in-process federation engine stub used by step implementations."""

    def __init__(self):
        self._sources: Dict[str, Dict[str, Any]] = {}

    def register_source(self, source_id: str, *, reachable: bool) -> Dict[str, Any]:
        record = {
            "source_id": source_id,
            "reachable": reachable,
            "materialized": None,  # will be set by _decide_materialization
            "perf_cache_ttl": None,
            "refresh_policy": None,
            "landed_at": None,
        }
        self._decide_materialization(record)
        self._sources[source_id] = record
        return record

    def _decide_materialization(self, record: Dict[str, Any]) -> None:
        """
        Core REQ-930 logic:
        - Unreachable  -> materialized (mandatory), first land is unconditional.
        - Reachable    -> NOT materialized, queried live; optional perf-caching via TTL.
        """
        if not record["reachable"]:
            record["materialized"] = True
            record["refresh_policy"] = "change_signal"  # ttl/probe/push
            record["landed_at"] = time.time()
        else:
            record["materialized"] = False
            record["refresh_policy"] = "live"
            # perf-caching is optional and applied separately
            record["perf_cache_ttl"] = None

    def apply_perf_cache(self, source_id: str, ttl_seconds: int) -> None:
        record = self._sources[source_id]
        if not record["reachable"]:
            raise ValueError("Perf-caching is only applicable to reachable sources")
        record["perf_cache_ttl"] = ttl_seconds

    def query_live(self, source_id: str) -> Dict[str, Any]:
        record = self._sources[source_id]
        if not record["reachable"]:
            raise RuntimeError(f"Source {source_id} is not reachable; use materialized landing.")
        return {"source_id": source_id, "mode": "live", "data": "query_result"}

    def trigger_refresh(self, source_id: str, *, signal: str) -> Dict[str, Any]:
        record = self._sources[source_id]
        assert record["materialized"], "Refresh is only for materialized sources"
        assert signal in ("ttl", "probe", "push"), f"Unknown signal: {signal}"
        record["landed_at"] = time.time()
        return {"source_id": source_id, "refreshed_via": signal, "landed_at": record["landed_at"]}


@pytest.fixture
def federation_engine() -> _FederationEngine:
    return _FederationEngine()


# ---------------------------------------------------------------------------
# Step definitions - Scenario: REQ-930 default behaviour
# ---------------------------------------------------------------------------

# ---- Given steps -----------------------------------------------------------


@given("a source that the federation engine cannot reach live", target_fixture="shared_data")
def given_unreachable_source(federation_engine, shared_data):
    source_id = f"unreachable-{uuid.uuid4().hex[:8]}"
    shared_data["engine"] = federation_engine
    shared_data["unreachable_source_id"] = source_id
    shared_data["unreachable"] = True
    return shared_data


@given("a source that the engine can reach live", target_fixture="shared_data")
def given_reachable_source(shared_data):
    # The engine was already stored in shared_data by the first Given.
    federation_engine = shared_data["engine"]
    source_id = f"reachable-{uuid.uuid4().hex[:8]}"
    shared_data["reachable_source_id"] = source_id
    # Register the reachable source now so subsequent steps can use it.
    record = federation_engine.register_source(source_id, reachable=True)
    shared_data["reachable_record"] = record
    return shared_data


# ---- When steps ------------------------------------------------------------


@when("the source is first added", target_fixture="shared_data")
def when_source_first_added(shared_data):
    federation_engine: _FederationEngine = shared_data["engine"]
    source_id: str = shared_data["unreachable_source_id"]

    record = federation_engine.register_source(source_id, reachable=False)
    shared_data["unreachable_record"] = record
    return shared_data


@when("queries reference it", target_fixture="shared_data")
def when_queries_reference_reachable_source(shared_data):
    federation_engine: _FederationEngine = shared_data["engine"]
    source_id: str = shared_data["reachable_source_id"]

    query_result = federation_engine.query_live(source_id)
    shared_data["live_query_result"] = query_result
    return shared_data


# ---- Then steps ------------------------------------------------------------


@then("it is materialized/landed unconditionally")
def then_materialized_unconditionally(shared_data):
    record = shared_data["unreachable_record"]

    # The engine must have decided materialization = True without any user input.
    assert record["materialized"] is True, (
        f"Expected materialized=True for unreachable source, got {record['materialized']}"
    )
    assert record["landed_at"] is not None, (
        "Expected landed_at to be set on first unconditional landing"
    )
    # Confirm it was NOT the user who flipped a knob - the engine decided based on
    # reachability alone (no explicit materialization flag was passed by the test).
    assert record["reachable"] is False


@then("subsequent refreshes follow the change signal (ttl/probe/push)")
def then_subsequent_refreshes_follow_change_signal(shared_data):
    federation_engine: _FederationEngine = shared_data["engine"]
    source_id: str = shared_data["unreachable_source_id"]
    record = shared_data["unreachable_record"]

    # Policy must be change_signal-driven.
    assert record["refresh_policy"] == "change_signal", (
        f"Expected refresh_policy='change_signal', got {record['refresh_policy']!r}"
    )

    # Exercise each valid change-signal type.
    for signal in ("ttl", "probe", "push"):
        result = federation_engine.trigger_refresh(source_id, signal=signal)
        assert result["refreshed_via"] == signal, (
            f"Refresh via '{signal}' returned wrong signal: {result['refreshed_via']!r}"
        )
        assert result["landed_at"] is not None


@then("it is queried live without materialization")
def then_queried_live_without_materialization(shared_data):
    record = shared_data["reachable_record"]
    live_result = shared_data["live_query_result"]

    # Source must NOT be materialized.
    assert record["materialized"] is False, (
        f"Reachable source must not be materialized; got materialized={record['materialized']}"
    )
    # The query result must confirm live mode.
    assert live_result["mode"] == "live", f"Expected mode='live', got {live_result['mode']!r}"
    assert live_result["source_id"] == record["source_id"]


@then("optional perf-caching may be applied via TTL")
def then_optional_perf_caching_via_ttl(shared_data):
    federation_engine: _FederationEngine = shared_data["engine"]
    source_id: str = shared_data["reachable_source_id"]
    record = shared_data["reachable_record"]

    # Before applying, perf-cache TTL is None (optional, not mandatory).
    assert record["perf_cache_ttl"] is None, (
        "Perf-caching must be absent by default for reachable sources"
    )

    # Apply optional TTL-based perf-caching.
    ttl = 300  # 5 minutes
    federation_engine.apply_perf_cache(source_id, ttl_seconds=ttl)

    assert record["perf_cache_ttl"] == ttl, (
        f"Expected perf_cache_ttl={ttl}, got {record['perf_cache_ttl']}"
    )

    # Verify that applying perf-cache to an unreachable source raises an error
    # (it is only valid for reachable sources).
    unreachable_id = shared_data["unreachable_source_id"]
    with pytest.raises(ValueError, match="only applicable to reachable sources"):
        federation_engine.apply_perf_cache(unreachable_id, ttl_seconds=60)
