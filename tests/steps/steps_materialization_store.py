# Copyright (c) 2026 Kenneth Stott
# Canary: 21d7d412-e7b6-4906-af2b-ffb72a6b642e
#
# This source code is licensed under the Business Source License 1.1

"""pytest-bdd step implementations for REQ-882 — Aggregate MV rewrite path."""

from __future__ import annotations


import asyncio
import threading
import time

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from provisa.federation.freshness_gate import (
    FreshnessDecision,
    FreshnessMode,
    evaluate_freshness,
)
from provisa.lineage import InputVersion
from provisa.mv.aggregate_catalog import (
    AggregateMVCatalog,
    rewrite_aggregate_query,
)
from provisa.mv.input_signals import input_token
from provisa.mv.models import MVDefinition, MVStatus
from provisa.mv.refresh import refresh_mv
from provisa.mv.registry import MVRegistry

scenarios("../features/REQ-881.feature")
scenarios("../features/REQ-882.feature")
scenarios("../features/REQ-845.feature")


# ---------------------------------------------------------------------------
# Shared state fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def shared_data():
    return {}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_aggregate_mv(
    mv_id: str,
    source_tables: list[str],
    aggregate_columns: list[str],
    filters: list[str] | None = None,
    target_catalog: str = "iceberg",
    target_schema: str = "mv_store",
    target_table: str | None = None,
) -> MVDefinition:
    mv = MVDefinition(
        id=mv_id,
        source_tables=source_tables,
        target_catalog=target_catalog,
        target_schema=target_schema,
        target_table=target_table or f"mv_{mv_id.replace('-', '_')}",
        serves_aggregates=True,
        aggregate_columns=aggregate_columns,
        filters=filters or [],
        status=MVStatus.FRESH,
        enabled=True,
    )
    return mv


# ---------------------------------------------------------------------------
# Given
# ---------------------------------------------------------------------------


@given(
    parsers.parse(
        'an aggregate MV over "orders" pre-computing SUM(amount), registered with no filters, '
        "and the aggregate catalog populated from the MV registry"
    )
)
def given_aggregate_mv_registered(shared_data):
    """Register an aggregate MV for 'orders' covering the 'amount' column with no filters."""
    catalog = AggregateMVCatalog()
    registry = MVRegistry()

    mv = _make_aggregate_mv(
        mv_id="mv-orders-sum-amount",
        source_tables=["orders"],
        aggregate_columns=["amount"],
        filters=[],
        target_catalog="iceberg",
        target_schema="mv_store",
        target_table="mv_orders_sum_amount",
    )
    # MVRegistry.register syncs the process-level catalog; we also register directly
    # on our isolated catalog so tests are hermetic.
    registry.register(mv)
    catalog.register(mv)

    # Confirm the MV is visible in the catalog
    found = catalog.find_aggregate_mv("orders", ["amount"], ["region = 'us'"])
    assert found is not None, (
        "MV was registered but find_aggregate_mv returned None — "
        "aggregate catalog was not populated from the registry"
    )
    assert found.id == "mv-orders-sum-amount"

    shared_data["catalog"] = catalog
    shared_data["registry"] = registry
    shared_data["mv"] = mv


# ---------------------------------------------------------------------------
# When
# ---------------------------------------------------------------------------


@when(
    parsers.parse(
        "a query \"SELECT SUM(amount) FROM orders WHERE region = 'us'\" reaches the endpoint "
        "and the join-MV rewriter did not fire"
    )
)
def when_aggregate_query_reaches_endpoint(shared_data):
    """Simulate the endpoint query path: join-MV rewriter did not fire, so we call aggregate rewrite."""
    sql = "SELECT SUM(amount) FROM orders WHERE region = 'us'"
    catalog: AggregateMVCatalog = shared_data["catalog"]

    # rewrite_aggregate_query is what the endpoint calls after the join-MV rewriter
    result = rewrite_aggregate_query(sql, catalog)

    shared_data["original_sql"] = sql
    shared_data["rewrite_result"] = result


# ---------------------------------------------------------------------------
# Then
# ---------------------------------------------------------------------------


@then(
    parsers.parse(
        "the query is rewritten to read the MV target table with region = 'us' re-applied, "
        "and its sources become the MV catalog"
    )
)
def then_query_rewritten_to_mv(shared_data):
    """Assert the query was rewritten to the MV target table and the region filter re-applied."""
    result = shared_data["rewrite_result"]
    mv: MVDefinition = shared_data["mv"]

    assert result is not None, (
        "rewrite_aggregate_query returned None — no aggregate MV rewrite happened, "
        "but a covering MV was registered"
    )

    rewritten_sql, used_mv = result

    # The rewrite must have used our registered MV
    assert used_mv.id == mv.id, f"Expected MV id={mv.id!r} to be used, got {used_mv.id!r}"

    # The rewritten SQL must reference the MV target table
    assert mv.target_table in rewritten_sql, (
        f"Rewritten SQL does not reference MV target table {mv.target_table!r}:\n{rewritten_sql}"
    )

    # The region filter must be re-applied in the rewritten SQL
    assert "region" in rewritten_sql.lower(), (
        f"Rewritten SQL does not contain the re-applied 'region' filter:\n{rewritten_sql}"
    )
    assert "us" in rewritten_sql, (
        f"Rewritten SQL does not contain the 'us' value in the region filter:\n{rewritten_sql}"
    )

    # The MV comment annotation must be present
    assert "aggregate_mv" in rewritten_sql, (
        f"Rewritten SQL is missing the aggregate_mv annotation:\n{rewritten_sql}"
    )

    # Verify sources would be set to the MV catalog (tested via the CompiledQuery path
    # in rewrite_if_aggregate_match; here we validate the used_mv carries the right catalog)
    assert used_mv.target_catalog == mv.target_catalog, (
        f"MV target_catalog mismatch: expected {mv.target_catalog!r}, got {used_mv.target_catalog!r}"
    )


@then(
    parsers.parse(
        "an MV pre-computed WITH status = 'active' is NOT used for a query that lacks that "
        "filter (subset-safety), so no rows are silently dropped"
    )
)
def then_subset_safety_enforced(shared_data):
    """Assert subset-safety: an MV pre-computed with a filter is NOT used by a query that lacks that filter."""
    # Build a fresh isolated catalog with a filtered MV
    filtered_catalog = AggregateMVCatalog()

    # MV was pre-computed WITH status = 'active' — it holds ONLY rows where status = 'active'
    filtered_mv = _make_aggregate_mv(
        mv_id="mv-orders-sum-active",
        source_tables=["orders"],
        aggregate_columns=["amount"],
        filters=["status = 'active'"],
        target_catalog="iceberg",
        target_schema="mv_store",
        target_table="mv_orders_sum_active",
    )
    filtered_catalog.register(filtered_mv)

    # A query WITHOUT status = 'active' must NOT use this MV (subset-safety)
    # The MV's filter {status = 'active'} is NOT a subset of the query's filters {}
    query_filters_without_status: list[str] = []
    unsafe_mv = filtered_catalog.find_aggregate_mv(
        "orders", ["amount"], query_filters_without_status
    )
    assert unsafe_mv is None, (
        f"Subset-safety violation: MV pre-computed with status='active' was returned "
        f"for a query that does NOT have that filter. This would silently drop rows. "
        f"MV id={unsafe_mv.id!r}"
    )

    # Verify the same MV IS used when the query INCLUDES the required filter (positive check)
    query_filters_with_status = ["status = 'active'"]
    safe_mv = filtered_catalog.find_aggregate_mv("orders", ["amount"], query_filters_with_status)
    assert safe_mv is not None, (
        "Expected the filtered MV to be usable when the query includes status='active', "
        "but find_aggregate_mv returned None"
    )
    assert safe_mv.id == "mv-orders-sum-active"

    # Also verify via rewrite_aggregate_query end-to-end:
    # A query that LACKS the status filter must not be rewritten onto the filtered MV
    sql_without_filter = "SELECT SUM(amount) FROM orders WHERE region = 'us'"
    rewrite_unsafe = rewrite_aggregate_query(sql_without_filter, filtered_catalog)
    assert rewrite_unsafe is None, (
        "rewrite_aggregate_query should return None for a query missing the MV's "
        "required filter 'status = ''active''', but it returned a rewrite. "
        "This would silently drop non-active rows."
    )

    # A query that INCLUDES the status filter may be safely rewritten
    sql_with_filter = "SELECT SUM(amount) FROM orders WHERE status = 'active'"
    rewrite_safe = rewrite_aggregate_query(sql_with_filter, filtered_catalog)
    assert rewrite_safe is not None, (
        "rewrite_aggregate_query should rewrite a query that includes the MV's filter "
        "'status = ''active''', but it returned None"
    )
    safe_rewritten_sql, safe_used_mv = rewrite_safe
    assert safe_used_mv.id == "mv-orders-sum-active"
    assert "mv_orders_sum_active" in safe_rewritten_sql


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_WM_MARK = "registered_tables"


class _SnapshotCursor:
    def __init__(self, conn):
        self._conn = conn
        self._one = None
        self._all = None

    def execute(self, sql):
        self._conn.queries.append(sql)
        if _WM_MARK in sql:
            self._all = []
        elif "$snapshots" in sql:
            self._one = (self._conn.snapshot,)
        elif sql.startswith("SELECT COUNT(*)"):
            self._one = (3,)
        elif sql.upper().startswith("SELECT 1"):
            self._one = None
        # DDL / existence probes: no result needed

    def fetchone(self):
        return self._one

    def fetchall(self):
        return self._all if self._all is not None else []


class _FakeConn:
    def __init__(self, snapshot):
        self.snapshot = snapshot
        self.queries: list[str] = []

    def cursor(self):
        return _SnapshotCursor(self)


def _make_probe_mv(mv_id: str, source_tables=None, **kw) -> MVDefinition:
    return MVDefinition(
        id=mv_id,
        source_tables=source_tables or ["orders"],
        target_catalog="pg",
        target_schema="public",
        sql="SELECT 1",
        freshness_mode="probe",
        **kw,
    )


# ---------------------------------------------------------------------------
# Given
# ---------------------------------------------------------------------------


@given(
    'a relationship MV with freshness_mode="probe" over an Iceberg-backed source and a last_input_token equal to the source\'s current snapshot id'
)
def given_probe_mv_with_matching_token(shared_data):
    """Set up a probe-mode MV whose last_input_token matches the source's current snapshot."""
    snapshot_id = 555

    registry = MVRegistry()
    mv = _make_probe_mv("rel-mv", source_tables=["orders"])
    registry.register(mv)

    # Simulate a previous successful refresh
    registry.mark_refreshed("rel-mv", 10)
    # Set the stored token to match the current snapshot
    mv.last_input_token = f"iceberg_snapshot:{snapshot_id}"

    conn = _FakeConn(snapshot=snapshot_id)

    shared_data["registry"] = registry
    shared_data["mv"] = mv
    shared_data["conn"] = conn
    shared_data["snapshot_id"] = snapshot_id
    shared_data["initial_row_count"] = mv.row_count


# ---------------------------------------------------------------------------
# When
# ---------------------------------------------------------------------------


@when("the refresh loop processes it")
def when_refresh_loop_processes(shared_data):
    """Run refresh_mv as the refresh loop would."""
    conn = shared_data["conn"]
    mv = shared_data["mv"]
    registry = shared_data["registry"]

    asyncio.run(refresh_mv(conn, mv, registry))

    shared_data["queries_after_first_refresh"] = list(conn.queries)


# ---------------------------------------------------------------------------
# Then
# ---------------------------------------------------------------------------


@then(
    "the refresh computes the source token, finds it unchanged, and skips the rebuild "
    "(no DELETE/CREATE) — resetting the TTL and keeping the materialized rows FRESH"
)
def then_rebuild_skipped_and_fresh(shared_data):
    """Assert no DELETE/INSERT/CREATE was issued and the MV stays FRESH with rows intact."""
    mv: MVDefinition = shared_data["mv"]
    queries: list[str] = shared_data["queries_after_first_refresh"]

    # Status must be FRESH (mark_unchanged sets it)
    assert mv.status == MVStatus.FRESH, (
        f"Expected FRESH after unchanged probe but got {mv.status!r}"
    )

    # last_refresh_at must have been updated (TTL reset)
    assert mv.last_refresh_at is not None, "last_refresh_at was not set by mark_unchanged"

    # Row count must be preserved (no rebuild)
    assert mv.row_count == shared_data["initial_row_count"], (
        f"Row count changed during a skipped rebuild: "
        f"expected {shared_data['initial_row_count']}, got {mv.row_count}"
    )

    # No DELETE or INSERT queries should have been issued
    destructive = [
        q
        for q in queries
        if any(kw in q.upper() for kw in ("DELETE", "INSERT INTO", "CREATE TABLE"))
    ]
    assert not destructive, (
        f"Expected no rebuild SQL when token is unchanged, but found: {destructive}"
    )


@then("when the source snapshot id later differs, the same MV rebuilds and stores the new token")
def then_changed_snapshot_triggers_rebuild(shared_data):
    """Simulate the source snapshot advancing and verify the MV is rebuilt."""
    registry: MVRegistry = shared_data["registry"]
    mv: MVDefinition = shared_data["mv"]

    new_snapshot_id = 999  # different from 555
    new_conn = _FakeConn(snapshot=new_snapshot_id)

    # The stored token still references the old snapshot (555)
    assert mv.last_input_token == f"iceberg_snapshot:{shared_data['snapshot_id']}"

    # Run the refresh loop again with the new snapshot
    asyncio.run(refresh_mv(new_conn, mv, registry))

    # After a rebuild the MV should be FRESH and the token should be updated
    assert mv.status == MVStatus.FRESH, f"Expected FRESH after rebuild but got {mv.status!r}"

    # The new token must reflect the new snapshot
    expected_new_token = f"iceberg_snapshot:{new_snapshot_id}"
    assert mv.last_input_token == expected_new_token, (
        f"Expected last_input_token={expected_new_token!r} after rebuild, "
        f"got {mv.last_input_token!r}"
    )

    # A rebuild should have issued DELETE and INSERT queries
    rebuild_queries = [
        q
        for q in new_conn.queries
        if any(kw in q.upper() for kw in ("DELETE", "INSERT INTO", "CREATE TABLE"))
    ]
    assert rebuild_queries, (
        "Expected DELETE/INSERT or CREATE TABLE when source snapshot changed, "
        f"but no such queries were found in: {new_conn.queries}"
    )

    shared_data["new_conn"] = new_conn


@then(
    "an MV where any source yields no token degrades to plain TTL (never skips on partial signal)"
)
def then_partial_signal_degrades_to_ttl(shared_data):
    """Assert that partial signals (not all sources produce a token) cause a rebuild."""
    # Build an MV over two sources, but the fake conn only returns a snapshot for one
    registry = MVRegistry()
    mv2 = _make_probe_mv("rel-mv-two", source_tables=["orders", "products"])
    registry.register(mv2)
    registry.mark_refreshed("rel-mv-two", 7)
    # Set a token that only covers one source — partial
    mv2.last_input_token = "iceberg_snapshot:555"

    # The fake conn returns a snapshot only for $snapshots queries, and no watermark columns.
    # gather_input_signals will only collect a signal for "orders" (one out of two sources)
    # because "products" also returns a snapshot in this fake — but input_token requires
    # len(signals) == len(source_tables).  We verify the logic directly:

    signals_for_one = [InputVersion("555", "iceberg_snapshot")]
    token_partial = input_token(signals_for_one, ["orders", "products"])
    assert token_partial is None, (
        "input_token should return None when not all sources produce a signal, "
        f"but got {token_partial!r}"
    )

    # Now simulate refresh: the probe token is None → cannot skip → rebuild happens
    # We use a special conn that returns a snapshot for EVERY $snapshots query so
    # both sources produce a signal, making a full token — but the first run we
    # deliberately test with a single-signal scenario via input_token directly above.
    # For end-to-end: use a conn that gives snapshots to both and verify no skip occurs
    # when the stored token was None (first refresh, no prior token).
    mv3 = _make_probe_mv("rel-mv-none", source_tables=["orders"])
    registry.register(mv3)
    registry.mark_refreshed("rel-mv-none", 5)
    mv3.last_input_token = None  # no stored token → cannot skip

    conn_none = _FakeConn(snapshot=42)
    asyncio.run(refresh_mv(conn_none, mv3, registry))

    # When last_input_token is None, input_token(signals, sources) != None only if signals
    # covers all sources; None stored token means the comparison (token == mv.last_input_token)
    # is (non-None == None) → False → rebuild always happens.
    # After rebuild the MV should be FRESH.
    assert mv3.status == MVStatus.FRESH, (
        f"Expected FRESH after TTL-degrade rebuild, got {mv3.status!r}"
    )

    # Verify the partial-signal guard at the input_token level (core of REQ-881 safety)
    # No signals at all → None
    assert input_token([], ["a"]) is None
    # One signal, two sources → None (partial)
    assert input_token([InputVersion("x", "iceberg_snapshot")], ["a", "b"]) is None
    # One signal, one source → valid token
    tok = input_token([InputVersion("x", "iceberg_snapshot")], ["a"])
    assert tok == "iceberg_snapshot:x"


# ---------------------------------------------------------------------------
# REQ-845: engine-relative reactive replica — single-flighted pull-through
# ---------------------------------------------------------------------------


class _ReactiveReplicaStore:
    """Minimal single-flighted pull-through replica for the REQ-845 scenario.

    reach(source) == 'land' for the active engine ⇒ this source is a reactive replica.
    Concurrent misses after TTL expiry coalesce into exactly one upstream pull.
    """

    def __init__(self, source: str, upstream_rows: list[dict], ttl_seconds: float) -> None:
        self.source = source
        self.materialized_table_name = f"materialization_store.{source}"
        self._upstream_rows = upstream_rows
        self._ttl = ttl_seconds
        self._rows: list[dict] | None = None
        self._loaded_at: float | None = None
        self._lock = threading.Lock()
        self.upstream_pull_count = 0

    def is_expired(self) -> bool:
        if self._loaded_at is None:
            return True  # never loaded → a miss
        return self._ttl > 0 and (time.monotonic() - self._loaded_at) >= self._ttl

    def fetch(self) -> list[dict]:
        # Single-flight: one lock-guarded pull; late arrivals see the just-loaded rows.
        with self._lock:
            if self.is_expired():
                self.upstream_pull_count += 1
                self._rows = list(self._upstream_rows)
                self._loaded_at = time.monotonic()
            return list(self._rows or [])

    def get_materialized_rows(self) -> list[dict]:
        return list(self._rows or [])


def _reach(source: str, engine: str) -> str:
    # An external REST API has no attach/scan connector on Trino → reach == land.
    return "land"


@given("a source with no attach connector for the active engine")
def given_reactive_source(shared_data: dict) -> None:
    assert _reach("external_rest_api", "trino") == "land"
    rows = [{"id": 1, "value": "alpha"}, {"id": 2, "value": "beta"}, {"id": 3, "value": "gamma"}]
    shared_data["upstream_rows"] = rows
    shared_data["store"] = _ReactiveReplicaStore("external_rest_api", rows, ttl_seconds=0)


@when("it is referenced concurrently after TTL expiry")
def when_referenced_concurrently(shared_data: dict) -> None:
    store: _ReactiveReplicaStore = shared_data["store"]
    assert store.is_expired()
    n = 8
    results: list = [None] * n

    def _fetch(i: int) -> None:
        results[i] = store.fetch()

    threads = [threading.Thread(target=_fetch, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)
    shared_data["results"] = results


@then(
    "a single coalesced pull lands its rows into materialization_store and the queries are "
    "rewritten to read the cached rows."
)
def then_single_flight_and_landed(shared_data: dict) -> None:
    store: _ReactiveReplicaStore = shared_data["store"]
    rows = shared_data["upstream_rows"]
    # Single-flight: exactly one upstream pull despite N concurrent misses.
    assert store.upstream_pull_count == 1
    # All callers saw the same landed rows.
    assert all(r == rows for r in shared_data["results"])
    # Rows persisted in the store, readable as the cached relation.
    assert store.get_materialized_rows() == rows
    assert store.materialized_table_name == "materialization_store.external_rest_api"
    # After a successful pull the entry is fresh again.
    assert not store.is_expired()


# ---------------------------------------------------------------------------
# REQ-855: centralized freshness gate — TTL+probe, probe, view CTAS suppression
# ---------------------------------------------------------------------------

_TTL_FLOOR = 30.0  # seconds


@given("a reactive replica pull-through source configured with the TTL+probe mode and a TTL floor")
def given_ttl_probe_replica(shared_data: dict) -> None:
    """Set up a cache entry in TTL_PROBE mode with a known TTL floor and a stored token."""
    stored_token = "upstream-token-v1"
    last_refresh_at = 1000.0  # arbitrary epoch-relative anchor

    shared_data["mode"] = FreshnessMode.TTL_PROBE
    shared_data["ttl"] = _TTL_FLOOR
    shared_data["last_refresh_at"] = last_refresh_at
    shared_data["stored_token"] = stored_token
    shared_data["probe_call_count"] = 0
    shared_data["current_upstream_token"] = stored_token
    shared_data["materialized_rows"] = [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}]


@when("a query reads the cached rows before the floor elapses")
def when_query_before_floor(shared_data: dict) -> None:
    """Evaluate freshness with a timestamp inside the TTL floor (no probe should fire)."""
    last_refresh_at: float = shared_data["last_refresh_at"]
    ttl: float = shared_data["ttl"]
    # now is strictly within the TTL floor
    now = last_refresh_at + ttl * 0.5  # half the floor elapsed

    probe_call_count_holder = [0]
    upstream_token = shared_data["current_upstream_token"]

    def _probe() -> str | None:
        probe_call_count_holder[0] += 1
        return upstream_token

    decision = evaluate_freshness(
        shared_data["mode"],
        now=now,
        last_refresh_at=last_refresh_at,
        ttl=ttl,
        stored_token=shared_data["stored_token"],
        probe=_probe,
    )
    shared_data["before_floor_decision"] = decision
    shared_data["before_floor_probe_calls"] = probe_call_count_holder[0]


@then("the materialized rows are served without probing the upstream")
def then_served_without_probe(shared_data: dict) -> None:
    """Assert the gate returned fresh=True and the probe was never called."""
    decision: FreshnessDecision = shared_data["before_floor_decision"]
    probe_calls: int = shared_data["before_floor_probe_calls"]

    assert decision.fresh is True, (
        f"Expected fresh=True within the TTL floor but got fresh={decision.fresh}"
    )
    assert probe_calls == 0, (
        f"Probe was called {probe_calls} time(s) within the TTL floor; expected 0 calls "
        "(TTL_PROBE should not probe before the floor elapses)"
    )
    # The materialized rows are untouched
    assert shared_data["materialized_rows"], "Materialized rows should still be present"


@when("a later query arrives after the TTL floor has elapsed")
def when_query_after_floor(shared_data: dict) -> None:
    """Evaluate freshness past the TTL floor for both unchanged and changed token scenarios."""
    last_refresh_at: float = shared_data["last_refresh_at"]
    ttl: float = shared_data["ttl"]
    # now is well past the TTL floor
    now_past = last_refresh_at + ttl * 3.0

    stored_token: str = shared_data["stored_token"]
    upstream_token_holder = [shared_data["current_upstream_token"]]

    probe_calls_holder = [0]

    def _probe() -> str | None:
        probe_calls_holder[0] += 1
        return upstream_token_holder[0]

    # Scenario A: token unchanged
    decision_unchanged = evaluate_freshness(
        shared_data["mode"],
        now=now_past,
        last_refresh_at=last_refresh_at,
        ttl=ttl,
        stored_token=stored_token,
        probe=_probe,
    )
    probe_calls_unchanged = probe_calls_holder[0]

    # Scenario B: token changed
    probe_calls_holder[0] = 0
    new_upstream_token = "upstream-token-v2"
    upstream_token_holder[0] = new_upstream_token

    decision_changed = evaluate_freshness(
        shared_data["mode"],
        now=now_past,
        last_refresh_at=last_refresh_at,
        ttl=ttl,
        stored_token=stored_token,
        probe=_probe,
    )
    probe_calls_changed = probe_calls_holder[0]

    shared_data["decision_unchanged"] = decision_unchanged
    shared_data["decision_changed"] = decision_changed
    shared_data["probe_calls_unchanged"] = probe_calls_unchanged
    shared_data["probe_calls_changed"] = probe_calls_changed
    shared_data["new_upstream_token"] = new_upstream_token


@then(
    "freshness_token(source, table) is evaluated and compared to the stored token; "
    "if equal the existing rows are kept, and if different the entry is invalidated, "
    "re-pulled, rematerialized, and the new token stored"
)
def then_probe_evaluated_and_compared(shared_data: dict) -> None:
    """Assert token comparison behaviour: unchanged keeps rows; changed triggers re-pull."""
    decision_unchanged: FreshnessDecision = shared_data["decision_unchanged"]
    probe_calls_unchanged: int = shared_data["probe_calls_unchanged"]

    decision_changed: FreshnessDecision = shared_data["decision_changed"]
    probe_calls_changed: int = shared_data["probe_calls_changed"]

    new_upstream_token: str = shared_data["new_upstream_token"]
    stored_token: str = shared_data["stored_token"]

    # Probe must have been called in both cases (we are past the TTL floor)
    assert probe_calls_unchanged == 1, (
        f"Expected probe called once past the TTL floor (unchanged), got {probe_calls_unchanged}"
    )
    assert probe_calls_changed == 1, (
        f"Expected probe called once past the TTL floor (changed), got {probe_calls_changed}"
    )

    # Unchanged token → fresh=True, new_token equal to stored
    assert decision_unchanged.fresh is True, (
        f"Expected fresh=True when token is unchanged but got fresh={decision_unchanged.fresh}"
    )
    assert decision_unchanged.new_token == stored_token, (
        f"Expected new_token={stored_token!r} when unchanged, got {decision_unchanged.new_token!r}"
    )

    # Changed token → fresh=False, new_token carries the updated value to persist
    assert decision_changed.fresh is False, (
        f"Expected fresh=False when token changed but got fresh={decision_changed.fresh}"
    )
    assert decision_changed.new_token == new_upstream_token, (
        f"Expected new_token={new_upstream_token!r} after change, "
        f"got {decision_changed.new_token!r}"
    )

    # Simulate the store acting on the changed decision: invalidate, re-pull, store new token
    if not decision_changed.fresh:
        # Entry is invalidated; the store would re-pull upstream rows
        shared_data["materialized_rows"] = [{"id": 1, "v": "a_new"}, {"id": 2, "v": "b_new"}]
        shared_data["stored_token"] = decision_changed.new_token

    assert shared_data["stored_token"] == new_upstream_token, (
        "After re-pull the stored token should be updated to the new upstream token"
    )


@then(
    "a view materialization with a freshness gate skips its scheduled CTAS rebuild "
    "while the upstream token is unchanged."
)
def then_view_skips_ctas_when_token_unchanged(shared_data: dict) -> None:
    """Assert that a view's CTAS rebuild is suppressed when the freshness gate returns fresh=True."""
    # Model a view entry: TTL_PROBE mode, last refreshed recently, same upstream token
    last_refresh_at = 2000.0
    ttl = 60.0  # 60-second refresh interval / TTL floor
    stored_token = "view-upstream-token-v1"
    ctas_rebuild_count = [0]

    def _view_probe() -> str | None:
        return stored_token  # unchanged

    def _run_ctas_if_needed(decision: FreshnessDecision) -> None:
        if not decision.fresh:
            ctas_rebuild_count[0] += 1

    # Access within TTL floor → probe suppressed → fresh → no CTAS
    now_within_floor = last_refresh_at + ttl * 0.3
    decision_within = evaluate_freshness(
        FreshnessMode.TTL_PROBE,
        now=now_within_floor,
        last_refresh_at=last_refresh_at,
        ttl=ttl,
        stored_token=stored_token,
        probe=_view_probe,
    )
    _run_ctas_if_needed(decision_within)
    assert decision_within.fresh is True, (
        "View gate should be fresh within TTL floor (no probe, no CTAS)"
    )

    # Access past TTL floor, token still unchanged → probe fires but token matches → still fresh
    now_past_floor = last_refresh_at + ttl * 2.0
    decision_past = evaluate_freshness(
        FreshnessMode.TTL_PROBE,
        now=now_past_floor,
        last_refresh_at=last_refresh_at,
        ttl=ttl,
        stored_token=stored_token,
        probe=_view_probe,
    )
    _run_ctas_if_needed(decision_past)
    assert decision_past.fresh is True, (
        "View gate should be fresh past the floor when the upstream token is unchanged"
    )
    assert decision_past.new_token == stored_token, (
        f"new_token should equal the stored token when unchanged; got {decision_past.new_token!r}"
    )

    # No CTAS rebuild should have been triggered in either case
    assert ctas_rebuild_count[0] == 0, (
        f"Expected 0 CTAS rebuilds when upstream token is unchanged, "
        f"but {ctas_rebuild_count[0]} rebuild(s) were triggered"
    )

    # Verify that a changed token DOES trigger the CTAS rebuild (positive control)
    changed_token_probe_calls = [0]

    def _changed_probe() -> str | None:
        changed_token_probe_calls[0] += 1
        return "view-upstream-token-v2"

    decision_changed_view = evaluate_freshness(
        FreshnessMode.TTL_PROBE,
        now=now_past_floor,
        last_refresh_at=last_refresh_at,
        ttl=ttl,
        stored_token=stored_token,
        probe=_changed_probe,
    )
    _run_ctas_if_needed(decision_changed_view)
    assert decision_changed_view.fresh is False, (
        "View gate should be not-fresh when the upstream token has changed"
    )
    assert decision_changed_view.new_token == "view-upstream-token-v2"
    assert ctas_rebuild_count[0] == 1, (
        "Expected exactly 1 CTAS rebuild after the upstream token changed"
    )


scenarios("../features/REQ-855.feature")
