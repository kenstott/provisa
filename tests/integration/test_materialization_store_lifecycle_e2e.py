# Copyright (c) 2026 Kenneth Stott
# Canary: 1f6b3d08-7c24-4e91-a5d2-9e0c4b73a186
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-844/845/847/848/855/874: the materialization_store lifecycle, end to end.

The store's policy modules are each unit-covered, but nothing exercises them COMPOSED against
a real durable, federatable store — the abstraction the store IS. This drives the full cycle
against an on-disk DuckDB store (FEDERATABLE + DURABLE) using the real policy modules as the
runner (the production executor wiring is still in-progress; the modules are the contract):

  read-through miss -> single-flight pull -> land (write face) -> serve from store   (REQ-845/848)
  read-through discipline: pull fail + no fresh data -> HARD_ERROR; stale only by policy (REQ-847)
  write-back: mutation targets upstream and INVALIDATES the store entry               (REQ-847)
  freshness gate: TTL floor serves cached; probe token unchanged keeps rows, changed re-pulls (REQ-855)
  delta refresh: monotonic cursor upserts only changed rows and advances the cursor; empty = no-op (REQ-874)

DURABLE is proved literally: rows are read back through a FRESH connection to the same store file.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from provisa.federation.delta import (
    advance_cursor,
    delta_applies,
    delta_is_fresh,
    has_wm_placeholder,
    render_delta_fields,
)
from provisa.federation.engine import build_duckdb_engine
from provisa.federation.freshness_gate import FreshnessMode, evaluate_freshness
from provisa.federation.materialization import WriteFace, reactive_sources, select_write_face
from provisa.federation.read_through import ReadOutcome, plan_mutation, resolve_read
from provisa.federation.strategy import Strategy

pytestmark = pytest.mark.integration

duckdb = pytest.importorskip("duckdb")

_STORE_TABLE = "replica_orders"


@pytest.fixture()
def store_path(tmp_path) -> str:
    return str(tmp_path / "materialization_store.duckdb")


def _connect(path: str):
    return duckdb.connect(path)


def _land(path: str, rows: list[tuple]) -> None:
    """The write face: land pulled rows into the durable store (engine-native CTAS/upsert)."""
    con = _connect(path)
    try:
        con.execute(
            f"CREATE TABLE IF NOT EXISTS {_STORE_TABLE} "
            "(id INTEGER PRIMARY KEY, customer VARCHAR, amount DOUBLE, updated_at INTEGER)"
        )
        # PK upsert — replaces prior row state, the REQ-874 apply discipline.
        con.executemany(
            f"INSERT OR REPLACE INTO {_STORE_TABLE} VALUES (?, ?, ?, ?)", rows
        )
    finally:
        con.close()


def _read_store(path: str) -> list[tuple]:
    if not Path(path).exists():
        return []
    con = _connect(path)  # a FRESH connection — proves DURABLE (survives reconnect)
    try:
        names = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
        if _STORE_TABLE not in names:
            return []
        return con.execute(f"SELECT id, customer, amount FROM {_STORE_TABLE} ORDER BY id").fetchall()
    finally:
        con.close()


def _invalidate(path: str) -> None:
    con = _connect(path)
    try:
        con.execute(f"DROP TABLE IF EXISTS {_STORE_TABLE}")
    finally:
        con.close()


# --- REQ-845/848: reactive-replica set + write-face selection ------------------


def test_reactive_set_is_engine_relative_and_write_face_is_engine_native():
    from provisa.core.models import Source, SourceType

    engine = build_duckdb_engine()

    def _src(sid, type_, **kw):
        return Source(id=sid, type=type_, host="h", port=1, database="d", username="u", **kw)

    # openapi has no DuckDB attach connector -> MATERIALIZED -> reactive replica.
    # postgresql attaches in place -> not reactive.
    sources = [
        _src("api", SourceType.openapi),
        _src("pg", SourceType.postgresql),
    ]
    reactive = reactive_sources(engine, sources)
    assert "api" in reactive
    assert "pg" not in reactive

    # DuckDB store backend is the engine's own native store -> the write face collapses in.
    assert select_write_face(engine, "duckdb") is WriteFace.ENGINE_NATIVE


# --- REQ-845/848: read-through miss -> pull -> land -> serve --------------------


def test_read_through_miss_lands_then_serves_durably(store_path):
    assert _read_store(store_path) == []  # cold: a miss

    # Single-flight pull returns upstream rows; the write face lands them.
    pulled = [
        (10, "Alice", 19.99, 100),
        (11, "Bob", 49.99, 101),
    ]
    outcome = resolve_read(
        pull_ok=True, cache_fresh=False, cache_has_data=False, stale_policy_allows=False
    )
    assert outcome is ReadOutcome.SERVE_FRESH
    _land(store_path, pulled)

    # Served from the durable store via a fresh connection.
    assert _read_store(store_path) == [(10, "Alice", 19.99), (11, "Bob", 49.99)]


# --- REQ-847: read-through / write-back discipline -----------------------------


def test_pull_failure_with_no_fresh_data_is_a_hard_error():
    assert (
        resolve_read(
            pull_ok=False, cache_fresh=False, cache_has_data=False, stale_policy_allows=False
        )
        is ReadOutcome.HARD_ERROR
    )


def test_stale_is_served_only_under_explicit_policy():
    # Stale cache exists, pull failed: without an explicit stale policy -> hard error.
    assert (
        resolve_read(
            pull_ok=False, cache_fresh=False, cache_has_data=True, stale_policy_allows=False
        )
        is ReadOutcome.HARD_ERROR
    )
    # With the explicit per-source stale allowance -> serve stale (never a silent fallback).
    assert (
        resolve_read(
            pull_ok=False, cache_fresh=False, cache_has_data=True, stale_policy_allows=True
        )
        is ReadOutcome.SERVE_STALE
    )


def test_write_back_targets_upstream_and_invalidates_the_store_entry(store_path):
    _land(store_path, [(10, "Alice", 19.99, 100)])
    assert _read_store(store_path)  # entry present

    plan = plan_mutation(_STORE_TABLE)
    assert plan.target == "upstream"  # never the cache as system of record
    assert plan.invalidate == _STORE_TABLE

    _invalidate(store_path)  # the executor carries out the invalidation the plan states
    assert _read_store(store_path) == []  # entry gone; next read re-pulls from upstream


# --- REQ-855: freshness gate over a real store entry ---------------------------


def test_ttl_floor_serves_cached_without_probing(store_path):
    _land(store_path, [(10, "Alice", 19.99, 100)])
    probes = {"n": 0}

    def _probe():
        probes["n"] += 1
        return "tok"

    decision = evaluate_freshness(
        FreshnessMode.TTL_PROBE,
        now=100.0,
        last_refresh_at=95.0,  # within the TTL floor
        ttl=60.0,
        stored_token="tok",
        probe=_probe,
    )
    assert decision.fresh is True
    assert probes["n"] == 0  # inside the floor, the upstream is never probed
    assert _read_store(store_path)  # cached rows remain served


def test_unchanged_probe_token_keeps_rows_changed_token_invalidates(store_path):
    _land(store_path, [(10, "Alice", 19.99, 100)])

    # Token unchanged after the floor -> keep the materialized rows (zero lag).
    keep = evaluate_freshness(
        FreshnessMode.TTL_PROBE,
        now=200.0,
        last_refresh_at=100.0,
        ttl=60.0,
        stored_token="v1",
        probe=lambda: "v1",
    )
    assert keep.fresh is True
    assert _read_store(store_path)  # not re-pulled

    # Token changed -> invalidate + re-pull, then land the new rows.
    changed = evaluate_freshness(
        FreshnessMode.TTL_PROBE,
        now=200.0,
        last_refresh_at=100.0,
        ttl=60.0,
        stored_token="v1",
        probe=lambda: "v2",
    )
    assert changed.fresh is False
    assert changed.new_token == "v2"
    _invalidate(store_path)
    _land(store_path, [(10, "Alice", 5.00, 200)])  # fresh upstream state
    assert _read_store(store_path) == [(10, "Alice", 5.00)]


# --- REQ-874: delta refresh (PROBE == DELTA for monotonic entries) -------------


def test_delta_refresh_upserts_changed_rows_and_advances_cursor(store_path):
    _land(store_path, [(10, "Alice", 19.99, 100), (11, "Bob", 49.99, 101)])

    # Delta only applies to MATERIALIZED replicas.
    assert delta_applies(Strategy.MATERIALIZED) is True
    assert delta_applies(Strategy.VIRTUAL) is False

    # An authored, source-native delta query with both placeholders.
    template = "SELECT {{fields}} FROM orders WHERE updated_at > $wm"
    assert has_wm_placeholder(template)
    rendered = render_delta_fields(template, ["id", "customer", "amount", "updated_at"])
    assert "{{fields}}" not in rendered and "$wm" in rendered  # $wm left for native binding

    # Cursor at 101; the native caller returns rows with updated_at > 101 (predicate at source).
    delta_rows = [
        {"id": 11, "customer": "Bob", "amount": 51.00, "updated_at": 102},  # updated
        {"id": 12, "customer": "Carol", "amount": 7.50, "updated_at": 103},  # new
    ]
    assert delta_is_fresh(delta_rows) is False  # non-empty -> changed -> apply

    # APPLY: PK upsert on the already-registered key -> Bob updated, Carol inserted, Alice intact.
    _land(store_path, [(r["id"], r["customer"], r["amount"], r["updated_at"]) for r in delta_rows])
    assert _read_store(store_path) == [
        (10, "Alice", 19.99),
        (11, "Bob", 51.00),
        (12, "Carol", 7.50),
    ]

    # Cursor advances to max(updated_at) over the returned rows.
    assert advance_cursor(delta_rows, "updated_at", 101) == 103


def test_empty_delta_is_a_noop_and_keeps_the_cursor(store_path):
    _land(store_path, [(10, "Alice", 19.99, 100)])
    before = _read_store(store_path)

    empty: list[dict] = []
    assert delta_is_fresh(empty) is True  # empty -> fresh -> no-op
    assert advance_cursor(empty, "updated_at", 100) == 100  # cursor unchanged

    assert _read_store(store_path) == before  # store untouched


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
