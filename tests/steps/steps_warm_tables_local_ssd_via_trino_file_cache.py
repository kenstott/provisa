# Copyright (c) 2026 Kenneth Stott
# Canary: e44285b7-031a-4c3f-b561-4430692f8415
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1

"""BDD steps for tables replicated because they are busy (the former warm tier).

Covers:
- REQ-239 — promotion when a table's statement count passes its threshold, demotion when it
  falls away.
- REQ-238 — a busy table's reads come from its replica in the engine's store instead of a round
  trip to the source.

Since REQ-826 a warm table is a table whose ``replicate`` setting is Default or Hot-N and that
passed its threshold: it is promoted in the replica state, the data replicator builds its
replica, and reads route to that replica exactly as an Always table's do. REQ-239's scenario is
written against that mechanism; REQ-238's wording predates it (Iceberg, the Trino file cache) and
its steps exercise the mechanism as it is.
"""

# Requirements: REQ-238, REQ-239, REQ-826

from __future__ import annotations

import pytest
from pytest_bdd import given, scenarios, then, when

from provisa.core.replicate import resolved_replicate
from provisa.federation.replica_routing import reads_replica, table_floor
from tests.steps.hot_replication_world import THRESHOLD, World

scenarios("../features/REQ-239.feature")
scenarios("../features/REQ-238.feature")


@pytest.fixture
def shared_data() -> dict:
    return {}


@pytest.fixture
def world(tmp_path, monkeypatch):
    built = World(tmp_path, monkeypatch, {"orders": 1, "archive": 2, "customers": 3})
    yield built
    built.close()


# --- REQ-239: promotion at the threshold, demotion below half of it ---


def _reads_replica(world, table: str, serving: frozenset) -> bool:
    """Whether a read of ``table`` goes to its replica, as routing decides it."""
    row = {"source_id": "pg", "replicate": None, "load_protected": None}
    engine = world.state.federation_engine.engine
    return reads_replica(world.source, row, engine, promoted=world.key(table) in serving)


@given(
    "a table left at Default whose statement count within replication.hot_interval reaches "
    "replication.hot_threshold"
)
def table_reaching_threshold(world, shared_data):
    # orders, left at Default, reaches the global threshold exactly within the interval.
    assert resolved_replicate(world.source, {"source_id": "pg", "replicate": None}) is None
    world.statements_read("orders", THRESHOLD)
    assert world.count("orders") == THRESHOLD
    # Two tables promoted earlier: archive's traffic fell below half the threshold, customers'
    # sits exactly at half.
    for table, statements in (("archive", THRESHOLD // 2 - 1), ("customers", THRESHOLD // 2)):
        world.promoted_and_built(table)
        world.statements_read(table, statements)
    assert world.promotion()[0] == frozenset({world.key("archive"), world.key("customers")})


@when("the promotion check runs")
def promotion_check_runs(world, shared_data):
    shared_data["outcome"] = world.evaluate()


@then("the table is marked promoted and a replica build is requested from the data replicator")
def table_promoted_and_build_requested(world, shared_data):
    outcome = shared_data["outcome"]
    assert world.key("orders") in outcome.promoted
    assert world.key("orders") in world.promotion()[0]
    orders = world.record("orders")
    assert (orders.build_state, orders.requested_reason) == ("requested", "hot")
    # It was sized through the engine first (it is under the size ceiling).
    assert world.state.federation_engine.sent == ['SELECT COUNT(*) FROM "pg"."public"."orders"']


@then("reads stay live until that build completes in the engine's store")
def reads_live_until_built(world, shared_data):
    promoted, serving = world.promotion()
    assert world.key("orders") in promoted and world.key("orders") not in serving
    assert _reads_replica(world, "orders", serving) is False
    # The replicator's build completes in this engine's store: reads move to the replica.
    world.promoted_and_built("orders")
    serving = world.promotion()[1]
    assert world.key("orders") in serving
    assert _reads_replica(world, "orders", serving) is True


@then("the table is demoted when its count falls below half the threshold")
def demoted_below_half(world, shared_data):
    outcome = shared_data["outcome"]
    # Below half: demoted and read live again; its replica is left for the replicator to retire.
    assert outcome.demoted == (world.key("archive"),)
    promoted, serving = world.promotion()
    assert world.key("archive") not in promoted and world.key("archive") not in serving
    assert _reads_replica(world, "archive", serving) is False
    assert world.record("archive").exists
    # Exactly half is not below it: still promoted and still served from its replica.
    assert world.key("customers") in promoted and world.key("customers") in serving


# --- REQ-238: a busy table's reads come from its replica ---


@given("a table materialized into the Iceberg results catalog with Trino file cache enabled")
def table_materialized_with_file_cache(world, shared_data):
    # Driven past its threshold, promoted, and its replica built in this engine's store.
    world.statements_read("customers", 200)
    assert world.evaluate().promoted == (world.key("customers"),)
    world.promoted_and_built("customers")
    promoted, serving = world.promotion()
    assert world.key("customers") in promoted and world.key("customers") in serving
    shared_data["serving"] = serving


@when("a query targets that table")
def query_targets_warm_table(world, shared_data):
    table = {"source_id": "pg", "replicate": None, "load_protected": None}
    serving = world.key("customers") in shared_data["serving"]
    engine = world.state.federation_engine.engine
    shared_data["reads_replica"] = reads_replica(world.source, table, engine, promoted=serving)
    shared_data["floor"] = table_floor(world.source, table, promoted=serving)
    shared_data["before_it_was_built"] = reads_replica(world.source, table, engine, promoted=False)
    shared_data["setting"] = resolved_replicate(world.source, table)


@then("Trino serves the result from local SSD Parquet cache at ~10-50ms latency")
def served_from_local_ssd_cache(shared_data):
    # The read goes to the replica in the engine's store, not to the source...
    assert shared_data["reads_replica"] is True
    # ...and a request may not ask for the source instead: the floor names the setting.
    assert shared_data["floor"] == "replicate"
    # Nothing but its traffic put it there: its setting is still Default, and until the
    # replica existed the same table was read live.
    assert shared_data["setting"] is None
    assert shared_data["before_it_was_built"] is False
