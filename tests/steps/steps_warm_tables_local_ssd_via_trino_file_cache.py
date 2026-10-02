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
replica, and reads route to that replica exactly as an Always table's do. The scenarios' wording
predates that (Iceberg, the Trino file cache); the steps exercise the mechanism as it is.
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


@given("a table whose query count exceeds warm_tables.query_threshold within a refresh interval")
def table_exceeding_threshold(world, shared_data):
    # The busy table: well past the threshold within the interval.
    world.statements_read("orders", 150)
    # A table promoted earlier whose traffic has fallen away: a handful of statements now.
    world.promoted_and_built("archive")
    world.statements_read("archive", 5)
    assert world.count("orders") >= THRESHOLD
    assert world.count("archive") < THRESHOLD / 2
    assert world.promotion()[0] == frozenset({world.key("archive")})


@when("the promotion check runs")
def promotion_check_runs(world, shared_data):
    shared_data["outcome"] = world.evaluate()


@then("the table is auto-materialized into Iceberg; tables falling below threshold are demoted")
def table_promoted_and_demoted(world, shared_data):
    outcome = shared_data["outcome"]
    # Promotion: the busy table is promoted and its replica is requested of the replicator.
    assert outcome.promoted == (world.key("orders"),)
    orders = world.record("orders")
    assert (orders.build_state, orders.requested_reason) == ("requested", "hot")
    # It was sized through the engine first (it is under the size ceiling).
    assert world.state.federation_engine.sent == ['SELECT COUNT(*) FROM "pg"."public"."orders"']
    # Demotion: the table whose traffic fell away is read live again.
    assert outcome.demoted == (world.key("archive"),)
    promoted, serving = world.promotion()
    assert promoted == frozenset({world.key("orders")})
    assert world.key("archive") not in serving
    # Its replica is not dropped here: the replicator retires it.
    assert world.record("archive").exists


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
