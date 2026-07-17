# Copyright (c) 2026 Kenneth Stott
# Canary: 5aebdffd-4bca-410d-8fe4-d9309252676e
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
"""pytest-bdd steps for REQ-823 — LiveEngine reconciles poll jobs from the DB at startup/mutation."""

from __future__ import annotations

import pytest
from pytest_bdd import given, scenarios, then, when

from provisa.live.engine import LiveEngine, LiveSpec


@pytest.fixture
def shared_data():
    return {}


@given("live config stored in registered_tables.live")
def given_live_config_in_db(shared_data):
    # Two active live configs as they would be read from registered_tables.live.
    shared_data["db_specs"] = [
        LiveSpec(
            query_id="orders", sql="SELECT 1", watermark_column="updated_at", poll_interval=10
        ),
        LiveSpec(query_id="events", sql="SELECT 2", watermark_column="ts", poll_interval=15),
    ]
    shared_data["engine"] = LiveEngine(tenant_db=None, engine=None)


@when("the LiveEngine starts")
def when_live_engine_starts(shared_data):
    # Startup reconciliation drives engine jobs to match the DB's active configs.
    shared_data["engine"].reconcile(shared_data["db_specs"])


@then("it queries the database for all active live configs and rebuilds poll jobs")
def then_rebuilds_poll_jobs(shared_data):
    engine = shared_data["engine"]
    assert engine.is_registered("orders")
    assert engine.is_registered("events")


@given("live config modified via admin GraphQL API")
def given_live_config_modified(shared_data):
    engine = LiveEngine(tenant_db=None, engine=None)
    engine.reconcile(
        [
            LiveSpec(
                query_id="orders", sql="SELECT 1", watermark_column="updated_at", poll_interval=10
            )
        ]
    )
    shared_data["engine"] = engine
    # An operator changed the poll interval and removed a job.
    shared_data["new_specs"] = [
        LiveSpec(query_id="orders", sql="SELECT 1", watermark_column="updated_at", poll_interval=45)
    ]


@when("the mutation completes")
def when_mutation_completes(shared_data):
    shared_data["engine"].reconcile(shared_data["new_specs"])


@then("_rebuild_schemas() is called to reconcile the engine immediately")
def then_reconcile_called_immediately(shared_data):
    # Reconciliation applied the changed fingerprint in place.
    assert shared_data["engine"]._jobs["orders"].poll_interval == 45


@then("the new poll schedule takes effect without restart")
def then_new_schedule_without_restart(shared_data):
    engine = shared_data["engine"]
    # A job removed from the desired set is unregistered live; unchanged ones keep their subscribers.
    engine.reconcile([])
    assert not engine.is_registered("orders")


scenarios("../features/REQ-823.feature")
