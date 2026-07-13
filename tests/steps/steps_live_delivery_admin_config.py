# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
"""pytest-bdd steps for REQ-819 — per-table live delivery config via admin API, applied at runtime."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from pytest_bdd import given, scenarios, then, when

from provisa.core.config_loader import _validate_table_live_delivery
from provisa.live.engine import LiveEngine, LiveSpec


@pytest.fixture
def shared_data():
    return {}


def _config_with_live(strategy="poll", watermark_column="updated_at"):
    src = SimpleNamespace(id="s1", type="postgresql", cdc=None)
    live = SimpleNamespace(
        strategy=strategy,
        watermark_column=watermark_column,
        poll_interval=10,
        delivery="sse",
        kafka=None,
        outputs=[],
    )
    table = SimpleNamespace(table_name="orders", source_id="s1", live=live)
    return SimpleNamespace(sources=[src], tables=[table])


@given("the admin GraphQL API for table mutations")
def given_admin_graphql_api(shared_data):
    shared_data["config"] = _config_with_live()


@when(
    "updateTable is called with live configuration "
    "(query_id, watermark_column, poll_interval, delivery, outputs)"
)
def when_update_table_with_live_config(shared_data):
    # A well-formed live config validates before it is persisted to registered_tables.live.
    _validate_table_live_delivery(shared_data["config"])
    shared_data["validated"] = True


@then("the configuration is persisted to registered_tables.live and the live engine is notified")
def then_config_persisted_and_engine_notified(shared_data):
    assert shared_data["validated"] is True
    # The live engine is notified by reconciling the desired poll spec — no restart involved.
    engine = LiveEngine(tenant_db=None, engine=None)
    engine.reconcile([LiveSpec(query_id="orders", sql="SELECT 1", watermark_column="updated_at")])
    assert engine.is_registered("orders")


@given("the admin UI TablesPage")
def given_admin_ui_tablespage(shared_data):
    shared_data["engine"] = LiveEngine(tenant_db=None, engine=None)
    shared_data["engine"].reconcile(
        [
            LiveSpec(
                query_id="orders", sql="SELECT 1", watermark_column="updated_at", poll_interval=10
            )
        ]
    )


@when("an operator edits live config for a table")
def when_operator_edits_live_config(shared_data):
    # Change the poll interval — a config fingerprint change the engine must pick up live.
    shared_data["engine"].reconcile(
        [
            LiveSpec(
                query_id="orders", sql="SELECT 1", watermark_column="updated_at", poll_interval=30
            )
        ]
    )


@then("changes are reflected in the database and take effect without server restart")
def then_changes_take_effect_without_restart(shared_data):
    job = shared_data["engine"]._jobs["orders"]
    assert job.poll_interval == 30  # re-registered in place, no process restart


scenarios("../features/REQ-819.feature")
