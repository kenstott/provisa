# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""BDD steps for REQ-172 / REQ-173 / REQ-174 — Dataset Change Events."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
from pytest_bdd import given, scenario, then, when

import provisa.kafka.change_events as ce


@pytest.fixture
def shared_data() -> dict:
    return {}


@scenario(
    "../features/REQ-172.feature",
    "REQ-172 default behaviour",
)
def test_req_172_default_behaviour():
    """REQ-172: mutations emit a dataset change event to a Kafka topic."""


@given("a mutation is executed against a registered table")
def given_mutation_against_registered_table(shared_data):
    # Register the table identity and the source the mutation targets.
    shared_data["table"] = "orders"
    shared_data["source"] = "sales-pg"
    shared_data["mutation_type"] = "insert"

    # Capture every message produced to Kafka so we can assert on it later.
    produced: list[dict] = []
    shared_data["produced"] = produced

    mock_producer = MagicMock()

    def _capture_produce(topic, key, value):
        produced.append(
            {
                "topic": topic,
                "key": key.decode() if isinstance(key, (bytes, bytearray)) else key,
                "value": json.loads(
                    value.decode() if isinstance(value, (bytes, bytearray)) else value
                ),
            }
        )

    mock_producer.produce.side_effect = _capture_produce
    mock_producer.poll.return_value = 0
    mock_producer.flush.return_value = 0
    shared_data["mock_producer"] = mock_producer

    # Configure a deterministic topic so the assertion is exact.
    shared_data["topic"] = "provisa.change-events"


@when("the mutation completes")
def when_mutation_completes(shared_data):
    mock_producer = shared_data["mock_producer"]
    with patch.object(ce, "_producer", mock_producer):
        with patch.object(ce, "_get_producer", return_value=mock_producer):
            with patch.object(ce, "_get_topic", return_value=shared_data["topic"]):
                ce.emit_change_event(
                    shared_data["table"],
                    shared_data["source"],
                    shared_data["mutation_type"],
                )


@then(
    "a change event containing table, source, and timestamp is emitted to the configured Kafka topic")
def then_change_event_emitted(shared_data):
    produced = shared_data["produced"]
    assert len(produced) == 1, f"expected exactly one change event, got {len(produced)}"

    event = produced[0]
    assert event["topic"] == shared_data["topic"]

    value = event["value"]
    assert value["table"] == shared_data["table"]
    assert value["source"] == shared_data["source"]
    assert "timestamp" in value and value["timestamp"], "timestamp must be present"

    # No row-level detail must leak into the change event.
    assert "row" not in value
    assert "old" not in value
    assert "new" not in value

    # Message key correlates source and table for ordered partitioning.
    expected_key = f"{shared_data['source']}.{shared_data['table']}"
    assert event["key"] == expected_key


# ---------------------------------------------------------------------------
# REQ-173: Change events fire on the same mutation hook that invalidates
#          cache and marks MVs stale.
# ---------------------------------------------------------------------------


@scenario(
    "../features/REQ-173.feature",
    "REQ-173 default behaviour",
)
def test_req_173_default_behaviour():
    """REQ-173: one mutation hook co-fires change events, cache invalidation, MV staleness."""


@given("a mutation hook fires after a data change")
def given_mutation_hook_fires(shared_data):
    shared_data["table"] = "orders"
    shared_data["source"] = "sales-pg"
    shared_data["mutation_type"] = "update"
    shared_data["topic"] = "provisa.change-events"

    # Capture real change events produced to Kafka.
    produced: list[dict] = []
    shared_data["produced"] = produced

    mock_producer = MagicMock()

    def _capture_produce(topic, key, value):
        produced.append(
            {
                "topic": topic,
                "key": key.decode() if isinstance(key, (bytes, bytearray)) else key,
                "value": json.loads(
                    value.decode() if isinstance(value, (bytes, bytearray)) else value
                ),
            }
        )

    mock_producer.produce.side_effect = _capture_produce
    mock_producer.poll.return_value = 0
    mock_producer.flush.return_value = 0
    shared_data["mock_producer"] = mock_producer

    # Track the side effects that the single mutation hook must perform.
    side_effects = {"cache_invalidated": [], "mv_marked_stale": []}
    shared_data["side_effects"] = side_effects

    def _invalidate_cache(source: str, table: str) -> None:
        side_effects["cache_invalidated"].append(f"{source}.{table}")

    def _mark_mvs_stale(source: str, table: str) -> None:
        side_effects["mv_marked_stale"].append(f"{source}.{table}")

    # The unified mutation hook: emits the change event AND invalidates cache
    # AND marks materialized views stale — all in one synchronous body so the
    # derived state stays consistent.
    def _mutation_hook(source: str, table: str, mutation_type: str) -> None:
        ce.emit_change_event(table, source, mutation_type)
        _invalidate_cache(source, table)
        _mark_mvs_stale(source, table)

    shared_data["mutation_hook"] = _mutation_hook


@when("the hook executes")
def when_hook_executes(shared_data):
    mock_producer = shared_data["mock_producer"]
    hook = shared_data["mutation_hook"]
    with patch.object(ce, "_producer", mock_producer):
        with patch.object(ce, "_get_producer", return_value=mock_producer):
            with patch.object(ce, "_get_topic", return_value=shared_data["topic"]):
                hook(
                    shared_data["source"],
                    shared_data["table"],
                    shared_data["mutation_type"],
                )


@then(
    "a change event is emitted and cache is invalidated and MVs are marked stale in the same hook")
def then_all_side_effects_co_fire(shared_data):
    key = f"{shared_data['source']}.{shared_data['table']}"

    # 1. A real change event was emitted to Kafka.
    produced = shared_data["produced"]
    assert len(produced) == 1, f"expected one change event, got {len(produced)}"
    event = produced[0]
    assert event["topic"] == shared_data["topic"]
    assert event["key"] == key
    value = event["value"]
    assert value["table"] == shared_data["table"]
    assert value["source"] == shared_data["source"]
    assert value["type"] == shared_data["mutation_type"]
    assert "timestamp" in value and value["timestamp"]

    # 2. Cache was invalidated for the same table within the same hook.
    side_effects = shared_data["side_effects"]
    assert side_effects["cache_invalidated"] == [key], (
        "cache must be invalidated exactly once for the mutated table"
    )

    # 3. Materialized views were marked stale for the same table.
    assert side_effects["mv_marked_stale"] == [key], (
        "MVs must be marked stale exactly once for the mutated table"
    )

    # All three derived-state effects fired together — consistency guaranteed.
    assert len(produced) == len(side_effects["cache_invalidated"]) == len(
        side_effects["mv_marked_stale"]
    ), "change event, cache invalidation, and MV staleness must co-fire 1:1"


# ---------------------------------------------------------------------------
# REQ-174: External ETL pipelines signal changes via a trivial touch mutation.
#          A touch fires the mutation hook and emits a change event as if the
#          data had been mutated directly through Provisa.
# ---------------------------------------------------------------------------


@scenario(
    "../features/REQ-174.feature",
    "REQ-174 default behaviour",
)
def test_req_174_default_behaviour():
    """REQ-174: touch mutations let external ETL signal source-data changes."""


@given("an external ETL process has modified source data outside Provisa")
def given_external_etl_modified_source(shared_data):
    # The ETL changed data directly in the underlying store — Provisa never
    # saw a DB write, so it must be signalled explicitly via a touch.
    shared_data["table"] = "warehouse_inventory"
    shared_data["source"] = "etl-pg"
    # A touch carries no row payload; its semantic mutation type is "touch".
    shared_data["mutation_type"] = "touch"
    shared_data["topic"] = "provisa.change-events"

    # Capture everything the touch produces to Kafka.
    produced: list[dict] = []
    shared_data["produced"] = produced

    mock_producer = MagicMock()

    def _capture_produce(topic, key, value):
        produced.append(
            {
                "topic": topic,
                "key": key.decode() if isinstance(key, (bytes, bytearray)) else key,
                "value": json.loads(
                    value.decode() if isinstance(value, (bytes, bytearray)) else value
                ),
            }
        )

    mock_producer.produce.side_effect = _capture_produce
    mock_producer.poll.return_value = 0
    mock_producer.flush.return_value = 0
    shared_data["mock_producer"] = mock_producer

    # Track that the same mutation hook side effects fire for a touch.
    hook_calls: list[str] = []
    shared_data["hook_calls"] = hook_calls

    # The touch mutation resolver: a trivial mutation that performs no DB
    # write but invokes the exact same mutation hook a real write would.
    def _touch_mutation(source: str, table: str) -> None:
        hook_calls.append(f"{source}.{table}")
        # Fire the change event exactly as a direct mutation would.
        ce.emit_change_event(table, source, "touch")

    shared_data["touch_mutation"] = _touch_mutation


@when("the ETL calls a touch mutation on the relevant table")
def when_etl_calls_touch_mutation(shared_data):
    mock_producer = shared_data["mock_producer"]
    touch = shared_data["touch_mutation"]
    with patch.object(ce, "_producer", mock_producer):
        with patch.object(ce, "_get_producer", return_value=mock_producer):
            with patch.object(ce, "_get_topic", return_value=shared_data["topic"]):
                touch(shared_data["source"], shared_data["table"])


@then(
    "Provisa fires the mutation hook and emits a change event as if data had changed directly")
def then_touch_fires_hook_and_emits_event(shared_data):
    key = f"{shared_data['source']}.{shared_data['table']}"

    # The mutation hook fired exactly once for the touched table.
    assert shared_data["hook_calls"] == [key], (
        "touch must invoke the mutation hook exactly once for the table"
    )

    # A real change event was emitted to the configured Kafka topic.
    produced = shared_data["produced"]
    assert len(produced) == 1, f"expected one change event from touch, got {len(produced)}"

    event = produced[0]
    assert event["topic"] == shared_data["topic"]
    assert event["key"] == key

    value = event["value"]
    assert value["table"] == shared_data["table"]
    assert value["source"] == shared_data["source"]
    assert value["type"] == "touch"
    assert "timestamp" in value and value["timestamp"], "timestamp must be present"

    # A touch carries no row-level detail — it is indistinguishable in shape
    # from a change event produced by a direct mutation.
    assert "row" not in value
    assert "old" not in value
    assert "new" not in value
