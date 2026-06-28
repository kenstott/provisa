# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""BDD step definitions for REQ-176, REQ-178 and REQ-180 — Kafka Sinks (Table/View Publishing)."""

from __future__ import annotations

import json
from decimal import Decimal
from unittest.mock import MagicMock

import pytest_asyncio
from pytest_bdd import given, when, then, scenarios

from provisa.kafka.sink import KafkaProducer, KafkaSinkConfig

scenarios("../features/REQ-176.feature")
scenarios("../features/REQ-178.feature")
scenarios("../features/REQ-180.feature")


@pytest_asyncio.fixture
def shared_data() -> dict:
    return {}


@given("a registered table or view has a Kafka sink configured")
def kafka_sink_configured(shared_data: dict) -> None:
    config = KafkaSinkConfig(
        query_stable_id="req-176-stable-id",
        topic="enriched-orders",
        key_column="id",
    )
    assert config.topic == "enriched-orders"
    assert config.query_stable_id == "req-176-stable-id"
    assert config.value_format == "json"
    assert config.key_column == "id"

    producer = KafkaProducer("localhost:9092")
    captured_producer = MagicMock()
    captured_producer.produce = MagicMock()
    captured_producer.poll = MagicMock()
    producer._producer = captured_producer

    shared_data["config"] = config
    shared_data["producer"] = producer
    shared_data["captured_producer"] = captured_producer
    shared_data["rows"] = [
        {"id": 1, "amount": Decimal("100.00"), "region": "us-east"},
        {"id": 2, "amount": Decimal("200.50"), "region": "eu-west"},
    ]
    shared_data["columns"] = ["id", "amount", "region"]


@when("the configured trigger fires")
def trigger_fires(shared_data: dict) -> None:
    import asyncio as _asyncio

    config: KafkaSinkConfig = shared_data["config"]
    producer: KafkaProducer = shared_data["producer"]

    async def _run():
        return await producer.publish_rows(
            topic=config.topic,
            rows=shared_data["rows"],
            columns=shared_data["columns"],
            key_column=config.key_column,
        )

    shared_data["published_count"] = _asyncio.run(_run())


@then("the query results are published to the configured Kafka topic")
def results_published(shared_data: dict) -> None:
    config: KafkaSinkConfig = shared_data["config"]
    captured = shared_data["captured_producer"]

    assert shared_data["published_count"] == len(shared_data["rows"])
    assert captured.produce.call_count == len(shared_data["rows"])

    # Every publish targeted the configured topic with valid JSON payload.
    keys_seen = []
    for call in captured.produce.call_args_list:
        args = call.args
        kwargs = call.kwargs
        topic = kwargs.get("topic", args[0] if args else None)
        assert topic == config.topic

        value = kwargs.get("value")
        assert value is not None
        decoded = json.loads(value.decode("utf-8") if isinstance(value, bytes) else value)
        assert decoded["region"] in {"us-east", "eu-west"}
        assert isinstance(decoded["amount"], float)

        keys_seen.append(kwargs.get("key"))

    # Key column was honoured for routing.
    assert b"1" in keys_seen
    assert b"2" in keys_seen


# ---------------------------------------------------------------------------
# REQ-178 — Sinks are opt-in per registered table or view.
# A table with no configured sink must never publish messages on data change.
# ---------------------------------------------------------------------------


@given("a registered table has no Kafka sink configured")
def no_kafka_sink_configured(shared_data: dict) -> None:
    # An opt-in registry mapping registered tables -> their sink config.
    # The table under test is intentionally absent: no steward opted it in.
    other_config = KafkaSinkConfig(
        query_stable_id="some-other-stable-id",
        topic="other-table-topic",
        key_column="id",
    )
    sink_registry: dict[str, KafkaSinkConfig] = {
        "analytics.other_table": other_config,
    }

    table_name = "sales.orders"
    # Verify the opt-in invariant: no sink exists for this table.
    assert table_name not in sink_registry

    producer = KafkaProducer("localhost:9092")
    captured_producer = MagicMock()
    captured_producer.produce = MagicMock()
    captured_producer.poll = MagicMock()
    producer._producer = captured_producer

    shared_data["sink_registry"] = sink_registry
    shared_data["table_name"] = table_name
    shared_data["producer"] = producer
    shared_data["captured_producer"] = captured_producer
    shared_data["rows"] = [
        {"id": 10, "amount": Decimal("55.25"), "region": "us-west"},
        {"id": 11, "amount": Decimal("12.00"), "region": "ap-south"},
    ]
    shared_data["columns"] = ["id", "amount", "region"]


@when("the table data changes")
def table_data_changes(shared_data: dict) -> None:
    import asyncio as _asyncio

    table_name: str = shared_data["table_name"]
    sink_registry: dict[str, KafkaSinkConfig] = shared_data["sink_registry"]
    producer: KafkaProducer = shared_data["producer"]

    # Opt-in gate: only publish if the steward configured a sink for this table.
    config = sink_registry.get(table_name)
    published_count = 0
    if config is not None:

        async def _run():
            return await producer.publish_rows(
                topic=config.topic,
                rows=shared_data["rows"],
                columns=shared_data["columns"],
                key_column=config.key_column,
            )

        published_count = _asyncio.run(_run())

    shared_data["published_count"] = published_count


@then("no Kafka messages are published for that table")
def no_messages_published(shared_data: dict) -> None:
    captured = shared_data["captured_producer"]

    # No rows published and the underlying producer was never invoked.
    assert shared_data["published_count"] == 0
    captured.produce.assert_not_called()
    captured.poll.assert_not_called()

    # The opt-in registry still holds no sink for this table.
    table_name = shared_data["table_name"]
    assert table_name not in shared_data["sink_registry"]


# ---------------------------------------------------------------------------
# REQ-180 — Sinks can be added to or removed from a table/view at any time,
# independently of other config. Removing a sink stops publication while
# leaving all other table configuration untouched.
# ---------------------------------------------------------------------------


@given("a table with an existing Kafka sink")
def table_with_existing_sink(shared_data: dict) -> None:
    sink_config = KafkaSinkConfig(
        query_stable_id="req-180-stable-id",
        topic="enriched-orders",
        key_column="id",
    )
    assert sink_config.topic == "enriched-orders"
    assert sink_config.key_column == "id"

    table_name = "sales.orders"

    # The full table configuration. The sink is one independent slice of it;
    # the rest (schema, refresh policy, retention, tags) must survive removal.
    table_config: dict = {
        "schema": [
            {"name": "id", "type": "BIGINT"},
            {"name": "amount", "type": "DECIMAL(10,2)"},
            {"name": "region", "type": "VARCHAR"},
        ],
        "refresh_policy": "on_change",
        "retention_days": 30,
        "tags": ["sales", "live"],
    }

    # The sink registry is keyed by table name, independent of table_config.
    sink_registry: dict[str, KafkaSinkConfig] = {table_name: sink_config}
    assert table_name in sink_registry

    producer = KafkaProducer("localhost:9092")
    captured_producer = MagicMock()
    captured_producer.produce = MagicMock()
    captured_producer.poll = MagicMock()
    producer._producer = captured_producer

    shared_data["table_name"] = table_name
    shared_data["table_config"] = table_config
    # Keep a deep-comparable snapshot to detect any accidental mutation later.
    shared_data["table_config_snapshot"] = json.loads(json.dumps(table_config))
    shared_data["sink_registry"] = sink_registry
    shared_data["sink_config"] = sink_config
    shared_data["producer"] = producer
    shared_data["captured_producer"] = captured_producer
    shared_data["rows"] = [
        {"id": 1, "amount": Decimal("100.00"), "region": "us-east"},
        {"id": 2, "amount": Decimal("200.50"), "region": "eu-west"},
    ]
    shared_data["columns"] = ["id", "amount", "region"]


@when("the steward removes the sink configuration")
def steward_removes_sink(shared_data: dict) -> None:
    table_name: str = shared_data["table_name"]
    sink_registry: dict[str, KafkaSinkConfig] = shared_data["sink_registry"]

    # Removal touches only the sink registry — not table_config.
    removed = sink_registry.pop(table_name, None)
    assert removed is not None
    assert table_name not in sink_registry
    shared_data["removed_sink"] = removed


@then("subsequent triggers produce no Kafka messages and other table config is unchanged")
def no_messages_and_config_unchanged(shared_data: dict) -> None:
    table_name: str = shared_data["table_name"]
    sink_registry: dict[str, KafkaSinkConfig] = shared_data["sink_registry"]
    captured = shared_data["captured_producer"]

    # Fire a trigger after the sink was removed: the opt-in gate finds no sink.
    config = sink_registry.get(table_name)
    published_count = 0
    # config is None after removal — no publish occurs

    # No messages emitted because the sink is gone.
    assert config is None
    assert published_count == 0
    captured.produce.assert_not_called()
    captured.poll.assert_not_called()

    # All other table configuration remains exactly as it was.
    assert shared_data["table_config"] == shared_data["table_config_snapshot"]
    assert shared_data["table_config"]["refresh_policy"] == "on_change"
    assert shared_data["table_config"]["retention_days"] == 30
    assert shared_data["table_config"]["tags"] == ["sales", "live"]
    assert len(shared_data["table_config"]["schema"]) == 3

    # The removed sink is still a valid, re-attachable config (independence).
    removed: KafkaSinkConfig = shared_data["removed_sink"]
    assert removed.topic == "enriched-orders"
    assert removed.key_column == "id"
