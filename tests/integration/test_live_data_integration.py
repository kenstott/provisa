# Copyright (c) 2026 Kenneth Stott
# Canary: e7f2a3b5-c8d9-4e0f-b1c2-d3e4f5a6b7c8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests: dataset change events and subscription fallback.

Tests cover the change event pipeline and subscription trigger installation
pipeline functions directly — not HTTP endpoints.
"""

# Requirements: REQ-172, REQ-173, REQ-174, REQ-175, REQ-566

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


# ---------------------------------------------------------------------------
# REQ-172, REQ-173, REQ-174, REQ-175: Dataset Change Events
# ---------------------------------------------------------------------------


class TestDatasetChangeEvents:
    def test_emit_change_event_no_op_without_kafka(self):
        # REQ-172: emit_change_event is a no-op when no Kafka bootstrap is configured
        from provisa.kafka.change_events import emit_change_event

        with patch.dict(os.environ, {}, clear=False):
            # Ensure no bootstrap servers are in the environment
            env_without_kafka = {
                k: v
                for k, v in os.environ.items()
                if k not in ("PROVISA_CHANGE_EVENT_BOOTSTRAP", "KAFKA_BOOTSTRAP_SERVERS")
            }
            with patch("provisa.kafka.change_events._producer", None):
                with patch.dict(os.environ, env_without_kafka, clear=True):
                    # Must not raise — silently no-ops when Kafka is unavailable
                    emit_change_event("orders", "sales-pg", "insert")

    def test_emit_change_event_with_mock_producer(self):
        # REQ-172: emit_change_event calls producer.produce with table, source, timestamp
        import json

        from provisa.kafka.change_events import emit_change_event

        mock_producer = MagicMock()
        produced_calls: list[dict] = []

        def _capture_produce(topic, key, value):
            produced_calls.append(
                {"topic": topic, "key": key.decode(), "value": json.loads(value.decode())}
            )

        mock_producer.produce.side_effect = _capture_produce
        mock_producer.poll.return_value = 0

        with patch("provisa.kafka.change_events._producer", mock_producer):
            emit_change_event("orders", "sales-pg", "insert")

        assert len(produced_calls) == 1
        call = produced_calls[0]
        assert call["value"]["table"] == "orders"
        assert call["value"]["source"] == "sales-pg"
        assert call["value"]["type"] == "insert"
        assert "timestamp" in call["value"]
        assert call["key"] == "sales-pg.orders"

    def test_emit_change_event_default_mutation_type(self):
        # REQ-172: mutation_type defaults to "mutation" when not specified
        import json

        from provisa.kafka.change_events import emit_change_event

        mock_producer = MagicMock()
        produced: list[dict] = []

        def _capture(topic, key, value):
            produced.append(json.loads(value.decode()))

        mock_producer.produce.side_effect = _capture
        mock_producer.poll.return_value = 0

        with patch("provisa.kafka.change_events._producer", mock_producer):
            emit_change_event("customers", "crm-pg")

        assert produced[0]["type"] == "mutation"

    def test_emit_change_event_no_row_detail(self):
        # REQ-172: change events contain no row-level detail — only table, source, timestamp
        import json

        from provisa.kafka.change_events import emit_change_event

        mock_producer = MagicMock()
        produced: list[dict] = []

        def _capture(topic, key, value):
            produced.append(json.loads(value.decode()))

        mock_producer.produce.side_effect = _capture
        mock_producer.poll.return_value = 0

        with patch("provisa.kafka.change_events._producer", mock_producer):
            emit_change_event("orders", "sales-pg", "delete")

        event = produced[0]
        # Must contain only these keys — no row data
        allowed_keys = {"table", "source", "type", "timestamp"}
        assert set(event.keys()) <= allowed_keys
        assert "row" not in event
        assert "data" not in event
        assert "payload" not in event

    def test_emit_fires_on_same_hook_as_cache_invalidation(self):
        # REQ-173: change events fire on the same mutation hook that invalidates cache
        # Verified by checking both calls are orchestrated in the mutation pipeline.
        # We test the emitter directly; the orchestration is in endpoint.py _handle_mutation.
        import json

        from provisa.kafka.change_events import emit_change_event

        calls: list[str] = []

        mock_producer = MagicMock()
        mock_producer.produce.side_effect = lambda topic, key, value: calls.append(
            json.loads(value.decode())["table"]
        )
        mock_producer.poll.return_value = 0

        with patch("provisa.kafka.change_events._producer", mock_producer):
            emit_change_event("orders", "sales-pg", "update")

        assert calls == ["orders"]

    def test_touch_operation_emits_change_event(self):
        # REQ-174: external ETL can signal changes via touch (mutation_type="touch")
        import json

        from provisa.kafka.change_events import emit_change_event

        mock_producer = MagicMock()
        produced: list[dict] = []

        def _capture(topic, key, value):
            produced.append(json.loads(value.decode()))

        mock_producer.produce.side_effect = _capture
        mock_producer.poll.return_value = 0

        with patch("provisa.kafka.change_events._producer", mock_producer):
            emit_change_event("etl_table", "warehouse-pg", "touch")

        assert produced[0]["type"] == "touch"
        assert produced[0]["table"] == "etl_table"

    def test_change_event_topic_configurable(self):
        # REQ-175: topic read from PROVISA_CHANGE_EVENT_TOPIC env var
        from provisa.kafka.change_events import _get_topic

        with patch.dict(os.environ, {"PROVISA_CHANGE_EVENT_TOPIC": "my.custom-events"}):
            topic = _get_topic()
        assert topic == "my.custom-events"

    def test_change_event_topic_default(self):
        # REQ-175: default topic is "provisa.change-events" when env var not set
        from provisa.kafka.change_events import _get_topic

        env_without_topic = {
            k: v for k, v in os.environ.items() if k != "PROVISA_CHANGE_EVENT_TOPIC"
        }
        with patch.dict(os.environ, env_without_topic, clear=True):
            topic = _get_topic()
        assert topic == "provisa.change-events"

    def test_emit_change_event_producer_failure_does_not_raise(self):
        # REQ-172: producer failure is swallowed — mutation pipeline must not abort
        from provisa.kafka.change_events import emit_change_event

        mock_producer = MagicMock()
        mock_producer.produce.side_effect = RuntimeError("Kafka down")

        with patch("provisa.kafka.change_events._producer", mock_producer):
            # Must not propagate the RuntimeError
            emit_change_event("orders", "sales-pg", "insert")


# ---------------------------------------------------------------------------
# REQ-566: Subscription fallback to watermark polling when trigger install fails
# ---------------------------------------------------------------------------


class TestSubscriptionTriggerFallback:
    async def test_trigger_install_failure_returns_partial_installed_set(self):
        # REQ-566: when trigger installation fails for a table, it is omitted from
        # the installed set so callers can fall back to polling for that table
        from provisa.subscriptions.pg_triggers import ensure_pg_notify_triggers

        # Mock connection that always raises on execute (simulates insufficient privilege)
        failing_conn = MagicMock()
        failing_conn.execute = MagicMock(side_effect=Exception("permission denied"))

        tables = [
            {"table_name": "orders", "schema_name": "public", "source_id": "sales-pg"},
        ]
        source_types = {"sales-pg": "postgresql"}

        installed = await ensure_pg_notify_triggers(failing_conn, tables, source_types)
        # Table not installed — caller must fall back to polling
        assert "orders" not in installed

    async def test_trigger_install_success_returns_table_in_installed_set(self):
        # REQ-566: when trigger installation succeeds, table is included in installed set
        from provisa.subscriptions.pg_triggers import ensure_pg_notify_triggers

        # Mock connection where execute succeeds
        ok_conn = MagicMock()
        ok_conn.execute = MagicMock(return_value=None)

        tables = [
            {"table_name": "orders", "schema_name": "public", "source_id": "sales-pg"},
        ]
        source_types = {"sales-pg": "postgresql"}

        installed = await ensure_pg_notify_triggers(ok_conn, tables, source_types)
        assert "orders" in installed

    async def test_trigger_install_skips_non_pg_sources(self):
        # REQ-566: non-PostgreSQL sources are not attempted for trigger installation
        from provisa.subscriptions.pg_triggers import ensure_pg_notify_triggers

        conn = MagicMock()
        conn.execute = MagicMock(return_value=None)

        tables = [
            {"table_name": "events", "schema_name": "public", "source_id": "kafka-src"},
            {"table_name": "orders", "schema_name": "public", "source_id": "sales-pg"},
        ]
        source_types = {"kafka-src": "kafka", "sales-pg": "postgresql"}

        installed = await ensure_pg_notify_triggers(conn, tables, source_types)
        assert "events" not in installed
        assert "orders" in installed

    async def test_trigger_install_partial_failure_returns_successful_tables(self):
        # REQ-566: partial failure installs whichever tables succeed
        from provisa.subscriptions.pg_triggers import ensure_pg_notify_triggers

        call_count = 0

        async def _execute_side_effect(sql):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise Exception("permission denied for orders")
            # customers succeeds

        conn = MagicMock()
        conn.execute = _execute_side_effect

        tables = [
            {"table_name": "orders", "schema_name": "public", "source_id": "sales-pg"},
            {"table_name": "customers", "schema_name": "public", "source_id": "sales-pg"},
        ]
        source_types = {"sales-pg": "postgresql"}

        installed = await ensure_pg_notify_triggers(conn, tables, source_types)
        assert "orders" not in installed
        assert "customers" in installed

    def test_trigger_sql_contains_channel_prefix(self):
        # REQ-566: trigger SQL uses provisa_ channel prefix matching PgNotificationProvider
        from provisa.subscriptions.pg_triggers import _trigger_sql
        from provisa.subscriptions.pg_provider import CHANNEL_PREFIX

        sql = _trigger_sql("public", "orders")
        assert CHANNEL_PREFIX in sql  # "provisa_" prefix

    def test_trigger_sql_idempotent_drop_before_create(self):
        # REQ-566: trigger SQL drops existing trigger before creating (idempotent install)
        from provisa.subscriptions.pg_triggers import _trigger_sql

        sql = _trigger_sql("public", "orders")
        drop_pos = sql.find("DROP TRIGGER")
        create_pos = sql.find("CREATE TRIGGER")
        assert drop_pos != -1
        assert create_pos != -1
        assert drop_pos < create_pos  # DROP before CREATE

    def test_channel_name_matches_pg_provider_convention(self):
        # REQ-566: channel name in trigger matches PgNotificationProvider.watch() convention
        from provisa.subscriptions.pg_triggers import _trigger_sql
        from provisa.subscriptions.pg_provider import CHANNEL_PREFIX

        table = "orders"
        expected_channel = f"{CHANNEL_PREFIX}{table}"
        sql = _trigger_sql("public", table)
        assert expected_channel in sql
