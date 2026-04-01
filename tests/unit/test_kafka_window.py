# Copyright (c) 2025 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Tests for Kafka time-window and discriminator injection."""

from provisa.compiler.sql_gen import CompiledQuery, CompilationContext, TableMeta
from provisa.kafka.window import (
    inject_kafka_filters, inject_kafka_window,
    KafkaTableConfig, _parse_window,
)


def _ctx(source_id="event-stream", table_name="order_events"):
    ctx = CompilationContext()
    ctx.tables["order_events"] = TableMeta(
        table_id=1, field_name="order_events", type_name="OrderEvents",
        source_id=source_id, catalog_name="event_stream",
        schema_name="public", table_name=table_name,
    )
    return ctx


def _compiled(sql="SELECT * FROM order_events"):
    return CompiledQuery(
        sql=sql, params=[], root_field="order_events",
        columns=[], sources={"event-stream"},
    )


class TestParseWindow:
    def test_hours(self):
        assert _parse_window("1h") == "INTERVAL '1' HOUR"

    def test_minutes(self):
        assert _parse_window("30m") == "INTERVAL '30' MINUTE"

    def test_days(self):
        assert _parse_window("7d") == "INTERVAL '7' DAY"

    def test_seconds(self):
        assert _parse_window("60s") == "INTERVAL '60' SECOND"


class TestInjectKafkaFilters:
    def test_injects_window(self):
        result = inject_kafka_filters(
            _compiled(), _ctx(),
            source_types={"event-stream": "kafka"},
            kafka_configs={"order_events": KafkaTableConfig(window="1h")},
        )
        assert "_timestamp" in result.sql
        assert "INTERVAL '1' HOUR" in result.sql

    def test_ands_onto_existing_where(self):
        result = inject_kafka_filters(
            _compiled("SELECT * FROM order_events WHERE amount > 100"),
            _ctx(),
            source_types={"event-stream": "kafka"},
            kafka_configs={"order_events": KafkaTableConfig(window="1h")},
        )
        assert "_timestamp" in result.sql
        assert "amount > 100" in result.sql

    def test_no_injection_for_non_kafka(self):
        result = inject_kafka_filters(
            _compiled(), _ctx(source_id="sales-pg"),
            source_types={"sales-pg": "postgresql"},
            kafka_configs={},
        )
        assert "_timestamp" not in result.sql

    def test_no_double_injection_when_client_filters(self):
        sql = 'SELECT * FROM order_events WHERE "_timestamp" >= TIMESTAMP \'2026-01-01\''
        result = inject_kafka_filters(
            _compiled(sql), _ctx(),
            source_types={"event-stream": "kafka"},
            kafka_configs={"order_events": KafkaTableConfig(window="1h")},
        )
        assert result.sql.count("_timestamp") == 1

    def test_no_window_no_discriminator(self):
        result = inject_kafka_filters(
            _compiled(), _ctx(),
            source_types={"event-stream": "kafka"},
            kafka_configs={},
        )
        assert result.sql == "SELECT * FROM order_events"

    def test_discriminator_only(self):
        result = inject_kafka_filters(
            _compiled(), _ctx(),
            source_types={"event-stream": "kafka"},
            kafka_configs={
                "order_events": KafkaTableConfig(
                    window=None,
                    discriminator_field="event_type",
                    discriminator_value="OrderCreated",
                ),
            },
        )
        assert '"event_type" = \'OrderCreated\'' in result.sql
        assert "_timestamp" not in result.sql

    def test_discriminator_and_window(self):
        result = inject_kafka_filters(
            _compiled(), _ctx(),
            source_types={"event-stream": "kafka"},
            kafka_configs={
                "order_events": KafkaTableConfig(
                    window="2h",
                    discriminator_field="event_type",
                    discriminator_value="OrderShipped",
                ),
            },
        )
        assert '"event_type" = \'OrderShipped\'' in result.sql
        assert "INTERVAL '2' HOUR" in result.sql

    def test_discriminator_sql_injection_safe(self):
        result = inject_kafka_filters(
            _compiled(), _ctx(),
            source_types={"event-stream": "kafka"},
            kafka_configs={
                "order_events": KafkaTableConfig(
                    discriminator_field="event_type",
                    discriminator_value="Order'; DROP TABLE --",
                ),
            },
        )
        assert "Order''; DROP TABLE --" in result.sql  # escaped


class TestBackwardCompat:
    def test_inject_kafka_window_legacy(self):
        result = inject_kafka_window(
            _compiled(), _ctx(),
            source_types={"event-stream": "kafka"},
            kafka_windows={"event-stream": "1h"},
        )
        assert "_timestamp" in result.sql
        assert "INTERVAL '1' HOUR" in result.sql
