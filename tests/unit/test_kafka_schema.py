# Copyright (c) 2026 Kenneth Stott
# Canary: b6127859-73e5-40bd-90fa-3d1427ec3b6a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for Kafka schema → column mapping."""

import json

import pytest

from provisa.kafka.source import (
    KafkaColumn,
    KafkaSourceConfig,
    KafkaTopicConfig,
    generate_trino_kafka_properties,
    map_avro_to_trino,
    map_json_schema_to_trino,
)
from provisa.kafka.schema_registry import (
    columns_from_avro_schema,
    columns_from_json_schema,
)


class TestAvroToTrino:
    def test_string(self):
        assert map_avro_to_trino("string") == ("varchar", False)

    def test_int(self):
        assert map_avro_to_trino("int") == ("integer", False)

    def test_long(self):
        assert map_avro_to_trino("long") == ("bigint", False)

    def test_double(self):
        assert map_avro_to_trino("double") == ("double", False)

    def test_boolean(self):
        assert map_avro_to_trino("boolean") == ("boolean", False)

    def test_record_is_complex(self):
        assert map_avro_to_trino({"type": "record"}) == ("varchar", True)

    def test_array_is_complex(self):
        assert map_avro_to_trino({"type": "array"}) == ("varchar", True)

    def test_union_picks_non_null(self):
        assert map_avro_to_trino(["null", "string"]) == ("varchar", False)

    def test_union_null_int(self):
        assert map_avro_to_trino(["null", "int"]) == ("integer", False)

    def test_enum_is_string(self):
        assert map_avro_to_trino({"type": "enum"}) == ("varchar", False)


class TestJsonSchemaToTrino:
    def test_string(self):
        assert map_json_schema_to_trino("string") == ("varchar", False)

    def test_integer(self):
        assert map_json_schema_to_trino("integer") == ("bigint", False)

    def test_number(self):
        assert map_json_schema_to_trino("number") == ("double", False)

    def test_boolean(self):
        assert map_json_schema_to_trino("boolean") == ("boolean", False)

    def test_object_is_complex(self):
        assert map_json_schema_to_trino("object") == ("varchar", True)

    def test_array_is_complex(self):
        assert map_json_schema_to_trino("array") == ("varchar", True)


class TestColumnsFromAvroSchema:
    def test_simple_record(self):
        schema = json.dumps({
            "type": "record",
            "name": "Order",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "customer_id", "type": "int"},
                {"name": "amount", "type": "double"},
                {"name": "region", "type": "string"},
            ],
        })
        cols = columns_from_avro_schema(schema)
        assert len(cols) == 4
        assert cols[0].name == "id"
        assert cols[0].data_type == "integer"
        assert not cols[0].is_complex
        assert cols[2].data_type == "double"

    def test_complex_field_is_jsonb(self):
        schema = json.dumps({
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "metadata", "type": {"type": "record", "name": "Meta", "fields": []}},
            ],
        })
        cols = columns_from_avro_schema(schema)
        assert cols[1].is_complex

    def test_non_record_raises(self):
        with pytest.raises(ValueError, match="Expected Avro record"):
            columns_from_avro_schema(json.dumps({"type": "string"}))


class TestColumnsFromJsonSchema:
    def test_simple_object(self):
        schema = json.dumps({
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"},
                "active": {"type": "boolean"},
            },
        })
        cols = columns_from_json_schema(schema)
        assert len(cols) == 3
        names = {c.name for c in cols}
        assert names == {"id", "name", "active"}

    def test_non_object_raises(self):
        with pytest.raises(ValueError, match="Expected JSON Schema object"):
            columns_from_json_schema(json.dumps({"type": "array"}))


class TestTrinoKafkaProperties:
    def test_basic_properties(self):
        source = KafkaSourceConfig(
            id="events",
            bootstrap_servers="kafka:9092",
        )
        props = generate_trino_kafka_properties(source)
        assert "connector.name=kafka" in props
        assert "kafka.nodes=kafka:9092" in props

    def test_with_schema_registry(self):
        source = KafkaSourceConfig(
            id="events",
            bootstrap_servers="kafka:9092",
            schema_registry_url="http://schema-registry:8081",
        )
        props = generate_trino_kafka_properties(source)
        assert "schema-registry-url=http://schema-registry:8081" in props


class TestKafkaTopicConfig:
    def test_auto_table_name(self):
        topic = KafkaTopicConfig(
            id="order-events",
            topic="orders.events.v1",
            source_id="events",
        )
        assert topic.table_name == "orders_events_v1"

    def test_explicit_table_name(self):
        topic = KafkaTopicConfig(
            id="order-events",
            topic="orders.events.v1",
            source_id="events",
            table_name="order_events",
        )
        assert topic.table_name == "order_events"
