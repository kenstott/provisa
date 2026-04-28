# Copyright (c) 2026 Kenneth Stott
# Canary: f8ebc1e8-2e30-4cef-a29b-3ddf327dd3ae
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for Kafka sink message serialization."""

import json
from unittest.mock import MagicMock, patch

from provisa.kafka.sink import KafkaProducer, KafkaSinkConfig


class TestKafkaSinkConfig:
    def test_default_format(self):
        config = KafkaSinkConfig(
            query_stable_id="abc-123",
            topic="enriched-orders",
        )
        assert config.value_format == "json"
        assert config.key_column is None

    def test_with_key_column(self):
        config = KafkaSinkConfig(
            query_stable_id="abc-123",
            topic="enriched-orders",
            key_column="order_id",
        )
        assert config.key_column == "order_id"


class TestKafkaProducer:
    @patch("provisa.kafka.sink.KafkaProducer._ensure_producer")
    async def test_publish_rows_dict(self, mock_ensure):
        producer = KafkaProducer("localhost:9092")
        producer._producer = MagicMock()
        producer._producer.produce = MagicMock()
        producer._producer.poll = MagicMock()

        rows = [
            {"id": 1, "amount": 100.0, "region": "us"},
            {"id": 2, "amount": 200.0, "region": "eu"},
        ]

        count = await producer.publish_rows(
            topic="test-topic",
            rows=rows,
            columns=["id", "amount", "region"],
        )

        assert count == 2
        assert producer._producer.produce.call_count == 2

    @patch("provisa.kafka.sink.KafkaProducer._ensure_producer")
    async def test_publish_with_key_column(self, mock_ensure):
        producer = KafkaProducer("localhost:9092")
        producer._producer = MagicMock()
        producer._producer.produce = MagicMock()
        producer._producer.poll = MagicMock()

        rows = [{"id": 1, "name": "test"}]

        await producer.publish_rows(
            topic="test-topic",
            rows=rows,
            columns=["id", "name"],
            key_column="id",
        )

        call_kwargs = producer._producer.produce.call_args
        assert call_kwargs.kwargs.get("key") == b"1" or call_kwargs[1].get("key") == b"1"

    @patch("provisa.kafka.sink.KafkaProducer._ensure_producer")
    async def test_publish_empty_rows(self, mock_ensure):
        producer = KafkaProducer("localhost:9092")
        producer._producer = MagicMock()
        producer._producer.poll = MagicMock()

        count = await producer.publish_rows(
            topic="test-topic",
            rows=[],
            columns=["id"],
        )

        assert count == 0
