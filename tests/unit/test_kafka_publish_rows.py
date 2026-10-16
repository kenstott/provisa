# Copyright (c) 2026 Kenneth Stott
# Canary: f8ebc1e8-2e30-4cef-a29b-3ddf327dd3ae
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The rows a subscription redirected to Kafka publishes (X-Provisa-Sink, REQ-812): JSON, keyed by
an optional column, through the process's producer for the cluster."""

# Requirements: REQ-812

import json
from unittest.mock import MagicMock, patch


from provisa.kafka.sink import KafkaProducer


class TestKafkaProducer:
    @patch("provisa.kafka.sink.KafkaProducer._ensure_producer")
    async def test_publish_rows_dict(self, mock_ensure):
        producer = KafkaProducer("localhost:9092")
        producer._producer = MagicMock()
        producer._producer.send = MagicMock()

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
        assert producer._producer.send.call_count == 2

    @patch("provisa.kafka.sink.KafkaProducer._ensure_producer")
    async def test_publish_with_key_column(self, mock_ensure):
        producer = KafkaProducer("localhost:9092")
        producer._producer = MagicMock()
        producer._producer.send = MagicMock()

        rows = [{"id": 1, "name": "test"}]

        await producer.publish_rows(
            topic="test-topic",
            rows=rows,
            columns=["id", "name"],
            key_column="id",
        )

        call_kwargs = producer._producer.send.call_args
        assert call_kwargs.kwargs.get("key") == b"1" or call_kwargs[1].get("key") == b"1"

    @patch("provisa.kafka.sink.KafkaProducer._ensure_producer")
    async def test_publish_empty_rows(self, mock_ensure):
        producer = KafkaProducer("localhost:9092")
        producer._producer = MagicMock()

        count = await producer.publish_rows(
            topic="test-topic",
            rows=[],
            columns=["id"],
        )

        assert count == 0


class TestKafkaProducerMocked:
    async def test_sink_publishes_result_to_topic(self):
        """publish_rows hands the producer each row."""
        mock_producer = MagicMock()

        with patch("provisa.kafka.sink.KafkaProducer._ensure_producer"):
            producer = KafkaProducer("localhost:9092")
            producer._producer = mock_producer

            rows = [
                {"id": 1, "amount": 10.0},
                {"id": 2, "amount": 20.0},
            ]
            count = await producer.publish_rows(
                topic="test-topic",
                rows=rows,
                columns=["id", "amount"],
            )

        assert count == 2
        assert mock_producer.send.call_count == 2

    async def test_sink_respects_row_limit(self):
        """Exactly N rows handed over for N rows — no more, no less."""
        mock_producer = MagicMock()

        with patch("provisa.kafka.sink.KafkaProducer._ensure_producer"):
            producer = KafkaProducer("localhost:9092")
            producer._producer = mock_producer

            rows = [{"id": i, "val": i * 10} for i in range(50)]
            count = await producer.publish_rows(
                topic="bulk-topic",
                rows=rows,
                columns=["id", "val"],
            )

        assert count == 50
        assert mock_producer.send.call_count == 50

    async def test_sink_key_column_used_as_message_key(self):
        """When key_column is set, the row is handed over with the correct key bytes."""
        mock_producer = MagicMock()

        with patch("provisa.kafka.sink.KafkaProducer._ensure_producer"):
            producer = KafkaProducer("localhost:9092")
            producer._producer = mock_producer

            rows = [{"id": 42, "amount": 5.0}]
            await producer.publish_rows(
                topic="keyed-topic",
                rows=rows,
                columns=["id", "amount"],
                key_column="id",
            )

        call_kwargs = mock_producer.send.call_args
        assert call_kwargs.kwargs.get("key") == b"42" or (
            len(call_kwargs.args) > 2 and call_kwargs.args[2] == b"42"
        )

    async def test_sink_value_is_valid_json_bytes(self):
        """Each produced value is valid JSON bytes containing the row data."""
        produced_values: list[bytes] = []

        def capture_produce(topic, value=None, key=None, callback=None):
            produced_values.append(value)

        mock_producer = MagicMock()
        mock_producer.send.side_effect = capture_produce

        with patch("provisa.kafka.sink.KafkaProducer._ensure_producer"):
            producer = KafkaProducer("localhost:9092")
            producer._producer = mock_producer

            rows = [{"id": 7, "region": "apac", "amount": 3.14}]
            await producer.publish_rows(
                topic="json-check-topic",
                rows=rows,
                columns=["id", "region", "amount"],
            )

        assert len(produced_values) == 1
        decoded = json.loads(produced_values[0])
        assert decoded["id"] == 7
        assert decoded["region"] == "apac"

    async def test_sink_close_leaves_the_processes_producer_running(self):
        """close() ends this sink's use of the producer. The producer is the process's, shared
        with every other sink and with change events, and is stopped by the lifespan."""
        mock_producer = MagicMock()
        with patch("provisa.kafka.sink.KafkaProducer._ensure_producer"):
            producer = KafkaProducer("localhost:9092")
            producer._producer = mock_producer
            producer.close()

        mock_producer.stop.assert_not_called()
        assert producer._producer is None

    def test_a_sink_uses_the_processes_producer_for_its_cluster(self, monkeypatch):
        """One producer per cluster per process: two sinks on the same brokers share it."""
        made: list[str] = []
        monkeypatch.setattr(
            "provisa.kafka.sink.shared", lambda bootstrap: made.append(bootstrap) or bootstrap
        )
        first, second = KafkaProducer("broker:9092"), KafkaProducer("broker:9092")
        first._ensure_producer()
        second._ensure_producer()
        first._ensure_producer()  # already has it
        assert made == ["broker:9092", "broker:9092"]
        assert first._producer == second._producer == "broker:9092"
