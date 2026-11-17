# Copyright (c) 2026 Kenneth Stott
# Canary: 9e5f6a7b-8c9d-0123-ef01-234567890123
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Rows published to Kafka, on a real broker: by the product's own producer, as a subscription
redirected to Kafka publishes them (X-Provisa-Sink, REQ-812), and by a table's live output
(live.outputs, REQ-565). The mocked-producer tests are tests/unit/test_kafka_publish_rows.py."""

# Requirements: REQ-812, REQ-565, REQ-286

from __future__ import annotations

import json
import os
import uuid

import pytest


pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


# ---------------------------------------------------------------------------
# Infrastructure detection helpers
# ---------------------------------------------------------------------------


def _kafka_bootstrap() -> str:
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


# ---------------------------------------------------------------------------
# Real Kafka integration tests (skip if Kafka unavailable)
# ---------------------------------------------------------------------------


class TestKafkaSinkReal:
    pytestmark = [pytest.mark.requires_kafka]

    async def test_sink_publishes_and_consumer_reads_message(self):
        """Produce a row to Kafka; AIOKafkaConsumer receives it as JSON."""
        from aiokafka import AIOKafkaConsumer, AIOKafkaProducer  # noqa: PLC0415

        topic = f"provisa-test-{uuid.uuid4().hex[:8]}"
        bootstrap = _kafka_bootstrap()

        ak_producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
        await ak_producer.start()
        try:
            row = {"id": 1001, "amount": 42.5, "region": "us-east"}
            value = json.dumps(row).encode("utf-8")
            await ak_producer.send_and_wait(topic, value=value)
        finally:
            await ak_producer.stop()

        consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=bootstrap,
            auto_offset_reset="earliest",
            group_id=f"test-group-{uuid.uuid4().hex[:8]}",
            consumer_timeout_ms=5000,
        )
        await consumer.start()
        received: list[dict] = []
        try:
            async for msg in consumer:
                received.append(json.loads(msg.value))
                break
        except Exception:
            pass
        finally:
            await consumer.stop()

        assert len(received) == 1
        assert received[0]["id"] == 1001
        assert received[0]["region"] == "us-east"

    async def test_sink_trigger_on_change_event(self):
        """Change event triggers correct topic publication when Kafka available."""
        from aiokafka import AIOKafkaConsumer, AIOKafkaProducer  # noqa: PLC0415

        topic = f"provisa-change-{uuid.uuid4().hex[:8]}"
        bootstrap = _kafka_bootstrap()

        ak_producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
        await ak_producer.start()
        try:
            rows = [
                {"id": 2001, "amount": 88.0, "region": "eu-west"},
                {"id": 2002, "amount": 12.0, "region": "eu-west"},
            ]
            for row in rows:
                await ak_producer.send_and_wait(topic, value=json.dumps(row).encode())
        finally:
            await ak_producer.stop()

        consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=bootstrap,
            auto_offset_reset="earliest",
            group_id=f"change-group-{uuid.uuid4().hex[:8]}",
            consumer_timeout_ms=5000,
        )
        await consumer.start()
        received: list[dict] = []
        try:
            async for msg in consumer:
                received.append(json.loads(msg.value))
                if len(received) >= 2:
                    break
        except Exception:
            pass
        finally:
            await consumer.stop()

        assert len(received) == 2
        ids = {r["id"] for r in received}
        assert ids == {2001, 2002}


# The role a request acts as, as endpoint.py hands it on (require_role).
_WRITER = {"id": "analyst", "capabilities": ["write"]}
_READER = {"id": "analyst", "capabilities": []}


def _sink_call(tmp_path, monkeypatch, topic: str, role: dict):
    """handle_subscription_sse's arguments for a subscription on ``orders`` whose X-Provisa-Sink
    names ``topic`` on the test broker, acting as ``role``; and the platform store holding its
    place."""
    from types import SimpleNamespace
    from unittest.mock import MagicMock

    from graphql.language.ast import (
        DocumentNode,
        FieldNode,
        NameNode,
        OperationDefinitionNode,
        SelectionSetNode,
    )

    from provisa.auth.models import AuthIdentity
    from provisa.core.database import Database, create_engine_from_url
    from provisa.core.schema_admin import running_sinks

    bootstrap = _kafka_bootstrap()
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", bootstrap)
    engine = create_engine_from_url(f"sqlite:///{tmp_path / 'ps.db'}", pool_size=5, max_overflow=0)
    running_sinks.create(engine)
    sink_db = Database(engine, name="platform-state", holds="platform_state")

    table_meta = MagicMock(table_name="orders", type_name="Orders", source_id="pg1")
    table_meta.catalog_name = None
    table_meta.schema_name = None
    ctx = MagicMock(tables={"orders": table_meta}, joins={})
    state = MagicMock(
        source_types={"pg1": "postgresql"},
        tenant_db=None,
        pg_notify_tables=set(),
        table_watermarks={},
        source_pools={},
        platform_state_db=sink_db,
        auth_config={"assignments_source": "provisa", "default_role": "analyst"},
    )
    request = MagicMock()
    request.headers.get = lambda key, default="": {
        "x-provisa-sink": f"kafka://{bootstrap}/{topic}"
    }.get(key.lower(), default)
    request.state = SimpleNamespace(
        identity=AuthIdentity(
            user_id="alice", email=None, display_name="alice", roles=[], raw_claims={}
        ),
        active_org_id="org1",
    )
    document = DocumentNode(
        definitions=[
            OperationDefinitionNode(
                operation=None,  # type: ignore[arg-type]
                name=None,
                variable_definitions=[],
                directives=[],
                selection_set=SelectionSetNode(
                    selections=[FieldNode(name=NameNode(value="orders"), selection_set=None)]
                ),
            )
        ]
    )
    call = dict(
        document=document,
        ctx=ctx,
        rls=MagicMock(),
        state=state,
        variables=None,
        role=role,
        role_id=role["id"],
        raw_request=request,
        directives=MagicMock(sink_topic=None, sink_broker=None, watermark_column=None),
    )
    return call, sink_db


async def _read(topic: str, count: int) -> list[tuple[bytes | None, dict]]:
    """The first ``count`` messages on ``topic``, from its beginning."""
    import asyncio

    from aiokafka import AIOKafkaConsumer  # noqa: PLC0415

    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=_kafka_bootstrap(),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    await consumer.start()
    got: list[tuple[bytes | None, dict]] = []
    try:
        async with asyncio.timeout(60):
            async for message in consumer:
                got.append((message.key, json.loads(message.value.decode())))
                if len(got) == count:
                    break
    finally:
        await consumer.stop()
    return got


class TestTheProductsOwnProducerOnARealBroker:
    """Sinks and the live Kafka output once imported a Kafka client the product does not declare
    ("confluent-kafka is required ... pip install confluent-kafka"), so neither could publish in
    any install, and their unit tests passed against mocks. These go through the product's code
    to a real broker and read back what arrived."""

    pytestmark = [pytest.mark.requires_kafka]

    @pytest.fixture(autouse=True)
    async def _the_lifespan_stops_the_producers(self):
        yield
        from provisa.kafka import producer as kafka_producer

        await kafka_producer.stop_all()

    async def test_a_sinks_rows_land_on_its_topic_keyed_by_its_key_column(self):
        """REQ-176, REQ-181: each row is one JSON message, keyed by the sink's key column."""
        from provisa.kafka import producer as kafka_producer
        from provisa.kafka.sink import KafkaProducer

        topic = f"provisa-sink-{uuid.uuid4().hex[:8]}"
        sink = KafkaProducer(_kafka_bootstrap())
        rows = [
            {"id": 1, "region": "us-east", "amount": 10.5},
            {"id": 2, "region": "eu", "amount": 3},
        ]
        assert (
            await sink.publish_rows(
                topic, rows, columns=["id", "region", "amount"], key_column="id"
            )
            == 2
        )
        await kafka_producer.stop_all()  # as the lifespan does: what was handed over is sent

        assert await _read(topic, 2) == [(b"1", rows[0]), (b"2", rows[1])]

    async def test_a_live_outputs_rows_reach_its_topic(self):
        """REQ-286: the rows a live query emits are produced to the output's topic."""
        from provisa.kafka import producer as kafka_producer
        from provisa.live.outputs.kafka import KafkaSinkOutput

        topic = f"provisa-live-{uuid.uuid4().hex[:8]}"
        output = KafkaSinkOutput(_kafka_bootstrap(), topic, key_column="id")
        await output.send([{"id": 5, "status": "new"}, {"id": 6, "status": "open"}])
        await output.close()
        await kafka_producer.stop_all()

        assert await _read(topic, 2) == [
            (b"5", {"id": 5, "status": "new"}),
            (b"6", {"id": 6, "status": "open"}),
        ]

    async def test_a_sink_publishes_until_its_owner_loses_the_role(self, tmp_path, monkeypatch):
        """REQ-812 (amended 2026-11-16): a sink started over X-Provisa-Sink publishes to the broker
        while its owner holds the role it was started under, then ends when the role is revoked
        and frees its place under the owner's cap."""
        import asyncio
        from datetime import timedelta
        from unittest.mock import MagicMock, patch

        from provisa.api.data.subscription_sse import handle_subscription_sse
        from provisa.auth.models import RoleAssignment
        from provisa.core.platform_state import sinks as running
        from provisa.kafka import producer as kafka_producer

        monkeypatch.setattr(running, "HEARTBEAT", timedelta(seconds=0.2))
        topic = f"provisa-owned-{uuid.uuid4().hex[:8]}"
        call, sink_db = _sink_call(tmp_path, monkeypatch, topic, _WRITER)

        held = {"analyst"}

        async def held_assignments(_state, _identity, _org):
            return [RoleAssignment(role_id=r, domain_id="*") for r in sorted(held)]

        changes: asyncio.Queue = asyncio.Queue()

        class Feed:
            async def watch(self, _table):
                while True:
                    yield await changes.get()

            async def close(self):
                return None

        async def answer(*_a, **_k):
            return {"data": {"orders": [{"id": 1}]}}

        spawned: list = []
        with (
            patch(
                "provisa.core.connection_loop.spawn_long_lived",
                lambda coro, *, name=None: spawned.append(coro),
            ),
            patch("provisa.subscriptions.registry.get_provider", lambda *_a, **_k: Feed()),
            patch("provisa.api.data.endpoint._handle_query", answer),
            patch("provisa.compiler.parser.parse_query", MagicMock()),
            patch("provisa.auth.held_assignments.held_assignments", held_assignments),
        ):
            response = await handle_subscription_sse(**call)
            assert response.status_code == 202
            sink = asyncio.ensure_future(spawned[0])
            await changes.put({"op": "insert"})
            await asyncio.sleep(1.0)
            assert not sink.done()
            assert await running.held_by(sink_db, "alice") == 1
            held.clear()  # the role is revoked in the control plane
            async with asyncio.timeout(10):
                await sink
        await kafka_producer.stop_all()

        assert await _read(topic, 1) == [(None, {"orders": [{"id": 1}]})]
        assert await running.held_by(sink_db, "alice") == 0

    async def test_a_sink_without_the_write_right_is_refused_and_writes_nothing(
        self, tmp_path, monkeypatch
    ):
        """REQ-812 (amended 2026-11-16), REQ-868: a role without the write right is refused by name
        before anything is produced: the topic it named is never written, nor created."""
        from aiokafka.admin import AIOKafkaAdminClient  # noqa: PLC0415

        from provisa.api.data.subscription_sse import handle_subscription_sse
        from provisa.api.errors import ApiError
        from provisa.core.platform_state import sinks as running

        topic = f"provisa-refused-{uuid.uuid4().hex[:8]}"
        call, sink_db = _sink_call(tmp_path, monkeypatch, topic, _READER)

        with pytest.raises(ApiError) as refused:
            await handle_subscription_sse(**call)
        assert refused.value.status_code == 403
        assert refused.value.code == "subscribe.sink_needs_write"
        assert await running.held_by(sink_db, "alice") == 0

        admin = AIOKafkaAdminClient(bootstrap_servers=_kafka_bootstrap())
        await admin.start()
        try:
            assert topic not in await admin.list_topics()
        finally:
            await admin.close()
