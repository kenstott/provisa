# Copyright (c) 2026 Kenneth Stott
# Canary: 3b7e1d4a-6f2c-4a9e-b5d8-1c3e5f7a9b2d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holders.

"""Unit tests for REQ-812: X-Provisa-Sink header — header parsing and SSE-vs-sink branch."""

from __future__ import annotations

import json as _json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from provisa.api.data.subscription_sse import _parse_sink_uri, handle_subscription_sse


# ---------------------------------------------------------------------------
# _parse_sink_uri unit tests (pure, no I/O)
# ---------------------------------------------------------------------------


class TestParseSinkUri:
    def test_full_uri(self):
        broker, topic = _parse_sink_uri("kafka://broker1:9092/my-topic")
        assert broker == "broker1:9092"
        assert topic == "my-topic"

    def test_a_uri_without_a_broker_is_refused(self, monkeypatch):
        # REQ-812: the caller resolves the broker to the operator's cluster before this point;
        # a URI without one is refused, never filled from a default.
        monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)
        with pytest.raises(ValueError, match="No broker"):
            _parse_sink_uri("kafka:///events")

    def test_the_environment_supplies_no_broker_here(self, monkeypatch):
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-host:9093")
        with pytest.raises(ValueError, match="No broker"):
            _parse_sink_uri("kafka:///orders")

    def test_missing_topic_raises(self):
        with pytest.raises(ValueError, match="No topic"):
            _parse_sink_uri("kafka://broker:9092/")

    def test_topic_with_nested_path(self):
        _broker, topic = _parse_sink_uri("kafka://b:9092/ns.my-topic")
        assert topic == "ns.my-topic"


# ---------------------------------------------------------------------------
# handle_subscription_sse branch tests
# ---------------------------------------------------------------------------


def _make_minimal_document(field_name: str = "orders"):
    """Build a minimal mock GraphQL document with one subscription field."""
    from graphql.language.ast import (
        DocumentNode,
        FieldNode,
        NameNode,
        OperationDefinitionNode,
        SelectionSetNode,
    )

    field_node = FieldNode(name=NameNode(value=field_name), selection_set=None)
    selection_set = SelectionSetNode(selections=[field_node])
    op_def = OperationDefinitionNode(
        operation=None,  # type: ignore[arg-type]
        name=None,
        variable_definitions=[],
        directives=[],
        selection_set=selection_set,
    )
    return DocumentNode(definitions=[op_def])


def _make_state(table_name: str = "orders", source_id: str = "pg1"):
    table_meta = MagicMock()
    table_meta.table_name = table_name
    table_meta.type_name = table_name.capitalize()
    table_meta.source_id = source_id
    table_meta.catalog_name = None
    table_meta.schema_name = None

    ctx = MagicMock()
    ctx.tables = {table_name: table_meta}
    ctx.joins = {}

    state = MagicMock()
    state.source_types = {source_id: "postgresql"}
    state.contexts = {"role1": ctx}
    state.schemas = {"role1": MagicMock()}
    state.tenant_db = None
    state.pg_notify_tables = set()
    state.table_watermarks = {}
    state.source_pools = {}
    return state, ctx, table_meta


def _make_request(headers: dict[str, str]) -> MagicMock:
    req = MagicMock()
    req.headers = MagicMock()
    req.headers.get = lambda key, default="": headers.get(key.lower(), default)
    return req


class TestSinkBranchDecision:
    @pytest.mark.asyncio
    async def test_header_present_returns_202_and_launches_sink(self, monkeypatch):
        """X-Provisa-Sink header → 202 Accepted, the sink loop started as detached work.

        REQ-1882: started via spawn_long_lived — the sink outlives the request and runs until its
        source disconnects, so it gets its own thread (never the process loop or a pooled worker)."""
        document = _make_minimal_document("orders")
        state, ctx, _table_meta = _make_state()
        # REQ-030: a sink writes only to the operator's cluster.
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker1:9092")
        raw_request = _make_request({"x-provisa-sink": "kafka://broker1:9092/orders-topic"})

        directives = MagicMock()
        directives.sink_topic = None
        directives.sink_broker = None
        directives.watermark_column = None

        def _spawn(coro, *, name=None):
            spawned.append(name)
            coro.close()

        spawned: list = []
        with patch("provisa.core.connection_loop.spawn_long_lived", _spawn):
            response = await handle_subscription_sse(
                document=document,
                ctx=ctx,
                rls=MagicMock(),
                state=state,
                variables=None,
                role="user",
                role_id="role1",
                raw_request=raw_request,
                directives=directives,
            )

        from fastapi.responses import JSONResponse

        assert isinstance(response, JSONResponse)
        assert response.status_code == 202
        assert spawned == ["kafka-sink:orders"]

        raw_body = response.body
        body = _json.loads(bytes(raw_body) if isinstance(raw_body, memoryview) else raw_body)
        assert body["sink"] == "kafka://broker1:9092/orders-topic"
        assert body["table"] == "orders"

    @pytest.mark.asyncio
    async def test_header_absent_returns_streaming_response(self):
        """No X-Provisa-Sink header → StreamingResponse (SSE path), no sink launched."""
        document = _make_minimal_document("orders")
        state, ctx, _table_meta = _make_state()
        raw_request = _make_request({})  # no sink header

        directives = MagicMock()
        directives.sink_topic = None
        directives.sink_broker = None
        directives.watermark_column = None

        with patch("asyncio.create_task") as mock_create_task:
            # Prevent the disconnect-watcher task from actually running
            mock_create_task.return_value = MagicMock()
            # Prevent the registry import from failing
            with patch(
                "provisa.subscriptions.registry.get_provider",
                side_effect=RuntimeError("no provider"),
            ):
                response = await handle_subscription_sse(
                    document=document,
                    ctx=ctx,
                    rls=MagicMock(),
                    state=state,
                    variables=None,
                    role="user",
                    role_id="role1",
                    raw_request=raw_request,
                    directives=directives,
                )

        from fastapi.responses import StreamingResponse

        assert isinstance(response, StreamingResponse)
        # create_task may be called for the disconnect watcher — but NOT for a sink loop
        # Verify no 202 JSON response was produced
        assert not hasattr(response, "status_code") or response.status_code == 200

    @pytest.mark.asyncio
    async def test_sink_header_parsed_broker_and_topic_forwarded(self, monkeypatch):
        """Parsed broker and topic from header are forwarded to _launch_kafka_sink."""
        document = _make_minimal_document("events")
        state, ctx, _table_meta = _make_state(table_name="events")
        # REQ-030: a sink writes only to the operator's cluster.
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-host:9093")
        raw_request = _make_request({"x-provisa-sink": "kafka://kafka-host:9093/event-stream"})

        directives = MagicMock()
        directives.sink_topic = None
        directives.sink_broker = None
        directives.watermark_column = None

        with patch(
            "provisa.api.data.subscription_sse._launch_kafka_sink",
            new_callable=AsyncMock,
        ) as mock_launch:
            from fastapi.responses import JSONResponse

            mock_launch.return_value = JSONResponse(
                status_code=202,
                content={
                    "status": "streaming",
                    "sink": "kafka://kafka-host:9093/event-stream",
                    "table": "events",
                },
            )
            await handle_subscription_sse(
                document=document,
                ctx=ctx,
                rls=MagicMock(),
                state=state,
                variables=None,
                role="user",
                role_id="role1",
                raw_request=raw_request,
                directives=directives,
            )

        mock_launch.assert_called_once()
        call_kwargs = mock_launch.call_args.kwargs
        # sink_header is reconstructed from parsed broker + topic
        assert "kafka-host:9093" in call_kwargs["sink_header"]
        assert "event-stream" in call_kwargs["sink_header"]

    @pytest.mark.asyncio
    async def test_directive_sink_topic_takes_precedence_over_header(self, monkeypatch):
        """@sink directive topic takes precedence; header is secondary."""
        document = _make_minimal_document("orders")
        state, ctx, _table_meta = _make_state()
        # Header present but directive already has a topic
        # REQ-030: a sink writes only to the operator's cluster.
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "directive-broker:9092")
        raw_request = _make_request({"x-provisa-sink": "kafka://broker1:9092/header-topic"})

        directives = MagicMock()
        directives.sink_topic = "directive-topic"
        directives.sink_broker = "directive-broker:9092"
        directives.watermark_column = None

        with patch(
            "provisa.api.data.subscription_sse._launch_kafka_sink",
            new_callable=AsyncMock,
        ) as mock_launch:
            from fastapi.responses import JSONResponse

            mock_launch.return_value = JSONResponse(
                status_code=202,
                content={
                    "status": "streaming",
                    "sink": "kafka://directive-broker:9092/directive-topic",
                    "table": "orders",
                },
            )
            await handle_subscription_sse(
                document=document,
                ctx=ctx,
                rls=MagicMock(),
                state=state,
                variables=None,
                role="user",
                role_id="role1",
                raw_request=raw_request,
                directives=directives,
            )

        mock_launch.assert_called_once()
        call_kwargs = mock_launch.call_args.kwargs
        assert "directive-topic" in call_kwargs["sink_header"]


class TestKafkaSubscriptionBrokers:
    """REQ-812: a subscription to a Kafka-backed table reads its source's own brokers — never a
    default one (it used to read ``localhost:9092`` whatever the source named)."""

    def test_the_source_brokers_are_used(self):
        from types import SimpleNamespace

        from provisa.api.data.subscribe import _build_provider_config

        state = SimpleNamespace(kafka_bootstrap={"events-src": "k1:9093,k2:9093"})
        config = _build_provider_config("kafka", "events-src", "order_events", None, state)
        assert config == {"bootstrap_servers": "k1:9093,k2:9093"}

    def test_a_source_without_brokers_is_refused_by_name(self):
        from types import SimpleNamespace

        from provisa.api.data.subscribe import _build_provider_config

        with pytest.raises(ValueError, match="'events-src'"):
            _build_provider_config(
                "kafka", "events-src", "t", None, SimpleNamespace(kafka_bootstrap={})
            )


class TestStreamSourcesMustBeLoaded:
    """A subscription to an RSS or WebSocket table whose source is not loaded is refused by name,
    never handed an empty provider configuration."""

    @pytest.mark.parametrize(
        "source_type, attribute",
        [("rss", "rss_sources"), ("websocket", "websocket_sources")],
    )
    def test_an_unloaded_source_is_refused(self, source_type, attribute):
        from types import SimpleNamespace

        from provisa.api.data.subscribe import _build_provider_config

        with pytest.raises(ValueError, match="'feed-src' is not loaded"):
            _build_provider_config(
                source_type, "feed-src", "t", None, SimpleNamespace(**{attribute: {}})
            )
