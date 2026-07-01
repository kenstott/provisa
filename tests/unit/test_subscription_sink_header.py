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

    def test_topic_only_uses_env_or_default(self, monkeypatch):
        monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)
        broker, topic = _parse_sink_uri("kafka:///events")
        assert broker == "localhost:9092"
        assert topic == "events"

    def test_env_broker_fallback(self, monkeypatch):
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-host:9093")
        broker, topic = _parse_sink_uri("kafka:///orders")
        assert broker == "kafka-host:9093"
        assert topic == "orders"

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
    state.pg_pool = None
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
    async def test_header_present_returns_202_and_launches_sink(self):
        """X-Provisa-Sink header → 202 Accepted, asyncio.create_task called."""
        document = _make_minimal_document("orders")
        state, ctx, _table_meta = _make_state()
        raw_request = _make_request({"x-provisa-sink": "kafka://broker1:9092/orders-topic"})

        directives = MagicMock()
        directives.sink_topic = None
        directives.sink_broker = None
        directives.watermark_column = None

        with patch("asyncio.create_task") as mock_create_task:
            mock_create_task.return_value = MagicMock()
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
        mock_create_task.assert_called_once()

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
    async def test_sink_header_parsed_broker_and_topic_forwarded(self):
        """Parsed broker and topic from header are forwarded to _launch_kafka_sink."""
        document = _make_minimal_document("events")
        state, ctx, _table_meta = _make_state(table_name="events")
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
    async def test_directive_sink_topic_takes_precedence_over_header(self):
        """@sink directive topic takes precedence; header is secondary."""
        document = _make_minimal_document("orders")
        state, ctx, _table_meta = _make_state()
        # Header present but directive already has a topic
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
