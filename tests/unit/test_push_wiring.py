# Copyright (c) 2026 Kenneth Stott
# Canary: 225bc0af-0d8d-48a5-a6ae-d0d54536e99c
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for push_wiring._build_provider (REQ-1733) — the pure config→provider mapping,
without touching a real Kafka broker or WebSocket server (get_provider itself only constructs
the provider object; it never connects until .watch() is awaited)."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

import pytest

from provisa.core.models import Source, SourceType
from provisa.events.push_wiring import _build_provider


def _source(**kw) -> Source:
    defaults = {"id": "src", "type": SourceType.kafka, "host": "", "port": 0}
    defaults.update(kw)
    return Source(**defaults)


def test_kafka_missing_topic_returns_none(caplog):
    src = _source(type=SourceType.kafka, host="broker:9092")
    tbl = {"live": {}}
    with caplog.at_level(logging.WARNING):
        result = _build_provider(src, tbl, node="s.t")
    assert result is None
    assert "no live.kafka.topic" in caplog.text


def test_kafka_missing_bootstrap_servers_returns_none(caplog):
    src = _source(type=SourceType.kafka, host="")
    tbl = {"live": {"kafka": {"topic": "orders"}}}
    with caplog.at_level(logging.WARNING):
        result = _build_provider(src, tbl, node="s.t")
    assert result is None
    assert "no host configured" in caplog.text


def test_kafka_builds_provider_with_topic_as_watch_target():
    src = _source(type=SourceType.kafka, host="broker:9092")
    tbl = {"live": {"kafka": {"topic": "orders"}}}
    mock_provider = MagicMock()
    with patch(
        "provisa.subscriptions.registry.get_provider", return_value=mock_provider
    ) as get_provider:
        result = _build_provider(src, tbl, node="s.t")
    assert result == (mock_provider, "orders")
    get_provider.assert_called_once_with(
        "kafka", {"bootstrap_servers": "broker:9092", "group_id": "provisa-src"}
    )


def test_websocket_missing_url_and_host_returns_none(caplog):
    src = _source(type=SourceType.websocket, host="", port=0)
    with caplog.at_level(logging.WARNING):
        result = _build_provider(src, {}, node="s.t")
    assert result is None
    assert "no base_url or host" in caplog.text


def test_websocket_uses_base_url_override_when_set():
    src = _source(type=SourceType.websocket, base_url="wss://feed.example.com/stream")
    mock_provider = MagicMock()
    with patch(
        "provisa.subscriptions.registry.get_provider", return_value=mock_provider
    ) as get_provider:
        result = _build_provider(src, {}, node="s.t")
    assert result == (mock_provider, "s.t")
    assert get_provider.call_args[0][1]["url"] == "wss://feed.example.com/stream"


def test_websocket_derives_url_from_host_and_port_when_no_base_url():
    """Same override-else-derive shape airport/neo4j already use (REQ-1730)."""
    src = _source(type=SourceType.websocket, host="feed.example.com", port=8765)
    mock_provider = MagicMock()
    with patch(
        "provisa.subscriptions.registry.get_provider", return_value=mock_provider
    ) as get_provider:
        result = _build_provider(src, {}, node="s.t")
    assert result == (mock_provider, "s.t")
    assert get_provider.call_args[0][1]["url"] == "ws://feed.example.com:8765"


def test_non_push_source_type_returns_none():
    src = _source(type=SourceType.postgresql, host="db", port=5432)
    assert _build_provider(src, {}, node="s.t") is None


class TestPushSourceChangeSignal:
    """REQ-1907/REQ-929: a push source type carries an inherently-push change_signal. An unset
    signal resolves to the type's push signal (needs no cache_ttl, never built as a replica); an
    explicit non-push signal is refused by name — never silently overridden."""

    def test_unset_push_source_defaults_to_its_push_signal(self):
        from provisa.core.change_signal import is_push

        # host/port but NO change_signal -> the key is unset, so the type's push signal applies.
        assert _source(type=SourceType.ingest, host="", port=0).change_signal == "native"
        assert _source(type=SourceType.websocket, host="", port=0).change_signal == "native"
        assert _source(type=SourceType.kafka, host="", port=0).change_signal == "kafka"
        for st in (SourceType.ingest, SourceType.websocket, SourceType.kafka):
            assert is_push(_source(type=st, host="", port=0).change_signal)

    def test_an_explicit_push_signal_on_a_push_source_is_kept(self):
        assert _source(type=SourceType.kafka, change_signal="kafka").change_signal == "kafka"
        assert _source(type=SourceType.websocket, change_signal="native").change_signal == "native"

    def test_an_explicit_poll_signal_on_a_push_source_is_refused_by_name(self):
        from pydantic import ValidationError

        for st in (SourceType.kafka, SourceType.ingest, SourceType.websocket):
            with pytest.raises(ValidationError, match="is not a push signal"):
                _source(type=st, change_signal="ttl")

    def test_a_poll_source_keeps_the_ttl_default(self):
        assert _source(type=SourceType.postgresql, host="db", port=5432).change_signal == "ttl"
