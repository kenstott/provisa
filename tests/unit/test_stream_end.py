# Copyright (c) 2026 Kenneth Stott
# Canary: 85b83cd8-f9bf-461d-b14f-23b24a4db83e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A stream that ends on a failure tells its subscriber why, in one last frame.

An Avro topic on a source that names no schema registry is discovered at the first message, after
the stream has opened: the refusal had a name and the subscriber got a closed connection. The
same was true of every failure after a stream opened, on both subscription surfaces."""

from __future__ import annotations

import json
import logging

import pytest

from provisa.api.data import stream_end
from provisa.api.errors import ApiError
from provisa.kafka.avro_registry import refuse_avro_without_registry


async def _feed(*chunks, then: BaseException | None = None):
    for chunk in chunks:
        yield chunk
    if then is not None:
        raise then


async def _all(stream) -> list[str]:
    return [chunk async for chunk in stream]


def _table_frame(frame: str) -> dict:
    event, data = frame.rstrip("\n").split("\n")
    assert event == "event: stream_error"
    assert frame.endswith("\n\n")
    return json.loads(data.removeprefix("data: "))


async def test_a_stream_that_ends_normally_sends_nothing_more():
    assert await _all(stream_end.ending_by_name(_feed("a", "b"), "s1", "table t")) == ["a", "b"]


async def test_a_named_refusal_reaches_the_subscriber_with_its_code_and_params():
    refused = refuse_avro_without_registry("dbserver1.app.orders")
    frames = await _all(stream_end.ending_by_name(_feed("a", then=refused), "s1", "table orders"))
    assert frames[0] == "a" and len(frames) == 2  # what was delivered, then one last frame
    assert _table_frame(frames[1]) == {
        "detail": str(refused),
        "code": "subscribe.avro_topic_without_registry",
        "params": {"topic": "dbserver1.app.orders"},
        "subscription_id": "s1",
    }


async def test_an_api_error_is_said_as_the_http_handler_says_it():
    refused = ApiError(503, "subscribe.db_pool_unavailable", "Database pool not available")
    (frame,) = await _all(stream_end.ending_by_name(_feed(then=refused), "s2", "table t"))
    assert _table_frame(frame) == {
        "detail": "Database pool not available",
        "code": "subscribe.db_pool_unavailable",
        "params": {},
        "subscription_id": "s2",
    }


async def test_an_unexpected_failure_says_no_more_than_a_request_would_be_told(caplog):
    """The HTTP handler answers an unexpected exception with "Internal server error" and its
    type. The stream says the same; the message and the traceback go to the log, under the id
    the frame carries."""
    boom = RuntimeError("connection string postgres://user:hunter2@db/x is wrong")
    with caplog.at_level(logging.ERROR, logger="provisa.api.data.stream_end"):
        (frame,) = await _all(stream_end.ending_by_name(_feed(then=boom), "s3", "table t"))
    body = _table_frame(frame)
    assert body == {
        "detail": "Internal server error",
        "type": "RuntimeError",
        "subscription_id": "s3",
    }
    assert "hunter2" not in frame
    (record,) = caplog.records
    assert "s3" in record.getMessage() and record.exc_info is not None
    assert "hunter2" in caplog.text  # the whole exception is in the server's log


async def test_a_deadline_says_the_request_timed_out():
    (frame,) = await _all(stream_end.ending_by_name(_feed(then=TimeoutError()), "s4", "table t"))
    assert _table_frame(frame) == {"detail": "Request timed out", "subscription_id": "s4"}


async def test_a_refusal_with_a_code_is_not_logged_as_an_error(caplog):
    refused = refuse_avro_without_registry("t")
    with caplog.at_level(logging.INFO, logger="provisa.api.data.stream_end"):
        await _all(stream_end.ending_by_name(_feed(then=refused), "s5", "table t"))
    (record,) = caplog.records
    assert record.levelno == logging.INFO and record.exc_info is None
    assert (
        "s5" in record.getMessage()
        and "subscribe.avro_topic_without_registry" in record.getMessage()
    )


async def test_a_graphql_subscription_keeps_the_graphql_shape():
    refused = refuse_avro_without_registry("dbserver1.app.orders")
    (frame,) = await _all(
        stream_end.ending_by_name(
            _feed(then=refused), "s6", "change feed", frame=stream_end.graphql_stream_error
        )
    )
    assert frame.startswith("data: ") and frame.endswith("\n\n") and "event:" not in frame
    assert json.loads(frame.removeprefix("data: ")) == {
        "errors": [
            {
                "message": str(refused),
                "extensions": {
                    "code": "subscribe.avro_topic_without_registry",
                    "params": {"topic": "dbserver1.app.orders"},
                    "subscription_id": "s6",
                },
            }
        ]
    }


async def test_a_cancelled_stream_is_not_answered():
    """The subscriber left: there is no one to tell, and cancellation is not swallowed."""
    import asyncio

    with pytest.raises(asyncio.CancelledError):
        await _all(stream_end.ending_by_name(_feed(then=asyncio.CancelledError()), "s7", "t"))


def test_the_bodies_are_the_http_handlers_bodies():
    """The unexpected-failure and timeout bodies are the ones provisa/api/app.py answers with."""
    from pathlib import Path

    app = (Path(stream_end.__file__).parents[1] / "app.py").read_text()
    assert 'content={"detail": "Internal server error", "type": type(exc).__name__}' in app
    assert 'content={"detail": "Request timed out"}' in app
    assert stream_end.failure_body(ValueError("x")) == {
        "detail": "Internal server error",
        "type": "ValueError",
    }


# --- through the table subscription's endpoint ---------------------------------------------------


class _AvroWithoutRegistry:
    """A change feed whose first message is Avro on a source that names no registry."""

    closed = False

    async def watch(self, table):
        raise refuse_avro_without_registry(f"dbserver1.app.{table}")
        yield  # pragma: no cover - makes this an async generator

    async def close(self):
        self.closed = True


async def test_a_subscriber_of_an_avro_topic_with_no_registry_is_told_so_and_the_stream_closes(
    monkeypatch,
):
    from types import SimpleNamespace

    from provisa.api.data import subscribe as sub
    from provisa.core.request_context import reset_current_org, set_current_org

    meta = SimpleNamespace(table_id=7, source_id="shop", domain_id="sales")
    state = SimpleNamespace(
        tenant_db=object(),
        roles={"analyst": {}},
        view_context=SimpleNamespace(tables={"orders": meta}, pk_columns={7: ["id"]}),
        table_path_maps={},
        source_types={"shop": "mysql"},
        cdc_sources={"shop": SimpleNamespace(cdc=SimpleNamespace(schema_registry_url=None))},
    )
    feed = _AvroWithoutRegistry()

    async def _no_rows(sql, key, params=None):
        return []

    async def _no_slot(_state, _role):
        return None

    async def _still_there():
        return False

    monkeypatch.setattr("provisa.api.app.state", state)
    monkeypatch.setattr("provisa.live.governed.table_ref", lambda _m: '"sales"."orders"')
    monkeypatch.setattr("provisa.live.governed.governed_rows", _no_rows)
    monkeypatch.setattr(sub, "_resolve_provider_type", lambda *a: "debezium")
    monkeypatch.setattr(sub, "_build_provider_config", lambda *a: {})
    monkeypatch.setattr(sub, "_acquire_sse_slot", _no_slot)
    monkeypatch.setattr("provisa.subscriptions.registry.get_provider", lambda *a: feed)
    request = SimpleNamespace(state=SimpleNamespace(role="analyst"), is_disconnected=_still_there)

    token = set_current_org("acme")
    try:
        response = await sub.subscribe("orders", request)  # the stream opens: 200
        frames = [chunk async for chunk in response.body_iterator]
    finally:
        reset_current_org(token)

    assert response.status_code == 200
    # The stream's opening comment, then one last frame, and the stream is over.
    assert len(frames) == 2 and frames[0] == ": connected\n\n", frames
    frame = frames[1]
    body = _table_frame(frame)
    assert body["code"] == "subscribe.avro_topic_without_registry"
    assert body["params"] == {"topic": "dbserver1.app.orders"}
    assert len(body["subscription_id"]) == 16
    assert feed.closed
