# Copyright (c) 2026 Kenneth Stott
# Canary: 6b0d3e71-52c9-4a8f-9e14-c7a2f5d08b39
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A request with a large output limit is streamed where the vendor's SDK requires it (REQ-464),
and a failed request is described by a catalogued message (REQ-413).

The Anthropic SDK is real here and its HTTP transport is faked (respx): no vendor is called.

Source: provisa/llm/client.py
"""

from __future__ import annotations

import json

import httpx
import pytest
import respx

from provisa.llm import client as llm_client
from provisa.llm.client import (
    CALL_TIMEOUT_S,
    LLMEmptyResponseError,
    LLMTimeoutError,
    ProvisaLLMClient,
    failure,
)

ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
MODEL = "claude-fable-5-2"


def _client(vendor: str = "anthropic", endpoints: list[dict] | None = None) -> ProvisaLLMClient:
    cfg = {
        "ai_models": {"relationship_inference": {"vendor": vendor, "model": MODEL}},
        "ai_endpoints": endpoints or [],
    }
    return ProvisaLLMClient("relationship_inference", config=cfg, api_keys={vendor: "test-key"})


def _sse(texts: list[str], stop_reason: str, output_tokens: int) -> httpx.Response:
    """The Messages API's event stream for one text answer."""
    events: list[tuple[str, dict]] = [
        (
            "message_start",
            {
                "type": "message_start",
                "message": {
                    "id": "msg_1",
                    "type": "message",
                    "role": "assistant",
                    "model": MODEL,
                    "content": [],
                    "stop_reason": None,
                    "stop_sequence": None,
                    "usage": {"input_tokens": 12, "output_tokens": 1},
                },
            },
        ),
        (
            "content_block_start",
            {
                "type": "content_block_start",
                "index": 0,
                "content_block": {"type": "text", "text": ""},
            },
        ),
        *[
            (
                "content_block_delta",
                {
                    "type": "content_block_delta",
                    "index": 0,
                    "delta": {"type": "text_delta", "text": text},
                },
            )
            for text in texts
        ],
        ("content_block_stop", {"type": "content_block_stop", "index": 0}),
        (
            "message_delta",
            {
                "type": "message_delta",
                "delta": {"stop_reason": stop_reason, "stop_sequence": None},
                "usage": {"output_tokens": output_tokens},
            },
        ),
        ("message_stop", {"type": "message_stop"}),
    ]
    body = "".join(f"event: {name}\ndata: {json.dumps(data)}\n\n" for name, data in events)
    return httpx.Response(200, headers={"content-type": "text/event-stream"}, content=body)


def _whole(text: str) -> httpx.Response:
    """The Messages API's answer to an unstreamed request."""
    return httpx.Response(
        200,
        json={
            "id": "msg_1",
            "type": "message",
            "role": "assistant",
            "model": MODEL,
            "content": [{"type": "text", "text": text}],
            "stop_reason": "end_turn",
            "stop_sequence": None,
            "usage": {"input_tokens": 12, "output_tokens": 3},
        },
    )


def _sent(route: respx.Route) -> dict:
    return json.loads(route.calls.last.request.content)


@respx.mock
def test_an_anthropic_request_for_128000_tokens_is_streamed_into_a_completion():
    route = respx.post(ANTHROPIC_URL).mock(return_value=_sse(["[1,", " 2]"], "end_turn", 7))

    answer = _client().complete_sync_detailed("list them", "You are a data analyst.", 128_000)

    assert answer.text == "[1, 2]"
    assert (answer.vendor, answer.model) == ("anthropic", MODEL)
    assert answer.cut_off is False
    assert (answer.usage.prompt_tokens, answer.usage.completion_tokens) == (12, 7)
    sent = _sent(route)
    assert sent["stream"] is True
    assert sent["max_tokens"] == 128_000
    assert sent["system"] == "You are a data analyst."
    assert sent["messages"] == [{"role": "user", "content": "list them"}]


@respx.mock
def test_a_streamed_answer_that_stopped_at_its_limit_is_cut_off():
    respx.post(ANTHROPIC_URL).mock(return_value=_sse(['[{"a": 1}, {"a"'], "max_tokens", 128_000))

    answer = _client().complete_sync_detailed("list them", "s", 128_000)

    assert answer.cut_off is True
    assert answer.text == '[{"a": 1}, {"a"'
    assert answer.usage.completion_tokens == 128_000


@respx.mock
def test_an_anthropic_style_endpoint_is_streamed_at_its_own_address_with_its_key():
    route = respx.post("https://gw.test/anthropic/v1/messages").mock(
        return_value=_sse(["ok"], "end_turn", 1)
    )
    endpoints = [{"id": "gw", "style": "anthropic", "base_url": "https://gw.test/anthropic"}]

    answer = _client("gw", endpoints).complete_sync_detailed("hi", "s", 128_000)

    assert (answer.text, answer.vendor, answer.cut_off) == ("ok", "gw", False)
    assert _sent(route)["stream"] is True
    assert route.calls.last.request.headers["x-api-key"] == "test-key"


@respx.mock
def test_a_streamed_request_carries_the_json_answer_and_reasoning_off_settings():
    route = respx.post(ANTHROPIC_URL).mock(return_value=_sse(['{"a": 1}'], "end_turn", 4))
    schema = {"type": "object", "properties": {"a": {"type": "integer"}}, "required": ["a"]}

    import asyncio

    answer = asyncio.run(
        _client().complete_detailed(
            "p", "s", 128_000, reasoning_off=True, json_schema=schema, json_name="x"
        )
    )

    assert answer.text == '{"a": 1}'
    sent = _sent(route)
    assert sent["thinking"] == {"type": "disabled"}
    assert sent["output_config"] == {"format": {"type": "json_schema", "schema": schema}}


@respx.mock
def test_a_streamed_answer_with_no_text_is_an_empty_response():
    respx.post(ANTHROPIC_URL).mock(return_value=_sse([], "max_tokens", 128_000))

    with pytest.raises(LLMEmptyResponseError) as raised:
        _client().complete_sync_detailed("p", "s", 128_000)

    assert raised.value.answer.cut_off is True


@respx.mock
def test_a_small_request_is_sent_whole_as_before():
    route = respx.post(ANTHROPIC_URL).mock(return_value=_whole(" fine "))

    answer = _client().complete_sync_detailed("p", "s", 1024)

    assert answer.text == "fine"
    assert "stream" not in _sent(route)


@pytest.mark.parametrize(
    ("model", "max_tokens"),
    [
        (MODEL, 1024),
        (MODEL, 21_333),
        (MODEL, 21_334),
        (MODEL, 128_000),
        ("claude-opus-4-1-20250805", 8_192),
        ("claude-opus-4-1-20250805", 8_193),
    ],
)
def test_the_streaming_rule_is_the_anthropic_sdks_own(model: str, max_tokens: int):
    """The client streams exactly the requests the SDK refuses to send whole."""
    import anthropic
    from anthropic._constants import MODEL_NONSTREAMING_TOKENS

    sdk = anthropic.Anthropic(api_key="test-key")
    try:
        sdk._calculate_nonstreaming_timeout(max_tokens, MODEL_NONSTREAMING_TOKENS.get(model))
        refused = False
    except ValueError:
        refused = True

    assert llm_client.anthropic_requires_stream(model, max_tokens) is refused


def test_an_openai_style_request_is_never_streamed():
    endpoints = [{"id": "gw", "style": "openai", "base_url": "https://gw.test/v1"}]
    client = _client("gw", endpoints)

    assert client.streams("gw", MODEL, 128_000) is False
    assert _client("openai").streams("openai", "gpt-x", 128_000) is False


def test_a_streamed_request_is_held_to_the_time_its_answer_may_take():
    client = _client()

    # The SDK expects 128,000 tokens to take an hour; a streamed request gets that in proportion.
    assert client.call_timeout_s("anthropic", MODEL, 128_000) == 3600.0
    assert client.call_timeout_s("anthropic", MODEL, 64_000) == 1800.0
    # A request sent whole keeps the one bound of every call.
    assert client.call_timeout_s("anthropic", MODEL, 1024) == CALL_TIMEOUT_S
    assert _client("openai").call_timeout_s("openai", "gpt-x", 128_000) == CALL_TIMEOUT_S


def test_a_call_that_outlives_its_bound_names_the_seconds(monkeypatch):
    import threading

    monkeypatch.setattr(llm_client, "CALL_TIMEOUT_S", 0.05)
    client = _client()
    release = threading.Event()

    def stalled(*_a, **_k):
        release.wait(5)
        raise RuntimeError("late")

    monkeypatch.setattr(client, "_respond", stalled)
    try:
        with pytest.raises(LLMTimeoutError) as raised:
            client.complete_sync_detailed("p", "s", 1024)
    finally:
        release.set()
    assert raised.value.seconds == 0.05


class TestFailure:
    """What a failed request is, as the product says it: a code and its params."""

    def test_a_vendor_error_is_a_failed_request_carrying_the_vendors_reason(self):
        said = failure(RuntimeError("overloaded_error: try again"))
        assert said["code"] == "llm.request_failed"
        assert said["params"] == {"reason": "overloaded_error: try again"}
        assert said["message"] == "The request to the model failed: overloaded_error: try again"

    def test_no_answer_in_time(self):
        said = failure(LLMTimeoutError("anthropic:m gave no answer within 300 seconds", 300.0))
        assert said["code"] == "llm.no_answer_in_time"
        assert said["params"] == {"seconds": 300}
        assert said["message"] == "The model gave no answer within 300 seconds."

    def test_an_answer_with_no_text(self):
        from provisa.llm.client import Completion

        exc = LLMEmptyResponseError("no content", Completion("", "v", "m", None, False))
        said = failure(exc)
        assert said["code"] == "llm.empty_answer"
        assert said["params"] == {}
        assert said["message"] == "The model returned no text."

    def test_an_error_with_no_text_is_named_by_its_type(self):
        assert failure(KeyError())["params"] == {"reason": "KeyError"}
