# Copyright (c) 2026 Kenneth Stott
# Canary: 4d1e6b2a-9f3c-4a7e-8b1d-2c5a7f9e0d6b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Unit tests for ProvisaLLMClient custom AI-endpoint dispatch (REQ-1790).

Source: provisa/llm/client.py — ProvisaLLMClient._complete_sync
"""

from __future__ import annotations

import asyncio
import os
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from provisa.llm.client import LLMEmptyResponseError, ProvisaLLMClient


def _aisuite_stub():
    """A stand-in for the `aisuite` module: records every Client(config) call and returns a
    completion whose text echoes back what it was called with, so assertions read the call shape
    off the response instead of inspecting a mock's call history."""
    created: list[dict] = []

    class _FakeClient:
        def __init__(self, config=None):
            created.append(config or {})
            self.chat = MagicMock()
            self.chat.completions.create.return_value.choices = [
                MagicMock(message=MagicMock(content=" ok "))
            ]

    stub = MagicMock()
    stub.Client = _FakeClient
    return stub, created


class TestAiEndpointDispatch:
    def test_custom_endpoint_overrides_base_url_and_style(self):
        cfg = {
            "ai_models": {"sql_generation": {"vendor": "my-gateway", "model": "gpt-4o"}},
            "ai_endpoints": [
                {
                    "id": "my-gateway",
                    "style": "openai",
                    "base_url": "https://gateway.internal/v1",
                    "api_key_env": "MY_GATEWAY_KEY",
                    "enabled": True,
                }
            ],
        }
        client = ProvisaLLMClient(operation="sql_generation", config=cfg)
        stub, created = _aisuite_stub()
        with (
            patch.dict(os.environ, {"MY_GATEWAY_KEY": "secret-key"}),
            patch.dict("sys.modules", {"aisuite": stub}),
        ):
            result = client.complete_sync("hello")
        assert result == "ok"
        assert created == [
            {"openai": {"base_url": "https://gateway.internal/v1", "api_key": "secret-key"}}
        ]

    def test_custom_endpoint_dispatches_to_anthropic_style_model_id(self):
        cfg = {
            "ai_models": {"sql_generation": {"vendor": "my-gateway", "model": "claude-x"}},
            "ai_endpoints": [
                {"id": "my-gateway", "style": "anthropic", "base_url": "https://gw/anthropic"}
            ],
        }
        client = ProvisaLLMClient(operation="sql_generation", config=cfg)
        stub, created = _aisuite_stub()
        with patch.dict("sys.modules", {"aisuite": stub}):
            client.complete_sync("hi")
        # No api_key_env configured: no key sent, and the created config carries only base_url.
        assert created == [{"anthropic": {"base_url": "https://gw/anthropic"}}]

    def test_missing_api_key_env_raises_rather_than_calling_unauthenticated(self):
        cfg = {
            "ai_models": {"sql_generation": {"vendor": "my-gateway", "model": "gpt-4o"}},
            "ai_endpoints": [
                {
                    "id": "my-gateway",
                    "style": "openai",
                    "base_url": "https://gw",
                    "api_key_env": "UNSET_ENV_VAR_FOR_TEST",
                }
            ],
        }
        client = ProvisaLLMClient(operation="sql_generation", config=cfg)
        with pytest.raises(Exception):
            client.complete_sync("hi")

    def test_disabled_endpoint_is_not_dispatched_as_a_custom_endpoint(self):
        # Disabled entries are excluded from the resolved endpoint map, so a role naming this
        # vendor falls through to aisuite's own resolution for a literal vendor named "my-gateway"
        # (which aisuite doesn't know), rather than silently using the disabled endpoint's config.
        cfg = {
            "ai_models": {"sql_generation": {"vendor": "my-gateway", "model": "gpt-4o"}},
            "ai_endpoints": [
                {
                    "id": "my-gateway",
                    "style": "openai",
                    "base_url": "https://gw",
                    "enabled": False,
                }
            ],
        }
        client = ProvisaLLMClient(operation="sql_generation", config=cfg)
        assert "my-gateway" not in client._endpoints


class TestEmptyContent:
    def test_none_content_raises_named_error(self):
        cfg = {
            "ai_models": {"sql_generation": {"vendor": "gw", "model": "m"}},
            "ai_endpoints": [
                {"id": "gw", "style": "openai", "base_url": "https://gw/v1", "enabled": True}
            ],
        }
        client = ProvisaLLMClient(operation="sql_generation", config=cfg)
        stub, _ = _aisuite_stub()
        orig = stub.Client

        class _NoneClient(orig):
            def __init__(self, config=None):
                super().__init__(config)
                self.chat.completions.create.return_value.choices = [
                    MagicMock(message=MagicMock(content=None), finish_reason="length")
                ]

        stub.Client = _NoneClient
        with patch.dict("sys.modules", {"aisuite": stub}):
            with pytest.raises(LLMEmptyResponseError, match="finish_reason='length'"):
                client.complete_sync("hello", max_tokens=16)


def _detailed(*, content, finish_reason, completion_tokens, max_tokens=16):
    """``complete_detailed`` of a real client over a vendor that answers as told."""

    class _Answering:
        def __init__(self, config=None):
            self.chat = SimpleNamespace(
                completions=SimpleNamespace(
                    create=lambda **kwargs: SimpleNamespace(
                        choices=[
                            SimpleNamespace(
                                message=SimpleNamespace(content=content),
                                finish_reason=finish_reason,
                            )
                        ],
                        usage=SimpleNamespace(completion_tokens=completion_tokens),
                    )
                )
            )

    client = ProvisaLLMClient(
        "x", config={"ai_models": {"x": {"vendor": "anthropic", "model": "m"}}}
    )
    stub = MagicMock()
    stub.Client = _Answering
    with patch.dict("sys.modules", {"aisuite": stub}):
        return asyncio.run(client.complete_detailed("p", max_tokens=max_tokens))


class TestCutOff:
    @pytest.mark.parametrize(
        ("finish_reason", "completion_tokens", "cut_off"),
        [
            ("stop", 5, False),
            (None, 5, False),
            ("length", 5, True),
            ("max_tokens", 5, True),
            # aisuite drops finish_reason for the OpenAI-shaped vendors it converts: usage shows it.
            (None, 16, True),
        ],
    )
    def test_the_answer_says_whether_it_stopped_at_the_limit(
        self, finish_reason, completion_tokens, cut_off
    ):
        answered = _detailed(
            content="[", finish_reason=finish_reason, completion_tokens=completion_tokens
        )
        assert answered.cut_off is cut_off

    def test_no_text_at_the_limit_carries_the_cut_off_answer(self):
        with pytest.raises(LLMEmptyResponseError) as empty:
            _detailed(content=None, finish_reason="length", completion_tokens=16)
        assert empty.value.answer.cut_off is True
        assert empty.value.answer.usage.completion_tokens == 16
        assert empty.value.answer.text == ""

    def test_no_text_short_of_the_limit_is_not_cut_off(self):
        with pytest.raises(LLMEmptyResponseError) as empty:
            _detailed(content=None, finish_reason="stop", completion_tokens=0)
        assert empty.value.answer.cut_off is False


class TestReasoningOff:
    """``reasoning_off`` is stated on each wire in that wire's own words, or the call fails."""

    @staticmethod
    def _sent(cfg: dict, env: dict[str, str] | None = None) -> tuple[list[dict], list[dict]]:
        sent: list[dict] = []
        created: list[dict] = []

        class _Recording:
            def __init__(self, config=None):
                created.append(config or {})
                self.chat = SimpleNamespace(completions=SimpleNamespace(create=self.create))

            def create(self, **request):
                sent.append(request)
                return SimpleNamespace(
                    choices=[
                        SimpleNamespace(message=SimpleNamespace(content="ok"), finish_reason="stop")
                    ],
                    usage=SimpleNamespace(completion_tokens=1),
                )

        client = ProvisaLLMClient("x", config=cfg, api_keys={"anthropic": "k"})
        stub = MagicMock()
        stub.Client = _Recording
        with patch.dict(os.environ, env or {}), patch.dict("sys.modules", {"aisuite": stub}):
            asyncio.run(client.complete_detailed("p", max_tokens=16, reasoning_off=True))
        return sent, created

    @staticmethod
    def _endpoint(style: str, base_url: str) -> dict:
        return {
            "ai_models": {"x": {"vendor": "gw", "model": "m"}},
            "ai_endpoints": [{"id": "gw", "style": style, "base_url": base_url}],
        }

    def test_openrouter_is_sent_its_reasoning_object_disabled(self):
        sent, _ = self._sent(self._endpoint("openai", "https://openrouter.ai/api/v1"))
        assert sent == [
            {
                "model": "openai:m",
                "messages": [
                    {"role": "system", "content": "You are a helpful assistant."},
                    {"role": "user", "content": "p"},
                ],
                "max_tokens": 16,
                "extra_body": {"reasoning": {"enabled": False}},
            }
        ]

    def test_anthropic_is_sent_thinking_disabled(self):
        sent, _ = self._sent({"ai_models": {"x": {"vendor": "anthropic", "model": "m"}}})
        assert sent[0]["thinking"] == {"type": "disabled"}

    def test_an_anthropic_style_endpoint_is_sent_thinking_disabled(self):
        sent, _ = self._sent(self._endpoint("anthropic", "https://gw/anthropic"))
        assert sent[0]["thinking"] == {"type": "disabled"}

    def test_an_openai_wire_that_is_not_openrouter_is_sent_no_reasoning_setting(self):
        # Maintainer decision 2026-11-04: only wires with a documented switch get one.
        sent, _ = self._sent(self._endpoint("openai", "https://gw/v1"))
        assert len(sent) == 1
        assert "extra_body" not in sent[0] and "thinking" not in sent[0]

    def test_a_vendor_without_a_reasoning_switch_is_sent_no_reasoning_setting(self):
        sent, _ = self._sent({"ai_models": {"x": {"vendor": "groq", "model": "m"}}})
        assert len(sent) == 1
        assert "extra_body" not in sent[0] and "thinking" not in sent[0]

    def test_a_call_not_asking_for_reasoning_off_is_sent_as_before(self):
        cfg = self._endpoint("openai", "https://openrouter.ai/api/v1")
        client = ProvisaLLMClient("x", config=cfg)
        sent: list[dict] = []
        stub = MagicMock()
        stub.Client.return_value.chat.completions.create.side_effect = lambda **r: (
            sent.append(r)
            or SimpleNamespace(
                choices=[
                    SimpleNamespace(message=SimpleNamespace(content="ok"), finish_reason="stop")
                ],
                usage=None,
            )
        )
        with patch.dict("sys.modules", {"aisuite": stub}):
            asyncio.run(client.complete_detailed("p", max_tokens=16))
        assert "extra_body" not in sent[0] and "thinking" not in sent[0]
