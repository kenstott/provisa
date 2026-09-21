# Copyright (c) 2026 Kenneth Stott
# Canary: 4d1e6b2a-9f3c-4a7e-8b1d-2c5a7f9e0d6b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Unit tests for ProvisaLLMClient custom AI-endpoint dispatch (REQ-1790).

Source: provisa/llm/client.py — ProvisaLLMClient._complete_sync
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest

from provisa.llm.client import ProvisaLLMClient


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
