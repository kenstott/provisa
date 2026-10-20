# Copyright (c) 2026 Kenneth Stott
# Canary: dbacca7d-9603-4de0-8334-11a294534729
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A demo sandbox's AI models (REQ-2006): the tier's one endpoint and model for every completion
role, never an embedding model, its key from the deployment org's vault and nobody else's."""

# Requirements: REQ-2006

from __future__ import annotations

import pytest

from provisa.core import sandbox_ai
from provisa.core.request_context import current_org

_URL = "https://openrouter.ai/api/v1"
_KEY = "${secret:sandbox_openrouter}"


@pytest.fixture
def tier_ai(monkeypatch):
    monkeypatch.setenv("PROVISA_SANDBOX_AI_BASE_URL", _URL)
    monkeypatch.setenv("PROVISA_SANDBOX_AI_API_KEY", _KEY)
    monkeypatch.delenv("PROVISA_SANDBOX_AI_MODEL", raising=False)


def test_off_until_the_endpoint_and_its_key_are_named(monkeypatch):
    monkeypatch.delenv("PROVISA_SANDBOX_AI_BASE_URL", raising=False)
    monkeypatch.setenv("PROVISA_SANDBOX_AI_API_KEY", _KEY)
    assert sandbox_ai.defaults_for("sbx0123456789ab") == {}
    monkeypatch.setenv("PROVISA_SANDBOX_AI_BASE_URL", _URL)
    monkeypatch.delenv("PROVISA_SANDBOX_AI_API_KEY", raising=False)
    assert sandbox_ai.defaults_for("sbx0123456789ab") == {}


def test_every_completion_role_of_a_sandbox_tier_org_uses_the_tier_model(tier_ai):
    for org in ("sbx0123456789ab", "sbxtplweblogs"):
        defaults = sandbox_ai.defaults_for(org)
        assert defaults["ai_endpoints"] == [
            {
                "id": "provisa-sandbox-ai",
                "style": "openai",
                "base_url": _URL,
                "api_key_env": _KEY,
                "enabled": True,
            }
        ]
        assert defaults["ai_models"] == {
            role: {"vendor": "provisa-sandbox-ai", "model": "anthropic/claude-haiku-5.5"}
            for role in (
                "table_description",
                "column_description",
                "relationship_inference",
                "sql_generation",
                "table_selection",
                "mcp_chat",
            )
        }
        assert "vector_models" not in defaults  # no embedding model in a sandbox
    assert sandbox_ai.defaults_for("acme") == {}  # any other org: nothing
    assert sandbox_ai.defaults_for(None) == {}


def test_the_roles_are_every_role_the_ai_models_page_assigns():
    from provisa.api.admin.ai_models_router import _AI_MODEL_ROLES

    assert set(sandbox_ai.AI_ROLES) == set(_AI_MODEL_ROLES)


async def test_a_sandboxs_own_settings_layer_on_the_tier_models(tier_ai, monkeypatch):
    from provisa.core import org_settings

    async def overrides(_db):
        return {"ai_models": {"mcp_chat": {"vendor": "anthropic", "model": "claude-haiku-5-5"}}}

    monkeypatch.setattr(org_settings, "read_org_overrides", overrides)
    monkeypatch.setattr("provisa.api.admin._config_io.read_config", lambda: {"ai_models": {}})
    token = current_org.set("sbx0123456789ab")
    try:
        resolved = await org_settings.resolve_org_config(object())  # type: ignore[arg-type]
    finally:
        current_org.reset(token)
    assert resolved["ai_models"]["mcp_chat"] == {"vendor": "anthropic", "model": "claude-haiku-5-5"}
    assert resolved["ai_models"]["sql_generation"]["vendor"] == "provisa-sandbox-ai"
    assert resolved["ai_endpoints"][0]["id"] == "provisa-sandbox-ai"


def test_only_the_tier_endpoint_as_the_setting_names_it_reads_the_deployment_vault(
    tier_ai, monkeypatch
):
    monkeypatch.setenv("ORG_ID", "default")
    ours = sandbox_ai.defaults_for("sbx0123456789ab")["ai_endpoints"][0]
    assert sandbox_ai.key_org(ours, "sbx0123456789ab") == "default"
    # A visitor's own endpoint -- the same id and key reference pointed at their own server, or
    # another reference -- is resolved in the sandbox's own vault, as any tenant field is.
    assert (
        sandbox_ai.key_org({**ours, "base_url": "https://evil.example/v1"}, "sbx0123456789ab")
        is None
    )
    assert sandbox_ai.key_org({**ours, "api_key_env": "${secret:other}"}, "sbx0123456789ab") is None
    assert sandbox_ai.key_org({**ours, "id": "mine"}, "sbx0123456789ab") is None
    assert sandbox_ai.key_org(ours, "acme") is None  # no other org reaches the deployment vault


async def test_the_tier_key_resolves_in_the_deployment_orgs_vault(tier_ai, monkeypatch):
    monkeypatch.setenv("ORG_ID", "default")
    seen = []

    async def resolve(value):
        seen.append((value, current_org.get()))
        return "sk-test"

    monkeypatch.setattr("provisa.core.secrets.resolve_api_key_field", resolve)
    token = current_org.set("sbx0123456789ab")
    try:
        keys = await sandbox_ai.vendor_keys("sbx0123456789ab")
        assert current_org.get() == "sbx0123456789ab"  # restored
    finally:
        current_org.reset(token)
    assert keys == {"provisa-sandbox-ai": "sk-test"}
    assert seen == [(_KEY, "default")]


async def test_a_key_that_resolves_to_nothing_is_refused_by_name(tier_ai, monkeypatch):
    async def resolve(_value):
        return None

    monkeypatch.setattr("provisa.core.secrets.resolve_api_key_field", resolve)
    with pytest.raises(ValueError, match="sandbox.ai_api_key"):
        await sandbox_ai.vendor_keys("sbx0123456789ab")


def test_a_roles_client_uses_the_key_resolved_for_the_tier_endpoint(tier_ai, monkeypatch):
    """The completion roles' client (provisa/llm/client.py) takes the key the caller resolved for
    the endpoint by its id; it never looks for an env var named after a ${secret:} reference."""
    import sys
    import types

    from provisa.llm.client import ProvisaLLMClient

    made = {}

    class FakeClient:
        def __init__(self, providers):
            made["providers"] = providers

        @property
        def chat(self):
            return self

        @property
        def completions(self):
            return self

        def create(self, **kw):
            made["model"] = kw["model"]
            msg = types.SimpleNamespace(content="ok")
            return types.SimpleNamespace(choices=[types.SimpleNamespace(message=msg)])

    monkeypatch.setitem(sys.modules, "aisuite", types.SimpleNamespace(Client=FakeClient))
    monkeypatch.setattr("provisa.api.admin._config_io.read_config", lambda: {})
    token = current_org.set("sbx0123456789ab")
    try:
        client = ProvisaLLMClient("sql_generation", api_keys={"provisa-sandbox-ai": "sk-test"})
        assert client.complete_sync("q", "s", 10) == "ok"
    finally:
        current_org.reset(token)
    assert made["providers"] == {"openai": {"base_url": _URL, "api_key": "sk-test"}}
    assert made["model"] == "openai:anthropic/claude-haiku-5.5"


async def test_a_visitor_cannot_choose_the_model_the_tier_endpoint_runs(tier_ai, monkeypatch):
    """The endpoint and its key are the deployment's and nothing meters a visitor's use: a
    sandbox's override of a role's model on it is ignored; a role on the visitor's own endpoint
    keeps their model."""
    from provisa.core import org_settings

    async def overrides(_db):
        return {
            "ai_models": {
                "mcp_chat": {"vendor": "provisa-sandbox-ai", "model": "anthropic/claude-opus-5.5"},
                "sql_generation": {"vendor": "mine", "model": "my-own-model"},
            },
            "ai_endpoints": [
                {"id": "mine", "style": "openai", "base_url": "https://mine.example/v1"}
            ],
        }

    monkeypatch.setattr(org_settings, "read_org_overrides", overrides)
    monkeypatch.setattr("provisa.api.admin._config_io.read_config", lambda: {"ai_models": {}})
    token = current_org.set("sbx0123456789ab")
    try:
        resolved = await org_settings.resolve_org_config(object())  # type: ignore[arg-type]
    finally:
        current_org.reset(token)
    assert resolved["ai_models"]["mcp_chat"] == {
        "vendor": "provisa-sandbox-ai",
        "model": "anthropic/claude-haiku-5.5",
    }
    assert resolved["ai_models"]["sql_generation"] == {"vendor": "mine", "model": "my-own-model"}


async def test_a_sandboxs_polly_runs_on_the_tiers_openrouter_haiku(monkeypatch):
    """cloud.provisa.dev's tier (scripts/deploy-cloud.sh push_overlay): OpenRouter, the key the
    node's own OPENROUTER_API_KEY by reference. A sandbox cloned from a template, with no AI
    settings of its own, has Polly ready on the tier's endpoint and Haiku -- and, without the
    key, is told which one it needs rather than ANTHROPIC_API_KEY."""
    from types import SimpleNamespace

    from provisa.api.mcp import chat
    from provisa.core import org_settings

    monkeypatch.setenv("PROVISA_SANDBOX_AI_BASE_URL", _URL)
    monkeypatch.setenv("PROVISA_SANDBOX_AI_API_KEY", "${env:OPENROUTER_API_KEY}")
    monkeypatch.delenv("PROVISA_SANDBOX_AI_MODEL", raising=False)
    monkeypatch.delenv("PROVISA_MCP_CHAT_MODEL", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.setenv("ORG_ID", "default")

    async def no_overrides(_db):
        return {}

    monkeypatch.setattr(org_settings, "read_org_overrides", no_overrides)
    monkeypatch.setattr("provisa.api.admin._config_io.read_config", lambda: {"ai_models": {}})
    state = SimpleNamespace(model_db=object())
    token = current_org.set("sbx0123456789ab")
    try:
        monkeypatch.setenv("OPENROUTER_API_KEY", "sk-or-test")
        assert await chat._resolve_vendor(state) == "provisa-sandbox-ai"
        assert await chat._resolve_model(state) == "anthropic/claude-haiku-5.5"
        assert await chat._llm_configured(state) == (True, "")
        monkeypatch.delenv("OPENROUTER_API_KEY")
        ready, why = await chat._llm_configured(state)
    finally:
        current_org.reset(token)
    assert not ready
    assert "OPENROUTER_API_KEY" in why and "ANTHROPIC_API_KEY" not in why
