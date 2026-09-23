# Copyright (c) 2026 Kenneth Stott
# Canary: 4e8a1c7d-2f5b-4a90-9d6e-7c3f1a8b5e02
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1349: MCP chat/tools must resolve the ACTING ORG's config (org_settings overrides layered
on the deployment config), not the static deployment-only config loaded once at process startup.

Before this fix, provisa/api/mcp/tools.py's _resolve_embedding_model and provisa/api/mcp/chat.py's
_resolve_vendor/_llm_configured/_resolve_model read state.config / the bare deployment file only —
an org that registered a vector model or picked an mcp_chat vendor through the AI Models UI (which
writes to the org_settings table, per REQ-1349) would never see it take effect, no matter how the
process was restarted, because these code paths never looked at org_settings at all.
"""

from __future__ import annotations

import types

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from provisa.api.mcp import chat as chat_mod
from provisa.api.mcp import tools as mcp_tools
from provisa.core.database import Database
from provisa.core.org_settings import write_org_overrides
from provisa.core.schema_org import org_settings

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def tenant_db(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'org_cfg.db'}")
    async with engine.begin() as c:
        await c.run_sync(lambda s: org_settings.metadata.create_all(s, tables=[org_settings]))
    db = Database(engine, name="tenant")
    yield db
    await engine.dispose()


def _state(tenant_db):
    return types.SimpleNamespace(tenant_db=tenant_db, contexts={"analyst": object()})


class TestEmbeddingModelHonorsOrgOverride:
    async def test_org_registered_vector_model_is_picked_up(self, tenant_db, monkeypatch):
        monkeypatch.setattr("provisa.api.admin._config_io.read_config", lambda: {"sources": []})
        await write_org_overrides(
            tenant_db,
            {
                "vector_models": [
                    {
                        "id": "nomic-embed-text",
                        "provider": "ollama",
                        "dimensions": 768,
                        "base_url": "http://localhost:11434",
                        "enabled": True,
                    }
                ]
            },
            updated_by="test",
        )
        model = await mcp_tools._resolve_embedding_model(_state(tenant_db))
        assert model.id == "nomic-embed-text"
        assert model.provider == "ollama"
        assert model.dimensions == 768

    async def test_no_org_override_and_no_deployment_entry_fails_loud(self, tenant_db, monkeypatch):
        monkeypatch.setattr("provisa.api.admin._config_io.read_config", lambda: {"sources": []})
        with pytest.raises(ValueError, match="embedding model"):
            await mcp_tools._resolve_embedding_model(_state(tenant_db))


class TestChatVendorHonorsOrgOverride:  # REQ-1349, REQ-1797
    async def test_org_selected_vendor_is_picked_up(self, tenant_db, monkeypatch):
        monkeypatch.setattr("provisa.api.admin._config_io.read_config", lambda: {"sources": []})
        await write_org_overrides(
            tenant_db,
            {"ai_models": {"mcp_chat": {"vendor": "openai", "model": "gpt-5"}}},
            updated_by="test",
        )
        assert await chat_mod._resolve_vendor(_state(tenant_db)) == "openai"
        assert await chat_mod._resolve_model(_state(tenant_db)) == "gpt-5"

    async def test_llm_configured_sees_org_vendor_and_missing_key(self, tenant_db, monkeypatch):
        monkeypatch.setattr("provisa.api.admin._config_io.read_config", lambda: {"sources": []})
        monkeypatch.delenv("OPENAI_API_KEY", raising=False)
        await write_org_overrides(
            tenant_db,
            {"ai_models": {"mcp_chat": {"vendor": "openai", "model": "gpt-5"}}},
            updated_by="test",
        )
        ok, reason = await chat_mod._llm_configured(_state(tenant_db))
        assert ok is False
        assert "OPENAI_API_KEY" in reason
