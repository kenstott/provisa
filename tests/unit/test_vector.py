# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Vector search — Phase B0 foundation (REQ-419/420/421/422)."""

from __future__ import annotations

import types

import pytest

from provisa.vector.capability import (
    native_vector_capability,
    supports_native_vectors,
)
from provisa.vector.providers import (
    EmbeddingError,
    OllamaProvider,
    OpenAICompatibleProvider,
    get_provider,
)
from provisa.vector.registry import VectorModel, VectorModelError, VectorModelRegistry

pytestmark = [pytest.mark.asyncio(loop_scope="session")]


# --- REQ-419: model registry allowlist ---


def _registry() -> VectorModelRegistry:
    return VectorModelRegistry.from_config(
        [
            {
                "id": "text-embedding-3-small",
                "provider": "openai",
                "dimensions": 1536,
                "api_key_env": "OPENAI_API_KEY",
            },
            {"id": "legacy", "provider": "openai", "dimensions": 768, "enabled": False},
        ]
    )


class TestRegistry:
    async def test_get_returns_registered_model(self):
        m = _registry().get("text-embedding-3-small")
        assert m.dimensions == 1536 and m.provider == "openai"

    async def test_unregistered_model_blocked(self):
        with pytest.raises(VectorModelError, match="not registered"):
            _registry().get("nope")

    async def test_disabled_model_blocked(self):
        with pytest.raises(VectorModelError, match="disabled"):
            _registry().get("legacy")

    async def test_is_allowed_and_list_enabled(self):
        reg = _registry()
        assert reg.is_allowed("text-embedding-3-small") is True
        assert reg.is_allowed("legacy") is False  # disabled
        assert reg.is_allowed("nope") is False
        assert [m.id for m in reg.list_enabled()] == ["text-embedding-3-small"]


# --- REQ-420: providers ---


class TestProviders:
    async def test_get_provider_resolves_three_backends(self):
        from provisa.vector.providers import (
            HuggingFaceLocalProvider,
            OllamaProvider as _Ol,
            OpenAICompatibleProvider as _Oa,
        )

        assert isinstance(get_provider("openai"), _Oa)
        assert isinstance(get_provider("ollama"), _Ol)
        assert isinstance(get_provider("huggingface"), HuggingFaceLocalProvider)

    async def test_unknown_provider_raises(self):
        with pytest.raises(EmbeddingError, match="Unknown embedding provider"):
            get_provider("bogus")

    async def test_openai_provider_posts_and_parses(self, monkeypatch):
        captured = {}

        class _Resp:
            def raise_for_status(self):
                pass

            def json(self):
                return {"data": [{"embedding": [0.1, 0.2]}, {"embedding": [0.3, 0.4]}]}

        class _Client:
            def __init__(self, *a, **k):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, *a):
                return False

            async def post(self, url, headers=None, json=None):
                captured["url"] = url
                captured["headers"] = headers
                captured["json"] = json
                return _Resp()

        import httpx

        monkeypatch.setattr(httpx, "AsyncClient", _Client)
        monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
        model = VectorModel(id="m", provider="openai", dimensions=2, api_key_env="OPENAI_API_KEY")
        vecs = await OpenAICompatibleProvider().embed(["a", "b"], model)
        assert vecs == [[0.1, 0.2], [0.3, 0.4]]
        assert captured["url"].endswith("/embeddings")
        assert captured["headers"]["Authorization"] == "Bearer sk-test"
        assert captured["json"] == {"model": "m", "input": ["a", "b"]}

    async def test_ollama_provider_embeds_per_text(self, monkeypatch):
        class _Resp:
            def __init__(self, v):
                self._v = v

            def raise_for_status(self):
                pass

            def json(self):
                return {"embedding": self._v}

        class _Client:
            def __init__(self, *a, **k):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, *a):
                return False

            async def post(self, url, json=None):
                return _Resp([float(len(json["prompt"]))])

        import httpx

        monkeypatch.setattr(httpx, "AsyncClient", _Client)
        model = VectorModel(id="nomic", provider="ollama", dimensions=1, base_url="http://x:11434")
        vecs = await OllamaProvider().embed(["ab", "abc"], model)
        assert vecs == [[2.0], [3.0]]


# --- REQ-422: source vector capability ---


class TestCapability:
    async def test_native_capability_by_source_type(self):
        assert native_vector_capability("postgresql") == "pgvector"
        assert native_vector_capability("mongodb") == "atlas_vector"
        assert native_vector_capability("snowflake") == "cortex"
        assert native_vector_capability("mysql") is None
        assert supports_native_vectors("postgresql") is True
        assert supports_native_vectors("mysql") is False

    async def test_has_pgvector_probe(self):
        from provisa.vector.capability import has_pgvector

        class _Conn:
            def __init__(self, val):
                self._val = val

            async def fetchval(self, q):
                return self._val

        assert await has_pgvector(_Conn(1)) is True
        assert await has_pgvector(_Conn(None)) is False


# --- REQ-421: embedding column declaration ---


class TestEmbeddingColumn:
    async def test_column_declares_embedding(self):
        from provisa.core.models import Column

        c = Column(
            name="title_vec",
            visible_to=["admin"],
            embedding=True,
            embedding_model="text-embedding-3-small",
            embedding_source_column="title",
        )
        assert c.embedding is True
        assert c.embedding_model == "text-embedding-3-small"
        assert c.embedding_source_column == "title"

    async def test_column_defaults_non_embedding(self):
        from provisa.core.models import Column

        c = Column(name="title", visible_to=["admin"])
        assert c.embedding is False
        assert c.embedding_model is None


# --- config wiring ---


class TestConfigWiring:
    async def test_provisa_config_parses_vector_models(self):
        from provisa.core.models import VectorModelConfig

        vm = VectorModelConfig(id="m", provider="ollama", dimensions=384)
        assert vm.enabled is True and vm.dimensions == 384

    async def test_registry_from_config_objects(self):
        from provisa.core.models import VectorModelConfig

        cfgs = [VectorModelConfig(id="m", provider="openai", dimensions=2).model_dump()]
        reg = VectorModelRegistry.from_config(cfgs)
        assert reg.is_allowed("m")


_ = types  # silence unused-import lints in some environments
