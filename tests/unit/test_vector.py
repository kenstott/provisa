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
                assert json is not None
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


# --- Phase B1: native tier (REQ-423/429/430) ---


class TestCosineSimilaritySql:
    async def test_pgvector_translation(self):
        from provisa.vector.query import cosine_similarity_sql

        sql = cosine_similarity_sql("title_vec", [0.1, 0.2, 0.3], "pgvector")
        assert sql == "(1 - (title_vec <=> '[0.1,0.2,0.3]'::vector))"

    async def test_cortex_translation(self):
        from provisa.vector.query import cosine_similarity_sql

        sql = cosine_similarity_sql("v", [0.1, 0.2], "cortex")
        assert sql == "VECTOR_COSINE_SIMILARITY(v, [0.1,0.2]::VECTOR(FLOAT, 2))"

    async def test_atlas_has_no_sql_scalar(self):
        from provisa.vector.query import VectorQueryError, cosine_similarity_sql

        with pytest.raises(VectorQueryError, match="vectorSearch"):
            cosine_similarity_sql("v", [0.1], "atlas_vector")

    async def test_no_native_capability_directs_to_fallback(self):
        from provisa.vector.query import VectorQueryError, cosine_similarity_sql

        with pytest.raises(VectorQueryError, match="fallback"):
            cosine_similarity_sql("v", [0.1], None)

    async def test_empty_vector_rejected(self):
        from provisa.vector.query import VectorQueryError, cosine_similarity_sql

        with pytest.raises(VectorQueryError, match="empty"):
            cosine_similarity_sql("v", [], "pgvector")


class TestModelLocking:
    async def test_dimension_match_ok(self):
        from provisa.vector.query import validate_vector_dimensions

        result = validate_vector_dimensions([1.0, 2.0, 3.0], VectorModel("m", "openai", 3))
        assert result is None  # returns None when dimensions match

    async def test_dimension_mismatch_rejected(self):
        from provisa.vector.query import validate_vector_dimensions

        with pytest.raises(VectorModelError, match="expects 3"):
            validate_vector_dimensions([1.0, 2.0], VectorModel("m", "openai", 3))


class TestResolveQueryVector:
    async def test_raw_vector_passthrough_with_validation(self):
        from provisa.vector.query import resolve_query_vector

        m = VectorModel("m", "openai", 3)
        assert await resolve_query_vector([0.1, 0.2, 0.3], m) == [0.1, 0.2, 0.3]

    async def test_raw_vector_wrong_dimension_rejected(self):
        from provisa.vector.query import resolve_query_vector

        with pytest.raises(VectorModelError):
            await resolve_query_vector([0.1, 0.2], VectorModel("m", "openai", 3))

    async def test_text_input_vectorized_via_locked_model(self):
        from provisa.vector.query import resolve_query_vector

        class _Provider:
            async def embed(self, texts, model):
                assert texts == ["hello"]
                return [[0.5, 0.6, 0.7]]

        m = VectorModel("m", "openai", 3)
        assert await resolve_query_vector("hello", m, provider=_Provider()) == [0.5, 0.6, 0.7]

    async def test_text_input_dimension_mismatch_rejected(self):
        from provisa.vector.query import resolve_query_vector

        class _Provider:
            async def embed(self, texts, model):
                return [[0.1, 0.2]]  # 2 dims, model expects 3

        with pytest.raises(VectorModelError):
            await resolve_query_vector("hello", VectorModel("m", "openai", 3), provider=_Provider())

    async def test_bad_input_type_rejected(self):
        from provisa.vector.query import VectorQueryError, resolve_query_vector

        with pytest.raises(VectorQueryError, match="text or a vector"):
            await resolve_query_vector(42, VectorModel("m", "openai", 3))


# --- Phase B2: fallback cache + invalidation (REQ-424/425) ---


class TestFallbackCache:
    async def test_cache_ddl_creates_table_and_hnsw_index(self):
        from provisa.vector.fallback_cache import cache_ddl

        ddl = cache_ddl("vcache.docs", 384)
        assert "vector(384)" in ddl[0]
        assert "PRIMARY KEY" in ddl[0]
        assert "USING hnsw (embedding vector_cosine_ops)" in ddl[1]

    async def test_fallback_similarity_sql_joins_pks_back(self):
        from provisa.vector.fallback_cache import fallback_similarity_sql

        sql = fallback_similarity_sql("docs", "id", "vcache.docs", [0.1, 0.2], limit=5)
        assert "FROM docs AS s" in sql
        assert "embedding <=> '[0.1,0.2]'::vector" in sql
        assert "LIMIT 5" in sql
        assert "ON s.id = c.pk" in sql
        assert "ORDER BY c._score DESC" in sql

    async def test_materialize_embeds_and_upserts(self):
        from provisa.vector.fallback_cache import materialize

        executed = []

        class _Conn:
            async def execute(self, sql, *args):
                executed.append((sql, args))

        class _Provider:
            async def embed(self, texts, model):
                return [[float(len(t))] for t in texts]

        rows = [{"id": 1, "body": "ab"}, {"id": 2, "body": "abc"}]
        n = await materialize(
            _Conn(), "vc", rows, "id", "body", VectorModel("m", "openai", 1), provider=_Provider()
        )
        assert n == 2
        assert all("INSERT INTO vc" in e[0] and "ON CONFLICT" in e[0] for e in executed)
        assert executed[0][1] == ("1", "[2.0]")


class TestInvalidation:
    def _state(self, **kw):
        from provisa.vector.cache_invalidation import CacheState

        return CacheState(
            last_refresh_ts=kw.get("last_refresh_ts", 900.0),
            ttl_seconds=kw.get("ttl_seconds", 50),
            source_row_count=kw.get("source_row_count", 10),
            cache_row_count=kw.get("cache_row_count", 10),
            mutated_since_refresh=kw.get("mutated_since_refresh", False),
            manual_refresh_requested=kw.get("manual_refresh_requested", False),
        )

    async def test_ttl_expiry(self):
        from provisa.vector.cache_invalidation import InvalidationReason, invalidation_reason

        assert (
            invalidation_reason(self._state(last_refresh_ts=900.0), 1000.0)
            is InvalidationReason.TTL
        )

    async def test_drift(self):
        from provisa.vector.cache_invalidation import InvalidationReason, invalidation_reason

        assert (
            invalidation_reason(self._state(last_refresh_ts=990.0, cache_row_count=8), 1000.0)
            is InvalidationReason.DRIFT
        )

    async def test_mutation_takes_precedence(self):
        from provisa.vector.cache_invalidation import InvalidationReason, invalidation_reason

        s = self._state(last_refresh_ts=990.0, mutated_since_refresh=True)
        assert invalidation_reason(s, 1000.0) is InvalidationReason.MUTATION

    async def test_valid_cache_returns_none(self):
        from provisa.vector.cache_invalidation import invalidation_reason, needs_refresh

        s = self._state(last_refresh_ts=995.0)
        assert invalidation_reason(s, 1000.0) is None
        assert needs_refresh(s, 1000.0) is False

    async def test_no_ttl_never_expires(self):
        from provisa.vector.cache_invalidation import invalidation_reason

        s = self._state(ttl_seconds=None, last_refresh_ts=0.0)
        assert invalidation_reason(s, 10**9) is None


# --- Phase B3: generation, scheduled refresh, governance (REQ-427/428/426) ---


class TestGeneration:
    async def test_spec_from_column(self):
        from provisa.core.models import Column
        from provisa.vector.generation import spec_from_column

        col = Column(
            name="vec",
            visible_to=["admin"],
            embedding=True,
            embedding_model="m",
            embedding_source_column="body",
        )
        spec = spec_from_column(col)
        assert spec is not None and spec.source_column == "body" and spec.model_id == "m"

    async def test_spec_none_when_not_embedding(self):
        from provisa.core.models import Column
        from provisa.vector.generation import spec_from_column

        assert spec_from_column(Column(name="x", visible_to=["admin"])) is None

    async def test_validate_generation_passes(self):
        from provisa.vector.generation import GeneratedEmbeddingSpec, validate_generation

        class _P:
            async def embed(self, texts, model):
                return [[0.1, 0.2, 0.3] for _ in texts]

        spec = GeneratedEmbeddingSpec("vec", "body", "m")
        result = await validate_generation(
            [{"body": "hi"}], spec, VectorModel("m", "openai", 3), provider=_P()
        )
        assert result is None  # returns None when sample validates successfully

    async def test_validate_generation_rejects_empty_source(self):
        from provisa.vector.generation import (
            GeneratedEmbeddingSpec,
            GenerationError,
            validate_generation,
        )

        class _P:
            async def embed(self, texts, model):
                return [[0.1, 0.2, 0.3]]

        spec = GeneratedEmbeddingSpec("vec", "body", "m")
        with pytest.raises(GenerationError, match="empty"):
            await validate_generation(
                [{"body": ""}], spec, VectorModel("m", "openai", 3), provider=_P()
            )

    async def test_validate_generation_rejects_dim_mismatch(self):
        from provisa.vector.generation import GeneratedEmbeddingSpec, validate_generation

        class _P:
            async def embed(self, texts, model):
                return [[0.1, 0.2]]  # 2 dims, model wants 3

        spec = GeneratedEmbeddingSpec("vec", "body", "m")
        with pytest.raises(VectorModelError):
            await validate_generation(
                [{"body": "hi"}], spec, VectorModel("m", "openai", 3), provider=_P()
            )


class TestScheduledRefresh:
    async def test_incremental_when_nothing_structural_changed(self):
        from provisa.vector.scheduled_refresh import RefreshMode, plan_refresh

        plan = plan_refresh([1, 2], "m", 3, old_model_id="m", old_dimensions=3)
        assert plan.mode is RefreshMode.INCREMENTAL and plan.pks == [1, 2]

    async def test_full_rebuild_on_model_change(self):
        from provisa.vector.scheduled_refresh import RefreshMode, plan_refresh

        plan = plan_refresh([1], "m2", 3, old_model_id="m", old_dimensions=3)
        assert plan.mode is RefreshMode.FULL

    async def test_full_rebuild_on_dimension_change(self):
        from provisa.vector.scheduled_refresh import RefreshMode, plan_refresh

        plan = plan_refresh([1], "m", 768, old_model_id="m", old_dimensions=384)
        assert plan.mode is RefreshMode.FULL

    async def test_full_rebuild_on_schema_change(self):
        from provisa.vector.scheduled_refresh import RefreshMode, plan_refresh

        plan = plan_refresh([1], "m", 3, old_model_id="m", old_dimensions=3, schema_changed=True)
        assert plan.mode is RefreshMode.FULL


class TestEmbeddingGovernance:
    async def test_visible_role_can_search(self):
        from provisa.core.models import Column
        from provisa.vector.governance import can_search_embedding

        col = Column(name="v", visible_to=["admin", "analyst"], embedding=True)
        assert can_search_embedding("analyst", col) is True
        assert can_search_embedding("guest", col) is False

    async def test_masked_column_blocks_search_for_masked_role(self):
        from provisa.core.models import Column
        from provisa.vector.governance import can_search_embedding

        col = Column(
            name="v",
            visible_to=["admin", "analyst"],
            mask_type="constant",
            unmasked_to=["admin"],
            embedding=True,
        )
        assert can_search_embedding("admin", col) is True
        assert can_search_embedding("analyst", col) is False  # masked for analyst

    async def test_assert_raises_for_unauthorized(self):
        from provisa.core.models import Column
        from provisa.vector.governance import EmbeddingAccessError, assert_search_allowed

        col = Column(name="v", visible_to=["admin"], embedding=True)
        with pytest.raises(EmbeddingAccessError):
            assert_search_allowed("guest", col)


_ = types  # silence unused-import lints in some environments
