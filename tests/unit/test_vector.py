# Copyright (c) 2026 Kenneth Stott
# Canary: b414ed54-379c-4304-aea7-875da0e899b7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Vector search — Phase B0 foundation (REQ-419/420/421/422)."""

from __future__ import annotations

import types

import pytest

from provisa.vector.capability import detect_vector_capability
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

    async def test_a_local_model_used_by_two_requests_at_once_is_loaded_once_and_used_in_turn(
        self, monkeypatch
    ):
        """Each request runs on its own thread: two using a local model together load it once
        and encode one after the other -- two encoding at once on Apple's GPU abort the process."""
        import asyncio
        import sys
        import threading
        import time

        from provisa.vector.providers import HuggingFaceLocalProvider

        loads: list[str] = []
        encoding: list[int] = []  # how many encodes run at once, each time one starts

        class _Model:
            def __init__(self, model_id):
                loads.append(model_id)
                time.sleep(0.2)  # long enough for the other thread to arrive meanwhile

            def encode(self, texts):
                encoding.append(len(encoding) + 1 - len(done))
                time.sleep(0.2)
                done.append(1)
                return [[1.0, 0.0] for _ in texts]

        done: list[int] = []

        monkeypatch.setitem(
            sys.modules, "sentence_transformers", types.SimpleNamespace(SentenceTransformer=_Model)
        )
        monkeypatch.setattr(HuggingFaceLocalProvider, "_cache", {})
        model = VectorModel(id="mini", provider="huggingface", dimensions=2)
        got: list = []

        def _request():
            got.append(asyncio.run(HuggingFaceLocalProvider().embed(["a line"], model)))

        threads = [threading.Thread(target=_request) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        assert loads == ["mini"]
        assert encoding == [1, 1]  # never two at once
        assert got == [[[1.0, 0.0]], [[1.0, 0.0]]]

    async def test_a_local_model_s_version_is_read_on_a_host_that_never_had_it(
        self, monkeypatch, tmp_path
    ):
        """REQ-2002: embed() records the model's version before any text is embedded. On a host
        whose hub cache does not hold the model yet, the version is read once the model is
        fetched -- a cache-only lookup first failed the statement ('outgoing traffic has been
        disabled')."""
        import sys

        from provisa.vector.providers import HuggingFaceLocalProvider

        cached: set[str] = set()

        class _Model:
            def __init__(self, model_id):
                cached.add(model_id)  # loading fetches it into the hub cache

        def _snapshot_download(model_id, local_files_only):
            assert local_files_only
            if model_id not in cached:
                raise FileNotFoundError("outgoing traffic has been disabled")
            return str(tmp_path / "snapshots" / "abc123")

        monkeypatch.setitem(
            sys.modules, "sentence_transformers", types.SimpleNamespace(SentenceTransformer=_Model)
        )
        monkeypatch.setitem(
            sys.modules,
            "huggingface_hub",
            types.SimpleNamespace(snapshot_download=_snapshot_download),
        )
        monkeypatch.setattr(HuggingFaceLocalProvider, "_cache", {})
        model = VectorModel(id="mini", provider="huggingface", dimensions=2)
        assert await HuggingFaceLocalProvider().reported_version(model) == "abc123"

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

    async def test_openai_provider_resolves_a_secret_reference_api_key(self, monkeypatch):
        # REQ-1827: api_key_env carrying a ${secret:NAME} reference must resolve through the SAME
        # shared grammar the MCP chat custom-endpoint path uses — before this it silently resolved
        # to nothing (a bare os.environ.get on a string that is never itself a real env var name),
        # so the provider posted with no Authorization header at all.
        captured = {}

        class _Resp:
            def raise_for_status(self):
                pass

            def json(self):
                return {"data": [{"embedding": [0.1, 0.2]}]}

        class _Client:
            def __init__(self, *a, **k):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, *a):
                return False

            async def post(self, url, headers=None, json=None):
                captured["headers"] = headers
                return _Resp()

        import httpx
        from contextlib import asynccontextmanager

        @asynccontextmanager
        async def fake_bound_to_request_org():
            yield

        monkeypatch.setattr(httpx, "AsyncClient", _Client)
        monkeypatch.setattr("provisa.core.secrets.resolve_secrets", lambda ref: "sk-from-vault")
        monkeypatch.setattr(
            "provisa.core.secrets_store.bound_to_request_org", fake_bound_to_request_org
        )
        model = VectorModel(
            id="m", provider="openai", dimensions=2, api_key_env="${secret:my_embedding_key}"
        )
        vecs = await OpenAICompatibleProvider().embed(["a"], model)
        assert vecs == [[0.1, 0.2]]
        assert captured["headers"]["Authorization"] == "Bearer sk-from-vault"

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


class _Driver:
    """A connected direct driver whose probe answers ``rows``."""

    def __init__(self, rows):
        self.rows = rows
        self.asked: list[str] = []

    async def execute(self, sql, params=None):
        self.asked.append(sql)
        return types.SimpleNamespace(rows=self.rows)


class TestCapability:
    async def test_a_postgres_source_with_the_extension_has_pgvector(self):
        driver = _Driver([(1,)])
        assert await detect_vector_capability("postgres", driver) == "pgvector"
        assert "pg_extension" in driver.asked[0]

    async def test_a_postgres_source_without_it_is_the_engines(self):
        assert await detect_vector_capability("postgres", _Driver([])) is None

    async def test_snowflake_always_has_its_vector_functions(self):
        driver = _Driver([])
        assert await detect_vector_capability("snowflake", driver) == "snowflake"
        assert driver.asked == []

    async def test_every_other_source_is_the_engines(self):
        for dialect in ("mysql", "tsql", "bigquery", "clickhouse", "mongodb"):
            assert await detect_vector_capability(dialect, _Driver([(1,)])) is None


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


# --- REQ-429: model locking ---


class TestModelLocking:
    async def test_dimension_match_ok(self):
        from provisa.vector.query import validate_vector_dimensions

        result = validate_vector_dimensions([1.0, 2.0, 3.0], VectorModel("m", "openai", 3))
        assert result is None  # returns None when dimensions match

    async def test_dimension_mismatch_rejected(self):
        from provisa.vector.query import validate_vector_dimensions

        with pytest.raises(VectorModelError, match="expects 3"):
            validate_vector_dimensions([1.0, 2.0], VectorModel("m", "openai", 3))


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


class TestEmbeddingIsNeverMasked:
    async def test_registration_refuses_a_mask_on_an_embedding_column(self):  # REQ-652
        import pydantic

        from provisa.core.models import Column

        with pytest.raises(pydantic.ValidationError, match="cannot be masked"):
            Column(
                name="v",
                visible_to=["admin"],
                masks=[{"to": ["admin"]}, {"to": ["*"], "mask": {"type": "constant"}}],
                embedding=True,
                embedding_model="m",
            )

    async def test_a_mask_on_another_column_is_kept(self):
        from provisa.core.models import Column

        masks = [{"to": ["*"], "mask": {"type": "constant"}}]
        assert Column(name="email", visible_to=["admin"], masks=masks).masks


_ = types  # silence unused-import lints in some environments


class TestCapabilityIsDetectedWhenASourceConnects:  # REQ-422
    async def test_the_pool_holds_each_sources_capability(self, monkeypatch):
        import provisa.executor.pool as P

        class _Fake:
            def __init__(self, rows):
                self.rows = rows

            def bind_source(self, source_id):  # REQ-2000: a driver knows its source
                pass

            def configure(self, extra):
                pass

            async def connect(self, *a):
                pass

            async def execute(self, sql, params=None):
                return types.SimpleNamespace(rows=self.rows)

            async def close(self):
                pass

        drivers = {"postgresql": _Fake([(1,)]), "mysql": _Fake([(1,)])}
        monkeypatch.setattr(P, "create_driver", lambda t, use_pgbouncer=False: drivers[t])
        pool = P.SourcePool()
        await pool.add("pg", "postgresql", "h", 5432, "db", "u", "p")
        await pool.add("my", "mysql", "h", 3306, "db", "u", "p")
        assert pool.vector_capability("pg") == "pgvector"
        assert pool.vector_capability("my") is None
        await pool.remove("pg")
        assert pool.vector_capability("pg") is None
